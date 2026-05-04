# -*- coding: utf-8 -*-
"""
godpick_column_manager.py
v36：全頁面全表格欄位管理共用工具（防遞迴修正版）

用途：
- 讓 07 股神推薦、08 股神推薦紀錄、10 推薦清單、11 資料診斷、12 股神管理中心
  所有 st.dataframe / st.data_editor 表格都套用同一套欄位管理方式。
- 支援欄位顯示 / 隱藏 / 排序 / 快速模板 / 只保留有資料欄位。
- 設定永久保存到 godpick_management_ui_config.json；若 GitHub Token 正常，也會同步寫回 GitHub。
- 對 data_editor 採「畫面欄位管理、回傳完整資料」策略，避免隱藏欄位後造成匯入/刪除/同步功能壞掉。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime
import base64
import json

import pandas as pd
import streamlit as st

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "godpick_management_ui_config.json"
CONFIG_VERSION = "v36"
EMPTY_VALUES = {"", "None", "none", "nan", "NaN", "null", "NULL", "<NA>"}


def _is_empty_value(v: Any) -> bool:
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip() in EMPTY_VALUES


def safe_text(v: Any, blank: str = "") -> str:
    if _is_empty_value(v):
        return blank
    if isinstance(v, (dict, list, tuple, set)):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    return str(v)


def _safe_col_name(c: Any) -> str:
    s = str(c).strip()
    return s if s else "未命名欄位"


def clean_display_df(df: pd.DataFrame, hide_empty_columns: bool = False) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [_safe_col_name(c) for c in out.columns]
    out = out.loc[:, ~out.columns.duplicated()].copy()
    for c in out.columns:
        if out[c].dtype == "object":
            out[c] = out[c].map(lambda x: "" if _is_empty_value(x) else x)
        else:
            out[c] = out[c].where(~out[c].isna(), "")
    if hide_empty_columns:
        keep: List[str] = []
        for c in out.columns:
            s = out[c].map(lambda x: "" if _is_empty_value(x) else str(x).strip())
            if s.ne("").any():
                keep.append(c)
        out = out[keep] if keep else out.iloc[:, :0]
    return out


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _default_config() -> Dict[str, Any]:
    return {"version": CONFIG_VERSION, "updated_at": "", "profiles": {}}


def _normalize_config(payload: Any) -> Dict[str, Any]:
    cfg = _default_config()
    if isinstance(payload, dict):
        cfg.update({k: v for k, v in payload.items() if k != "profiles"})
        profiles = payload.get("profiles", {}) if isinstance(payload.get("profiles", {}), dict) else {}
        fixed: Dict[str, Any] = {}
        for key, prof in profiles.items():
            if not isinstance(prof, dict):
                continue
            fixed[str(key)] = {
                "label": str(prof.get("label", str(key))),
                "columns": [str(x) for x in prof.get("columns", []) if str(x).strip()],
                "hidden": [str(x) for x in prof.get("hidden", []) if str(x).strip()],
                "updated_at": str(prof.get("updated_at", "")),
            }
        cfg["profiles"] = fixed
    cfg["version"] = CONFIG_VERSION
    return cfg


def _read_local_config() -> Dict[str, Any]:
    try:
        if CONFIG_PATH.exists():
            txt = CONFIG_PATH.read_text(encoding="utf-8")
            if txt.strip():
                return _normalize_config(json.loads(txt))
    except Exception:
        pass
    return _default_config()


def _write_local_config(payload: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True, f"本機已儲存：{CONFIG_PATH.name}"
    except Exception as exc:
        return False, f"本機儲存失敗：{exc}"


def _github_cfg() -> Dict[str, str]:
    try:
        secrets = st.secrets
    except Exception:
        secrets = {}
    return {
        "token": str(secrets.get("GITHUB_TOKEN", "")),
        "owner": str(secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": str(secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": str(secrets.get("GITHUB_REPO_BRANCH", "main")),
        "path": str(secrets.get("GODPICK_MANAGEMENT_UI_CONFIG_GITHUB_PATH", "godpick_management_ui_config.json")),
    }


def _github_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _github_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}


def _read_github_config() -> Tuple[Dict[str, Any], str]:
    cfg = _github_cfg()
    token = cfg.get("token", "")
    if not token or requests is None:
        return _default_config(), "未設定 GITHUB_TOKEN，欄位設定只會儲存在本機。"
    try:
        resp = requests.get(_github_url(cfg["owner"], cfg["repo"], cfg["path"]), headers=_github_headers(token), params={"ref": cfg["branch"]}, timeout=12)
        if resp.status_code == 404:
            return _default_config(), "GitHub 尚未建立欄位設定檔，套用後會自動建立。"
        if resp.status_code >= 400:
            return _default_config(), f"GitHub 讀取欄位設定失敗：{resp.status_code}"
        data = resp.json()
        txt = base64.b64decode(data.get("content", "")).decode("utf-8")
        return _normalize_config(json.loads(txt) if txt.strip() else {}), "GitHub 欄位設定讀取成功。"
    except Exception as exc:
        return _default_config(), f"GitHub 讀取例外：{exc}"


def _write_github_config(payload: Dict[str, Any]) -> Tuple[bool, str]:
    cfg = _github_cfg()
    token = cfg.get("token", "")
    if not token or requests is None:
        return False, "未設定 GITHUB_TOKEN，已跳過 GitHub 永久寫入。"
    try:
        url = _github_url(cfg["owner"], cfg["repo"], cfg["path"])
        headers = _github_headers(token)
        sha = ""
        get_resp = requests.get(url, headers=headers, params={"ref": cfg["branch"]}, timeout=12)
        if get_resp.status_code == 200:
            sha = get_resp.json().get("sha", "")
        body = {
            "message": f"Update godpick column config {payload.get('updated_at', '')}",
            "content": base64.b64encode(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")).decode("ascii"),
            "branch": cfg["branch"],
        }
        if sha:
            body["sha"] = sha
        put_resp = requests.put(url, headers=headers, json=body, timeout=20)
        if put_resp.status_code in (200, 201):
            return True, f"GitHub 已永久儲存：{cfg['path']}"
        return False, f"GitHub 寫入失敗：{put_resp.status_code}"
    except Exception as exc:
        return False, f"GitHub 寫入例外：{exc}"


def _config_ts(payload: Dict[str, Any]) -> str:
    return str(payload.get("updated_at", "")) if isinstance(payload, dict) else ""


@st.cache_data(show_spinner=False, ttl=30)
def _load_config_cached(_seq: int = 0) -> Tuple[Dict[str, Any], str]:
    local = _read_local_config()
    gh, gh_msg = _read_github_config()
    if _config_ts(local) >= _config_ts(gh):
        return local, f"使用本機欄位設定。{gh_msg}"
    return gh, gh_msg


def load_column_config() -> Dict[str, Any]:
    seq = int(st.session_state.get("godpick_column_config_refresh_seq", 0))
    cfg, _ = _load_config_cached(seq)
    return cfg


def save_column_config(config: Dict[str, Any]) -> bool:
    config = _normalize_config(config)
    config["version"] = CONFIG_VERSION
    config["updated_at"] = _now_text()
    local_ok, _ = _write_local_config(config)
    gh_ok, _ = _write_github_config(config)
    st.session_state["godpick_column_config_refresh_seq"] = int(st.session_state.get("godpick_column_config_refresh_seq", 0)) + 1
    try:
        _load_config_cached.clear()
    except Exception:
        pass
    return bool(local_ok or gh_ok)


def unique_existing_columns(cols: Iterable[str], df: Optional[pd.DataFrame] = None) -> List[str]:
    seen, out = set(), []
    available = set(df.columns) if isinstance(df, pd.DataFrame) else None
    for c in cols or []:
        c = str(c)
        if not c or c in seen:
            continue
        if available is not None and c not in available:
            continue
        seen.add(c)
        out.append(c)
    return out


def _non_empty_columns(df: pd.DataFrame, cols: List[str]) -> List[str]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return cols[:]
    out: List[str] = []
    for c in cols:
        if c not in df.columns:
            continue
        s = df[c].map(lambda x: "" if _is_empty_value(x) else str(x).strip())
        if s.ne("").any():
            out.append(c)
    return out


def column_templates(all_cols: Iterable[str]) -> Dict[str, List[str]]:
    all_cols = list(all_cols or [])
    return {
        "核心推薦欄位": ["勾選", "匯入自選", "刪除", "推薦日期", "推薦時間", "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦模式", "推薦等級", "推薦分數", "股神決策分數", "買點分級", "最新價", "推薦價格"],
        "操作與倉位欄位": ["v21操作優先順序", "追蹤分級", "今日操作建議", "建議動作", "股神建議動作", "股神信心", "進場時機", "股神進場區間", "等待條件", "建議倉位%", "動態建議倉位%", "第一筆進場%", "分批策略"],
        "風控停利停損欄位": ["高風險狀態", "品質分級", "品質建議", "風險說明", "停損價", "停損參考", "停利目標", "賣出目標1", "賣出目標2", "最大回撤%", "風險報酬比", "R/R", "等待條件"],
        "族群大盤欄位": ["類別", "產業", "族群資金說明", "族群策略建議", "族群資金流分數", "族群資金流說明", "族群輪動狀態", "大盤情境分析", "大盤情境調權說明", "大盤策略建議", "大盤風控", "大盤交易時段"],
        "績效追蹤欄位": ["推薦後1日%", "推薦後1日勝率", "推薦後3日%", "推薦後3日勝率", "推薦後5日%", "推薦後5日勝率", "推薦後10日%", "推薦後10日勝率", "推薦後20日%", "推薦後20日勝率", "最大漲幅%", "最大回撤%", "目前績效%"],
        "全部欄位": all_cols,
    }


def get_table_columns(table_key: str, default_cols: Iterable[str], df: Optional[pd.DataFrame] = None) -> List[str]:
    cfg = load_column_config()
    prof = cfg.get("profiles", {}).get(table_key, {}) if isinstance(cfg.get("profiles", {}), dict) else {}
    saved = prof.get("columns", []) if isinstance(prof, dict) else []
    candidates = list(df.columns) if isinstance(df, pd.DataFrame) else list(default_cols or [])
    if saved:
        cols = [c for c in saved if c in candidates]
        for c in default_cols or []:
            if c in candidates and c not in cols:
                cols.append(c)
        return cols or candidates
    return unique_existing_columns(default_cols, df) or candidates


def set_table_columns(table_key: str, columns: Iterable[str], template: str = "custom", label: str = "") -> bool:
    cfg = load_column_config()
    cfg.setdefault("profiles", {})[table_key] = {
        "label": label or table_key,
        "columns": [str(c) for c in columns if str(c).strip()],
        "hidden": [],
        "template": template,
        "updated_at": _now_text(),
    }
    return save_column_config(cfg)


def render_column_manager(table_key: str, table_label: str, df: pd.DataFrame, default_cols: Optional[Iterable[str]] = None) -> List[str]:
    """渲染與 12_股神管理中心相同的欄位管理 UI，並回傳要顯示的欄位。"""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []
    clean = clean_display_df(df, hide_empty_columns=False)
    candidates = list(clean.columns)
    default_cols = list(default_cols or candidates)
    current = get_table_columns(table_key, default_cols, clean)
    if not current:
        current = candidates[:]

    cfg, cfg_msg = _load_config_cached(int(st.session_state.get("godpick_column_config_refresh_seq", 0)))
    with st.expander(f"🧩 {table_label} 欄位管理 / 永久記錄", expanded=False):
        st.caption("可調整顯示、隱藏與順序；按下『套用並永久記錄』後會寫入 godpick_management_ui_config.json，重新整理或重開頁面仍會保留。")
        st.caption(cfg_msg)
        k = table_key.replace(" ", "_").replace("/", "_").replace(":", "_")
        c0, c1, c2, c3 = st.columns([1.1, 1.1, 1.1, 1.4])
        with c0:
            preset = st.selectbox("快速模板", ["目前設定", "全部欄位", "只保留有資料欄位"] + list(column_templates(candidates).keys()), key=f"{k}_preset")
        with c1:
            keyword = st.text_input("搜尋欄位", key=f"{k}_kw", placeholder="輸入欄位關鍵字")
        with c2:
            selected_only = st.toggle("只看已顯示", value=False, key=f"{k}_selected_only")
        with c3:
            st.write("")
            st.write(f"目前顯示 **{len(current)}** 欄 / 可用 **{len(candidates)}** 欄")

        if preset == "全部欄位":
            base_cols = candidates[:]
        elif preset == "只保留有資料欄位":
            base_cols = _non_empty_columns(clean, candidates)
        elif preset in column_templates(candidates):
            base_cols = [c for c in column_templates(candidates)[preset] if c in candidates]
            for c in current:
                if c in candidates and c not in base_cols:
                    base_cols.append(c)
        else:
            base_cols = current[:]

        ordered: List[str] = []
        for c in base_cols + current + candidates:
            if c in candidates and c not in ordered:
                ordered.append(c)
        non_empty = set(_non_empty_columns(clean, candidates))
        selected = set(base_cols)
        rows = []
        for i, col in enumerate(ordered, 1):
            if keyword and keyword not in col:
                continue
            if selected_only and col not in selected:
                continue
            rows.append({"顯示": col in selected, "順序": i if col in selected else 9999, "欄位名稱": col, "有資料": "是" if col in non_empty else "否"})
        work = pd.DataFrame(rows)
        # 重要：欄位管理器自身的設定表必須呼叫 Streamlit 原始 data_editor，
        # 不可呼叫被 install_auto_column_manager 包裝後的 st.data_editor，
        # 否則會形成「表格管理器 → data_editor → 表格管理器」的遞迴，
        # 導致瀏覽器 STATUS_STACK_OVERFLOW 或 Streamlit 無限重繪。
        _raw_data_editor = getattr(st, "_godpick_original_data_editor", st.data_editor)
        edited = _raw_data_editor(
            work,
            use_container_width=True,
            hide_index=True,
            height=420,
            disabled=["欄位名稱", "有資料"],
            column_config={
                "顯示": st.column_config.CheckboxColumn("顯示"),
                "順序": st.column_config.NumberColumn("順序", min_value=1, step=1),
                "欄位名稱": st.column_config.TextColumn("欄位名稱", width="large"),
                "有資料": st.column_config.TextColumn("有資料", width="small"),
            },
            key=f"{k}_editor",
        )
        b1, b2, b3, b4 = st.columns(4)
        apply_btn = b1.button("✅ 套用並永久記錄", type="primary", use_container_width=True, key=f"{k}_apply")
        reset_btn = b2.button("↩️ 恢復系統預設", use_container_width=True, key=f"{k}_reset")
        all_btn = b3.button("📋 全部欄位顯示", use_container_width=True, key=f"{k}_all")
        nonempty_btn = b4.button("🧹 隱藏全空欄位", use_container_width=True, key=f"{k}_nonempty")

        if reset_btn:
            final_cols = [c for c in default_cols if c in candidates]
        elif all_btn:
            final_cols = candidates[:]
        elif nonempty_btn:
            final_cols = _non_empty_columns(clean, candidates)
        else:
            final_cols = current[:]
            if isinstance(edited, pd.DataFrame) and {"顯示", "順序", "欄位名稱"}.issubset(set(edited.columns)):
                tmp = edited[edited["顯示"].astype(bool)].copy()
                tmp["順序"] = pd.to_numeric(tmp["順序"], errors="coerce").fillna(9999)
                tmp = tmp.sort_values(["順序", "欄位名稱"], ascending=[True, True])
                final_cols = [str(x) for x in tmp["欄位名稱"].tolist() if str(x) in candidates]

        if apply_btn or reset_btn or all_btn or nonempty_btn:
            if not final_cols:
                st.error("至少要保留 1 個欄位。")
            else:
                cfg = _normalize_config(cfg)
                cfg.setdefault("profiles", {})[table_key] = {
                    "label": table_label,
                    "columns": final_cols,
                    "hidden": [c for c in candidates if c not in final_cols],
                    "updated_at": _now_text(),
                }
                ok = save_column_config(cfg)
                if ok:
                    st.success(f"{table_label} 欄位設定已套用並永久記錄。")
                else:
                    st.warning(f"{table_label} 欄位設定已嘗試儲存，但可能未完全寫入。")
                st.rerun()
    return [c for c in current if c in candidates]


def apply_columns(df: pd.DataFrame, table_key: str, default_cols: Iterable[str], hide_empty_columns: bool = False) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    clean = clean_display_df(df, hide_empty_columns=False)
    cols = get_table_columns(table_key, default_cols, clean)
    out = clean[cols].copy() if cols else clean.copy()
    if hide_empty_columns:
        out = clean_display_df(out, hide_empty_columns=True)
    return out


def managed_dataframe(df: pd.DataFrame, table_key: str, table_label: str, default_cols: Optional[Iterable[str]] = None, hide_empty_columns: bool = False, **kwargs: Any) -> None:
    cols = render_column_manager(table_key, table_label, df, default_cols or (list(df.columns) if isinstance(df, pd.DataFrame) else []))
    show = apply_columns(df, table_key, cols or (list(df.columns) if isinstance(df, pd.DataFrame) else []), hide_empty_columns=hide_empty_columns)
    return st._godpick_original_dataframe(show, **kwargs)  # type: ignore[attr-defined]


def managed_data_editor(df: pd.DataFrame, table_key: str, table_label: str, default_cols: Optional[Iterable[str]] = None, hide_empty_columns: bool = False, **kwargs: Any) -> Any:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return st._godpick_original_data_editor(df, **kwargs)  # type: ignore[attr-defined]
    original = df.copy()
    cols = render_column_manager(table_key, table_label, original, default_cols or list(original.columns))
    show = apply_columns(original, table_key, cols or list(original.columns), hide_empty_columns=hide_empty_columns)
    # data_editor 的 column_config 只保留正在顯示的欄位，避免 Streamlit 因 hidden 欄位設定報錯。
    cfg = kwargs.get("column_config")
    if isinstance(cfg, dict):
        kwargs["column_config"] = {k: v for k, v in cfg.items() if k in show.columns}
    edited = st._godpick_original_data_editor(show, **kwargs)  # type: ignore[attr-defined]
    # 回傳完整資料：把顯示欄位的修改寫回原始表，隱藏欄位仍保留，避免後續按鈕功能壞掉。
    if isinstance(edited, pd.DataFrame) and len(edited) == len(original):
        full = original.copy()
        for c in edited.columns:
            full[c] = edited[c].values
        return full
    return edited


def install_auto_column_manager(page_key: str) -> None:
    """自動攔截當前頁面所有 st.dataframe / st.data_editor，統一套用欄位管理。

    v36 修正：
    - 保留真正原始的 Streamlit 函式。
    - 欄位管理器內部的設定表一律呼叫原始 data_editor。
    - 避免重複包裝造成遞迴與瀏覽器 STATUS_STACK_OVERFLOW。
    """
    current_df = st.dataframe
    current_de = st.data_editor
    if not hasattr(st, "_godpick_original_dataframe") or getattr(current_df, "__name__", "") != "_wrapped_dataframe":
        if getattr(current_df, "__name__", "") != "_wrapped_dataframe":
            st._godpick_original_dataframe = current_df  # type: ignore[attr-defined]
    if not hasattr(st, "_godpick_original_data_editor") or getattr(current_de, "__name__", "") != "_wrapped_data_editor":
        if getattr(current_de, "__name__", "") != "_wrapped_data_editor":
            st._godpick_original_data_editor = current_de  # type: ignore[attr-defined]
    st.session_state["godpick_column_manager_page_key"] = page_key
    st.session_state["godpick_column_manager_call_index"] = 0

    def _next_key(kind: str, data: Any) -> Tuple[str, str]:
        idx = int(st.session_state.get("godpick_column_manager_call_index", 0)) + 1
        st.session_state["godpick_column_manager_call_index"] = idx
        cols = len(data.columns) if isinstance(data, pd.DataFrame) else 0
        rows = len(data) if isinstance(data, pd.DataFrame) else 0
        label = f"{page_key}｜表格 {idx:02d}（{rows}筆 / {cols}欄）"
        key = f"auto::{page_key}::{kind}_{idx:02d}"
        return key, label

    def _wrapped_dataframe(data=None, *args: Any, **kwargs: Any) -> Any:
        if isinstance(data, pd.DataFrame) and len(data.columns) >= 2:
            key, label = _next_key("dataframe", data)
            cols = render_column_manager(key, label, data, list(data.columns))
            show = apply_columns(data, key, cols or list(data.columns), hide_empty_columns=False)
            return st._godpick_original_dataframe(show, *args, **kwargs)  # type: ignore[attr-defined]
        return st._godpick_original_dataframe(data, *args, **kwargs)  # type: ignore[attr-defined]

    def _wrapped_data_editor(data=None, *args: Any, **kwargs: Any) -> Any:
        if isinstance(data, pd.DataFrame) and len(data.columns) >= 2:
            key, label = _next_key("data_editor", data)
            original = data.copy()
            cols = render_column_manager(key, label, original, list(original.columns))
            show = apply_columns(original, key, cols or list(original.columns), hide_empty_columns=False)
            cfg = kwargs.get("column_config")
            if isinstance(cfg, dict):
                kwargs["column_config"] = {k: v for k, v in cfg.items() if k in show.columns}
            edited = st._godpick_original_data_editor(show, *args, **kwargs)  # type: ignore[attr-defined]
            if isinstance(edited, pd.DataFrame) and len(edited) == len(original):
                full = original.copy()
                for c in edited.columns:
                    full[c] = edited[c].values
                return full
            return edited
        return st._godpick_original_data_editor(data, *args, **kwargs)  # type: ignore[attr-defined]

    st.dataframe = _wrapped_dataframe  # type: ignore[assignment]
    st.data_editor = _wrapped_data_editor  # type: ignore[assignment]
