# -*- coding: utf-8 -*-
"""
godpick_column_manager.py
v41：欄位管理表單套用版（輸入順序不即時重算，按套用才運算）

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
CONFIG_VERSION = "v41"
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
    """v39：頁面載入時不主動讀 GitHub。

    原因：7/8/10/11/12 每個頁面都有多張表，如果每次 rerun 都向 GitHub
    讀取欄位設定，Streamlit Cloud 會明顯變慢。

    正確流程：
    - 平常讀取 repo 內的 godpick_management_ui_config.json，本機/雲端都很快。
    - 使用者按「套用並永久記錄」時，才寫入 GitHub。
    - GitHub 重新部署後，設定檔會跟著 repo 進來。
    """
    return _default_config(), "v39：頁面載入不讀 GitHub，避免每頁卡頓；套用欄位設定時才寫回 GitHub。"


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


@st.cache_data(show_spinner=False, ttl=300)
def _load_config_cached(_seq: int = 0) -> Tuple[Dict[str, Any], str]:
    local = _read_local_config()
    _, gh_msg = _read_github_config()
    return local, f"使用 repo 內欄位設定。{gh_msg}"


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



def _key_safe(s: str) -> str:
    return str(s).replace(" ", "_").replace("/", "_").replace(":", "_").replace("｜", "_").replace("\\", "_")


def _parse_column_text(raw: str, candidates: List[str]) -> List[str]:
    """將文字排序清單轉成有效欄位；支援一行一欄、逗號、tab。"""
    if not raw:
        return []
    parts: List[str] = []
    for line in str(raw).replace("\t", "\n").replace(",", "\n").replace("，", "\n").splitlines():
        c = line.strip()
        if c:
            parts.append(c)
    valid = []
    seen = set()
    cand = set(candidates)
    for c in parts:
        if c in cand and c not in seen:
            valid.append(c)
            seen.add(c)
    return valid


def render_column_manager(table_key: str, table_label: str, df: pd.DataFrame, default_cols: Optional[Iterable[str]] = None) -> List[str]:
    """v41：欄位管理表單套用版。

    核心修正：
    - 欄位順序文字框、搜尋、模板選擇全部放入 st.form。
    - 使用者輸入欄位順序時，不會即時 rerun / 不會即時重排表格。
    - 只有按「套用並永久記錄」或「套用到本次畫面」時，才解析欄位與重繪表格。
    - 避免 60~100 欄大表在每次鍵入、每次移動欄位時重新運算造成卡頓。
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []

    clean = clean_display_df(df, hide_empty_columns=False)
    candidates = list(clean.columns)
    default_cols = list(default_cols or candidates)
    current = get_table_columns(table_key, default_cols, clean)
    if not current:
        current = candidates[:]
    current = [c for c in current if c in candidates]
    if not current:
        current = candidates[:]

    safe_key = _key_safe(table_key)

    # 平常模式：只套用已儲存 / 本次預覽欄位，不渲染管理 UI。
    preview_cols = st.session_state.get(f"{safe_key}_preview_cols_v41")
    if isinstance(preview_cols, list) and preview_cols:
        current = [c for c in preview_cols if c in candidates] or current

    if not bool(st.session_state.get("godpick_column_manager_edit_mode", False)):
        return current

    active_key = st.session_state.get("godpick_column_manager_active_table", "")

    # 管理模式下也不一次展開所有表格，只顯示輕量入口。
    with st.expander(f"🧩 {table_label} 欄位管理｜v41 表單套用版", expanded=(active_key == table_key)):
        c_left, c_mid, c_right = st.columns([1.4, 1.0, 1.2])
        with c_left:
            st.caption(f"目前顯示 {len(current)} 欄 / 可用 {len(candidates)} 欄。")
        with c_mid:
            if st.button("🎯 管理這張表", use_container_width=True, key=f"{safe_key}_activate_v41"):
                st.session_state["godpick_column_manager_active_table"] = table_key
                st.rerun()
        with c_right:
            if active_key == table_key and st.button("收合管理", use_container_width=True, key=f"{safe_key}_deactivate_v41"):
                st.session_state["godpick_column_manager_active_table"] = ""
                st.rerun()

        if active_key != table_key:
            st.info("v41 高速模式：只在按『管理這張表』後載入欄位設定；輸入順序不會即時重算。")
            return current

        cfg, cfg_msg = _load_config_cached(int(st.session_state.get("godpick_column_config_refresh_seq", 0)))
        st.caption(cfg_msg)
        st.caption("重點：欄位順序文字框已改成表單模式，輸入 / 貼上 / 調整時不會立即重算；按下方按鈕才會套用。")

        templates = column_templates(candidates)
        text_key = f"{safe_key}_order_text_v41"
        if text_key not in st.session_state:
            st.session_state[text_key] = "\n".join(current)

        # v41：所有會造成大量 rerun 的控制項放進 form，避免每輸入一個字就重算整頁。
        with st.form(key=f"{safe_key}_column_form_v41", clear_on_submit=False):
            c0, c1, c2, c3 = st.columns([1.2, 1.4, 1.0, 1.0])
            with c0:
                preset = st.selectbox(
                    "快速模板",
                    ["目前設定", "全部欄位", "只保留有資料欄位"] + [k for k in templates.keys() if k not in ("全部欄位",)],
                    key=f"{safe_key}_preset_v41",
                )
            with c1:
                keyword = st.text_input("欄位搜尋", key=f"{safe_key}_kw_v41", placeholder="輸入關鍵字快速找欄位")
            with c2:
                hide_empty_choice = st.checkbox("套用時隱藏全空欄", value=False, key=f"{safe_key}_hide_empty_v41")
            with c3:
                st.write("")
                st.write(f"可用欄位：**{len(candidates)}**")

            # 只顯示搜尋後的候選欄位；在表單內不會即時觸發整頁重算。
            options = [c for c in candidates if (not keyword or keyword in c)]
            default_visible = [c for c in current if c in options]
            selected_cols = st.multiselect(
                "顯示欄位（勾選顯示；順序以下方文字框為準）",
                options=options,
                default=default_visible,
                key=f"{safe_key}_visible_cols_v41",
                help="欄位很多時請用搜尋縮小範圍；實際順序請調整下方文字框。",
            )

            order_text = st.text_area(
                "欄位順序（一行一欄；可直接剪下貼上調整）",
                key=text_key,
                height=260,
                help="v41：輸入與貼上不會立即重算；只有按套用按鈕才解析與更新表格。",
            )

            p1, p2, p3, p4 = st.columns(4)
            apply_btn = p1.form_submit_button("✅ 套用並永久記錄", type="primary", use_container_width=True)
            preview_btn = p2.form_submit_button("👁️ 套用到本次畫面", use_container_width=True)
            reset_btn = p3.form_submit_button("↩️ 恢復系統預設", use_container_width=True)
            nonempty_btn = p4.form_submit_button("🧹 只保留有資料欄", use_container_width=True)

        # form 外才開始解析，表示只有按 submit 後才進行欄位計算。
        submitted = apply_btn or preview_btn or reset_btn or nonempty_btn
        if not submitted:
            st.caption("目前尚未套用新的欄位順序；調整完請按『套用並永久記錄』。")
            return current

        if reset_btn:
            final_cols = [c for c in default_cols if c in candidates] or candidates[:]
            st.session_state[text_key] = "\n".join(final_cols)
            st.session_state[f"{safe_key}_preview_cols_v41"] = final_cols
            st.success("已恢復系統預設並套用到本次畫面；要永久保存請再按『套用並永久記錄』。")
            st.rerun()

        elif nonempty_btn:
            final_cols = _non_empty_columns(clean, candidates) or current[:]
            st.session_state[text_key] = "\n".join(final_cols)
            st.session_state[f"{safe_key}_preview_cols_v41"] = final_cols
            st.success("已產生有資料欄位清單並套用到本次畫面；要永久保存請再按『套用並永久記錄』。")
            st.rerun()

        else:
            # 依提交時的模板決定基準欄位，再與文字框 / 勾選欄位合併。
            if preset == "全部欄位":
                base_cols = candidates[:]
            elif preset == "只保留有資料欄位":
                base_cols = _non_empty_columns(clean, candidates)
            elif preset in templates:
                base_cols = [c for c in templates[preset] if c in candidates]
                for c in current:
                    if c in candidates and c not in base_cols:
                        base_cols.append(c)
            else:
                base_cols = current[:]

            if hide_empty_choice:
                non_empty = set(_non_empty_columns(clean, candidates))
                base_cols = [c for c in base_cols if c in non_empty]
                if not base_cols:
                    base_cols = current[:1] or candidates[:1]

            parsed_order = _parse_column_text(order_text, candidates)
            selected_set = set(selected_cols) if selected_cols else set(parsed_order or base_cols)
            final_cols = [c for c in parsed_order if c in selected_set]
            for c in selected_cols:
                if c in candidates and c not in final_cols:
                    final_cols.append(c)
            # 若文字框為空，使用模板 / 目前設定作為保底。
            if not final_cols:
                final_cols = [c for c in base_cols if c in candidates] or current[:]

            if preview_btn:
                st.session_state[f"{safe_key}_preview_cols_v41"] = final_cols
                st.success("已套用到本次畫面，尚未永久儲存。")
                st.rerun()

            if apply_btn:
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
                    st.session_state[f"{safe_key}_preview_cols_v41"] = final_cols
                    if ok:
                        st.success(f"{table_label} 欄位設定已套用並永久記錄。")
                    else:
                        st.warning(f"{table_label} 欄位設定已嘗試儲存，但可能未完全寫入。")
                    st.rerun()

    return current

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
    """v40：欄位管理極速精簡版。

    重要修正：
    - 不再全域攔截 st.dataframe / st.data_editor。
    - 避免每一張小表、統計表、診斷表都建立欄位管理器。
    - 避免頁面互動時所有表格反覆重排，降低開頁與調欄位等待時間。
    - 已存在於各頁面的主表欄位管理功能仍保留。

    使用原則：
    - 7_股神推薦：保留完整推薦表原本欄位管理。
    - 8_股神推薦紀錄：保留推薦紀錄總表原本欄位管理。
    - 12_股神管理中心：保留投資組合 / 今日追蹤 / 品質明細欄位管理。
    - 10_推薦清單與 11_資料診斷：不再自動套所有表格，避免慢與卡住。
    """
    try:
        with st.sidebar.expander("🧩 欄位管理｜v41 表單套用模式", expanded=False):
            st.caption("已停用全頁表格自動攔截；欄位順序調整改成表單提交，輸入時不即時重算。")
            st.caption("只保留各頁主表欄位管理；按「套用並永久記錄」後才解析欄位順序與永久保存。")
            if st.button("🔄 重新讀取欄位設定", use_container_width=True, key=f"{page_key}_column_cfg_reload_v40"):
                st.session_state["godpick_column_config_refresh_seq"] = int(st.session_state.get("godpick_column_config_refresh_seq", 0)) + 1
                try:
                    _load_config_cached.clear()
                except Exception:
                    pass
                st.rerun()
    except Exception:
        pass
    # v40：刻意不覆寫 st.dataframe / st.data_editor。
    return None
