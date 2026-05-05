# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import base64
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st

try:
    import requests
except Exception:
    requests = None


try:
    from godpick_column_schema import (
        UNIFIED_RECOMMEND_DISPLAY_COLUMNS,
        UNIFIED_MANAGEMENT_COLUMNS as SHARED_UNIFIED_MANAGEMENT_COLUMNS,
        normalize_godpick_dataframe,
        unified_display_columns,
        dedupe_keep_order as shared_dedupe_keep_order,
    )
except Exception:
    UNIFIED_RECOMMEND_DISPLAY_COLUMNS = []
    SHARED_UNIFIED_MANAGEMENT_COLUMNS = []
    normalize_godpick_dataframe = None
    unified_display_columns = None
    shared_dedupe_keep_order = None

try:
    from utils import inject_pro_theme, render_pro_hero
except Exception:
    inject_pro_theme = None
    render_pro_hero = None

PAGE_TITLE = "股神管理中心｜v48 欄位管理免即時重算版"
BASE_DIR = Path(__file__).resolve().parents[1]
MANAGEMENT_UI_CONFIG_PATH = BASE_DIR / "godpick_management_ui_config.json"

RECOMMEND_FILES = [
    BASE_DIR / "godpick_recommend_list.json",
    BASE_DIR / "recommend_list.json",
]
RECORD_FILES = [
    BASE_DIR / "godpick_records.json",
    BASE_DIR / "god_recommend_records.json",
]
ALL_DATA_FILES = RECORD_FILES + RECOMMEND_FILES

PORTFOLIO_COLUMNS = [
    "v21操作優先順序", "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦分數",
    "推薦模式", "推薦型態", "機會型態", "進場時機", "建議動作", "等待條件",
    "建議倉位%", "動態建議倉位%", "建議投入等級", "第一筆進場%", "分批策略", "第二筆加碼條件",
    "追高風險等級", "單檔風險等級", "最大風險%", "近端支撐", "近端壓力", "停損參考",
    "停利策略", "停損策略", "族群集中警示", "組合配置建議", "大盤策略模式", "大盤策略建議",
    "強勢族群等級", "族群輪動狀態", "族群資金流分數", "命中結果", "狀態", "資料來源檔",
]

DAILY_COLUMNS = [
    "追蹤分級", "今日操作建議", "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦分數",
    "推薦型態", "機會型態", "進場時機", "建議動作", "建議倉位%", "動態建議倉位%",
    "追高風險等級", "單檔風險等級", "近端支撐", "近端壓力", "停損參考", "停利策略", "停損策略",
    "大盤策略模式", "大盤策略建議", "族群集中警示", "強勢族群等級", "族群輪動狀態",
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%",
    "推薦後最大漲幅%", "推薦後最大回撤%", "命中結果", "績效評語", "狀態", "資料來源檔",
]

QUALITY_COLUMNS = [
    "品質分級", "品質建議", "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦模式",
    "推薦型態", "機會型態", "進場時機", "建議動作", "推薦分數", "買點分級", "追高風險等級", "單檔風險等級",
    "建議倉位%", "動態建議倉位%", "大盤策略模式", "強勢族群等級", "族群輪動狀態",
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%",
    "推薦後最大漲幅%", "推薦後最大回撤%", "命中結果", "績效評語", "狀態", "資料來源檔",
]

GROUP_FIELDS = [
    "推薦模式", "推薦型態", "機會型態", "進場時機", "追高風險等級", "單檔風險等級",
    "大盤策略模式", "強勢族群等級", "族群輪動狀態", "類別", "產業",
]


# v25：管理中心統一欄位。這組欄位會同時套用在「推薦清單 / 目前追蹤」與「股神推薦紀錄 / 歷史全部」，並對歷史舊資料進行智慧補值。
UNIFIED_MANAGEMENT_COLUMNS = [
    "v21操作優先順序", "追蹤分級", "今日操作建議", "品質分級", "品質建議",
    "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦時間",
    "推薦模式", "推薦型態", "機會型態", "進場時機", "建議動作", "等待條件",
    "推薦分數", "股神決策分數", "買點分級", "上漲機率%", "上漲機率信心",
    "建議倉位%", "動態建議倉位%", "建議投入等級", "第一筆進場%", "分批策略", "第二筆加碼條件",
    "建議價位", "股神進場區間", "推薦價格", "最新價", "近端支撐", "近端壓力", "突破確認價",
    "停損參考", "停損價", "賣出目標1", "賣出目標2", "停利策略", "停損策略",
    "追高風險等級", "單檔風險等級", "最大風險%", "風險說明", "股神推論", "推薦理由", "推薦原因",
    "大盤策略模式", "大盤策略建議", "大盤情境分桶", "大盤橋接狀態", "大盤橋接風控", "大盤情境調權說明",
    "族群集中警示", "組合配置建議", "強勢族群等級", "族群輪動狀態", "族群策略建議", "族群資金流分數", "族群資金流說明",
    "K線驗證標記", "K線檢視提示", "雷達訊號", "籌碼訊號", "量能訊號",
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%",
    "推薦後最大漲幅%", "推薦後最大回撤%", "命中結果", "績效評語", "狀態", "資料來源檔",
]

NUMERIC_MANAGEMENT_COLUMNS = {
    "推薦分數", "股神決策分數", "上漲機率%", "建議倉位%", "動態建議倉位%", "第一筆進場%",
    "建議價位", "推薦價格", "最新價", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2",
    "最大風險%", "族群資金流分數", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%",
    "推薦後最大漲幅%", "推薦後最大回撤%",
}


# v26 欄位統一：管理中心改用共用 schema，與 7_股神推薦、8_股神推薦紀錄、10_推薦清單共用欄序。
try:
    if SHARED_UNIFIED_MANAGEMENT_COLUMNS:
        UNIFIED_MANAGEMENT_COLUMNS = list(SHARED_UNIFIED_MANAGEMENT_COLUMNS)
        NUMERIC_MANAGEMENT_COLUMNS = set(NUMERIC_MANAGEMENT_COLUMNS) | {
            "推薦總分", "推薦分數", "股神決策分數", "上漲機率估計%", "上漲機率%", "推薦價格", "推薦日價格", "最新價", "建議價位",
            "近端支撐", "主要支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2",
            "建議倉位%", "動態建議倉位%", "建議部位%", "第一筆進場%", "最大風險%", "風險報酬比", "追價風險分",
            "大盤橋接分數", "大盤影響加減分", "族群資金流分數", "同類股領先幅度", "類股熱度分數", "技術結構分數", "起漲前兆分數", "飆股起漲分數", "交易可行分數", "自動因子總分", "爆發力分數",
            "實際買進價", "實際賣出價", "實際報酬%", "損益金額", "損益幅%", "損益%", "持有天數",
            "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
        }
except Exception:
    pass


def _safe_load_json(path: Path) -> Any:
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8")
        if not text.strip():
            return []
        return json.loads(text)
    except Exception:
        return []



# =========================================================
# v28 欄位管理永久記錄：投資組合 / 每日追蹤 / 推薦品質共用
# - 本機 JSON + GitHub JSON 雙寫
# - 欄位顯示、隱藏、順序、預設模板都可永久保存
# =========================================================

COLUMN_CONFIG_VERSION = "v28"

COLUMN_PRESETS: Dict[str, List[str]] = {
    "核心推薦欄位": [
        "v21操作優先順序", "追蹤分級", "今日操作建議", "品質分級", "品質建議",
        "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦時間",
        "推薦模式", "推薦型態", "機會型態", "進場時機", "建議動作", "等待條件",
        "推薦分數", "股神決策分數", "買點分級", "上漲機率%", "股神信心", "股神進場區間", "最新價",
    ],
    "操作與倉位欄位": [
        "v21操作優先順序", "股票代號", "股票名稱", "類別", "產業", "推薦日期", "推薦分數",
        "進場時機", "建議動作", "等待條件", "建議倉位%", "動態建議倉位%", "建議投入等級",
        "第一筆進場%", "分批策略", "第二筆加碼條件", "建議價位", "股神進場區間", "最新價",
    ],
    "風控停利停損欄位": [
        "股票代號", "股票名稱", "推薦日期", "推薦分數", "建議動作", "等待條件", "股神進場區間", "最新價",
        "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2",
        "停利策略", "停損策略", "追高風險等級", "單檔風險等級", "最大風險%", "風險說明",
    ],
    "族群大盤欄位": [
        "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦分數",
        "大盤策略模式", "大盤策略建議", "大盤情境分桶", "大盤橋接狀態", "大盤橋接風控", "大盤情境調權說明",
        "族群集中警示", "組合配置建議", "強勢族群等級", "族群輪動狀態", "族群策略建議", "族群資金流分數", "族群資金流說明",
    ],
    "績效追蹤欄位": [
        "股票代號", "股票名稱", "推薦日期", "推薦時間", "推薦分數", "推薦價格", "最新價", "狀態",
        "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%",
        "推薦後最大漲幅%", "推薦後最大回撤%", "命中結果", "績效評語", "實際買進價", "實際賣出價", "實際報酬%",
    ],
}

COLUMN_PROFILE_LABELS: Dict[str, str] = {
    "page12_portfolio_detail": "投資組合明細",
    "page12_daily_detail": "今日追蹤明細",
    "page12_quality_detail": "品質明細",
    "page12_fail_detail": "失敗案例檢討",
}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _config_ts(payload: Any) -> str:
    if isinstance(payload, dict):
        return str(payload.get("updated_at", ""))
    return ""


def _default_column_config() -> Dict[str, Any]:
    return {
        "version": COLUMN_CONFIG_VERSION,
        "updated_at": "",
        "profiles": {},
    }


def _safe_json_read_local_config(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            txt = path.read_text(encoding="utf-8")
            if txt.strip():
                return json.loads(txt)
    except Exception:
        pass
    return default


def _safe_json_write_local_config(path: Path, payload: Any) -> Tuple[bool, str]:
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True, f"本機已儲存：{path.name}"
    except Exception as exc:
        return False, f"本機儲存失敗：{exc}"


def _management_config_github_config() -> Dict[str, str]:
    return {
        "token": str(st.secrets.get("GITHUB_TOKEN", "")) if hasattr(st, "secrets") else "",
        "owner": str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")) if hasattr(st, "secrets") else "cheng07021028",
        "repo": str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")) if hasattr(st, "secrets") else "stock-app",
        "branch": str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) if hasattr(st, "secrets") else "main",
        "path": str(st.secrets.get("GODPICK_MANAGEMENT_UI_CONFIG_GITHUB_PATH", "godpick_management_ui_config.json")) if hasattr(st, "secrets") else "godpick_management_ui_config.json",
    }


def _github_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _read_management_config_from_github() -> Tuple[Dict[str, Any], str]:
    cfg = _management_config_github_config()
    token = cfg.get("token", "")
    if not token or requests is None:
        return _default_column_config(), "未設定 GITHUB_TOKEN，欄位設定只會儲存在本機。"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=15,
        )
        if resp.status_code == 404:
            return _default_column_config(), "GitHub 尚未建立欄位設定檔，套用後會自動建立。"
        if resp.status_code >= 400:
            return _default_column_config(), f"GitHub 讀取欄位設定失敗：{resp.status_code}"
        data = resp.json()
        content = base64.b64decode(data.get("content", "")).decode("utf-8")
        payload = json.loads(content) if content.strip() else _default_column_config()
        return _normalize_column_config(payload), "GitHub 欄位設定讀取成功。"
    except Exception as exc:
        return _default_column_config(), f"GitHub 欄位設定讀取例外：{exc}"


def _get_github_file_sha(path_name: str) -> Tuple[str, str]:
    cfg = _management_config_github_config()
    token = cfg.get("token", "")
    if not token or requests is None:
        return "", "缺少 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], path_name),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=15,
        )
        if resp.status_code == 404:
            return "", "GitHub 設定檔尚未存在"
        if resp.status_code >= 400:
            return "", f"取得 SHA 失敗：{resp.status_code}"
        return resp.json().get("sha", ""), "OK"
    except Exception as exc:
        return "", f"取得 SHA 例外：{exc}"


def _write_management_config_to_github(payload: Dict[str, Any]) -> Tuple[bool, str]:
    cfg = _management_config_github_config()
    token = cfg.get("token", "")
    if not token or requests is None:
        return False, "未設定 GITHUB_TOKEN，已跳過 GitHub 永久寫入。"
    try:
        path_name = cfg["path"]
        sha, _ = _get_github_file_sha(path_name)
        body = {
            "message": f"Update godpick management column config {payload.get('updated_at', '')}",
            "content": base64.b64encode(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")).decode("ascii"),
            "branch": cfg["branch"],
        }
        if sha:
            body["sha"] = sha
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], path_name),
            headers=_github_headers(token),
            json=body,
            timeout=25,
        )
        if resp.status_code in (200, 201):
            return True, f"GitHub 已永久儲存：{path_name}"
        return False, f"GitHub 寫入失敗：{resp.status_code} {resp.text[:200]}"
    except Exception as exc:
        return False, f"GitHub 寫入例外：{exc}"


def _normalize_column_config(payload: Any) -> Dict[str, Any]:
    cfg = _default_column_config()
    if isinstance(payload, dict):
        cfg.update({k: v for k, v in payload.items() if k != "profiles"})
        profiles = payload.get("profiles", {}) if isinstance(payload.get("profiles", {}), dict) else {}
        fixed_profiles: Dict[str, Any] = {}
        for key, prof in profiles.items():
            if not isinstance(prof, dict):
                continue
            fixed_profiles[str(key)] = {
                "label": str(prof.get("label", COLUMN_PROFILE_LABELS.get(str(key), str(key)))),
                "columns": [str(x) for x in prof.get("columns", []) if str(x).strip()],
                "hidden": [str(x) for x in prof.get("hidden", []) if str(x).strip()],
                "updated_at": str(prof.get("updated_at", "")),
            }
        cfg["profiles"] = fixed_profiles
    cfg["version"] = COLUMN_CONFIG_VERSION
    return cfg


@st.cache_data(show_spinner=False, ttl=30)
def _load_management_column_config_cached(_refresh_seq: int = 0) -> Tuple[Dict[str, Any], str]:
    github_payload, github_msg = _read_management_config_from_github()
    local_payload = _safe_json_read_local_config(MANAGEMENT_UI_CONFIG_PATH, _default_column_config())
    local_payload = _normalize_column_config(local_payload)
    if _config_ts(local_payload) >= _config_ts(github_payload):
        return local_payload, f"使用本機欄位設定。{github_msg}"
    return github_payload, github_msg


def _load_management_column_config() -> Tuple[Dict[str, Any], str]:
    seq = int(st.session_state.get("v28_column_config_refresh_seq", 0))
    return _load_management_column_config_cached(seq)


def _save_management_column_config(payload: Dict[str, Any]) -> Tuple[bool, str]:
    payload = _normalize_column_config(payload)
    payload["version"] = COLUMN_CONFIG_VERSION
    payload["updated_at"] = _now_text()
    local_ok, local_msg = _safe_json_write_local_config(MANAGEMENT_UI_CONFIG_PATH, payload)
    github_ok, github_msg = _write_management_config_to_github(payload)
    st.session_state["v28_column_config_refresh_seq"] = int(st.session_state.get("v28_column_config_refresh_seq", 0)) + 1
    try:
        _load_management_column_config_cached.clear()
    except Exception:
        pass
    return (local_ok or github_ok), f"{local_msg}｜{github_msg}"


def _columns_non_empty(df: pd.DataFrame, cols: List[str]) -> List[str]:
    out: List[str] = []
    if df is None or df.empty:
        return [c for c in cols]
    for c in cols:
        if c in df.columns and any(not _is_blank_value(v) for v in df[c].tolist()):
            out.append(c)
    return out


def _column_candidates(schema_df: pd.DataFrame, default_cols: List[str]) -> List[str]:
    cols: List[str] = []
    for source in [default_cols, UNIFIED_MANAGEMENT_COLUMNS, list(schema_df.columns) if schema_df is not None else []]:
        for c in source:
            if c and c not in cols and not str(c).startswith("_"):
                cols.append(str(c))
    return cols


def _profile_columns_from_config(table_key: str, candidates: List[str], default_cols: List[str]) -> List[str]:
    cfg, _ = _load_management_column_config()
    prof = cfg.get("profiles", {}).get(table_key, {}) if isinstance(cfg, dict) else {}
    saved_cols = [c for c in prof.get("columns", []) if c in candidates]
    hidden = set([c for c in prof.get("hidden", []) if c in candidates])
    if saved_cols:
        selected = [c for c in saved_cols if c not in hidden]
        # 新欄位自動接在後面，但不破壞既有順序。
        for c in default_cols:
            if c in candidates and c not in saved_cols and c not in hidden:
                selected.append(c)
        return selected
    return [c for c in default_cols if c in candidates]



def _parse_column_order_text_v42(raw: str, candidates: List[str]) -> List[str]:
    """v42：把一行一欄文字轉成有效欄位順序。"""
    if not raw:
        return []
    out: List[str] = []
    seen = set()
    cand = set(candidates)
    text = str(raw).replace("\t", "\n").replace(",", "\n").replace("，", "\n")
    for line in text.splitlines():
        c = str(line).strip()
        if c and c in cand and c not in seen:
            out.append(c)
            seen.add(c)
    return out


def _render_column_manager(table_key: str, table_label: str, schema_df: pd.DataFrame, default_cols: List[str]) -> List[str]:
    """v48：欄位管理免即時重算＋套用免整頁重跑版。

    修正重點：
    - 不再使用 st.data_editor 調欄位順序，避免每改一格就整頁 rerun。
    - 欄位順序、搜尋、模板全部放進 st.form。
    - 使用者輸入 / 貼上 / 調整欄位順序時，不會立即重排表格。
    - 只有按「套用並永久記錄」或「套用到本次畫面」才解析欄位與重繪。
    """
    candidates = _column_candidates(schema_df, default_cols)
    cfg, cfg_msg = _load_management_column_config()
    current_cols = _profile_columns_from_config(table_key, candidates, default_cols)
    if not current_cols:
        current_cols = candidates[:]
    current_cols = [c for c in current_cols if c in candidates] or candidates[:]

    k_prefix = f"v48_colmgr_{table_key}"
    preview_key = f"{k_prefix}_preview_cols"
    text_key = f"{k_prefix}_order_text"

    preview_cols = st.session_state.get(preview_key)
    if isinstance(preview_cols, list) and preview_cols:
        current_cols = [c for c in preview_cols if c in candidates] or current_cols

    with st.expander(f"🧩 {table_label} 欄位管理 / v48 免即時重算", expanded=False):
        st.caption("調整欄位順序時不會立即重算；只有按『套用並永久記錄』或『套用到本次畫面』才套用。")
        st.caption(cfg_msg)
        st.caption(f"目前顯示 **{len(current_cols)}** 欄 / 可用 **{len(candidates)}** 欄。")

        if text_key not in st.session_state:
            st.session_state[text_key] = "\n".join(current_cols)

        with st.form(key=f"{k_prefix}_form", clear_on_submit=False):
            c0, c1, c2 = st.columns([1.2, 1.2, 1.0])
            with c0:
                preset = st.selectbox(
                    "快速模板",
                    ["目前設定", "全部欄位", "只保留有資料欄位"] + list(COLUMN_PRESETS.keys()),
                    key=f"{k_prefix}_preset",
                )
            with c1:
                keyword = st.text_input("欄位搜尋", key=f"{k_prefix}_kw", placeholder="輸入欄位關鍵字")
            with c2:
                hide_empty_choice = st.checkbox("套用時隱藏全空欄", value=False, key=f"{k_prefix}_hide_empty")

            visible_options = [c for c in candidates if (not keyword or keyword in c)]
            selected_default = [c for c in current_cols if c in visible_options]
            selected_cols = st.multiselect(
                "顯示欄位（可用搜尋縮小範圍；實際順序以下方文字框為準）",
                options=visible_options,
                default=selected_default,
                key=f"{k_prefix}_selected_cols",
            )
            order_text = st.text_area(
                "欄位順序（一行一欄；可直接剪下貼上調整）",
                key=text_key,
                height=320,
                help="v48：在這裡輸入不會立即運算；按套用後不強制整頁重跑。",
            )

            b1, b2, b3, b4 = st.columns(4)
            apply_btn = b1.form_submit_button("✅ 套用並永久記錄", type="primary", use_container_width=True)
            preview_btn = b2.form_submit_button("👁️ 套用到本次畫面", use_container_width=True)
            reset_btn = b3.form_submit_button("↩️ 恢復系統預設", use_container_width=True)
            nonempty_btn = b4.form_submit_button("🧹 只保留有資料欄", use_container_width=True)

        submitted = apply_btn or preview_btn or reset_btn or nonempty_btn
        if not submitted:
            st.info("調整完欄位順序後，請按『套用並永久記錄』；輸入過程不會觸發表格重算。")
            return [c for c in current_cols if c in candidates]

        if reset_btn:
            final_cols = [c for c in default_cols if c in candidates] or candidates[:]
        elif nonempty_btn:
            final_cols = _columns_non_empty(schema_df, candidates) or current_cols[:]
        else:
            if preset == "全部欄位":
                base_cols = candidates[:]
            elif preset == "只保留有資料欄位":
                base_cols = _columns_non_empty(schema_df, candidates) or current_cols[:]
            elif preset in COLUMN_PRESETS:
                base_cols = [c for c in COLUMN_PRESETS[preset] if c in candidates]
                for c in current_cols:
                    if c in candidates and c not in base_cols:
                        base_cols.append(c)
            else:
                base_cols = current_cols[:]

            if hide_empty_choice:
                non_empty = set(_columns_non_empty(schema_df, candidates))
                base_cols = [c for c in base_cols if c in non_empty]

            parsed = _parse_column_order_text_v42(order_text, candidates)
            selected_set = set(selected_cols) if selected_cols else set(parsed or base_cols)
            final_cols = [c for c in parsed if c in selected_set]
            for c in selected_cols:
                if c in candidates and c not in final_cols:
                    final_cols.append(c)
            if not final_cols:
                final_cols = [c for c in base_cols if c in candidates] or current_cols[:1] or candidates[:1]

        if not final_cols:
            st.error("至少要保留 1 個欄位。")
            return [c for c in current_cols if c in candidates]

        st.session_state[preview_key] = final_cols

        if preview_btn or reset_btn or nonempty_btn:
            st.success("已套用到本次畫面；未強制整頁重跑。若要永久保存，請再按『套用並永久記錄』。")
            return final_cols

        if apply_btn:
            cfg = _normalize_column_config(cfg)
            cfg.setdefault("profiles", {})[table_key] = {
                "label": table_label,
                "columns": final_cols,
                "hidden": [c for c in candidates if c not in final_cols],
                "updated_at": _now_text(),
            }
            ok, msg = _save_management_column_config(cfg)
            if ok:
                st.success(f"{table_label} 欄位設定已套用並永久記錄；未強制整頁重跑。{msg}")
            else:
                st.warning(f"{table_label} 欄位設定已嘗試儲存，但可能未完全寫入；未強制整頁重跑。{msg}")
            return final_cols

    return [c for c in current_cols if c in candidates]

def _file_status_rows(paths: List[Path]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for path in paths:
        exists = path.exists()
        size = path.stat().st_size if exists else 0
        mtime = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if exists else ""
        rows.append({
            "資料檔": path.name,
            "是否存在": "是" if exists else "否",
            "檔案大小KB": round(size / 1024, 2) if exists else 0,
            "最後修改時間": mtime,
            "路徑": str(path),
        })
    return pd.DataFrame(rows)


def _refresh_management_data() -> None:
    try:
        st.cache_data.clear()
    except Exception:
        pass
    st.session_state["v21_management_refresh_seq"] = int(st.session_state.get("v21_management_refresh_seq", 0)) + 1
    st.session_state["v21_management_last_refresh"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _extract_rows(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for key in ["records", "data", "items", "rows", "recommendations", "list", "history"]:
            val = obj.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        if any(k in obj for k in ["股票代號", "code", "stock_code", "symbol"]):
            return [obj]
    return []




def _dedupe_columns_keep_first_valid(df: pd.DataFrame) -> pd.DataFrame:
    """v23：處理 JSON 合併後可能出現的重複欄位，避免 pandas / Streamlit 型別錯誤。"""
    if df is None or df.empty:
        return df
    if not df.columns.duplicated().any():
        return df
    out = pd.DataFrame(index=df.index)
    seen: List[str] = []
    for col in list(df.columns):
        if col in seen:
            continue
        seen.append(col)
        block = df.loc[:, df.columns == col]
        if block.shape[1] == 1:
            out[col] = block.iloc[:, 0]
        else:
            # 同名欄位多個時，逐列取第一個非空值
            vals = []
            for _, row in block.iterrows():
                val = ""
                for x in row.tolist():
                    if not _is_blank_value(x):
                        val = x
                        break
                vals.append(val)
            out[col] = vals
    return out

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = _dedupe_columns_keep_first_valid(df.copy())
    alias = {
        "code": "股票代號", "stock_code": "股票代號", "symbol": "股票代號", "股票": "股票代號",
        "name": "股票名稱", "stock_name": "股票名稱", "market": "市場別",
        "industry": "產業", "category": "類別", "sector": "產業",
        "date": "推薦日期", "recommend_date": "推薦日期", "created_at": "推薦日期", "建立時間": "推薦日期",
        "score": "推薦分數", "total_score": "推薦分數", "final_score": "推薦分數",
        "status": "狀態", "result": "命中結果", "opportunity_type": "機會型態", "entry_timing": "進場時機",
    }
    for src, dst in alias.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]
    if "股票代號" in df.columns:
        df["股票代號"] = df["股票代號"].astype(str).str.strip()
        df = df[df["股票代號"].ne("")]
    if "推薦日期" in df.columns:
        df["推薦日期"] = df["推薦日期"].astype(str).replace({"NaT": "", "nan": "", "None": ""})
    df = _backfill_management_fields(df)
    # v26 欄位統一：共用 schema 回補欄位別名。
    try:
        if normalize_godpick_dataframe is not None:
            df = normalize_godpick_dataframe(df, add_missing=True)
    except Exception:
        pass
    return df.reset_index(drop=True)


def _load_many(paths: List[Path], dedupe_latest: bool = False) -> Tuple[pd.DataFrame, List[str]]:
    rows: List[Dict[str, Any]] = []
    notes: List[str] = []
    for path in paths:
        obj = _safe_load_json(path)
        part = _extract_rows(obj)
        notes.append(f"{path.name}：{len(part)} 筆" if part else f"{path.name}：0 筆或不存在")
        for r in part:
            rr = dict(r)
            rr.setdefault("資料來源檔", path.name)
            rows.append(rr)
    if not rows:
        return pd.DataFrame(), notes
    df = _normalize_df(pd.DataFrame(rows))
    if dedupe_latest and "股票代號" in df.columns:
        df["_dt"] = pd.to_datetime(df.get("推薦日期", pd.Series([None] * len(df))), errors="coerce")
        df["_seq"] = range(len(df))
        df = df.sort_values(["股票代號", "_dt", "_seq"]).drop_duplicates("股票代號", keep="last")
        df = df.drop(columns=["_dt", "_seq"], errors="ignore")
    return _ensure_unified_management_schema(df).reset_index(drop=True), notes


def _num(val: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(val):
            return default
        s = str(val).strip().replace("%", "").replace(",", "")
        if not s or s.lower() in ["nan", "none", "null", "--", "-"]:
            return default
        return float(s)
    except Exception:
        return default


def _to_num(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False), errors="coerce").fillna(default)




def _is_blank_value(v: Any) -> bool:
    try:
        if v is None:
            return True
        if isinstance(v, (list, tuple, set, dict)):
            return len(v) == 0
        if pd.isna(v):
            return True
    except Exception:
        pass
    s = str(v).strip()
    return s == "" or s.lower() in {"none", "nan", "nat", "null", "<na>", "--", "-", "[]", "{}"}


def _clean_text_value(v: Any) -> str:
    if _is_blank_value(v):
        return ""
    if isinstance(v, (list, tuple, set)):
        vals = [_clean_text_value(x) for x in list(v)]
        vals = [x for x in vals if x]
        return "、".join(vals)
    if isinstance(v, dict):
        vals = []
        for k, x in v.items():
            sx = _clean_text_value(x)
            if sx:
                vals.append(f"{k}:{sx}")
        return "；".join(vals)
    return str(v).strip()


def _first_existing_value(row: pd.Series, cols: List[str], default: str = "") -> str:
    for c in cols:
        if c in row.index and not _is_blank_value(row.get(c)):
            return _clean_text_value(row.get(c))
    return default


def _fill_text_col(df: pd.DataFrame, target: str, sources: List[str], default: str = "") -> pd.DataFrame:
    # v23：文字欄位一律轉 object，避免原欄位是 float / category 時塞入文字造成 TypeError。
    df = _dedupe_columns_keep_first_valid(df)
    if target not in df.columns:
        df[target] = ""
    else:
        try:
            df[target] = df[target].astype("object")
        except Exception:
            df[target] = df[target].map(_clean_text_value).astype("object")
    safe_sources = [s for s in sources if s in df.columns and s != target]
    if target in sources:
        safe_sources.append(target)
    for idx, row in df.iterrows():
        if _is_blank_value(row.get(target)):
            df.at[idx, target] = _first_existing_value(row, safe_sources, default)
    df[target] = df[target].map(_clean_text_value).astype("object")
    return df


def _fill_num_col(df: pd.DataFrame, target: str, sources: List[str], default: Any = None) -> pd.DataFrame:
    df = _dedupe_columns_keep_first_valid(df)
    if target not in df.columns:
        df[target] = default
    cur = pd.to_numeric(df[target].astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False), errors="coerce")
    for s in sources:
        if s in df.columns and s != target:
            src = pd.to_numeric(df[s].astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False), errors="coerce")
            cur = cur.fillna(src)
    df[target] = cur
    return df



# =========================================================
# v25：歷史資料智慧補值
# 目的：舊版「股神推薦紀錄 / 歷史全部」缺少追蹤、品質、倉位、族群策略等新欄位時，
#      以既有推薦分數、買點、風險、建議動作、等待條件等欄位推導管理用資訊。
# 注意：本頁只做畫面與匯出補值，不寫回 JSON，不覆蓋 7/8/10 原始資料。
# =========================================================

def _contains_any(text: Any, keywords: List[str]) -> bool:
    s = _clean_text_value(text)
    return any(k in s for k in keywords)


def _score_bucket(score: float) -> str:
    if score >= 90:
        return "S｜超高分"
    if score >= 85:
        return "A｜高分"
    if score >= 80:
        return "B｜中高分"
    if score >= 70:
        return "C｜可觀察"
    return "D｜低分"


def _infer_risk_text(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["單檔風險等級", "追高風險等級"])
    if existing:
        return existing
    text = " ".join(_clean_text_value(row.get(c)) for c in ["風險說明", "建議動作", "等待條件", "推薦型態", "進場時機", "大盤策略模式"] if c in row.index)
    score = _num(row.get("推薦分數", row.get("股神決策分數", 0)))
    if any(k in text for k in ["追高", "風險偏高", "高風險", "不建議", "空頭", "過熱", "轉弱"]):
        return "高風險"
    if any(k in text for k in ["止跌", "低接", "拉回", "觀察", "確認", "等待"]):
        return "中"
    if score >= 85:
        return "中低"
    if score >= 75:
        return "中"
    return "中高"


def _infer_chase_risk_text(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["追高風險等級", "單檔風險等級"])
    if existing:
        return existing
    text = " ".join(_clean_text_value(row.get(c)) for c in ["建議動作", "等待條件", "進場時機", "推薦型態", "風險說明"] if c in row.index)
    if any(k in text for k in ["突破", "追價", "追高", "急漲", "過熱"]):
        return "偏高"
    if any(k in text for k in ["拉回", "低接", "止跌", "不追價", "等待"]):
        return "低"
    return "中"


def _infer_alloc_percent(row: pd.Series) -> float:
    explicit = _num(row.get("建議倉位%", None), default=-1)
    if explicit > 0:
        return round(explicit, 1)
    score = _num(row.get("推薦分數", row.get("股神決策分數", 0)))
    risk = _risk_rank(_infer_risk_text(row))
    action_text = _clean_text_value(row.get("建議動作")) + _clean_text_value(row.get("進場時機")) + _clean_text_value(row.get("推薦型態"))
    if score >= 90:
        base = 12.0
    elif score >= 85:
        base = 10.0
    elif score >= 80:
        base = 8.0
    elif score >= 75:
        base = 6.0
    elif score >= 70:
        base = 4.0
    else:
        base = 2.0
    if any(k in action_text for k in ["拉回", "分批", "可布局", "低接"]):
        base += 1.0
    if any(k in action_text for k in ["等待", "觀察", "確認", "不追價"]):
        base -= 1.0
    if risk >= 4:
        base = min(base, 3.0)
    elif risk == 3:
        base *= 0.75
    return round(max(0.0, min(base, 12.0)), 1)


def _infer_dynamic_alloc_percent(row: pd.Series) -> float:
    explicit = _num(row.get("動態建議倉位%", None), default=-1)
    if explicit > 0:
        return round(explicit, 1)
    base = _infer_alloc_percent(row)
    market_text = " ".join(_clean_text_value(row.get(c)) for c in ["大盤策略模式", "大盤策略建議", "大盤橋接風控", "大盤情境分桶"] if c in row.index)
    if any(k in market_text for k in ["空頭", "偏空", "防守", "高風險", "降權"]):
        base *= 0.65
    elif any(k in market_text for k in ["多頭", "偏多", "強勢", "加權"]):
        base *= 1.1
    return round(max(0.0, min(base, 12.0)), 1)


def _infer_entry_pct(row: pd.Series) -> float:
    explicit = _num(row.get("第一筆進場%", None), default=-1)
    if explicit > 0:
        return round(explicit, 1)
    alloc = _infer_dynamic_alloc_percent(row)
    risk = _risk_rank(_infer_risk_text(row))
    if alloc <= 0:
        return 0.0
    if risk >= 4:
        return 30.0
    if alloc >= 10:
        return 50.0
    return 40.0


def _infer_invest_level(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["建議投入等級", "股神信心", "信心等級", "推薦等級", "上漲機率信心"])
    if existing:
        return existing
    score = _num(row.get("推薦分數", row.get("股神決策分數", 0)))
    risk = _risk_rank(_infer_risk_text(row))
    if score >= 88 and risk <= 2:
        return "高｜可分批"
    if score >= 80 and risk <= 3:
        return "中｜等待確認"
    if risk >= 4:
        return "低｜僅觀察"
    return "低中｜觀察"


def _infer_batch_strategy(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["分批策略", "組合配置建議", "最佳操作劇本"])
    if existing:
        return existing
    action = _clean_text_value(row.get("建議動作"))
    timing = _clean_text_value(row.get("進場時機"))
    wait = _clean_text_value(row.get("等待條件"))
    entry_pct = _infer_entry_pct(row)
    if _contains_any(action + timing, ["等待", "觀察", "確認", "不追價"]):
        return f"先觀察，確認訊號後再進第一筆 {entry_pct:.0f}%；條件：{wait or '守支撐、量價轉強'}。"
    if _contains_any(action + timing, ["拉回", "低接", "分批", "可布局", "止跌"]):
        return f"分兩到三筆；第一筆 {entry_pct:.0f}%，拉回守支撐再加碼，跌破停損區不加碼。"
    return f"小量試單 {entry_pct:.0f}%，不追高，依支撐/壓力分批調整。"


def _infer_second_entry_condition(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["第二筆加碼條件", "等待條件"])
    if existing:
        return existing
    support = _clean_text_value(row.get("近端支撐"))
    pressure = _clean_text_value(row.get("近端壓力"))
    if support or pressure:
        return f"守住支撐 {support or '區間低點'} 並放量突破 {pressure or '壓力區'} 後再加碼。"
    return "放量轉強且未跌破前低，再考慮第二筆。"


def _infer_daily_action_v25(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["今日操作建議"])
    if existing:
        return existing
    action = _first_existing_value(row, ["建議動作", "股神建議動作", "操作建議"])
    wait = _first_existing_value(row, ["等待條件", "K線檢視提示"])
    risk = _risk_rank(_infer_risk_text(row))
    score = _num(row.get("推薦分數", row.get("股神決策分數", 0)))
    if risk >= 4:
        return "減碼 / 僅觀察"
    if action:
        return action
    if score >= 85:
        return "優先觀察 / 等回測支撐"
    if wait:
        return f"等待確認：{wait}"
    return "觀察等待訊號"


def _infer_tracking_grade_v25(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["追蹤分級"])
    if existing:
        return existing
    priority = _first_existing_value(row, ["v21操作優先順序"])
    if priority:
        if "可分批" in priority or "優先" in priority:
            return "A｜優先追蹤"
        if "觀察" in priority:
            return "B｜觀察確認"
        if "減碼" in priority or "風險" in priority:
            return "C｜風險控管"
    score = _num(row.get("推薦分數", row.get("股神決策分數", 0)))
    risk = _risk_rank(_infer_risk_text(row))
    if risk >= 4:
        return "C｜風險控管"
    if score >= 85 and risk <= 2:
        return "A｜優先追蹤"
    if score >= 78:
        return "B｜觀察確認"
    return "D｜低優先"


def _infer_quality_grade_v25(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["品質分級"])
    if existing:
        return existing
    # 有績效欄位時才用命中/失敗語意；沒有績效時只做管理品質分級，避免誤判成已命中。
    has_perf = any(_num(row.get(c, None), default=0) != 0 for c in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%"] if c in row.index)
    if has_perf:
        return _quality_grade(row)
    score = _num(row.get("推薦分數", row.get("股神決策分數", 0)))
    risk = _risk_rank(_infer_risk_text(row))
    if risk >= 4:
        return "C｜高風險待驗證"
    if score >= 88 and risk <= 2:
        return "A｜高分待驗證"
    if score >= 80:
        return "B｜中高分待驗證"
    return "C｜待觀察"


def _infer_quality_advice_v25(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["品質建議"])
    if existing:
        return existing
    risk_text = _infer_risk_text(row)
    wait = _first_existing_value(row, ["等待條件", "K線檢視提示"])
    action = _first_existing_value(row, ["建議動作", "今日操作建議"])
    if _risk_rank(risk_text) >= 4:
        return "高風險標的，僅能小部位觀察，未突破或未守支撐前不建議加碼。"
    if wait:
        return f"依等待條件追蹤：{wait}"
    if action:
        return f"依操作建議追蹤：{action}"
    return "資料尚未完成後續績效追蹤，先以分數、風險與買點條件觀察。"


def _infer_sector_strategy_v25(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["族群策略建議", "族群資金流說明", "族群集中警示"])
    if existing:
        return existing
    cat = _first_existing_value(row, ["類別", "產業"], default="未分類族群")
    typ = _first_existing_value(row, ["推薦型態", "機會型態"], default="觀察型態")
    score = _num(row.get("推薦分數", row.get("股神決策分數", 0)))
    if score >= 85:
        return f"{cat} 族群中分數偏強，屬 {typ}；可列入族群領先觀察，但需避免同族群過度集中。"
    return f"{cat} 族群屬 {typ}，先觀察族群輪動與量能是否延續。"


def _infer_market_strategy_v25(row: pd.Series) -> str:
    existing = _first_existing_value(row, ["大盤策略建議", "大盤風控建議", "大盤情境調權說明"])
    if existing:
        return existing
    mode = _first_existing_value(row, ["大盤策略模式", "大盤情境分桶", "大盤橋接狀態"])
    if not mode:
        return "未串接大盤資料；以個股風險控管為主，勿一次滿倉。"
    if _contains_any(mode, ["空", "弱", "防守", "風險"]):
        return "大盤偏弱，降低追價，分批小倉位，跌破支撐先退出。"
    if _contains_any(mode, ["多", "強"]):
        return "大盤偏多，可提高優質強勢股追蹤，但仍需分批。"
    return "大盤中性，依個股支撐壓力與量價訊號操作。"


def _apply_v25_smart_backfill(out: pd.DataFrame) -> pd.DataFrame:
    """v25：補齊歷史紀錄缺失的管理欄位，只補空值，不覆蓋原本已有內容。"""
    if out is None or out.empty:
        return out
    for col in [
        "追蹤分級", "今日操作建議", "品質分級", "品質建議", "建議投入等級", "第一筆進場%",
        "分批策略", "第二筆加碼條件", "追高風險等級", "單檔風險等級", "族群策略建議",
        "大盤策略建議", "大盤策略模式", "大盤橋接狀態", "大盤橋接風控", "族群輪動狀態",
        "族群集中警示", "組合配置建議", "K線驗證標記", "K線檢視提示"
    ]:
        if col not in out.columns:
            out[col] = ""
    for col in ["建議倉位%", "動態建議倉位%"]:
        if col not in out.columns:
            out[col] = pd.NA

    for idx, row in out.iterrows():
        if _is_blank_value(row.get("單檔風險等級")):
            out.at[idx, "單檔風險等級"] = _infer_risk_text(row)
        if _is_blank_value(row.get("追高風險等級")):
            out.at[idx, "追高風險等級"] = _infer_chase_risk_text(row)
        if _is_blank_value(row.get("建議倉位%")) or _num(row.get("建議倉位%"), default=0) <= 0:
            out.at[idx, "建議倉位%"] = _infer_alloc_percent(row)
        if _is_blank_value(row.get("動態建議倉位%")) or _num(row.get("動態建議倉位%"), default=0) <= 0:
            # 重新取一次 row，讓剛補好的建議倉位可被使用
            tmp = out.loc[idx]
            out.at[idx, "動態建議倉位%"] = _infer_dynamic_alloc_percent(tmp)
        if _is_blank_value(row.get("第一筆進場%")) or _num(row.get("第一筆進場%"), default=0) <= 0:
            out.at[idx, "第一筆進場%"] = _infer_entry_pct(out.loc[idx])
        if _is_blank_value(row.get("建議投入等級")):
            out.at[idx, "建議投入等級"] = _infer_invest_level(out.loc[idx])
        if _is_blank_value(row.get("分批策略")):
            out.at[idx, "分批策略"] = _infer_batch_strategy(out.loc[idx])
        if _is_blank_value(row.get("第二筆加碼條件")):
            out.at[idx, "第二筆加碼條件"] = _infer_second_entry_condition(out.loc[idx])
        if _is_blank_value(row.get("今日操作建議")):
            out.at[idx, "今日操作建議"] = _infer_daily_action_v25(out.loc[idx])
        if _is_blank_value(row.get("追蹤分級")):
            out.at[idx, "追蹤分級"] = _infer_tracking_grade_v25(out.loc[idx])
        if _is_blank_value(row.get("品質分級")):
            out.at[idx, "品質分級"] = _infer_quality_grade_v25(out.loc[idx])
        if _is_blank_value(row.get("品質建議")):
            out.at[idx, "品質建議"] = _infer_quality_advice_v25(out.loc[idx])
        if _is_blank_value(row.get("族群策略建議")):
            out.at[idx, "族群策略建議"] = _infer_sector_strategy_v25(out.loc[idx])
        if _is_blank_value(row.get("大盤策略建議")):
            out.at[idx, "大盤策略建議"] = _infer_market_strategy_v25(out.loc[idx])
        if _is_blank_value(row.get("族群輪動狀態")):
            cat = _first_existing_value(out.loc[idx], ["類別", "產業"], "未分類")
            out.at[idx, "族群輪動狀態"] = f"{cat} 族群觀察"
        if _is_blank_value(row.get("族群集中警示")):
            out.at[idx, "族群集中警示"] = "請檢查同族群持股比例，避免過度集中。"
        if _is_blank_value(row.get("組合配置建議")):
            out.at[idx, "組合配置建議"] = f"建議單檔動態倉位 {out.at[idx, '動態建議倉位%']}%，分批執行。"
        if _is_blank_value(row.get("K線驗證標記")):
            timing = _first_existing_value(out.loc[idx], ["進場時機", "等待條件"], "等待K線確認")
            out.at[idx, "K線驗證標記"] = timing
        if _is_blank_value(row.get("K線檢視提示")):
            out.at[idx, "K線檢視提示"] = _first_existing_value(out.loc[idx], ["等待條件", "K線驗證標記"], "觀察量價與支撐是否有效")
    return out

def _backfill_management_fields(df: pd.DataFrame) -> pd.DataFrame:
    """v23：把 7/8/10 不同版本的欄位名稱統一回管理中心要顯示的欄位。"""
    if df is None or df.empty:
        return df
    out = _dedupe_columns_keep_first_valid(df.copy())

    # 數值欄位回補
    out = _fill_num_col(out, "推薦分數", ["推薦總分", "推薦分數", "股神決策分數", "實戰買點分數", "交易可行分數", "score", "total_score", "final_score"])
    out = _fill_num_col(out, "股神決策分數", ["股神決策分數", "推薦總分", "推薦分數", "實戰買點分數", "交易可行分數"])
    out = _fill_num_col(out, "上漲機率%", ["上漲機率%", "上漲機率", "預估上漲機率", "勝率", "命中機率%"])
    out = _fill_num_col(out, "最新價", ["最新價", "現價", "收盤價", "推薦價格"])
    out = _fill_num_col(out, "推薦價格", ["推薦價格", "推薦價", "建議價位", "最新價", "現價", "收盤價"])
    out = _fill_num_col(out, "建議價位", ["建議價位", "推薦價格", "推薦價", "股神進場區間", "最新價"])
    out = _fill_num_col(out, "突破確認價", ["突破確認價", "近端壓力", "賣出目標1", "目標價"])
    out = _fill_num_col(out, "停損價", ["停損價", "停損參考", "失效價位"])
    out = _fill_num_col(out, "賣出目標1", ["賣出目標1", "第一停利", "目標價", "近端壓力"])
    out = _fill_num_col(out, "賣出目標2", ["賣出目標2", "第二停利", "目標價2"])
    out = _fill_num_col(out, "建議倉位%", ["建議部位%", "建議倉位%", "動態建議倉位%"])
    out = _fill_num_col(out, "動態建議倉位%", ["動態建議倉位%", "建議倉位%", "建議部位%"])
    out = _fill_num_col(out, "近端支撐", ["近端支撐", "主要支撐", "支撐價", "停損價"])
    out = _fill_num_col(out, "近端壓力", ["近端壓力", "突破確認價", "賣出目標1", "目標價"])
    out = _fill_num_col(out, "停損參考", ["停損參考", "停損價", "失效價位"])

    # 文字欄位回補
    fill_map = {
        "股票代號": ["股票代號", "code", "stock_code", "symbol"],
        "股票名稱": ["股票名稱", "name", "stock_name"],
        "市場別": ["市場別", "market"],
        "類別": ["類別", "正式產業別", "主題類別", "category", "industry", "產業"],
        "產業": ["產業", "類別", "正式產業別", "主題類別", "category", "industry"],
        "推薦日期": ["推薦日期", "推薦日", "date", "recommend_date", "建立時間", "created_at", "加入時間"],
        "推薦時間": ["推薦時間", "time", "created_time", "建立時間", "加入時間"],
        "推薦模式": ["推薦模式", "模式", "推薦分桶", "股神決策模式", "策略模式"],
        "推薦型態": ["推薦型態", "買點狀態", "進場型態", "起漲等級", "推薦分桶", "股神型態", "型態"],
        "機會型態": ["機會型態", "機會股說明", "起漲摘要", "推薦理由摘要", "股神推論", "股神推論邏輯", "機會型態說明"],
        "進場時機": ["進場時機", "進場時機說明", "股神進場區間", "股神進場建議", "操作區間", "K線驗證標記", "K線檢視提示"],
        "建議動作": ["建議動作", "股神建議動作", "股神進場建議", "實戰操作建議", "隔日操作建議", "操作建議", "股神建議"],
        "等待條件": ["等待條件", "等待條件說明", "第二筆加碼條件", "轉弱條件", "K線檢視提示", "條件說明"],
        "建議投入等級": ["建議投入等級", "股神信心", "上漲機率信心", "信心等級", "推薦等級"],
        "上漲機率信心": ["上漲機率信心", "股神信心", "信心等級"],
        "買點分級": ["買點分級", "推薦等級", "起漲等級", "買點等級"],
        "第一筆進場%": ["第一筆進場%"],
        "分批策略": ["分批策略", "組合配置建議", "最佳操作劇本"],
        "第二筆加碼條件": ["第二筆加碼條件", "等待條件"],
        "追高風險等級": ["追高風險等級", "單檔風險等級", "風險說明", "風險扣分原因"],
        "單檔風險等級": ["單檔風險等級", "追高風險等級", "風險說明", "風險扣分原因"],
        "最大風險%": ["最大風險%", "停損距離%"],
        "停利策略": ["停利策略", "賣出目標1", "賣出目標2", "目標報酬%", "停利建議"],
        "停損策略": ["停損策略", "停損價", "停損參考", "失效價位"],
        "族群集中警示": ["族群集中警示", "族群策略建議", "族群資金流說明", "族群集中說明"],
        "組合配置建議": ["組合配置建議", "分批策略", "資金風險說明"],
        "大盤策略模式": ["大盤策略模式", "大盤情境分桶", "大盤橋接狀態", "大盤橋接風控", "大盤模式"],
        "大盤策略建議": ["大盤策略建議", "大盤風控建議", "市場策略調整說明", "大盤影響說明", "大盤情境調權說明"],
        "大盤情境分桶": ["大盤情境分桶", "大盤策略模式", "大盤橋接狀態"],
        "大盤橋接狀態": ["大盤橋接狀態", "大盤橋接風控", "大盤策略模式"],
        "大盤橋接風控": ["大盤橋接風控", "大盤風控建議", "大盤策略建議"],
        "大盤情境調權說明": ["大盤情境調權說明", "大盤策略建議", "市場策略調整說明"],
        "強勢族群等級": ["強勢族群等級", "族群輪動狀態", "類別"],
        "族群輪動狀態": ["族群輪動狀態", "族群策略建議", "族群資金流說明"],
        "族群策略建議": ["族群策略建議", "族群集中警示", "族群資金流說明"],
        "族群資金流說明": ["族群資金流說明", "族群策略建議"],
        "K線驗證標記": ["K線驗證標記", "K線檢視提示", "進場時機"],
        "K線檢視提示": ["K線檢視提示", "K線驗證標記", "等待條件"],
        "雷達訊號": ["雷達訊號", "雷達訊號說明"],
        "籌碼訊號": ["籌碼訊號", "籌碼訊號說明"],
        "量能訊號": ["量能訊號", "量能訊號說明"],
        "股神進場區間": ["股神進場區間", "建議價位", "進場時機", "操作區間"],
        "股神推論": ["股神推論", "股神推論邏輯", "推薦理由", "推薦原因", "機會型態"],
        "推薦理由": ["推薦理由", "推薦原因", "股神推論", "股神推論邏輯"],
        "推薦原因": ["推薦原因", "推薦理由", "股神推論", "股神推論邏輯"],
        "風險說明": ["風險說明", "風險扣分原因", "追高風險等級", "單檔風險等級"],
        "命中結果": ["命中結果", "績效評語", "是否達標_回測", "是否停損_回測"],
        "狀態": ["狀態", "目前狀態"],
    }
    for target, sources in fill_map.items():
        out = _fill_text_col(out, target, sources)

    # 針對價格欄，組合出可讀文字，避免只顯示空白
    for idx, row in out.iterrows():
        if _is_blank_value(row.get("停利策略")):
            t1 = _clean_text_value(row.get("賣出目標1")) if "賣出目標1" in out.columns else ""
            t2 = _clean_text_value(row.get("賣出目標2")) if "賣出目標2" in out.columns else ""
            if t1 or t2:
                out.at[idx, "停利策略"] = f"目標1 {t1}" + (f" / 目標2 {t2}" if t2 else "")
        if _is_blank_value(row.get("停損策略")):
            stop = _clean_text_value(row.get("停損價")) if "停損價" in out.columns else ""
            ref = _clean_text_value(row.get("停損參考")) if "停損參考" in out.columns else ""
            if stop or ref:
                out.at[idx, "停損策略"] = f"停損 {stop or ref}"
        if _is_blank_value(row.get("狀態")):
            out.at[idx, "狀態"] = "觀察"

    # v25：歷史資料智慧補值，只補空欄，不覆蓋既有資料。
    out = _apply_v25_smart_backfill(out)

    # 統一清掉畫面上的 None / nan 字串
    for c in out.columns:
        if c not in ["推薦分數", "建議倉位%", "動態建議倉位%", "第一筆進場%", "近端支撐", "近端壓力", "停損參考", "族群資金流分數"]:
            out[c] = out[c].map(_clean_text_value)
    return out


def _drop_empty_display_columns(df: pd.DataFrame, keep_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    keep = set(keep_cols or [])
    cols: List[str] = []
    for c in df.columns:
        if c in keep:
            cols.append(c)
            continue
        s = df[c]
        if any(not _is_blank_value(v) for v in s.tolist()):
            cols.append(c)
    return df[cols]


def _safe_display_table(df: pd.DataFrame, keep_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out = _drop_empty_display_columns(out, keep_cols=keep_cols)
    for c in out.columns:
        if not pd.api.types.is_numeric_dtype(out[c]) and not pd.api.types.is_bool_dtype(out[c]):
            out[c] = out[c].map(_clean_text_value)
    return out



def _ensure_unified_management_schema(df: pd.DataFrame) -> pd.DataFrame:
    """v24：確保推薦清單與推薦紀錄進入管理中心後，擁有相同欄位、相同欄序與安全型別。"""
    if df is None or df.empty:
        return pd.DataFrame(columns=UNIFIED_MANAGEMENT_COLUMNS)
    out = _backfill_management_fields(_dedupe_columns_keep_first_valid(df.copy()))
    # v26 欄位統一：再次套用共用 schema，確保兩種資料來源欄位完全一致。
    try:
        if normalize_godpick_dataframe is not None:
            out = normalize_godpick_dataframe(out, add_missing=True)
    except Exception:
        pass
    for col in UNIFIED_MANAGEMENT_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA if col in NUMERIC_MANAGEMENT_COLUMNS else ""
    for col in NUMERIC_MANAGEMENT_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col].astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False), errors="coerce")
    for col in out.columns:
        if col not in NUMERIC_MANAGEMENT_COLUMNS:
            out[col] = out[col].map(_clean_text_value).astype("object")
    # 若推薦時間被塞在推薦日期裡，盡量拆出 HH:MM:SS，畫面更一致。
    if "推薦日期" in out.columns:
        dt = pd.to_datetime(out["推薦日期"], errors="coerce")
        if "推薦時間" in out.columns:
            time_from_dt = dt.dt.strftime("%H:%M:%S").replace("NaT", "")
            out["推薦時間"] = out["推薦時間"].where(out["推薦時間"].map(lambda x: not _is_blank_value(x)), time_from_dt)
        out["推薦日期"] = dt.dt.strftime("%Y-%m-%d").where(dt.notna(), out["推薦日期"].astype(str))
    ordered = [c for c in UNIFIED_MANAGEMENT_COLUMNS if c in out.columns]
    extras = [c for c in out.columns if c not in ordered and not str(c).startswith("_")]
    return out[ordered + extras].reset_index(drop=True)


def _unified_display_cols(df: pd.DataFrame, preferred: Optional[List[str]] = None, include_extra: bool = False) -> List[str]:
    """v24：兩種資料來源共用同一套欄位順序；空欄不顯示，但核心欄位固定保留。"""
    if df is None or df.empty:
        return []
    core_keep = {
        "v21操作優先順序", "追蹤分級", "今日操作建議", "品質分級", "品質建議",
        "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦時間",
        "推薦模式", "推薦型態", "機會型態", "進場時機", "建議動作", "等待條件", "推薦分數",
        "建議倉位%", "動態建議倉位%", "建議價位", "股神進場區間", "推薦價格", "最新價",
        "停損參考", "停損價", "賣出目標1", "賣出目標2", "停利策略", "停損策略",
        "追高風險等級", "單檔風險等級", "風險說明", "大盤策略模式", "大盤策略建議",
        "族群策略建議", "K線驗證標記", "股神推論", "狀態", "資料來源檔",
    }
    base = preferred or UNIFIED_MANAGEMENT_COLUMNS
    cols: List[str] = []
    for c in base:
        if c in df.columns and c not in cols:
            if c in core_keep or any(not _is_blank_value(v) for v in df[c].tolist()):
                cols.append(c)
    if include_extra:
        for c in df.columns:
            if c not in cols and not str(c).startswith("_") and any(not _is_blank_value(v) for v in df[c].tolist()):
                cols.append(c)
    return cols


def _clean_count_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    for c in out.columns:
        out[c] = out[c].map(_clean_text_value) if not pd.api.types.is_numeric_dtype(out[c]) else out[c]
    return out

def _risk_rank(value: Any) -> int:
    s = str(value or "")
    if any(x in s for x in ["極高", "高風險", "偏高", "不建議"]):
        return 4
    if any(x in s for x in ["中高", "中等", "中風險"]):
        return 3
    if any(x in s for x in ["低", "偏低", "保守"]):
        return 1
    return 2


def _hit_flag(row: pd.Series) -> bool:
    result = str(row.get("命中結果", "")) + str(row.get("是否達標_回測", "")) + str(row.get("績效評語", ""))
    if any(x in result for x in ["達標", "命中", "成功", "有效", "偏強"]):
        return True
    perf = _num(row.get("推薦後5日%", row.get("推薦後10日%", row.get("推薦後20日%", 0))))
    max_gain = _num(row.get("推薦後最大漲幅%", 0))
    max_dd = _num(row.get("推薦後最大回撤%", 0))
    return perf >= 3 or max_gain >= 6 or (perf > 0 and max_dd > -4)


def _fail_flag(row: pd.Series) -> bool:
    result = str(row.get("命中結果", "")) + str(row.get("是否停損_回測", "")) + str(row.get("績效評語", ""))
    if any(x in result for x in ["停損", "失敗", "回撤過大", "不佳"]):
        return True
    perf = _num(row.get("推薦後5日%", row.get("推薦後10日%", row.get("推薦後20日%", 0))))
    max_dd = _num(row.get("推薦後最大回撤%", 0))
    return perf <= -3 or max_dd <= -6


def _display_cols(df: pd.DataFrame, preferred: List[str], limit_extra: int = 25) -> List[str]:
    if df is None or df.empty:
        return []
    base_keep = ["股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦分數", "狀態"]
    cols = []
    for c in preferred:
        if c in df.columns and c not in cols:
            cols.append(c)
    for c in df.columns:
        if c not in cols and not str(c).startswith("_"):
            cols.append(c)
        if len(cols) >= len(preferred) + limit_extra:
            break
    # 空欄不顯示，但保留核心欄位
    shown = []
    for c in cols:
        if c in base_keep:
            shown.append(c)
            continue
        if c in df.columns and any(not _is_blank_value(v) for v in df[c].tolist()):
            shown.append(c)
    return shown

def _kpi_row(items: List[Tuple[str, str, Optional[str]]]) -> None:
    cols = st.columns(len(items))
    for col, (label, value, delta) in zip(cols, items):
        col.metric(label, value, delta=delta)


def _filter_df(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    with st.expander("篩選條件", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            if "推薦日期" in out.columns:
                dts = pd.to_datetime(out["推薦日期"], errors="coerce").dt.date.dropna()
                if not dts.empty:
                    min_d, max_d = dts.min(), dts.max()
                    val = st.date_input("推薦日期區間", value=(min_d, max_d), min_value=min_d, max_value=max_d, key=f"{prefix}_date")
                    if isinstance(val, tuple) and len(val) == 2:
                        start_d, end_d = val
                        mask = pd.to_datetime(out["推薦日期"], errors="coerce").dt.date.between(start_d, end_d)
                        out = out[mask.fillna(False)]
        with c2:
            if "推薦分數" in out.columns:
                min_score = st.slider("最低推薦分數", 0, 100, 0, key=f"{prefix}_score")
                out = out[_to_num(out["推薦分數"]) >= min_score]
        with c3:
            keyword = st.text_input("股票代號 / 名稱關鍵字", key=f"{prefix}_kw")
            if keyword:
                mask = pd.Series(False, index=out.index)
                for col in ["股票代號", "股票名稱"]:
                    if col in out.columns:
                        mask = mask | out[col].astype(str).str.contains(keyword, case=False, na=False)
                out = out[mask]
        filter_cols = ["類別", "產業", "推薦型態", "機會型態", "進場時機", "單檔風險等級", "追高風險等級", "大盤策略模式"]
        cols = st.columns(4)
        for i, col_name in enumerate(filter_cols):
            if col_name not in out.columns:
                continue
            values = sorted([x for x in out[col_name].dropna().astype(str).unique().tolist() if x.strip()])
            if not values:
                continue
            chosen = cols[i % 4].multiselect(col_name, values, default=[], key=f"{prefix}_{col_name}")
            if chosen:
                out = out[out[col_name].astype(str).isin(chosen)]
    return out.reset_index(drop=True)


def _allocation_action(row: pd.Series) -> str:
    score = _num(row.get("推薦分數", 0))
    risk = _risk_rank(row.get("單檔風險等級", row.get("追高風險等級", "")))
    timing = str(row.get("進場時機", ""))
    chase = str(row.get("追高風險等級", ""))
    market = str(row.get("大盤策略模式", ""))
    alloc = _num(row.get("動態建議倉位%", row.get("建議倉位%", 0)))
    if risk >= 4 or "不建議" in chase or "空頭" in market:
        return "減碼 / 僅觀察"
    if score >= 80 and risk <= 2 and alloc >= 15 and any(x in timing for x in ["可", "成熟", "分批", "接近"]):
        return "可分批建立"
    if score >= 70 and risk <= 3:
        return "觀察等確認"
    return "暫不加碼"


def _daily_action(row: pd.Series) -> str:
    risk = _risk_rank(row.get("單檔風險等級", row.get("追高風險等級", "")))
    status = str(row.get("狀態", ""))
    result = str(row.get("命中結果", ""))
    action = str(row.get("建議動作", ""))
    if any(x in status + result for x in ["停損", "失敗", "賣出"]):
        return "風險處理 / 檢查停損"
    if risk >= 4:
        return "避免追高 / 僅觀察"
    if any(x in action for x in ["分批", "觀察", "等待", "確認"]):
        return action[:40]
    return "優先觀察 / 等確認訊號"


def _tracking_grade(row: pd.Series) -> str:
    score = _num(row.get("推薦分數", 0))
    risk = _risk_rank(row.get("單檔風險等級", row.get("追高風險等級", "")))
    if risk >= 4:
        return "C｜高風險"
    if score >= 80 and risk <= 2:
        return "A｜優先追蹤"
    if score >= 70:
        return "B｜觀察確認"
    return "D｜低優先"


def _quality_grade(row: pd.Series) -> str:
    if _hit_flag(row):
        return "A｜有效"
    if _fail_flag(row):
        return "D｜失敗"
    perf = _num(row.get("推薦後5日%", row.get("推薦後10日%", row.get("推薦後20日%", 0))))
    if perf > 0:
        return "B｜偏正向"
    return "C｜待觀察"


def _quality_advice(row: pd.Series) -> str:
    if _fail_flag(row):
        risk = str(row.get("追高風險等級", ""))
        market = str(row.get("大盤策略模式", ""))
        if "高" in risk or "不建議" in risk:
            return "失敗偏向追高風險，後續同類型應降低權重或降倉位。"
        if "空" in market:
            return "失敗可能與大盤偏弱有關，空頭階段需提高風控。"
        return "建議回看K線與支撐是否失守，納入錯誤案例檢討。"
    if _hit_flag(row):
        return "型態表現有效，可納入後續權重正向參考。"
    return "樣本尚未明確，持續追蹤。"


def _group_quality(df: pd.DataFrame, field: str) -> pd.DataFrame:
    if df.empty or field not in df.columns:
        return pd.DataFrame()
    work = df.copy()
    work[field] = work[field].fillna("未分類").astype(str)
    work["_hit"] = work.apply(_hit_flag, axis=1)
    work["_fail"] = work.apply(_fail_flag, axis=1)
    work["_perf5"] = work.apply(lambda r: _num(r.get("推薦後5日%", r.get("推薦後10日%", r.get("推薦後20日%", 0)))), axis=1)
    work["_gain"] = work.apply(lambda r: _num(r.get("推薦後最大漲幅%", 0)), axis=1)
    work["_dd"] = work.apply(lambda r: _num(r.get("推薦後最大回撤%", 0)), axis=1)
    g = work.groupby(field, dropna=False).agg(
        樣本數=(field, "size"),
        命中率=("_hit", "mean"),
        失敗率=("_fail", "mean"),
        平均績效=("_perf5", "mean"),
        平均最大漲幅=("_gain", "mean"),
        平均最大回撤=("_dd", "mean"),
    ).reset_index()
    g["命中率%"] = (g["命中率"] * 100).round(1)
    g["失敗率%"] = (g["失敗率"] * 100).round(1)
    g["平均績效%"] = g["平均績效"].round(2)
    g["平均最大漲幅%"] = g["平均最大漲幅"].round(2)
    g["平均最大回撤%"] = g["平均最大回撤"].round(2)
    g["校正建議"] = g.apply(lambda r: _tune_suggestion(r), axis=1)
    return g[[field, "樣本數", "命中率%", "失敗率%", "平均績效%", "平均最大漲幅%", "平均最大回撤%", "校正建議"]].sort_values(["樣本數", "命中率%", "平均績效%"], ascending=[False, False, False])


def _tune_suggestion(row: pd.Series) -> str:
    n = int(row.get("樣本數", 0) or 0)
    hit = _num(row.get("命中率%", 0))
    fail = _num(row.get("失敗率%", 0))
    perf = _num(row.get("平均績效%", 0))
    dd = _num(row.get("平均最大回撤%", 0))
    if n < 5:
        return "樣本不足，暫不調權"
    if hit >= 60 and perf > 2 and dd > -5:
        return "建議提高權重"
    if fail >= 45 or perf < -1.5 or dd <= -7:
        return "建議降低權重"
    return "建議維持觀察"


def _portfolio_warnings(df: pd.DataFrame) -> List[str]:
    warnings: List[str] = []
    if df.empty:
        return warnings
    alloc_col = "動態建議倉位%" if "動態建議倉位%" in df.columns else ("建議倉位%" if "建議倉位%" in df.columns else None)
    sector_col = "類別" if "類別" in df.columns else ("產業" if "產業" in df.columns else None)
    if alloc_col:
        total_alloc = float(_to_num(df[alloc_col]).sum())
        if total_alloc > 100:
            warnings.append(f"建議倉位合計 {total_alloc:.1f}% 已超過 100%，請優先挑選低風險與高分標的，不要全部進場。")
        elif total_alloc > 70:
            warnings.append(f"建議倉位合計 {total_alloc:.1f}% 偏高，建議分批執行並保留現金。")
    if sector_col:
        top = df[sector_col].fillna("未分類").astype(str).value_counts(normalize=True).head(1)
        if not top.empty and float(top.iloc[0]) >= 0.4:
            warnings.append(f"族群集中度偏高：{top.index[0]} 佔 {float(top.iloc[0]) * 100:.1f}%，需注意同族群同步回檔。")
    risk_col = "單檔風險等級" if "單檔風險等級" in df.columns else ("追高風險等級" if "追高風險等級" in df.columns else None)
    if risk_col:
        high_risk_ratio = df[risk_col].map(lambda x: _risk_rank(x) >= 4).mean()
        if high_risk_ratio >= 0.25:
            warnings.append(f"高風險標的比例 {high_risk_ratio * 100:.1f}% 偏高，建議降低強勢追價股權重。")
    return warnings


def _render_source_status(notes: List[str]) -> None:
    with st.expander("資料來源狀態", expanded=False):
        st.code("\n".join(notes) if notes else "無資料來源")


def render_portfolio_tab(rec_df: pd.DataFrame, hist_df: pd.DataFrame, notes: List[str]) -> None:
    st.subheader("投資組合與資金配置")
    st.caption("整合原 v18 投資組合功能：檢查建議倉位、族群集中、風險標的與操作優先順序。")
    _render_source_status(notes)
    source = st.radio("分析資料來源", ["推薦清單 / 目前追蹤", "股神推薦紀錄 / 歷史全部"], horizontal=True, key="v21_port_src")
    # v24：欄位基準用兩個資料來源的聯集，讓「推薦清單 / 目前追蹤」與「股神推薦紀錄 / 歷史全部」切換時欄位一致。
    schema_basis = _ensure_unified_management_schema(pd.concat([rec_df, hist_df], ignore_index=True, sort=False)) if (not rec_df.empty or not hist_df.empty) else pd.DataFrame(columns=UNIFIED_MANAGEMENT_COLUMNS)
    df = rec_df.copy() if source.startswith("推薦清單") else hist_df.copy()
    df = _ensure_unified_management_schema(df)
    if df.empty:
        st.warning("目前沒有可分析資料。請先在 7_股神推薦 匯入 10_推薦清單，或在 8_股神推薦紀錄建立紀錄。")
        return
    df = _filter_df(_ensure_unified_management_schema(df), "v21_port")
    if df.empty:
        st.warning("篩選後沒有資料。")
        return
    df["v21操作優先順序"] = df.apply(_allocation_action, axis=1)
    df = _ensure_unified_management_schema(df)
    alloc_col = "動態建議倉位%" if "動態建議倉位%" in df.columns else ("建議倉位%" if "建議倉位%" in df.columns else None)
    avg_score = _to_num(df["推薦分數"]).mean() if "推薦分數" in df.columns else 0
    total_alloc = _to_num(df[alloc_col]).sum() if alloc_col else 0
    risk_col = "單檔風險等級" if "單檔風險等級" in df.columns else ("追高風險等級" if "追高風險等級" in df.columns else None)
    high_risk = int(df[risk_col].map(lambda x: _risk_rank(x) >= 4).sum()) if risk_col else 0
    _kpi_row([
        ("追蹤標的", f"{len(df)} 檔", None),
        ("平均推薦分數", f"{avg_score:.1f}", None),
        ("建議倉位合計", f"{total_alloc:.1f}%", None),
        ("高風險標的", f"{high_risk} 檔", None),
    ])
    warnings = _portfolio_warnings(df)
    if warnings:
        for w in warnings:
            st.warning(w)
    else:
        st.success("目前組合未偵測到明顯過度集中或高風險倉位問題。")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("#### 操作優先順序")
        st.dataframe(_clean_count_table(df["v21操作優先順序"].value_counts().rename_axis("建議").reset_index(name="檔數")), use_container_width=True, hide_index=True)
    with c2:
        sector_col = "類別" if "類別" in df.columns else ("產業" if "產業" in df.columns else None)
        st.markdown("#### 族群集中")
        if sector_col:
            st.dataframe(_clean_count_table(df[sector_col].map(_clean_text_value).replace({"":"未分類"}).value_counts().rename_axis(sector_col).reset_index(name="檔數")), use_container_width=True, hide_index=True)
        else:
            st.info("缺少類別 / 產業欄位。")
    with c3:
        type_col = "推薦型態" if "推薦型態" in df.columns else ("機會型態" if "機會型態" in df.columns else None)
        st.markdown("#### 推薦型態")
        if type_col:
            st.dataframe(_clean_count_table(df[type_col].map(_clean_text_value).replace({"":"未分類"}).value_counts().rename_axis(type_col).reset_index(name="檔數")), use_container_width=True, hide_index=True)
        else:
            st.info("缺少推薦型態欄位。")
    display_cols_default = _unified_display_cols(schema_basis, UNIFIED_MANAGEMENT_COLUMNS)
    # v28：欄位管理器會保存使用者調整後的顯示欄位與順序。
    st.markdown("#### 投資組合明細")
    display_cols = _render_column_manager("page12_portfolio_detail", "投資組合明細", schema_basis, display_cols_default)
    table_df = _safe_display_table(df[[c for c in display_cols if c in df.columns]], keep_cols=display_cols)
    st.dataframe(table_df, use_container_width=True, hide_index=True, height=520)
    st.download_button("下載投資組合分析 CSV", table_df.to_csv(index=False).encode("utf-8-sig"), file_name=f"godpick_management_portfolio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv", use_container_width=True)


def render_daily_tab(rec_df: pd.DataFrame, hist_df: pd.DataFrame, notes: List[str]) -> None:
    st.subheader("每日追蹤報告")
    st.caption("整合原 v19 每日追蹤功能：今日操作重點、優先觀察、避免追高、風險處理與追蹤清單匯出。")
    _render_source_status(notes)
    df = _ensure_unified_management_schema(rec_df.copy())
    if df.empty:
        df = _ensure_unified_management_schema(hist_df.copy())
    if df.empty:
        st.warning("目前沒有推薦清單或推薦紀錄可追蹤。")
        return
    df = _filter_df(_ensure_unified_management_schema(df), "v21_daily")
    if df.empty:
        st.warning("篩選後沒有資料。")
        return
    df["今日操作建議"] = df.apply(_daily_action, axis=1)
    df["追蹤分級"] = df.apply(_tracking_grade, axis=1)
    df = _ensure_unified_management_schema(df)
    high_risk = df["今日操作建議"].astype(str).str.contains("避免追高|風險處理|停損", na=False).sum()
    priority = df["追蹤分級"].astype(str).str.startswith("A").sum()
    alloc_col = "動態建議倉位%" if "動態建議倉位%" in df.columns else ("建議倉位%" if "建議倉位%" in df.columns else None)
    alloc = _to_num(df[alloc_col]).sum() if alloc_col else 0
    _kpi_row([
        ("今日追蹤檔數", f"{len(df)} 檔", None),
        ("A級優先追蹤", f"{int(priority)} 檔", None),
        ("需風險處理 / 避免追高", f"{int(high_risk)} 檔", None),
        ("建議倉位合計", f"{alloc:.1f}%", None),
    ])
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 今日操作重點")
        st.dataframe(_clean_count_table(df["今日操作建議"].value_counts().rename_axis("操作建議").reset_index(name="檔數")), use_container_width=True, hide_index=True)
    with c2:
        st.markdown("#### 追蹤分級")
        st.dataframe(_clean_count_table(df["追蹤分級"].value_counts().rename_axis("追蹤分級").reset_index(name="檔數")), use_container_width=True, hide_index=True)
    st.markdown("#### 今日追蹤明細")
    sort_cols = [c for c in ["追蹤分級", "推薦分數"] if c in df.columns]
    if sort_cols:
        ascending = [True if c == "追蹤分級" else False for c in sort_cols]
        df = df.sort_values(sort_cols, ascending=ascending)
    display_cols_default = _unified_display_cols(df, DAILY_COLUMNS)
    display_cols = _render_column_manager("page12_daily_detail", "今日追蹤明細", df, display_cols_default)
    table_df = _safe_display_table(df[[c for c in display_cols if c in df.columns]], keep_cols=["股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦分數"])
    st.dataframe(table_df, use_container_width=True, hide_index=True, height=560)
    st.download_button("下載每日追蹤報告 CSV", table_df.to_csv(index=False).encode("utf-8-sig"), file_name=f"godpick_daily_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv", use_container_width=True)


def render_quality_tab(all_df: pd.DataFrame, notes: List[str]) -> None:
    st.subheader("推薦品質儀表板")
    st.caption("整合原 v20 推薦品質功能：命中率、失敗率、型態品質、追高風險與校正建議。")
    _render_source_status(notes)
    if all_df.empty:
        st.warning("目前沒有推薦紀錄或推薦清單可分析。請先建立紀錄，並在 8_股神推薦紀錄執行推薦後績效更新。")
        return
    df = _filter_df(_ensure_unified_management_schema(all_df.copy()), "v21_quality")
    if df.empty:
        st.warning("篩選後沒有資料。")
        return
    df["品質分級"] = df.apply(_quality_grade, axis=1)
    df["品質建議"] = df.apply(_quality_advice, axis=1)
    df = _ensure_unified_management_schema(df)
    hits = int(df.apply(_hit_flag, axis=1).sum())
    fails = int(df.apply(_fail_flag, axis=1).sum())
    perf_col = "推薦後5日%" if "推薦後5日%" in df.columns else ("推薦後10日%" if "推薦後10日%" in df.columns else ("推薦後20日%" if "推薦後20日%" in df.columns else None))
    avg_perf = _to_num(df[perf_col]).mean() if perf_col else 0
    avg_gain = _to_num(df["推薦後最大漲幅%"]).mean() if "推薦後最大漲幅%" in df.columns else 0
    avg_dd = _to_num(df["推薦後最大回撤%"]).mean() if "推薦後最大回撤%" in df.columns else 0
    hit_rate = hits / len(df) * 100 if len(df) else 0
    fail_rate = fails / len(df) * 100 if len(df) else 0
    _kpi_row([
        ("分析樣本", f"{len(df)} 筆", None),
        ("命中率", f"{hit_rate:.1f}%", None),
        ("失敗率", f"{fail_rate:.1f}%", None),
        ("平均績效", f"{avg_perf:.2f}%", None),
        ("平均最大漲幅", f"{avg_gain:.2f}%", None),
        ("平均最大回撤", f"{avg_dd:.2f}%", None),
    ])
    c1, c2 = st.columns(2)
    with c1:
        field = st.selectbox("品質分組欄位", [f for f in GROUP_FIELDS if f in df.columns] or ["推薦型態"], key="v21_quality_group")
    with c2:
        min_samples = st.slider("最少樣本數", 1, 50, 3, key="v21_quality_min_samples")
    group_df = _group_quality(df, field) if field in df.columns else pd.DataFrame()
    if not group_df.empty:
        group_df = group_df[group_df["樣本數"] >= min_samples]
        st.markdown("#### 分組品質與校正建議")
        st.dataframe(group_df, use_container_width=True, hide_index=True, height=360)
    st.markdown("#### 失敗案例檢討")
    fail_df = df[df.apply(_fail_flag, axis=1)].copy()
    if fail_df.empty:
        st.success("目前沒有明確失敗案例。")
    else:
        fail_default_cols = _unified_display_cols(fail_df, QUALITY_COLUMNS)
        fail_cols = _render_column_manager("page12_fail_detail", "失敗案例檢討", fail_df, fail_default_cols)
        st.dataframe(_safe_display_table(fail_df[[c for c in fail_cols if c in fail_df.columns]]), use_container_width=True, hide_index=True, height=300)
    st.markdown("#### 品質明細")
    display_cols_default = _unified_display_cols(df, QUALITY_COLUMNS)
    display_cols = _render_column_manager("page12_quality_detail", "品質明細", df, display_cols_default)
    table_df = _safe_display_table(df[[c for c in display_cols if c in df.columns]], keep_cols=["股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦分數"])
    st.dataframe(table_df, use_container_width=True, hide_index=True, height=520)
    st.download_button("下載品質分析 CSV", table_df.to_csv(index=False).encode("utf-8-sig"), file_name=f"godpick_quality_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv", use_container_width=True)
    if not group_df.empty:
        st.download_button("下載分組校正建議 CSV", group_df.to_csv(index=False).encode("utf-8-sig"), file_name=f"godpick_quality_group_tune_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv", use_container_width=True)


def render_cleanup_tab() -> None:
    st.subheader("側邊欄整理建議")
    st.caption("v21 已把 12、13、14 的功能整合到同一個管理中心。若要側邊欄乾淨，可以移除舊獨立頁。")
    st.markdown("#### 建議保留")
    st.code("""7_股神推薦.py
8_股神推薦紀錄.py
10_推薦清單.py
12_股神管理中心.py""")
    st.markdown("#### 若已確認 v21 正常，可刪除或改副檔名 .bak")
    st.code("""pages/12_股神投資組合.py
pages/13_股神每日追蹤報告.py
pages/14_股神推薦品質儀表板.py

或 #U 編碼檔名：
pages/12_#U80a1#U795e#U6295#U8cc7#U7d44#U5408.py
pages/13_#U80a1#U795e#U6bcf#U65e5#U8ffd#U8e64#U5831#U544a.py
pages/14_#U80a1#U795e#U63a8#U85a6#U54c1#U8cea#U5100#U8868#U677f.py""")
    st.warning("不要刪除 7、8、10，也不要刪除任何 JSON。13、14 只是獨立分析頁，功能已整合進 v21。")
    st.info("如果你不想刪檔，也可以先把舊頁副檔名改成 .bak，確認一週沒問題後再刪除。")


def main() -> None:
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    # v40：啟用欄位管理極速模式；不再全頁攔截所有表格
    try:
        from godpick_column_manager import install_auto_column_manager
        install_auto_column_manager("12_股神管理中心")
    except Exception:
        pass
    if inject_pro_theme:
        try:
            inject_pro_theme()
        except Exception:
            pass
    if render_pro_hero:
        try:
            render_pro_hero("股神管理中心", "v48｜欄位管理免即時重算版：輸入不即時重算，套用不強制整頁重跑")
        except Exception:
            st.title(PAGE_TITLE)
    else:
        st.title(PAGE_TITLE)
    st.caption("本頁整合 v18 投資組合、v19 每日追蹤、v20 推薦品質儀表板；不修改推薦邏輯、不寫入 JSON、不影響掃描速度。")
    st.caption("v48 修正：欄位管理表單模式，輸入欄位順序時不重算；按套用後不強制整頁重跑。")

    c_refresh, c_status = st.columns([1.2, 4])
    with c_refresh:
        if st.button("🔄 資訊重整帶入", type="primary", use_container_width=True, help="重新讀取推薦清單、推薦紀錄與品質分析資料，並清除 Streamlit 快取。"):
            _refresh_management_data()
            st.success("已重新讀取資料來源並清除快取。")
    with c_status:
        last_refresh = st.session_state.get("v21_management_last_refresh", "尚未手動重整")
        st.info(f"目前資料讀取狀態：每次進頁會自動讀取 JSON；手動重整時間：{last_refresh}")

    with st.expander("資料檔案更新狀態", expanded=False):
        st.dataframe(_file_status_rows(ALL_DATA_FILES), use_container_width=True, hide_index=True)

    rec_df, rec_notes = _load_many(RECOMMEND_FILES, dedupe_latest=True)
    hist_df, hist_notes = _load_many(RECORD_FILES, dedupe_latest=False)
    all_df, all_notes = _load_many(ALL_DATA_FILES, dedupe_latest=False)
    notes = ["推薦清單："] + rec_notes + ["推薦紀錄："] + hist_notes

    tabs = st.tabs(["投資組合", "每日追蹤", "推薦品質", "側邊欄整理"])
    with tabs[0]:
        render_portfolio_tab(rec_df, hist_df, notes)
    with tabs[1]:
        render_daily_tab(rec_df, hist_df, notes)
    with tabs[2]:
        render_quality_tab(all_df, all_notes)
    with tabs[3]:
        render_cleanup_tab()


if __name__ == "__main__":
    main()
