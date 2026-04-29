# pages/8_股神推薦紀錄.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Any
import json
import base64
import io
import hashlib
import copy

import pandas as pd
import requests
import streamlit as st
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:
    firebase_admin = None
    credentials = None
    firestore = None

from utils import (
    format_number,
    get_history_data,
    get_realtime_stock_info,
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
    get_normalized_watchlist,
)

PAGE_TITLE = "股神推薦紀錄"
PFX = "godpick_record_"
GOD_DECISION_V10_LINK_VERSION = "record_v10_entry_decision_v1_20260428"
BACKTEST_V12_VERSION = "record_v46_keyerror_safe_20260429"
PRELAUNCH_789_VERSION = "record_prelaunch_789_delete_fix_v1_20260425"
DELETE_FIX_VERSION = "record_delete_hidden_id_fix_v1_20260425"
RECORD_FIX_VERSION = "record_prelaunch_grade_read_v2_verified_20260425"
MARKET_TREND_V38_LINK_VERSION = "record_market_trend_v38_full_fields_20260429"

GODPICK_RECORD_COLUMNS = [
    "record_id", "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦型態", "機會型態", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "機會股分數", "機會股說明", "進場時機", "進場時機分數", "建議動作", "等待條件", "近端支撐", "主要支撐", "近端壓力", "突破確認價", "停損參考", "操作區間", "風險報酬比_決策", "追高風險分數_決策", "追高風險等級", "是否建議追價", "風險扣分原因", "決策說明", "推薦等級", "推薦總分",
    "大盤橋接分數", "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間", "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響加減分", "大盤影響說明", "大盤資料診斷摘要",
    "股神決策模式",
    "股神進場建議",
    "推薦分層",
    "建議部位%",
    "建議倉位%",
    "建議投入等級",
    "分批策略",
    "第一筆進場%",
    "第二筆加碼條件",
    "停利策略",
    "停損策略",
    "最大風險%",
    "資金風險說明",
    "單檔風險等級",
    "族群集中警示",
    "組合配置建議", "大盤策略模式", "大盤多空分數", "推薦積極度係數", "適合推薦型態", "大盤策略建議", "大盤風控建議", "市場策略調整說明", "動態建議倉位%",

    "風險報酬比",
    "追價風險分",
    "停損距離%",
    "目標報酬%",
    "不建議買進原因",
    "最佳操作劇本",
    "隔日操作建議",
    "失效價位",
    "轉弱條件",
    "大盤情境調權說明",
    "大盤情境分桶",
    "買點分級", "風險說明", "股神推論邏輯", "權重設定", "推薦分桶", "起漲等級", "信心等級",
    "技術結構分數", "起漲前兆分數", "飆股起漲分數", "起漲摘要", "飆股起漲分數", "起漲摘要", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明",  "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", "同類股領先幅度", "是否領先同類股",
    "推薦標籤", "推薦理由摘要", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2", "推薦日期", "推薦時間",
    "建立時間", "更新時間", "目前狀態", "是否已實際買進", "實際買進價", "實際賣出價", "實際報酬%", "最新價",
    "最新更新時間", "損益金額", "損益幅%", "是否達停損", "是否達目標1", "是否達目標2", "持有天數",
    "模式績效標籤", "股神決策分數", "股神建議動作", "股神信心", "股神進場區間", "股神推論", "備註",
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
]

STATUS_OPTIONS = ["觀察", "持有", "已買進", "已賣出", "停損", "達標", "取消", "封存"]

DEFAULT_STANDARD_COLS = [
    "record_id", "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦型態", "機會型態", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "機會股分數", "機會股說明", "進場時機", "進場時機分數", "建議動作", "等待條件", "近端支撐", "主要支撐", "近端壓力", "突破確認價", "停損參考", "操作區間", "風險報酬比_決策", "追高風險分數_決策", "追高風險等級", "是否建議追價", "風險扣分原因", "決策說明", "推薦等級", "推薦總分",
    "大盤橋接分數", "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間", "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響加減分", "大盤影響說明", "大盤資料診斷摘要",
    "股神決策模式",
    "股神進場建議",
    "推薦分層",
    "建議部位%",
    "風險報酬比",
    "追價風險分",
    "停損距離%",
    "目標報酬%",
    "不建議買進原因",
    "最佳操作劇本",
    "隔日操作建議",
    "失效價位",
    "轉弱條件",
    "大盤情境調權說明",
    "大盤情境分桶",
    "買點分級", "風險說明", "股神推論邏輯",
    "股神決策分數", "股神建議動作", "股神信心", "股神進場區間",
    "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "近端支撐", "近端壓力", "突破確認價", "停損參考", "追高風險等級", "是否建議追價", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "最新價", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
    "目前狀態", "是否已實際買進", "實際買進價", "實際賣出價", "實際報酬%", "推薦日期", "推薦時間", "模式績效標籤", "備註"
]

DEFAULT_ADVANCED_COLS = [
    "record_id", "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦型態", "機會型態", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "機會股分數", "機會股說明", "進場時機", "進場時機分數", "建議動作", "等待條件", "近端支撐", "主要支撐", "近端壓力", "突破確認價", "停損參考", "操作區間", "風險報酬比_決策", "追高風險分數_決策", "追高風險等級", "是否建議追價", "風險扣分原因", "決策說明", "推薦等級", "推薦總分",
    "大盤橋接分數", "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間", "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響加減分", "大盤影響說明", "大盤資料診斷摘要",
    "股神決策模式",
    "股神進場建議",
    "推薦分層",
    "建議部位%",
    "風險報酬比",
    "追價風險分",
    "停損距離%",
    "目標報酬%",
    "不建議買進原因",
    "最佳操作劇本",
    "買點分級", "風險說明", "股神推論邏輯", "權重設定", "推薦分桶", "起漲等級", "信心等級",
    "技術結構分數", "起漲前兆分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明",  "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", "股神決策分數", "股神建議動作",
    "股神信心", "股神進場區間", "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "近端支撐", "近端壓力", "突破確認價", "停損參考", "追高風險等級", "是否建議追價", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2",
    "最新價", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "3日績效%", "5日績效%", "10日績效%", "20日績效%", "目前狀態", "是否已實際買進",
    "實際買進價", "實際賣出價", "實際報酬%", "是否達停損", "是否達目標1", "是否達目標2", "持有天數",
    "推薦日期", "推薦時間", "模式績效標籤", "股神推論", "機會股說明", "推薦理由摘要", "備註"
]

FAST_VISIBLE_LIMIT = 500
UI_CONFIG_DEFAULT = {
    "fast_mode": True,
    "visible_limit": FAST_VISIBLE_LIMIT,
    "profiles": {
        "標準": DEFAULT_STANDARD_COLS.copy(),
        "進階": DEFAULT_ADVANCED_COLS.copy(),
    },
    "updated_at": "",
}


def _dedupe_keep_order(seq):
    out = []
    seen = set()
    for x in seq:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

GODPICK_RECORD_COLUMNS = _dedupe_keep_order(GODPICK_RECORD_COLUMNS)
DEFAULT_STANDARD_COLS = _dedupe_keep_order(DEFAULT_STANDARD_COLS)
DEFAULT_ADVANCED_COLS = _dedupe_keep_order(DEFAULT_ADVANCED_COLS)



def _k(key: str) -> str:
    return f"{PFX}{key}"


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default=None):
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default



def _derive_prelaunch_grade_from_score(score: Any) -> str:
    """依起漲前兆分數補齊舊紀錄的起漲等級。"""
    s = _safe_float(score, 0) or 0
    if s >= 88:
        return "S｜強烈起漲"
    if s >= 78:
        return "A｜起漲優先"
    if s >= 68:
        return "B｜轉強確認"
    if s >= 55:
        return "C｜初步轉強"
    return "D｜尚未起漲"


# 相容保險：處理任何舊版呼叫名稱
def derive_prelaunch_grade_from_score(score: Any) -> str:
    return _derive_prelaunch_grade_from_score(score)



def _normalize_code(v: Any) -> str:
    s = _safe_str(v)
    if not s:
        return ""
    if s.isdigit():
        return s
    digits = "".join(ch for ch in s if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits
    return s


def _normalize_bool(v: Any) -> bool:
    return _safe_str(v).lower() in {"true", "1", "yes", "y", "是"}


def _normalize_category(v: Any) -> str:
    return _safe_str(v).replace("　", " ").strip()


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_date_text() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _now_time_text() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _create_record_id(code: str, rec_date: str, rec_time: str, mode: str) -> str:
    raw = f"{_safe_str(code)}|{_safe_str(rec_date)}|{_safe_str(rec_time)}|{_safe_str(mode)}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _set_status(msg: str, level: str = "info"):
    st.session_state[_k("status_msg")] = msg
    st.session_state[_k("status_type")] = level


def _github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_RECORDS_GITHUB_PATH", "godpick_records.json")) or "godpick_records.json",
    }


def _watchlist_github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("WATCHLIST_GITHUB_PATH", "watchlist.json")) or "watchlist.json",
    }


def _ui_config_github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_UI_CONFIG_GITHUB_PATH", "godpick_record_ui_config.json")) or "godpick_record_ui_config.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _firebase_config() -> dict[str, str]:
    return {
        "project_id": _safe_str(st.secrets.get("FIREBASE_PROJECT_ID", "")),
        "client_email": _safe_str(st.secrets.get("FIREBASE_CLIENT_EMAIL", "")),
        "private_key": _safe_str(st.secrets.get("FIREBASE_PRIVATE_KEY", "")),
    }


def _clean_private_key(raw_key: str) -> str:
    private_key = _safe_str(raw_key).replace("\\n", "\n").strip()
    if private_key.startswith("\ufeff"):
        private_key = private_key.lstrip("\ufeff")
    return private_key


def _init_firebase_app():
    if firebase_admin is None or credentials is None or firestore is None:
        raise RuntimeError("firebase-admin 未安裝或無法載入；已略過 Firestore，同步改用本機/GitHub。")
    try:
        return firebase_admin.get_app()
    except ValueError:
        pass

    cfg = _firebase_config()
    project_id = _safe_str(cfg["project_id"]).strip()
    client_email = _safe_str(cfg["client_email"]).strip()
    private_key = _clean_private_key(cfg["private_key"])

    if not project_id:
        raise ValueError("缺少 FIREBASE_PROJECT_ID")
    if not client_email:
        raise ValueError("缺少 FIREBASE_CLIENT_EMAIL")
    if not private_key:
        raise ValueError("缺少 FIREBASE_PRIVATE_KEY")
    if "BEGIN PRIVATE KEY" not in private_key or "END PRIVATE KEY" not in private_key:
        raise ValueError("FIREBASE_PRIVATE_KEY 不是有效 PEM 格式")

    cred_dict = {
        "type": "service_account",
        "project_id": project_id,
        "private_key": private_key,
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    cred = credentials.Certificate(cred_dict)
    return firebase_admin.initialize_app(cred, {"projectId": project_id})



def _derive_prelaunch_summary_from_row(row: pd.Series) -> str:
    s = _safe_float(row.get("飆股起漲分數"), row.get("起漲前兆分數")) or 0
    text = _safe_str(row.get("起漲摘要"))
    if text:
        return text
    parts = []
    if s >= 90:
        parts.append("接近漲停")
    elif s >= 78:
        parts.append("強漲")
    elif s >= 68:
        parts.append("明顯上漲")
    elif s >= 55:
        parts.append("小漲轉強")
    if _safe_float(row.get("爆發力分數"), 0) and _safe_float(row.get("爆發力分數"), 0) >= 70:
        parts.append("量能放大")
    if _safe_float(row.get("型態突破分數"), 0) and _safe_float(row.get("型態突破分數"), 0) >= 70:
        parts.append("突破結構")
    return "、".join(parts) if parts else "未見明顯起漲訊號"


def _ensure_godpick_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)

    x = df.copy()
    if "record_id" not in x.columns and "rec_id" in x.columns:
        x["record_id"] = x["rec_id"]

    for c in GODPICK_RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = None

    numeric_cols = [
        "推薦總分", "大盤橋接分數", "大盤可參考分數", "大盤加權分", "大盤影響加減分", "族群資金流分數", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "技術結構分數", "起漲前兆分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "進場時機分數", "近端支撐", "主要支撐", "近端壓力", "突破確認價", "停損參考", "風險報酬比_決策", "追高風險分數_決策", "飆股起漲分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", 
        "同類股領先幅度", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2",
        "實際買進價", "實際賣出價", "實際報酬%", "最新價", "損益金額", "損益幅%",
        "持有天數", "股神決策分數", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
    ]
    # v46 修正：舊紀錄或 Firestore 回補資料可能沒有部分數值欄。
    # 先補欄再轉型，避免 x[c] 觸發 KeyError 造成整頁無法開啟。
    for c in numeric_cols:
        if c not in x.columns:
            x[c] = None
        x[c] = pd.to_numeric(x[c], errors="coerce")

    bool_cols = ["是否領先同類股", "是否已實際買進", "是否達停損", "是否達目標1", "是否達目標2"]
    for c in bool_cols:
        if c not in x.columns:
            x[c] = False
        x[c] = x[c].fillna(False).map(_normalize_bool)

    text_cols = [
        "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦等級", "推薦標籤", "推薦理由摘要", "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間", "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響說明", "大盤資料診斷摘要",
        "推薦分桶", "起漲等級", "信心等級", "推薦日期", "推薦時間", "建立時間", "更新時間", "最新更新時間", "模式績效標籤", "股神建議動作", "股神信心", "股神進場區間", "股神推論", "備註",
    ]
    for c in text_cols:
        if c not in x.columns:
            x[c] = ""
        x[c] = x[c].fillna("").astype(str)

    if "目前狀態" not in x.columns:
        x["目前狀態"] = "觀察"
    if "股票代號" not in x.columns:
        x["股票代號"] = ""
    if "類別" not in x.columns:
        x["類別"] = ""

    x["股票代號"] = x["股票代號"].map(_normalize_code)
    x["類別"] = x["類別"].map(_normalize_category)
    x["目前狀態"] = x["目前狀態"].fillna("觀察").astype(str).replace("", "觀察")


    # 舊紀錄沒有起漲等級時，依起漲前兆分數自動補齊，避免 7頁/8頁/10頁欄位不一致。
    if "起漲等級" in x.columns:
        empty_grade = x["起漲等級"].fillna("").astype(str).str.strip() == ""
        if empty_grade.any():
            x.loc[empty_grade, "起漲等級"] = x.loc[empty_grade, "起漲前兆分數"].apply(_derive_prelaunch_grade_from_score)

    # 7/8/9 起漲欄位串聯補齊：舊資料沒有新欄位時自動用起漲前兆分數補。
    if "飆股起漲分數" in x.columns:
        x["飆股起漲分數"] = pd.to_numeric(x["飆股起漲分數"], errors="coerce")
        if "起漲前兆分數" in x.columns:
            x["飆股起漲分數"] = x["飆股起漲分數"].fillna(pd.to_numeric(x["起漲前兆分數"], errors="coerce"))
    if "起漲等級" in x.columns:
        empty_grade = x["起漲等級"].fillna("").astype(str).str.strip() == ""
        if empty_grade.any():
            x.loc[empty_grade, "起漲等級"] = x.loc[empty_grade, "飆股起漲分數"].apply(_derive_prelaunch_grade_from_score)
    if "起漲摘要" in x.columns:
        empty_summary = x["起漲摘要"].fillna("").astype(str).str.strip() == ""
        if empty_summary.any():
            x.loc[empty_summary, "起漲摘要"] = x.loc[empty_summary].apply(_derive_prelaunch_summary_from_row, axis=1)

    need_id = x["record_id"].isna() | (x["record_id"].astype(str).str.strip() == "")
    if need_id.any():
        for idx in x[need_id].index:
            rec_date = _safe_str(x.at[idx, "推薦日期"]) or _now_date_text()
            rec_time = _safe_str(x.at[idx, "推薦時間"]) or _now_time_text()
            x.at[idx, "record_id"] = _create_record_id(
                _safe_str(x.at[idx, "股票代號"]),
                rec_date,
                rec_time,
                _safe_str(x.at[idx, "推薦模式"]),
            )

    return x[GODPICK_RECORD_COLUMNS].copy()


def _append_records_dedup_by_business_key(base_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    base_df = _ensure_godpick_record_columns(base_df)
    new_df = _ensure_godpick_record_columns(new_df)
    if new_df.empty:
        return base_df.copy()

    merged = pd.concat([base_df, new_df], ignore_index=True)
    merged["_biz_key"] = (
        merged["股票代號"].fillna("").astype(str) + "|"
        + merged["推薦日期"].fillna("").astype(str) + "|"
        + merged["推薦時間"].fillna("").astype(str) + "|"
        + merged["推薦模式"].fillna("").astype(str)
    )
    merged["_upd"] = pd.to_datetime(merged["更新時間"], errors="coerce")
    merged = merged.sort_values(["_biz_key", "_upd"], ascending=[True, False], na_position="last")
    merged = merged.drop_duplicates(subset=["_biz_key"], keep="first")
    return _ensure_godpick_record_columns(merged.drop(columns=["_biz_key", "_upd"], errors="ignore"))


def _delete_records_by_ids(df: pd.DataFrame, record_ids: list[str]) -> pd.DataFrame:
    df = _ensure_godpick_record_columns(df)
    ids = {_safe_str(x) for x in (record_ids or []) if _safe_str(x)}
    if df.empty or not ids:
        return df.copy()
    out = df[~df["record_id"].astype(str).isin(ids)].copy()
    out["更新時間"] = _now_text()
    return _ensure_godpick_record_columns(out)


def _clear_filtered_records(df: pd.DataFrame, filtered_df: pd.DataFrame) -> pd.DataFrame:
    df = _ensure_godpick_record_columns(df)
    filtered_df = _ensure_godpick_record_columns(filtered_df)
    if df.empty or filtered_df.empty:
        return df.copy()

    ids = {_safe_str(x) for x in filtered_df["record_id"].astype(str).tolist() if _safe_str(x)}
    if ids:
        out = df[~df["record_id"].astype(str).isin(ids)].copy()
    else:
        drop_keys = {
            f"{_safe_str(r.get('股票代號'))}|{_safe_str(r.get('推薦日期'))}|{_safe_str(r.get('推薦時間'))}|{_safe_str(r.get('推薦模式'))}"
            for _, r in filtered_df.iterrows()
        }
        keep_mask = []
        for _, r in df.iterrows():
            key = f"{_safe_str(r.get('股票代號'))}|{_safe_str(r.get('推薦日期'))}|{_safe_str(r.get('推薦時間'))}|{_safe_str(r.get('推薦模式'))}"
            keep_mask.append(key not in drop_keys)
        out = df[pd.Series(keep_mask, index=df.index)].copy()

    out["更新時間"] = _now_text()
    return _ensure_godpick_record_columns(out)


def _read_records_from_github() -> tuple[pd.DataFrame, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "未設定 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), ""
        if resp.status_code != 200:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), f"GitHub 讀取失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = data.get("content", "")
        if not content:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), ""
        decoded = base64.b64decode(content).decode("utf-8")
        payload = json.loads(decoded)
        if isinstance(payload, list):
            return _ensure_godpick_record_columns(pd.DataFrame(payload)), ""
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), ""
    except Exception as e:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), f"GitHub 讀取例外：{e}"


def _get_records_sha() -> tuple[str, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return "", "缺少 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 200:
            return _safe_str(resp.json().get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取 SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 SHA 例外：{e}"


def _write_records_to_github(df: pd.DataFrame) -> tuple[bool, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"
    sha, err = _get_records_sha()
    if err:
        return False, err

    content_text = json.dumps(_ensure_godpick_record_columns(df).to_dict(orient="records"), ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": f"update godpick records at {_now_text()}",
        "content": encoded,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha

    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫 GitHub：{cfg['path']}"
        return False, f"GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"GitHub 寫入例外：{e}"


def _read_records_from_firestore() -> tuple[pd.DataFrame, str]:
    try:
        _init_firebase_app()
        db = firestore.client()
        docs = list(db.collection("godpick_records").stream())
        rows = []
        for doc in docs:
            data = doc.to_dict() or {}
            data.setdefault("record_id", doc.id)
            rows.append(data)
        return _ensure_godpick_record_columns(pd.DataFrame(rows)), ""
    except Exception as e:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), f"Firestore 讀取失敗：{e}"


def _write_records_to_firestore(df: pd.DataFrame) -> tuple[bool, str]:
    try:
        _init_firebase_app()
        db = firestore.client()
        batch = db.batch()
        now = firestore.SERVER_TIMESTAMP
        records_ref = db.collection("godpick_records")
        summary_ref = db.collection("system").document("godpick_records_summary")

        clean_df = _ensure_godpick_record_columns(df)
        batch.set(summary_ref, {"count": len(clean_df), "updated_at": now, "source": "streamlit_record_page"}, merge=True)

        existing_docs = list(records_ref.stream())
        existing_ids = {doc.id for doc in existing_docs}
        new_ids = set()
        for row in clean_df.to_dict(orient="records"):
            rec_id = _safe_str(row.get("record_id"))
            if not rec_id:
                continue
            new_ids.add(rec_id)
            payload = dict(row)
            payload["updated_at"] = now
            batch.set(records_ref.document(rec_id), payload, merge=True)
        for old_id in existing_ids - new_ids:
            batch.delete(records_ref.document(old_id))
        batch.commit()
        return True, "已同步寫入 Firestore"
    except Exception as e:
        return False, f"Firestore 寫入失敗：{e}"


def _save_records_dual(df: pd.DataFrame) -> bool:
    clean_df = _ensure_godpick_record_columns(df)
    ok1, msg1 = _write_records_to_github(clean_df)
    ok2, msg2 = _write_records_to_firestore(clean_df)
    st.session_state[_k("last_sync_detail")] = [
        f"GitHub: {'成功' if ok1 else '失敗'} | {msg1}",
        f"Firestore: {'成功' if ok2 else '失敗'} | {msg2}",
    ]
    if ok1 and ok2:
        _set_status("推薦紀錄 GitHub + Firestore 同步成功", "success")
        return True
    if ok1 or ok2:
        _set_status("推薦紀錄部分同步成功", "warning")
        return True
    _set_status("推薦紀錄同步失敗", "error")
    return False


def _normalize_watchlist_payload(data: dict[str, list[dict[str, str]]]) -> dict[str, list[dict[str, str]]]:
    payload: dict[str, list[dict[str, str]]] = {}
    for group_name, items in data.items():
        g = _safe_str(group_name)
        if not g:
            continue
        seen = set()
        normalized_items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            code = _normalize_code(item.get("code"))
            name = _safe_str(item.get("name")) or code
            market = _safe_str(item.get("market")) or "上市"
            if not code:
                continue
            key = (g, code)
            if key in seen:
                continue
            seen.add(key)
            normalized_items.append({"code": code, "name": name, "market": market})
        payload[g] = sorted(normalized_items, key=lambda x: (_normalize_code(x.get("code")), _safe_str(x.get("name"))))
    return payload


def _read_watchlist_from_github() -> tuple[dict[str, list[dict[str, str]]], str]:
    cfg = _watchlist_github_config()
    token = cfg["token"]
    if not token:
        return {}, "未設定 GITHUB_TOKEN，無法讀取 watchlist.json"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return {}, ""
        if resp.status_code != 200:
            return {}, f"watchlist GitHub 讀取失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = data.get("content", "")
        if not content:
            return {}, ""
        decoded = base64.b64decode(content).decode("utf-8")
        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            return {}, "watchlist.json 格式錯誤，根層必須是 dict"
        return _normalize_watchlist_payload(payload), ""
    except Exception as e:
        return {}, f"watchlist GitHub 讀取例外：{e}"


def _load_watchlist_payload() -> dict[str, list[dict[str, str]]]:
    payload, err = _read_watchlist_from_github()
    st.session_state[_k("watchlist_import_detail")] = err or "GitHub watchlist 讀取成功"
    return _normalize_watchlist_payload(payload)


def _get_watchlist_sha() -> tuple[str, str]:
    cfg = _watchlist_github_config()
    token = cfg["token"]
    if not token:
        return "", "缺少 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 200:
            return _safe_str(resp.json().get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取 watchlist SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 watchlist SHA 例外：{e}"


def _write_watchlist_to_github(payload: dict[str, list[dict[str, str]]]) -> tuple[bool, str]:
    cfg = _watchlist_github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN，無法回寫 watchlist.json"

    sha, err = _get_watchlist_sha()
    if err:
        return False, err

    content_text = json.dumps(_normalize_watchlist_payload(payload), ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": f"update watchlist from godpick record page at {_now_text()}",
        "content": encoded,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha

    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫自選股：{cfg['path']}"
        return False, f"watchlist GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"watchlist GitHub 寫入例外：{e}"


def _export_records_to_watchlist(records_df: pd.DataFrame, selected_ids: list[str], target_group: str) -> tuple[bool, str]:
    records_df = _ensure_godpick_record_columns(records_df)
    ids = {_safe_str(x) for x in (selected_ids or []) if _safe_str(x)}
    if records_df.empty:
        return False, "目前沒有推薦紀錄可匯出"
    if not ids:
        return False, "請先勾選要匯入自選股中心的股票"

    chosen = records_df[records_df["record_id"].astype(str).isin(ids)].copy()
    if chosen.empty:
        return False, "找不到要匯入的推薦紀錄"

    payload = _load_watchlist_payload()
    target_group = _safe_str(target_group) or "股神推薦"
    if target_group not in payload:
        payload[target_group] = []

    existing_codes = {_normalize_code(x.get("code")) for x in payload.get(target_group, [])}
    add_count = 0
    skip_count = 0

    for _, row in chosen.iterrows():
        code = _normalize_code(row.get("股票代號"))
        name = _safe_str(row.get("股票名稱")) or code
        market = _safe_str(row.get("市場別")) or "上市"
        if not code:
            skip_count += 1
            continue
        if code in existing_codes:
            skip_count += 1
            continue
        payload[target_group].append({"code": code, "name": name, "market": market})
        existing_codes.add(code)
        add_count += 1

    payload = _normalize_watchlist_payload(payload)
    ok, msg = _write_watchlist_to_github(payload)
    if ok:
        try:
            get_normalized_watchlist.clear()
        except Exception:
            pass
        st.session_state["watchlist_data"] = copy.deepcopy(payload)
        st.session_state["watchlist_version"] = int(st.session_state.get("watchlist_version", 0) or 0) + 1
        st.session_state["watchlist_last_saved_at"] = _now_text()
        st.session_state[_k("watchlist_import_detail")] = f"目標群組：{target_group}｜新增 {add_count} 檔｜略過 {skip_count} 檔"
        return True, f"{msg}｜匯入 {add_count} 檔，略過 {skip_count} 檔"
    return False, msg


def _safe_json_read_local(path_name: str, default):
    try:
        with open(path_name, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _safe_json_write_local(path_name: str, payload) -> tuple[bool, str]:
    try:
        with open(path_name, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        return True, f"已寫入本機 UI 設定：{path_name}"
    except Exception as e:
        return False, f"本機 UI 設定寫入失敗：{e}"


def _config_ts(payload: dict[str, Any]):
    raw = _safe_str(payload.get("updated_at")) if isinstance(payload, dict) else ""
    try:
        return datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.min


def _normalize_ui_config(payload: dict[str, Any] | None) -> dict[str, Any]:
    base = copy.deepcopy(UI_CONFIG_DEFAULT)
    if not isinstance(payload, dict):
        payload = {}

    base["fast_mode"] = bool(payload.get("fast_mode", base["fast_mode"]))
    base["visible_limit"] = int(_safe_float(payload.get("visible_limit"), base["visible_limit"]) or base["visible_limit"])
    base["visible_limit"] = max(100, min(base["visible_limit"], 5000))
    base["updated_at"] = _safe_str(payload.get("updated_at"))

    profiles = payload.get("profiles", {})
    if not isinstance(profiles, dict):
        profiles = {}

    for mode, defaults in UI_CONFIG_DEFAULT["profiles"].items():
        raw_cols = profiles.get(mode, defaults)
        if not isinstance(raw_cols, list):
            raw_cols = defaults
        clean_cols = []
        seen = set()
        for c in raw_cols:
            cc = _safe_str(c)
            if cc and cc not in seen:
                seen.add(cc)
                clean_cols.append(cc)
        remain = [c for c in defaults if c not in seen]
        base["profiles"][mode] = clean_cols + remain

    return base


def _read_ui_config_from_github() -> tuple[dict[str, Any], str]:
    cfg = _ui_config_github_config()
    token = cfg["token"]
    if not token:
        return copy.deepcopy(UI_CONFIG_DEFAULT), "未設定 GITHUB_TOKEN，無法讀取 UI 設定"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return copy.deepcopy(UI_CONFIG_DEFAULT), ""
        if resp.status_code != 200:
            return copy.deepcopy(UI_CONFIG_DEFAULT), f"UI 設定 GitHub 讀取失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = data.get("content", "")
        if not content:
            return copy.deepcopy(UI_CONFIG_DEFAULT), ""
        decoded = base64.b64decode(content).decode("utf-8")
        payload = json.loads(decoded)
        return _normalize_ui_config(payload), ""
    except Exception as e:
        return copy.deepcopy(UI_CONFIG_DEFAULT), f"UI 設定 GitHub 讀取例外：{e}"


def _get_ui_config_sha() -> tuple[str, str]:
    cfg = _ui_config_github_config()
    token = cfg["token"]
    if not token:
        return "", "缺少 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 200:
            return _safe_str(resp.json().get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取 UI 設定 SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 UI 設定 SHA 例外：{e}"


def _write_ui_config_to_github(payload: dict[str, Any]) -> tuple[bool, str]:
    cfg = _ui_config_github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN，無法回寫 UI 設定"

    sha, err = _get_ui_config_sha()
    if err:
        return False, err

    clean_payload = _normalize_ui_config(payload)
    clean_payload["updated_at"] = _now_text()
    content_text = json.dumps(clean_payload, ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": f"update godpick ui config at {_now_text()}",
        "content": encoded,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha

    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫 UI 設定：{cfg['path']}"
        return False, f"UI 設定 GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"UI 設定 GitHub 寫入例外：{e}"


def _load_ui_config_once():
    if st.session_state.get(_k("ui_config_loaded"), False):
        return

    github_payload, err = _read_ui_config_from_github()
    local_payload = _safe_json_read_local(_ui_config_github_config()["path"], {})

    github_payload = _normalize_ui_config(github_payload)
    local_payload = _normalize_ui_config(local_payload) if isinstance(local_payload, dict) and local_payload else {}

    # GitHub 舊資料不應覆蓋本機剛保存的欄位順序；有時間戳時取較新，無本機才用 GitHub。
    if local_payload and (_config_ts(local_payload) >= _config_ts(github_payload)):
        payload = local_payload
        detail = "已讀取本機 UI 設定；" + (err or "GitHub UI 設定也可讀取")
    else:
        payload = github_payload
        detail = err or "GitHub UI 設定讀取成功"

    payload = _normalize_ui_config(payload)
    st.session_state[_k("ui_config_loaded")] = True
    st.session_state[_k("ui_config_detail")] = detail
    st.session_state[_k("ui_config")] = copy.deepcopy(payload)
    st.session_state[_k("fast_mode")] = bool(payload.get("fast_mode", True))
    st.session_state[_k("visible_limit")] = int(payload.get("visible_limit", FAST_VISIBLE_LIMIT))
    for mode in ["標準", "進階"]:
        st.session_state[_get_profile_key(mode)] = payload.get("profiles", {}).get(mode, _get_default_col_profile(mode)).copy()


def _persist_ui_config() -> tuple[bool, str]:
    payload = {
        "fast_mode": bool(st.session_state.get(_k("fast_mode"), True)),
        "visible_limit": int(st.session_state.get(_k("visible_limit"), FAST_VISIBLE_LIMIT)),
        "profiles": {
            "標準": st.session_state.get(_get_profile_key("標準"), DEFAULT_STANDARD_COLS.copy()),
            "進階": st.session_state.get(_get_profile_key("進階"), DEFAULT_ADVANCED_COLS.copy()),
        },
        "updated_at": _now_text(),
    }
    payload = _normalize_ui_config(payload)
    st.session_state[_k("ui_config")] = copy.deepcopy(payload)
    local_ok, local_msg = _safe_json_write_local(_ui_config_github_config()["path"], payload)
    github_ok, github_msg = _write_ui_config_to_github(payload)
    msg = f"{local_msg}｜{github_msg}"
    st.session_state[_k("ui_save_detail")] = msg
    st.session_state[_k("ui_last_saved_at")] = _now_text()
    return (local_ok or github_ok), msg


@st.cache_data(ttl=120, show_spinner=False)
def _get_latest_close(stock_no: str, stock_name: str, market_type: str) -> tuple[float | None, str, str]:
    stock_no = _normalize_code(stock_no)
    stock_name = _safe_str(stock_name)
    market_type = _safe_str(market_type) or "上市"

    tried = []
    if market_type:
        tried.append(market_type)
    for mk in ["上市", "上櫃", "興櫃"]:
        if mk not in tried:
            tried.append(mk)

    for mk in tried:
        try:
            info = get_realtime_stock_info(stock_no, stock_name, mk, refresh_token=str(int(datetime.now().timestamp() * 1000)))
            price = _safe_float(info.get("price"))
            if price is not None and price > 0:
                src = _safe_str(info.get("price_source")) or "realtime"
                return float(price), _safe_str(info.get("market") or mk), src
        except Exception:
            pass

    today = date.today()
    start_date = today - timedelta(days=60)
    for mk in tried + [""]:
        try:
            try:
                df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_date=start_date, end_date=today)
            except TypeError:
                try:
                    df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_dt=start_date, end_dt=today)
                except Exception:
                    df = get_history_data(code=stock_no, start_date=start_date, end_date=today)
            if isinstance(df, pd.DataFrame) and not df.empty:
                temp = df.copy()
                if "日期" not in temp.columns:
                    for c in temp.columns:
                        if str(c).lower() in {"date", "日期"}:
                            temp = temp.rename(columns={c: "日期"})
                            break
                for c in temp.columns:
                    if str(c).lower() == "close":
                        temp = temp.rename(columns={c: "收盤價"})
                if "收盤價" not in temp.columns:
                    continue
                temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
                temp["收盤價"] = pd.to_numeric(temp["收盤價"], errors="coerce")
                temp = temp.dropna(subset=["日期", "收盤價"]).sort_values("日期")
                if not temp.empty:
                    return float(temp.iloc[-1]["收盤價"]), _safe_str(mk or market_type or "未知"), "history_close"
        except Exception:
            pass
    return None, _safe_str(market_type or "未知"), ""


@st.cache_data(ttl=3600, show_spinner=False)
def _get_forward_return(stock_no: str, stock_name: str, market_type: str, rec_date_text: str, days_after: int) -> float | None:
    rec_date = pd.to_datetime(rec_date_text, errors="coerce")
    if pd.isna(rec_date):
        return None

    start_date = rec_date.date() - timedelta(days=5)
    end_date = rec_date.date() + timedelta(days=max(days_after * 4, 40))
    tried = []
    primary = _safe_str(market_type)
    if primary:
        tried.append(primary)
    for mk in ["上市", "上櫃", "興櫃", ""]:
        if mk not in tried:
            tried.append(mk)

    for mk in tried:
        try:
            try:
                df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_date=start_date, end_date=end_date)
            except TypeError:
                try:
                    df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_dt=start_date, end_dt=end_date)
                except Exception:
                    df = get_history_data(code=stock_no, start_date=start_date, end_date=end_date)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            temp = df.copy()
            if "日期" not in temp.columns:
                for c in temp.columns:
                    if str(c).lower() in {"date", "日期"}:
                        temp = temp.rename(columns={c: "日期"})
                        break
            for c in temp.columns:
                if str(c).lower() == "close":
                    temp = temp.rename(columns={c: "收盤價"})
            if "日期" not in temp.columns or "收盤價" not in temp.columns:
                continue

            temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
            temp["收盤價"] = pd.to_numeric(temp["收盤價"], errors="coerce")
            temp = temp.dropna(subset=["日期", "收盤價"]).sort_values("日期").reset_index(drop=True)
            if temp.empty:
                continue

            base_candidates = temp[temp["日期"].dt.date >= rec_date.date()].reset_index(drop=True)
            if base_candidates.empty:
                continue
            if len(base_candidates) <= days_after:
                return None

            base_px = float(base_candidates.iloc[0]["收盤價"])
            target_px = float(base_candidates.iloc[days_after]["收盤價"])
            if base_px == 0:
                return None
            return (target_px - base_px) / base_px * 100
        except Exception:
            pass
    return None



@st.cache_data(ttl=3600, show_spinner=False)
def _get_forward_metrics(
    stock_no: str,
    stock_name: str,
    market_type: str,
    rec_date_text: str,
    stop_price: float | None,
    target_price: float | None,
) -> dict[str, Any]:
    """一次抓歷史資料，計算推薦後 1/3/5/10/20 日、最大漲幅、最大回撤與命中結果。"""
    rec_date = pd.to_datetime(rec_date_text, errors="coerce")
    if pd.isna(rec_date):
        return {}

    stock_no = _normalize_code(stock_no)
    stock_name = _safe_str(stock_name)
    primary = _safe_str(market_type)
    start_date = rec_date.date() - timedelta(days=5)
    end_date = rec_date.date() + timedelta(days=90)

    tried = []
    if primary:
        tried.append(primary)
    for mk in ["上市", "上櫃", "興櫃", ""]:
        if mk not in tried:
            tried.append(mk)

    for mk in tried:
        try:
            try:
                df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_date=start_date, end_date=end_date)
            except TypeError:
                try:
                    df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_dt=start_date, end_dt=end_date)
                except Exception:
                    df = get_history_data(code=stock_no, start_date=start_date, end_date=end_date)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            temp = df.copy()
            rename_map = {}
            for c in temp.columns:
                low = str(c).lower()
                if low in {"date", "日期"}:
                    rename_map[c] = "日期"
                elif low in {"close", "收盤價"}:
                    rename_map[c] = "收盤價"
                elif low in {"high", "最高價"}:
                    rename_map[c] = "最高價"
                elif low in {"low", "最低價"}:
                    rename_map[c] = "最低價"
            if rename_map:
                temp = temp.rename(columns=rename_map)
            if "日期" not in temp.columns or "收盤價" not in temp.columns:
                continue
            temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
            for c in ["收盤價", "最高價", "最低價"]:
                if c in temp.columns:
                    temp[c] = pd.to_numeric(temp[c], errors="coerce")
            temp = temp.dropna(subset=["日期", "收盤價"]).sort_values("日期").reset_index(drop=True)
            if temp.empty:
                continue

            window = temp[temp["日期"].dt.date >= rec_date.date()].reset_index(drop=True)
            if window.empty:
                continue
            base_px = _safe_float(window.iloc[0].get("收盤價"))
            if base_px in [None, 0]:
                continue

            result: dict[str, Any] = {}
            for d in [1, 3, 5, 10, 20]:
                key_new = f"推薦後{d}日%"
                if len(window) > d:
                    target_px = _safe_float(window.iloc[d].get("收盤價"))
                    result[key_new] = None if target_px in [None, 0] else round((target_px - base_px) / base_px * 100, 2)
                else:
                    result[key_new] = None

            use_window = window.head(min(len(window), 21)).copy()
            high_col = "最高價" if "最高價" in use_window.columns else "收盤價"
            low_col = "最低價" if "最低價" in use_window.columns else "收盤價"
            max_high = _safe_float(use_window[high_col].max())
            min_low = _safe_float(use_window[low_col].min())
            max_gain = None if max_high in [None, 0] else round((max_high - base_px) / base_px * 100, 2)
            max_drawdown = None if min_low in [None, 0] else round((min_low - base_px) / base_px * 100, 2)
            result["推薦後最大漲幅%"] = max_gain
            result["推薦後最大回撤%"] = max_drawdown

            tgt = _safe_float(target_price)
            stop = _safe_float(stop_price)
            target_hit = False
            stop_hit = False
            if tgt not in [None, 0] and max_high is not None:
                target_hit = max_high >= tgt
            elif max_gain is not None:
                target_hit = max_gain >= 8
            if stop not in [None, 0] and min_low is not None:
                stop_hit = min_low <= stop
            elif max_drawdown is not None:
                stop_hit = max_drawdown <= -6

            result["是否達標_回測"] = bool(target_hit)
            result["是否停損_回測"] = bool(stop_hit)
            ret20 = result.get("推薦後20日%")
            ret10 = result.get("推薦後10日%")
            ret5 = result.get("推薦後5日%")
            benchmark = ret20 if ret20 is not None else (ret10 if ret10 is not None else ret5)
            if target_hit and not stop_hit:
                hit_result = "達標"
            elif stop_hit and not target_hit:
                hit_result = "停損"
            elif benchmark is not None and benchmark >= 5:
                hit_result = "有效"
            elif benchmark is not None and benchmark <= -5:
                hit_result = "偏弱"
            else:
                hit_result = "觀察中"
            result["命中結果"] = hit_result
            if hit_result == "達標":
                comment = "推薦後已達標，型態有效，可納入權重正向校正"
            elif hit_result == "停損":
                comment = "推薦後觸及停損，需檢討追高、支撐或大盤風險"
            elif hit_result == "有效":
                comment = "推薦後報酬為正，持續觀察是否擴大漲幅"
            elif hit_result == "偏弱":
                comment = "推薦後轉弱，需檢討等待條件與停損設定"
            else:
                comment = "尚未形成明確績效，持續追蹤"
            result["績效評語"] = comment
            result["追蹤更新時間"] = _now_text()
            return result
        except Exception:
            pass
    return {}

def _clip(v: float | None, low: float, high: float, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        v = float(v)
    except Exception:
        return default
    return max(low, min(high, v))


def _fmt_pct(v: float | None) -> str:
    return "-" if v is None else f"{v:.2f}%"


def _build_entry_zone(rec_price: float | None, stop_price: float | None) -> str:
    if rec_price in [None, 0]:
        return "-"
    low = rec_price * 0.97
    high = rec_price * 1.03
    if stop_price not in [None, 0]:
        low = max(low, stop_price * 1.03)
    return f"{low:.2f} ~ {high:.2f}"


def _god_mode_decision(row: dict[str, Any]) -> dict[str, Any]:
    rec_price = _safe_float(row.get("推薦價格"))
    latest = _safe_float(row.get("最新價"))
    stop_price = _safe_float(row.get("停損價"))
    target1 = _safe_float(row.get("賣出目標1"))
    target2 = _safe_float(row.get("賣出目標2"))
    bought = _normalize_bool(row.get("是否已實際買進"))
    status = _safe_str(row.get("目前狀態")) or "觀察"

    rec_total = _clip(_safe_float(row.get("推薦總分"), 0), 0, 100, 0)
    tech = _clip(_safe_float(row.get("技術結構分數"), rec_total), 0, 100, rec_total)
    pre_move = _clip(_safe_float(row.get("起漲前兆分數"), rec_total), 0, 100, rec_total)
    trade = _clip(_safe_float(row.get("交易可行分數"), rec_total), 0, 100, rec_total)
    sector = _clip(_safe_float(row.get("類股熱度分數"), rec_total), 0, 100, rec_total)

    perf3 = _safe_float(row.get("3日績效%"))
    perf5 = _safe_float(row.get("5日績效%"))
    perf10 = _safe_float(row.get("10日績效%"))
    perf20 = _safe_float(row.get("20日績效%"))
    pnl_pct = _safe_float(row.get("損益幅%"))

    setup_score = rec_total * 0.28 + tech * 0.24 + pre_move * 0.20 + trade * 0.14 + sector * 0.14

    perf_score = 0.0
    for p, w in [(perf3, 0.15), (perf5, 0.20), (perf10, 0.25), (perf20, 0.40)]:
        if p is not None:
            perf_score += _clip(50 + p * 4, 0, 100, 50) * w
        else:
            perf_score += 50 * w

    mode_label = _safe_str(row.get("模式績效標籤"))
    mode_bonus = {"強勢模式": 8, "穩健模式": 4, "一般模式": 0, "觀察中": -2, "偏弱模式": -8, "弱": -10, "樣本不足": 0}.get(mode_label, 0)

    price_bonus = 0.0
    reasons = []
    if latest not in [None, 0] and rec_price not in [None, 0]:
        drift = (latest - rec_price) / rec_price * 100
        if -3 <= drift <= 3:
            price_bonus += 8
            reasons.append(f"股價接近推薦價({_fmt_pct(drift)})")
        elif 3 < drift <= 8:
            price_bonus += 2
            reasons.append(f"股價小幅高於推薦價({_fmt_pct(drift)})")
        elif drift > 15:
            price_bonus -= 10
            reasons.append(f"股價偏離推薦價過大({_fmt_pct(drift)})")
        elif drift < -8:
            price_bonus -= 6
            reasons.append(f"股價明顯跌破推薦價({_fmt_pct(drift)})")

    risk_penalty = 0.0
    if latest not in [None, 0] and stop_price not in [None, 0]:
        risk_gap = (latest - stop_price) / latest * 100
        if risk_gap <= 0:
            risk_penalty -= 25
            reasons.append("已跌破停損價")
        elif risk_gap < 2.5:
            risk_penalty -= 14
            reasons.append("距停損過近")
        elif risk_gap < 5:
            risk_penalty -= 6
            reasons.append("停損空間偏小")

    target_bonus = 0.0
    if latest not in [None, 0] and target1 not in [None, 0]:
        if latest >= target1:
            target_bonus -= 4
            reasons.append("已接近/到達目標1")
    if latest not in [None, 0] and target2 not in [None, 0] and latest >= target2:
        target_bonus -= 10
        reasons.append("已到達目標2")

    total_score = _clip(setup_score * 0.55 + perf_score * 0.25 + 50 * 0.20 + mode_bonus + price_bonus + risk_penalty + target_bonus, 0, 100, 0)

    if status in {"已賣出", "取消", "封存"}:
        action = "不追蹤"
    elif latest not in [None, 0] and stop_price not in [None, 0] and latest <= stop_price:
        action = "立即出場"
    elif latest not in [None, 0] and target2 not in [None, 0] and latest >= target2:
        action = "分批停利"
    elif bought or status in {"持有", "已買進"}:
        if total_score >= 78:
            action = "續抱"
        elif total_score >= 63:
            action = "續抱觀察"
        elif total_score >= 50:
            action = "減碼觀察"
        else:
            action = "轉弱出場"
    else:
        if total_score >= 80:
            action = "可進場"
        elif total_score >= 68:
            action = "拉回可布局"
        elif total_score >= 56:
            action = "觀察等待"
        else:
            action = "暫不進場"

    if total_score >= 85:
        confidence = "高"
    elif total_score >= 70:
        confidence = "中高"
    elif total_score >= 58:
        confidence = "中"
    else:
        confidence = "保守"

    if tech >= 75:
        reasons.append("技術結構分數強")
    if pre_move >= 75:
        reasons.append("起漲前兆明顯")
    if trade >= 70:
        reasons.append("交易可行性佳")
    if sector >= 70:
        reasons.append("類股熱度有支撐")
    if perf20 is not None and perf20 > 0:
        reasons.append(f"20日績效為正({_fmt_pct(perf20)})")
    elif perf20 is not None and perf20 < 0:
        reasons.append(f"20日績效轉弱({_fmt_pct(perf20)})")
    if pnl_pct is not None and bought:
        reasons.append(f"目前持倉損益{_fmt_pct(pnl_pct)}")

    # 去重保留前 5 項
    cleaned = []
    for r in reasons:
        if r and r not in cleaned:
            cleaned.append(r)
    reason_text = "；".join(cleaned[:5]) if cleaned else "依分數、價格位置、停損距離與歷史績效綜合判斷"

    return {
        "股神決策分數": round(total_score, 2),
        "股神建議動作": action,
        "股神信心": confidence,
        "股神進場區間": _build_entry_zone(rec_price, stop_price),
        "股神推論": reason_text,
    }


def _recalc_row(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
    src = dict(row)
    rec_price = _safe_float(src.get("推薦價格"))
    buy_price = _safe_float(src.get("實際買進價"))
    sell_price = _safe_float(src.get("實際賣出價"))
    latest_price = _safe_float(src.get("最新價"))
    stop_price = _safe_float(src.get("停損價"))
    target1 = _safe_float(src.get("賣出目標1"))
    target2 = _safe_float(src.get("賣出目標2"))
    status = _safe_str(src.get("目前狀態")) or "觀察"

    effective_cost = buy_price if buy_price not in [None, 0] else rec_price
    mark_price = sell_price if sell_price not in [None, 0] else latest_price

    pnl_amt = None
    pnl_pct = None
    if effective_cost not in [None, 0] and mark_price is not None:
        pnl_amt = mark_price - effective_cost
        pnl_pct = (pnl_amt / effective_cost) * 100

    actual_ret = None
    if buy_price not in [None, 0] and sell_price not in [None, 0]:
        actual_ret = (sell_price - buy_price) / buy_price * 100

    buy_flag = src.get("是否已實際買進")
    buy_flag = _normalize_bool(buy_flag) or buy_price not in [None, 0] or status in {"已買進", "持有"}

    hit_stop = _normalize_bool(src.get("是否達停損"))
    hit_t1 = _normalize_bool(src.get("是否達目標1"))
    hit_t2 = _normalize_bool(src.get("是否達目標2"))
    if latest_price is not None:
        if stop_price is not None and latest_price <= stop_price:
            hit_stop = True
        if target1 is not None and latest_price >= target1:
            hit_t1 = True
        if target2 is not None and latest_price >= target2:
            hit_t2 = True

    rec_date = pd.to_datetime(_safe_str(src.get("推薦日期")), errors="coerce")
    holding_days = _safe_float(src.get("持有天數"))
    if pd.notna(rec_date):
        holding_days = max((date.today() - rec_date.date()).days, 0)

    perf_label = _safe_str(src.get("模式績效標籤"))
    score_for_label = actual_ret if actual_ret is not None else pnl_pct
    if not perf_label and score_for_label is not None:
        if score_for_label >= 12:
            perf_label = "強"
        elif score_for_label >= 3:
            perf_label = "中"
        elif score_for_label > -3:
            perf_label = "觀察中"
        else:
            perf_label = "弱"

    if status == "停損":
        hit_stop = True
    if status == "達標":
        hit_t1 = True

    src["是否已實際買進"] = buy_flag
    src["損益金額"] = pnl_amt
    src["損益幅%"] = pnl_pct
    src["實際報酬%"] = actual_ret
    src["是否達停損"] = hit_stop
    src["是否達目標1"] = hit_t1
    src["是否達目標2"] = hit_t2
    src["持有天數"] = holding_days
    src["模式績效標籤"] = perf_label
    src.update(_god_mode_decision(src))
    src["更新時間"] = _now_text()
    return src


def _refresh_latest_prices(df: pd.DataFrame, only_active: bool = False) -> pd.DataFrame:
    if df is None or df.empty:
        return _ensure_godpick_record_columns(pd.DataFrame())

    rows = []
    active_status = {"觀察", "已買進", "持有", "追蹤"}

    for _, row in df.iterrows():
        payload = dict(row)
        status = _safe_str(payload.get("目前狀態")) or "觀察"

        if only_active and status not in active_status:
            rows.append(_recalc_row(payload))
            continue

        stock_no = _normalize_code(payload.get("股票代號"))
        stock_name = _safe_str(payload.get("股票名稱"))
        market = _safe_str(payload.get("市場別"))

        latest, used_market, price_src = _get_latest_close(stock_no, stock_name, market)
        if latest is not None:
            payload["最新價"] = latest
            payload["市場別"] = used_market or market
            payload["最新更新時間"] = _now_text()
            payload["備註"] = (_safe_str(payload.get("備註")) + f"｜最新價來源:{price_src}").strip("｜")

        payload = _recalc_row(payload)
        rows.append(payload)

    return _ensure_godpick_record_columns(pd.DataFrame(rows))


def _backfill_perf_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return _ensure_godpick_record_columns(pd.DataFrame())
    rows = []
    for _, row in df.iterrows():
        payload = dict(row)
        code = _normalize_code(payload.get("股票代號"))
        name = _safe_str(payload.get("股票名稱"))
        market = _safe_str(payload.get("市場別"))
        rec_date = _safe_str(payload.get("推薦日期"))
        stop_price = _safe_float(payload.get("停損參考")) or _safe_float(payload.get("停損價"))
        target_price = _safe_float(payload.get("賣出目標1")) or _safe_float(payload.get("近端壓力"))

        metrics = _get_forward_metrics(code, name, market, rec_date, stop_price, target_price)
        for k, v in metrics.items():
            if k in payload or k in GODPICK_RECORD_COLUMNS:
                payload[k] = v

        # 舊欄位保留，避免既有分析與圖表失效；新欄位為 v12 標準欄位。
        for d in [3, 5, 10, 20]:
            old_key = f"{d}日績效%"
            new_key = f"推薦後{d}日%"
            if _safe_float(payload.get(old_key)) is None:
                if _safe_float(payload.get(new_key)) is not None:
                    payload[old_key] = payload.get(new_key)
                else:
                    payload[old_key] = _get_forward_return(code, name, market, rec_date, d)
            if _safe_float(payload.get(new_key)) is None and _safe_float(payload.get(old_key)) is not None:
                payload[new_key] = payload.get(old_key)
        payload = _recalc_row(payload)
        rows.append(payload)
    return _ensure_godpick_record_columns(pd.DataFrame(rows))


def _load_records() -> pd.DataFrame:
    gh_df, gh_err = _read_records_from_github()
    fs_df, fs_err = _read_records_from_firestore()

    base_df = pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    if not gh_df.empty:
        base_df = gh_df.copy()
    if not fs_df.empty:
        base_df = _append_records_dedup_by_business_key(base_df, fs_df)

    st.session_state[_k("load_detail")] = [
        f"GitHub: {'OK' if not gh_err else gh_err}",
        f"Firestore: {'OK' if not fs_err else fs_err}",
    ]
    if not base_df.empty:
        base_df = _ensure_godpick_record_columns(pd.DataFrame([_recalc_row(r) for _, r in base_df.iterrows()]))
    return _ensure_godpick_record_columns(base_df)


def _save_state_df(df: pd.DataFrame):
    st.session_state[_k("records_df")] = _ensure_godpick_record_columns(df)
    st.session_state[_k("records_saved_at")] = _now_text()
    _invalidate_analysis_cache()


def _get_state_df() -> pd.DataFrame:
    df = st.session_state.get(_k("records_df"))
    if isinstance(df, pd.DataFrame):
        return _ensure_godpick_record_columns(df)
    return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)


def _format_df(df: pd.DataFrame) -> pd.DataFrame:
    show = df.copy()
    pct_cols = ["實際報酬%", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "3日績效%", "5日績效%", "10日績效%", "20日績效%"]
    num_cols = [
        "推薦總分", "族群資金流分數", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "技術結構分數", "起漲前兆分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明",  "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", "同類股領先幅度",
        "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2", "實際買進價", "實際賣出價", "最新價", "損益金額", "持有天數",
    ]
    for c in pct_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: f"{x:,.2f}%" if pd.notna(x) else "")
    for c in num_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 2) if pd.notna(x) else "")
    for c in ["是否已實際買進", "是否達停損", "是否達目標1", "是否達目標2", "是否達標_回測", "是否停損_回測"]:
        if c in show.columns:
            show[c] = show[c].map(lambda v: "是" if _normalize_bool(v) else "否")
    return show


def _df_signature(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "empty"
    base_cols = [c for c in ["record_id", "更新時間", "最新更新時間", "最新價", "損益幅%", "實際報酬%", "20日績效%", "推薦後20日%", "推薦後最大漲幅%", "命中結果", "追蹤更新時間"] if c in df.columns]
    if not base_cols:
        base_cols = list(df.columns[:8])
    try:
        sig_src = df[base_cols].fillna("").astype(str)
        return hashlib.md5(sig_src.to_csv(index=False).encode("utf-8")).hexdigest()
    except Exception:
        return hashlib.md5(str(df.shape).encode("utf-8")).hexdigest()


def _get_default_col_profile(mode: str) -> list[str]:
    return DEFAULT_ADVANCED_COLS.copy() if mode == "進階" else DEFAULT_STANDARD_COLS.copy()


def _get_profile_key(mode: str) -> str:
    return _k(f"col_profile_{mode}")


def _dedupe_cols(cols: list[str], available_cols: list[str]) -> list[str]:
    final = []
    seen = set()
    for c in cols or []:
        c = _safe_str(c)
        if c in available_cols and c not in seen:
            final.append(c)
            seen.add(c)
    return final


def _get_saved_col_profile(mode: str, available_cols: list[str]) -> list[str]:
    """
    重要修正：
    只要使用者已經按「套用設定並永久記錄」，就完全依照已套用順序顯示。
    不再把 DEFAULT_STANDARD_COLS / DEFAULT_ADVANCED_COLS 插回中間，
    避免「推薦日期」等欄位下次又跑回原始位置。
    """
    profile_key = _get_profile_key(mode)
    has_saved = profile_key in st.session_state and isinstance(st.session_state.get(profile_key), list) and len(st.session_state.get(profile_key)) > 0

    if has_saved:
        saved = _dedupe_cols(st.session_state.get(profile_key), available_cols)
        extra = [c for c in available_cols if c not in saved]
        return saved + extra

    default_cols = [c for c in _get_default_col_profile(mode) if c in available_cols]
    extra = [c for c in available_cols if c not in default_cols]
    return default_cols + extra


def _save_col_profile(mode: str, cols: list[str]):
    # 保存完整順序；後續只補新欄位到最後，不會重套預設順序。
    available = list(dict.fromkeys(cols or []))
    st.session_state[_get_profile_key(mode)] = available.copy()
    st.session_state[_k("last_col_profile_save")] = _now_text()
    ok, msg = _persist_ui_config()
    if ok:
        _set_status(f"欄位順序已保存：{mode}", "success")
    else:
        _set_status(f"欄位順序已更新本機狀態，但 GitHub 保存失敗：{msg}", "warning")


def _reset_col_profile(mode: str, available_cols: list[str]):
    default_cols = [c for c in _get_default_col_profile(mode) if c in available_cols]
    extra = [c for c in available_cols if c not in default_cols]
    _save_col_profile(mode, default_cols + extra)


def _stage_col_profile(mode: str, cols: list[str], available_cols: list[str]):
    st.session_state[_k(f"staged_col_profile_{mode}")] = _dedupe_cols(cols, available_cols) + [
        c for c in available_cols if c not in _dedupe_cols(cols, available_cols)
    ]


def _get_stage_col_profile(mode: str, applied_cols: list[str], available_cols: list[str]) -> list[str]:
    staged = st.session_state.get(_k(f"staged_col_profile_{mode}"))
    if isinstance(staged, list) and staged:
        staged = _dedupe_cols(staged, available_cols)
        return staged + [c for c in available_cols if c not in staged]
    return applied_cols.copy()


def _restore_original_col_profile_to_stage(mode: str, available_cols: list[str]):
    default_cols = [c for c in _get_default_col_profile(mode) if c in available_cols]
    extra = [c for c in available_cols if c not in default_cols]
    st.session_state[_k(f"staged_col_profile_{mode}")] = default_cols + extra


def _move_col(cols: list[str], col_name: str, direction: str) -> list[str]:
    x = cols.copy()
    if col_name not in x:
        return x
    idx = x.index(col_name)
    if direction == "up" and idx > 0:
        x[idx], x[idx - 1] = x[idx - 1], x[idx]
    elif direction == "down" and idx < len(x) - 1:
        x[idx], x[idx + 1] = x[idx + 1], x[idx]
    elif direction == "top" and idx > 0:
        x.insert(0, x.pop(idx))
    elif direction == "bottom" and idx < len(x) - 1:
        x.append(x.pop(idx))
    return x


def _build_filtered_view_df(
    df: pd.DataFrame,
    keyword: str,
    mode_filter: str,
    category_filter: str,
    status_filter: str,
    bought_filter: str,
    sort_by: str,
    sort_asc: bool,
) -> pd.DataFrame:
    view_df = df.copy()

    if keyword:
        mask = (
            view_df["股票代號"].astype(str).str.contains(keyword, case=False, na=False)
            | view_df["股票名稱"].astype(str).str.contains(keyword, case=False, na=False)
            | view_df["推薦理由摘要"].astype(str).str.contains(keyword, case=False, na=False)
        )
        view_df = view_df[mask].copy()

    if mode_filter != "全部":
        view_df = view_df[view_df["推薦模式"].astype(str) == mode_filter].copy()

    if category_filter != "全部":
        view_df = view_df[view_df["類別"].astype(str) == category_filter].copy()

    if status_filter != "全部":
        view_df = view_df[view_df["目前狀態"].astype(str) == status_filter].copy()

    if bought_filter != "全部":
        target_bool = bought_filter == "是"
        view_df = view_df[view_df["是否已實際買進"].fillna(False).map(_normalize_bool) == target_bool].copy()

    if sort_by in view_df.columns:
        view_df = view_df.sort_values(sort_by, ascending=sort_asc, na_position="last")

    return view_df.reset_index(drop=True)


def _get_analysis_cache(df: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], dict[str, Any], float | None, float | None]:
    sig = _df_signature(df)
    cache_key = _k("analysis_cache")
    cache = st.session_state.get(cache_key, {})

    if cache.get("sig") == sig:
        return cache["ana_tables"], cache["summary"], cache["avg_20"], cache["avg_real"]

    ana_tables = _build_analysis_tables(df)
    summary = _build_summary(df)
    avg_20 = pd.to_numeric(df.get("推薦後20日%", df.get("20日績效%")), errors="coerce").dropna().mean() if not df.empty else None
    avg_real = pd.to_numeric(df.loc[df["是否已實際買進"] == True, "實際報酬%"], errors="coerce").dropna().mean() if not df.empty else None

    st.session_state[cache_key] = {
        "sig": sig,
        "ana_tables": ana_tables,
        "summary": summary,
        "avg_20": avg_20,
        "avg_real": avg_real,
    }
    return ana_tables, summary, avg_20, avg_real


def _invalidate_analysis_cache():
    st.session_state.pop(_k("analysis_cache"), None)


def _get_editor_df(view_df: pd.DataFrame, use_cols: list[str], fast_mode: bool, visible_limit: int) -> tuple[pd.DataFrame, int, bool]:
    safe_cols = []
    seen = set()

    # record_id 是刪除 / 編輯 / 同步的必要識別欄。
    # 即使使用者欄位設定把它移除，也要保留在 editor_df 裡，畫面再用 column_config 隱藏。
    if "record_id" in view_df.columns:
        safe_cols.append("record_id")
        seen.add("record_id")

    for c in use_cols or []:
        if c in view_df.columns and c not in seen and c not in ["匯入自選", "刪除"]:
            safe_cols.append(c)
            seen.add(c)

    src = view_df[safe_cols].copy()
    # Streamlit data_editor 不允許重複欄位名稱；這裡再保險清除一次。
    src = src.loc[:, ~src.columns.duplicated()].copy()
    truncated = False
    total_rows = len(src)

    if fast_mode and total_rows > visible_limit:
        src = src.head(visible_limit).copy()
        truncated = True

    if "匯入自選" not in src.columns:
        src.insert(0, "匯入自選", False)
    if "刪除" not in src.columns:
        src.insert(1, "刪除", False)
    return src, total_rows, truncated


def _apply_sticky_editor_checkboxes(editor_key: str, edited_df: pd.DataFrame, id_col: str = "record_id", checkbox_cols: list[str] | None = None) -> pd.DataFrame:
    """v40：修正推薦紀錄 data_editor 勾選欄位跳回未勾選。"""
    if checkbox_cols is None:
        checkbox_cols = ["匯入自選", "刪除"]
    if edited_df is None or edited_df.empty or id_col not in edited_df.columns:
        return edited_df

    out = edited_df.copy()
    base_df = out.reset_index(drop=True)

    def _is_true(v: Any) -> bool:
        if isinstance(v, bool):
            return bool(v)
        return str(v).strip().lower() in {"true", "1", "yes", "y", "是", "勾選", "checked"}

    raw_state = st.session_state.get(editor_key, {})
    edited_rows = raw_state.get("edited_rows", {}) if isinstance(raw_state, dict) else {}
    visible_ids = [_safe_str(x) for x in base_df[id_col].astype(str).tolist() if _safe_str(x)]
    visible_id_set = set(visible_ids)

    for col in checkbox_cols:
        if col not in out.columns:
            continue
        state_key = _k(f"sticky_{editor_key}_{col}_ids")
        selected = {_safe_str(x) for x in st.session_state.get(state_key, []) if _safe_str(x)}

        for _, row in base_df.iterrows():
            rec_id = _safe_str(row.get(id_col))
            if rec_id and _is_true(row.get(col, False)):
                selected.add(rec_id)

        if isinstance(edited_rows, dict):
            for raw_idx, changes in edited_rows.items():
                try:
                    idx = int(raw_idx)
                except Exception:
                    continue
                if idx < 0 or idx >= len(base_df):
                    continue
                if not isinstance(changes, dict) or col not in changes:
                    continue
                rec_id = _safe_str(base_df.iloc[idx].get(id_col))
                if not rec_id:
                    continue
                if _is_true(changes.get(col)):
                    selected.add(rec_id)
                else:
                    selected.discard(rec_id)

        selected = {x for x in selected if x in visible_id_set}
        st.session_state[state_key] = [x for x in visible_ids if x in selected]
        out[col] = out[id_col].astype(str).map(lambda x: _safe_str(x) in selected)

    return out


def _build_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or df.empty:
        return {"count": 0, "buy_count": 0, "sold_count": 0, "avg_ret": 0, "win_rate": 0}
    ret_series = pd.to_numeric(df["實際報酬%"], errors="coerce")
    pnl_series = pd.to_numeric(df["損益幅%"], errors="coerce")
    used_ret = ret_series.fillna(pnl_series)
    valid = used_ret.dropna()
    buy_count = int(df["是否已實際買進"].fillna(False).map(_normalize_bool).sum())
    sold_count = int(df["目前狀態"].isin(["已賣出", "停損", "達標"]).sum())
    win_rate = float((valid > 0).mean() * 100) if not valid.empty else 0.0
    avg_ret = float(valid.mean()) if not valid.empty else 0.0
    return {"count": int(len(df)), "buy_count": buy_count, "sold_count": sold_count, "avg_ret": avg_ret, "win_rate": win_rate}


def _win_rate(series) -> float:
    s = pd.to_numeric(pd.Series(series), errors="coerce").dropna()
    return float((s > 0).mean() * 100) if len(s) else 0.0


def _build_analysis_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    local_df = _ensure_godpick_record_columns(df.copy())
    if local_df.empty:
        return {
            "mode": pd.DataFrame(columns=["推薦模式", "筆數", "平均系統報酬", "系統勝率", "平均3日績效", "平均5日績效", "平均10日績效", "平均20日績效", "3日勝率", "5日勝率", "10日勝率", "20日勝率", "達目標1比率", "停損率", "平均推薦總分"]),
            "category": pd.DataFrame(columns=["類別", "筆數", "平均系統報酬", "平均3日績效", "平均5日績效", "平均10日績效", "平均20日績效", "3日勝率", "5日勝率", "10日勝率", "20日勝率", "系統勝率", "達目標1比率", "停損率"]),
            "grade": pd.DataFrame(columns=["推薦等級", "筆數", "平均系統報酬", "系統勝率", "達目標1比率", "停損率"]),
            "trade_mode": pd.DataFrame(columns=["推薦模式", "筆數", "平均實際報酬", "實際勝率"]),
            "best_mode": pd.DataFrame(),
            "best_category": pd.DataFrame(),
        }

    x = local_df.copy()
    x["系統報酬基準"] = pd.to_numeric(x["損益幅%"], errors="coerce")
    x["實際交易基準"] = pd.to_numeric(x["實際報酬%"], errors="coerce")

    mode_df = x.groupby("推薦模式", dropna=False).agg(
        筆數=("record_id", "count"),
        平均系統報酬=("系統報酬基準", "mean"),
        系統勝率=("系統報酬基準", _win_rate),
        平均3日績效=("3日績效%", "mean"),
        平均5日績效=("5日績效%", "mean"),
        平均10日績效=("10日績效%", "mean"),
        平均20日績效=("20日績效%", "mean"),
        **{
            "3日勝率": ("3日績效%", _win_rate),
            "5日勝率": ("5日績效%", _win_rate),
            "10日勝率": ("10日績效%", _win_rate),
            "20日勝率": ("20日績效%", _win_rate),
        },
        達目標1比率=("是否達目標1", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
        停損率=("是否達停損", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
        平均推薦總分=("推薦總分", "mean"),
    ).reset_index()

    category_df = x.groupby("類別", dropna=False).agg(
        筆數=("record_id", "count"),
        平均系統報酬=("系統報酬基準", "mean"),
        平均3日績效=("3日績效%", "mean"),
        平均5日績效=("5日績效%", "mean"),
        平均10日績效=("10日績效%", "mean"),
        平均20日績效=("20日績效%", "mean"),
        **{
            "3日勝率": ("3日績效%", _win_rate),
            "5日勝率": ("5日績效%", _win_rate),
            "10日勝率": ("10日績效%", _win_rate),
            "20日勝率": ("20日績效%", _win_rate),
        },
        系統勝率=("系統報酬基準", _win_rate),
        達目標1比率=("是否達目標1", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
        停損率=("是否達停損", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
    ).reset_index()

    grade_df = x.groupby("推薦等級", dropna=False).agg(
        筆數=("record_id", "count"),
        平均系統報酬=("系統報酬基準", "mean"),
        系統勝率=("系統報酬基準", _win_rate),
        達目標1比率=("是否達目標1", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
        停損率=("是否達停損", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
    ).reset_index()

    trade_df = x[x["是否已實際買進"].fillna(False).map(_normalize_bool)].copy()
    if trade_df.empty:
        trade_mode_df = pd.DataFrame(columns=["推薦模式", "筆數", "平均實際報酬", "實際勝率"])
    else:
        trade_mode_df = trade_df.groupby("推薦模式", dropna=False).agg(
            筆數=("record_id", "count"),
            平均實際報酬=("實際交易基準", "mean"),
            實際勝率=("實際交易基準", _win_rate),
        ).reset_index()

    best_mode_df = mode_df.copy()
    if not best_mode_df.empty:
        best_mode_df["綜合模式分數"] = (
            best_mode_df["平均20日績效"].fillna(0) * 0.50
            + best_mode_df["20日勝率"].fillna(0) * 0.35
            + best_mode_df["平均推薦總分"].fillna(0) * 0.15
        )
        best_mode_df = best_mode_df.sort_values(["綜合模式分數", "平均20日績效", "20日勝率"], ascending=[False, False, False]).reset_index(drop=True)

    best_category_df = category_df.copy()
    if not best_category_df.empty:
        best_category_df["綜合類別分數"] = (
            best_category_df["平均20日績效"].fillna(0) * 0.55
            + best_category_df["20日勝率"].fillna(0) * 0.35
            + best_category_df["系統勝率"].fillna(0) * 0.10
        )
        best_category_df = best_category_df.sort_values(["綜合類別分數", "平均20日績效", "20日勝率"], ascending=[False, False, False]).reset_index(drop=True)

    return {"mode": mode_df, "category": category_df, "grade": grade_df, "trade_mode": trade_mode_df, "best_mode": best_mode_df, "best_category": best_category_df}


def _build_mode_performance_label(row: pd.Series | dict[str, Any], mode_stats_df: pd.DataFrame) -> str:
    src = dict(row)
    mode = _safe_str(src.get("推薦模式"))
    if mode_stats_df is None or mode_stats_df.empty or not mode:
        return _safe_str(src.get("模式績效標籤"))
    hit = mode_stats_df[mode_stats_df["推薦模式"].astype(str) == mode]
    if hit.empty:
        return _safe_str(src.get("模式績效標籤"))
    r = hit.iloc[0]
    avg_20 = _safe_float(r.get("平均20日績效"))
    win20 = _safe_float(r.get("20日勝率"))
    sample_n = int(_safe_float(r.get("筆數"), 0) or 0)
    if sample_n < 3:
        return "樣本不足"
    if avg_20 is not None and win20 is not None:
        if avg_20 >= 8 and win20 >= 65:
            return "強勢模式"
        if avg_20 >= 3 and win20 >= 55:
            return "穩健模式"
        if avg_20 < 0 and win20 < 45:
            return "偏弱模式"
        return "一般模式"
    return _safe_str(src.get("模式績效標籤"))


def _apply_mode_labels(df: pd.DataFrame) -> pd.DataFrame:
    x = _ensure_godpick_record_columns(df.copy())
    ana = _build_analysis_tables(x)
    x["模式績效標籤"] = x.apply(lambda r: _build_mode_performance_label(r, ana["mode"]), axis=1)
    return _ensure_godpick_record_columns(x)


def _v15_perf_series(df: pd.DataFrame) -> pd.Series:
    """V15：依可用欄位自動選擇回測績效基準，不改原始資料。"""
    for col in ["推薦後20日%", "20日績效%", "推薦後10日%", "10日績效%", "推薦後5日%", "5日績效%", "損益幅%"]:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            if s.notna().sum() > 0:
                return s
    return pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")


def _build_v15_auto_tune_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """V15：用推薦後績效反推哪些模式、買點、型態應提高或降低權重。"""
    x = _ensure_godpick_record_columns(df.copy())
    if x.empty:
        empty = pd.DataFrame(columns=["項目", "樣本數", "平均績效%", "勝率%", "平均最大回撤%", "校正分數", "建議"])
        return {"mode": empty.copy(), "type": empty.copy(), "entry": empty.copy(), "risk": empty.copy(), "sector": empty.copy(), "summary": empty.copy()}

    perf = _v15_perf_series(x)
    x["__v15_perf"] = perf
    x["__v15_win"] = perf > 0
    if "推薦後最大回撤%" in x.columns:
        x["__v15_dd"] = pd.to_numeric(x["推薦後最大回撤%"], errors="coerce")
    else:
        x["__v15_dd"] = pd.Series([float("nan")] * len(x), index=x.index, dtype="float64")

    def one_table(group_col: str, label: str) -> pd.DataFrame:
        if group_col not in x.columns:
            return pd.DataFrame(columns=[label, "樣本數", "平均績效%", "勝率%", "平均最大回撤%", "校正分數", "建議", "校正原因"])
        rows = []
        work = x.copy()
        work[group_col] = work[group_col].fillna("").astype(str).replace("", "未分類")
        for key, g in work.groupby(group_col, dropna=False):
            v = pd.to_numeric(g["__v15_perf"], errors="coerce").dropna()
            n = int(len(v))
            if n <= 0:
                continue
            avg = float(v.mean())
            win = float((v > 0).mean() * 100)
            dd = pd.to_numeric(g["__v15_dd"], errors="coerce").dropna()
            avg_dd = float(dd.mean()) if not dd.empty else float("nan")
            dd_penalty = min(abs(avg_dd), 12) if avg_dd == avg_dd else 3.0
            sample_bonus = min(n / 20 * 8, 8)
            tune_score = max(0.0, min(100.0, 50 + avg * 3.0 + (win - 50) * 0.45 - dd_penalty * 1.2 + sample_bonus))
            if n < 3:
                suggestion = "樣本不足，暫不調權"
                reason = "樣本少於3筆，先累積紀錄，避免過度擬合。"
            elif tune_score >= 68 and avg > 0 and win >= 55:
                suggestion = "建議提高權重"
                reason = "平均績效與勝率同時偏強，可提高此類訊號排序權重。"
            elif tune_score <= 42 or (avg < 0 and win < 50):
                suggestion = "建議降低權重"
                reason = "回測績效偏弱或勝率不足，建議降低排序權重並檢查追高風險。"
            else:
                suggestion = "建議維持觀察"
                reason = "績效尚可但優勢不明顯，先維持現有權重。"
            rows.append({
                label: key,
                "樣本數": n,
                "平均績效%": round(avg, 2),
                "勝率%": round(win, 2),
                "平均最大回撤%": None if avg_dd != avg_dd else round(avg_dd, 2),
                "校正分數": round(tune_score, 2),
                "建議": suggestion,
                "校正原因": reason,
            })
        out = pd.DataFrame(rows)
        if out.empty:
            return pd.DataFrame(columns=[label, "樣本數", "平均績效%", "勝率%", "平均最大回撤%", "校正分數", "建議", "校正原因"])
        return out.sort_values(["校正分數", "樣本數"], ascending=[False, False]).reset_index(drop=True)

    mode_df = one_table("推薦模式", "推薦模式")
    type_df = one_table("推薦型態", "推薦型態")
    entry_df = one_table("進場時機", "進場時機")
    risk_df = one_table("追高風險等級", "追高風險等級")
    sector_df = one_table("類別", "類別")

    summary_rows = []
    for name, table, key_col in [
        ("推薦模式", mode_df, "推薦模式"),
        ("推薦型態", type_df, "推薦型態"),
        ("進場時機", entry_df, "進場時機"),
        ("追高風險", risk_df, "追高風險等級"),
        ("類別", sector_df, "類別"),
    ]:
        if not table.empty:
            top = table.iloc[0]
            weak = table.sort_values(["校正分數", "樣本數"], ascending=[True, False]).iloc[0]
            summary_rows.append({
                "校正面向": name,
                "最強項目": _safe_str(top.get(key_col)),
                "最強校正分數": _safe_float(top.get("校正分數"), 0),
                "最強建議": _safe_str(top.get("建議")),
                "偏弱項目": _safe_str(weak.get(key_col)),
                "偏弱校正分數": _safe_float(weak.get("校正分數"), 0),
                "偏弱建議": _safe_str(weak.get("建議")),
            })
    summary_df = pd.DataFrame(summary_rows)
    return {"mode": mode_df, "type": type_df, "entry": entry_df, "risk": risk_df, "sector": sector_df, "summary": summary_df}


def _render_v15_auto_tune_panel(df: pd.DataFrame):
    """V15：顯示自動權重校正建議；只提供決策參考，不直接改權重，避免誤傷推薦邏輯。"""
    render_pro_section("V15 權重回饋校正建議", "根據推薦後績效、勝率與最大回撤，判斷哪些推薦模式/型態應提高或降低權重；此區不自動改設定，避免過度擬合。")
    tables = _build_v15_auto_tune_tables(df)
    if tables["summary"].empty:
        st.info("目前回測樣本不足。請先在本頁按『更新推薦後績效』，並累積更多推薦紀錄。")
        return
    st.dataframe(tables["summary"], use_container_width=True, hide_index=True)
    st.caption("判讀：校正分數越高，代表該模式/型態在目前紀錄中平均績效、勝率、回撤表現越好。樣本少於3筆不建議調權。")
    sub = st.tabs(["推薦模式", "推薦型態", "進場時機", "追高風險", "類別"])
    with sub[0]:
        st.dataframe(tables["mode"], use_container_width=True, hide_index=True)
    with sub[1]:
        st.dataframe(tables["type"], use_container_width=True, hide_index=True)
    with sub[2]:
        st.dataframe(tables["entry"], use_container_width=True, hide_index=True)
    with sub[3]:
        st.dataframe(tables["risk"], use_container_width=True, hide_index=True)
    with sub[4]:
        st.dataframe(tables["sector"], use_container_width=True, hide_index=True)


def _build_export_bytes(df: pd.DataFrame, tables: dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        _ensure_godpick_record_columns(df).to_excel(writer, sheet_name="推薦紀錄", index=False)
        tables["mode"].to_excel(writer, sheet_name="模式分析", index=False)
        tables["category"].to_excel(writer, sheet_name="類別分析", index=False)
        tables["grade"].to_excel(writer, sheet_name="等級分析", index=False)
        tables["trade_mode"].to_excel(writer, sheet_name="實際交易分析", index=False)
        if not tables["best_mode"].empty:
            tables["best_mode"].to_excel(writer, sheet_name="最強模式", index=False)
        if not tables["best_category"].empty:
            tables["best_category"].to_excel(writer, sheet_name="最強類別", index=False)
        try:
            for ws in writer.book.worksheets:
                ws.freeze_panes = "A2"
                for col_cells in ws.columns:
                    max_len = 0
                    col_letter = col_cells[0].column_letter
                    for cell in col_cells:
                        cell_val = "" if cell.value is None else str(cell.value)
                        max_len = max(max_len, len(cell_val))
                    ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 42)
        except Exception:
            pass
    output.seek(0)
    return output.getvalue()


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    if _k("status_msg") not in st.session_state:
        st.session_state[_k("status_msg")] = ""
        st.session_state[_k("status_type")] = "info"
    if _k("watchlist_target_group") not in st.session_state:
        st.session_state[_k("watchlist_target_group")] = "股神推薦"
    if _k("show_column_manager") not in st.session_state:
        st.session_state[_k("show_column_manager")] = False
    if _k("selected_col_to_move") not in st.session_state:
        st.session_state[_k("selected_col_to_move")] = ""
    if _k("ui_auto_save_flag") not in st.session_state:
        st.session_state[_k("ui_auto_save_flag")] = False

    _load_ui_config_once()

    render_pro_hero(
        title="股神推薦紀錄",
        subtitle="追蹤 7_股神推薦 推薦股票，支援 GitHub + Firestore 雙寫、每日更新、實際交易分析、績效統計、Excel 匯出，並可匯入 4_自選股中心。",
    )
    st.caption(f"目前8頁修正版：{RECORD_FIX_VERSION}")
    st.caption(f"刪除修正版：{DELETE_FIX_VERSION}")
    st.caption(f"7/8/9 起漲欄位版：{PRELAUNCH_789_VERSION}")
    st.caption(f"股神決策V10進場決策版：{GOD_DECISION_V10_LINK_VERSION}")
    st.caption(f"推薦績效追蹤V12回測校正版：{BACKTEST_V12_VERSION}")

    status_msg = _safe_str(st.session_state.get(_k("status_msg"), ""))
    status_type = _safe_str(st.session_state.get(_k("status_type"), "info"))
    if status_msg:
        if status_type == "success":
            st.success(status_msg)
        elif status_type == "warning":
            st.warning(status_msg)
        elif status_type == "error":
            st.error(status_msg)
        else:
            st.info(status_msg)

    top_cols = st.columns([1.1, 1.1, 1.1, 1.1, 1.2, 1.4, 2.0])
    with top_cols[0]:
        if st.button("🔄 重新載入", use_container_width=True):
            df = _load_records()
            _save_state_df(df)
            _load_ui_config_once()
            _set_status("推薦紀錄已重新載入", "success")
            st.rerun()
    with top_cols[1]:
        if st.button("📈 更新最新價", use_container_width=True):
            df = _get_state_df()
            before_sig = _df_signature(df)
            df = _refresh_latest_prices(df, only_active=bool(st.session_state.get(_k("only_active_update"), True)))
            after_sig = _df_signature(df)
            if before_sig != after_sig:
                df = _apply_mode_labels(df)
            _save_state_df(df)
            st.success("已更新最新價，尚未同步")
    with top_cols[2]:
        if st.button("💾 儲存同步", use_container_width=True):
            latest_df = _get_state_df()
            latest_df = _apply_mode_labels(latest_df)
            _save_state_df(latest_df)
            ok = _save_records_dual(latest_df)
            if ok:
                st.rerun()
    with top_cols[3]:
        if st.button("🧹 清除快取", use_container_width=True):
            try:
                _get_latest_close.clear()
                _get_forward_return.clear()
            except Exception:
                pass
            _invalidate_analysis_cache()
            st.success("快取已清除")
    with top_cols[4]:
        if st.button("🧮 更新推薦後績效", use_container_width=True):
            updated = _backfill_perf_columns(_get_state_df())
            updated = _apply_mode_labels(updated)
            _save_state_df(updated)
            st.success("已更新推薦後 1/3/5/10/20 日績效、最大漲幅/回撤、命中結果與模式績效標籤，尚未同步")
    with top_cols[5]:
        st.toggle("只更新未出場", value=True, key=_k("only_active_update"))
    with top_cols[6]:
        st.caption(
            f"GitHub紀錄：{'✅' if _safe_str(_github_config().get('token')) else '❌'} ｜ "
            f"Firestore：{'✅' if _safe_str(_firebase_config().get('project_id')) else '❌'} ｜ "
            f"自選股：{'✅' if _safe_str(_watchlist_github_config().get('token')) else '❌'} ｜ "
            f"UI設定：{'✅' if _safe_str(_ui_config_github_config().get('token')) else '❌'}"
        )

    df = _get_state_df()
    if df.empty:
        df = _load_records()
        _save_state_df(df)

    load_detail = st.session_state.get(_k("load_detail"), [])
    if load_detail:
        with st.expander("讀取來源明細", expanded=False):
            for line in load_detail:
                st.write(f"- {line}")

    sync_detail = st.session_state.get(_k("last_sync_detail"), [])
    if sync_detail:
        with st.expander("同步明細", expanded=False):
            for line in sync_detail:
                st.write(f"- {line}")

    ui_detail = _safe_str(st.session_state.get(_k("ui_config_detail"), ""))
    ui_save_detail = _safe_str(st.session_state.get(_k("ui_save_detail"), ""))
    if ui_detail or ui_save_detail:
        with st.expander("UI 設定明細", expanded=False):
            if ui_detail:
                st.write(f"- 載入：{ui_detail}")
            if ui_save_detail:
                st.write(f"- 保存：{ui_save_detail}")

    live_df = _ensure_godpick_record_columns(_get_state_df().copy())
    ana_tables, summary, avg_20, avg_real = _get_analysis_cache(live_df)

    render_pro_kpi_row([
        {"label": "總筆數", "value": summary["count"], "delta": "推薦紀錄", "delta_class": "pro-kpi-delta-flat"},
        {"label": "持有中", "value": int((live_df["目前狀態"] == "持有").sum()) if not live_df.empty else 0, "delta": "狀態追蹤", "delta_class": "pro-kpi-delta-flat"},
        {"label": "平均系統報酬%", "value": f"{summary['avg_ret']:.2f}%", "delta": f"勝率 {summary['win_rate']:.1f}%", "delta_class": "pro-kpi-delta-flat"},
        {"label": "平均20日績效%", "value": "-" if pd.isna(avg_20) else f"{avg_20:.2f}%", "delta": "-" if pd.isna(avg_real) else f"平均實際 {avg_real:.2f}%", "delta_class": "pro-kpi-delta-flat"},
    ])

    tabs = st.tabs(["📋 總表管理", "🧠 股神決策", "➕ 手動新增", "📊 系統績效分析", "💹 實際交易分析", "📤 Excel 匯出", "⚙️ 同步檢查"])

    with tabs[0]:
        render_pro_section("推薦紀錄總表", "先篩選再編輯，減少 data_editor 負擔。支援欄位順序永久保存、重新整理不還原。")
        if st.session_state.get(_k("last_delete_msg")):
            st.success(st.session_state.pop(_k("last_delete_msg")))

        opt_top = st.columns([1.2, 1.2, 1.2, 2.8])
        with opt_top[0]:
            fast_mode = st.toggle("快速模式", value=bool(st.session_state.get(_k("fast_mode"), True)), key=_k("fast_mode"))
        with opt_top[1]:
            visible_limit = st.number_input("顯示筆數上限", min_value=100, max_value=5000, step=100, key=_k("visible_limit"))
        with opt_top[2]:
            if st.button("🧩 欄位管理", use_container_width=True):
                st.session_state[_k("show_column_manager")] = not st.session_state.get(_k("show_column_manager"), False)
                st.rerun()
        with opt_top[3]:
            st.caption("快速模式開啟時，大表只先渲染前 N 筆；快速模式與顯示筆數也會永久保存到 GitHub。")

        auto_sig = f"{bool(fast_mode)}|{int(visible_limit)}"
        last_auto_sig = _safe_str(st.session_state.get(_k("ui_last_auto_sig")))
        if auto_sig != last_auto_sig:
            st.session_state[_k("ui_last_auto_sig")] = auto_sig
            ok, msg = _persist_ui_config()
            if ok:
                st.session_state[_k("ui_save_detail")] = msg
            else:
                st.session_state[_k("ui_save_detail")] = msg

        filter_cols = st.columns([1.1, 1.1, 1.1, 1.1, 1.1, 1.0, 1.0, 1.0])
        with filter_cols[0]:
            keyword = st.text_input("搜尋代號 / 名稱 / 理由", value="", key=_k("kw"))
        with filter_cols[1]:
            mode_filter = st.selectbox("推薦模式", ["全部"] + sorted([x for x in live_df["推薦模式"].dropna().astype(str).unique().tolist() if x]), index=0, key=_k("mode_filter"))
        with filter_cols[2]:
            category_filter = st.selectbox("類別", ["全部"] + sorted([x for x in live_df["類別"].dropna().astype(str).unique().tolist() if x]), index=0, key=_k("cat_filter"))
        with filter_cols[3]:
            status_filter = st.selectbox("狀態", ["全部"] + STATUS_OPTIONS, index=0, key=_k("status_filter"))
        with filter_cols[4]:
            bought_filter = st.selectbox("是否已買進", ["全部", "是", "否"], index=0, key=_k("buy_filter"))
        with filter_cols[5]:
            sort_by = st.selectbox("排序", ["推薦日期", "推薦總分", "20日績效%", "損益幅%", "實際報酬%", "持有天數"], index=0, key=_k("sort_by"))
        with filter_cols[6]:
            sort_asc = st.toggle("升冪", value=False, key=_k("sort_asc"))
        with filter_cols[7]:
            show_cols_mode = st.selectbox("顯示模式", ["標準", "進階"], index=0, key=_k("show_cols_mode"))

        view_df = _build_filtered_view_df(
            live_df,
            keyword=keyword,
            mode_filter=mode_filter,
            category_filter=category_filter,
            status_filter=status_filter,
            bought_filter=bought_filter,
            sort_by=sort_by,
            sort_asc=sort_asc,
        )

        # 欄位順序修正：以 view_df 實際欄位為可用欄位，避免推薦日期/新欄位被預設清單重新插回。
        available_cols = [c for c in view_df.columns if c not in ["匯入自選", "刪除"]]
        applied_cols = _get_saved_col_profile(show_cols_mode, available_cols)
        use_cols = applied_cols

        if st.session_state.get(_k("show_column_manager"), False):
            with st.expander("欄位順序管理", expanded=True):
                draft_cols = _get_stage_col_profile(show_cols_mode, applied_cols, available_cols)

                mgr_cols = st.columns([2.0, 0.9, 0.9, 0.9, 0.9, 1.2])
                with mgr_cols[0]:
                    selected_col = st.selectbox(
                        "選擇要移動的欄位",
                        options=draft_cols,
                        index=0 if draft_cols else None,
                        key=_k("selected_col_to_move"),
                    )
                with mgr_cols[1]:
                    if st.button("⬆ 上移", use_container_width=True, disabled=not draft_cols):
                        _stage_col_profile(show_cols_mode, _move_col(draft_cols, selected_col, "up"), available_cols)
                        st.rerun()
                with mgr_cols[2]:
                    if st.button("⬇ 下移", use_container_width=True, disabled=not draft_cols):
                        _stage_col_profile(show_cols_mode, _move_col(draft_cols, selected_col, "down"), available_cols)
                        st.rerun()
                with mgr_cols[3]:
                    if st.button("⏫ 置頂", use_container_width=True, disabled=not draft_cols):
                        _stage_col_profile(show_cols_mode, _move_col(draft_cols, selected_col, "top"), available_cols)
                        st.rerun()
                with mgr_cols[4]:
                    if st.button("⏬ 置底", use_container_width=True, disabled=not draft_cols):
                        _stage_col_profile(show_cols_mode, _move_col(draft_cols, selected_col, "bottom"), available_cols)
                        st.rerun()
                with mgr_cols[5]:
                    if st.button("↩ 還原原始設定", use_container_width=True):
                        _restore_original_col_profile_to_stage(show_cols_mode, available_cols)
                        st.rerun()

                st.markdown("**目前欄位順序**")
                st.code(" | ".join(draft_cols), language=None)

                apply_cols = st.columns([1.2, 1.2, 3.0])
                with apply_cols[0]:
                    if st.button("✅ 套用設定並永久記錄", use_container_width=True, type="primary"):
                        _save_col_profile(show_cols_mode, draft_cols)
                        st.session_state[_k(f"staged_col_profile_{show_cols_mode}")] = _get_saved_col_profile(show_cols_mode, available_cols)
                        st.success("欄位順序已套用並永久記錄；下次切換頁面或重新整理不會恢復原始設定。")
                        st.rerun()
                with apply_cols[1]:
                    if st.button("取消暫存", use_container_width=True):
                        st.session_state[_k(f"staged_col_profile_{show_cols_mode}")] = applied_cols.copy()
                        st.rerun()
                with apply_cols[2]:
                    if draft_cols != applied_cols:
                        st.warning("欄位順序已有暫存變更，按『套用設定並永久記錄』後才會正式保存。")
                    else:
                        st.caption("目前欄位順序已套用並永久保存。")

                st.markdown("**快速欄位方案**")
                preset_cols = st.columns(4)
                with preset_cols[0]:
                    if st.button("方案A：交易核心", use_container_width=True):
                        preset = [c for c in [
                            "record_id", "股票代號", "股票名稱", "推薦模式", "推薦等級", "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "近端支撐", "近端壓力", "突破確認價", "停損參考", "追高風險等級", "是否建議追價", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "最新價",
                            "損益幅%", "目前狀態", "是否已實際買進", "實際買進價", "實際賣出價", "實際報酬%", "備註"
                        ] if c in available_cols]
                        _stage_col_profile(show_cols_mode, preset, available_cols)
                        st.rerun()
                with preset_cols[1]:
                    if st.button("方案B：績效核心", use_container_width=True):
                        preset = [c for c in [
                            "record_id", "股票代號", "股票名稱", "類別", "推薦模式", "推薦總分",
                            "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "3日績效%", "5日績效%", "10日績效%", "20日績效%", "損益幅%", "模式績效標籤"
                        ] if c in available_cols]
                        _stage_col_profile(show_cols_mode, preset, available_cols)
                        st.rerun()
                with preset_cols[2]:
                    if st.button("方案C：完整預設", use_container_width=True):
                        _restore_original_col_profile_to_stage(show_cols_mode, available_cols)
                        st.rerun()
                with preset_cols[3]:
                    st.caption(f"最後保存：{_safe_str(st.session_state.get(_k('ui_last_saved_at'), '未保存'))}")

                use_cols = _get_saved_col_profile(show_cols_mode, available_cols)

        editor_df, total_rows, truncated = _get_editor_df(
            view_df=view_df,
            use_cols=use_cols,
            fast_mode=bool(st.session_state.get(_k("fast_mode"), True)),
            visible_limit=int(st.session_state.get(_k("visible_limit"), FAST_VISIBLE_LIMIT)),
        )

        if truncated:
            st.warning(f"快速模式啟用中：目前符合條件 {total_rows} 筆，只先顯示前 {len(editor_df)} 筆以加速操作。要編輯全部可關閉快速模式。")
        else:
            st.caption(f"目前顯示 {len(view_df)} / {len(live_df)} 筆")

        editor_key = _k(f"record_editor_{show_cols_mode}")
        editor_df = _apply_sticky_editor_checkboxes(editor_key, editor_df, "record_id", ["匯入自選", "刪除"])

        edited_df = st.data_editor(
            editor_df,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key=editor_key,
            column_config={
                "匯入自選": st.column_config.CheckboxColumn("匯入自選"),
                "刪除": st.column_config.CheckboxColumn("刪除"),
                "record_id": None,  # 隱藏但保留，供刪除 / 編輯用
                "股票代號": st.column_config.TextColumn("股票代號", disabled=True),
                "股票名稱": st.column_config.TextColumn("股票名稱", disabled=True),
                "推薦模式": st.column_config.TextColumn("推薦模式", disabled=True),
                "推薦等級": st.column_config.TextColumn("推薦等級", disabled=True),
                "推薦總分": st.column_config.NumberColumn("推薦總分", format="%.2f", disabled=True),
                "股神決策模式": st.column_config.TextColumn("股神決策模式", disabled=True),
                "股神進場建議": st.column_config.TextColumn("股神進場建議", disabled=True),
                "推薦分層": st.column_config.TextColumn("推薦分層", disabled=True),
                "建議部位%": st.column_config.NumberColumn("建議部位%", format="%.1f", disabled=True),
                "風險報酬比": st.column_config.NumberColumn("風險報酬比", format="%.2f", disabled=True),
                "追價風險分": st.column_config.NumberColumn("追價風險分", format="%.2f", disabled=True),
                "停損距離%": st.column_config.NumberColumn("停損距離%", format="%.2f", disabled=True),
                "目標報酬%": st.column_config.NumberColumn("目標報酬%", format="%.2f", disabled=True),
                "不建議買進原因": st.column_config.TextColumn("不建議買進原因", disabled=True),
                "最佳操作劇本": st.column_config.TextColumn("最佳操作劇本", disabled=True),
                "隔日操作建議": st.column_config.TextColumn("隔日操作建議", disabled=True),
                "轉弱條件": st.column_config.TextColumn("轉弱條件", disabled=True),

                "股神決策分數": st.column_config.NumberColumn("股神決策分數", format="%.2f", disabled=True),
                "股神建議動作": st.column_config.TextColumn("股神建議動作", disabled=True),
                "股神信心": st.column_config.TextColumn("股神信心", disabled=True),
                "股神進場區間": st.column_config.TextColumn("股神進場區間", disabled=True),
                "技術結構分數": st.column_config.NumberColumn("技術結構分數", format="%.2f", disabled=True),
                "起漲前兆分數": st.column_config.NumberColumn("起漲前兆分數", format="%.2f", disabled=True),
                "飆股起漲分數": st.column_config.NumberColumn("飆股起漲分數", format="%.2f", disabled=True),
                "起漲等級": st.column_config.TextColumn("起漲等級", disabled=True),
                "起漲摘要": st.column_config.TextColumn("起漲摘要", disabled=True),
                "交易可行分數": st.column_config.NumberColumn("交易可行分數", format="%.2f", disabled=True),
                "類股熱度分數": st.column_config.NumberColumn("類股熱度分數", format="%.2f", disabled=True),
                "強勢族群等級": st.column_config.TextColumn("強勢族群等級", disabled=True),
                "族群資金流分數": st.column_config.NumberColumn("族群資金流分數", format="%.2f", disabled=True),
                "族群輪動狀態": st.column_config.TextColumn("族群輪動狀態", disabled=True),
                "同族群強勢比例": st.column_config.NumberColumn("同族群強勢比例", format="%.2f", disabled=True),
                "同族群推薦密度": st.column_config.NumberColumn("同族群推薦密度", format="%.2f", disabled=True),
                "同族群平均量能分": st.column_config.NumberColumn("同族群平均量能分", format="%.2f", disabled=True),
                "族群策略建議": st.column_config.TextColumn("族群策略建議", disabled=True),
                "族群資金流說明": st.column_config.TextColumn("族群資金流說明", width="large", disabled=True),
                "最新價": st.column_config.NumberColumn("最新價", format="%.2f", disabled=True),
                "損益幅%": st.column_config.NumberColumn("損益幅%", format="%.2f", disabled=True),
                "3日績效%": st.column_config.NumberColumn("3日績效%", format="%.2f", disabled=True),
                "5日績效%": st.column_config.NumberColumn("5日績效%", format="%.2f", disabled=True),
                "10日績效%": st.column_config.NumberColumn("10日績效%", format="%.2f", disabled=True),
                "20日績效%": st.column_config.NumberColumn("20日績效%", format="%.2f", disabled=True),
                "目前狀態": st.column_config.SelectboxColumn("目前狀態", options=STATUS_OPTIONS),
                "是否已實際買進": st.column_config.CheckboxColumn("是否已實際買進"),
                "實際買進價": st.column_config.NumberColumn("實際買進價", format="%.2f"),
                "實際賣出價": st.column_config.NumberColumn("實際賣出價", format="%.2f"),
                "實際報酬%": st.column_config.NumberColumn("實際報酬%", format="%.2f", disabled=True),
                "是否達停損": st.column_config.CheckboxColumn("是否達停損"),
                "是否達目標1": st.column_config.CheckboxColumn("是否達目標1"),
                "是否達目標2": st.column_config.CheckboxColumn("是否達目標2"),
                "持有天數": st.column_config.NumberColumn("持有天數", format="%d", disabled=True),
                "推薦日期": st.column_config.TextColumn("推薦日期", disabled=True),
                "推薦時間": st.column_config.TextColumn("推薦時間", disabled=True),
                "模式績效標籤": st.column_config.TextColumn("模式績效標籤", disabled=True),
                "股神推論": st.column_config.TextColumn("股神推論", width="large", disabled=True),
                "推薦理由摘要": st.column_config.TextColumn("推薦理由摘要", width="large", disabled=True),
                "備註": st.column_config.TextColumn("備註", width="large"),
            },
        )

        edited_df = _apply_sticky_editor_checkboxes(editor_key, edited_df, "record_id", ["匯入自選", "刪除"])

        action_cols = st.columns([1.6, 1.2, 1.2, 1.2, 1.2, 2.6])
        with action_cols[0]:
            target_group = st.text_input("匯入自選群組", value=st.session_state.get(_k("watchlist_target_group"), "股神推薦"), key=_k("watchlist_target_group"))
        with action_cols[1]:
            if st.button("📥 匯入勾選到4_自選股", use_container_width=True):
                selected_ids = edited_df.loc[edited_df["匯入自選"] == True, "record_id"].astype(str).tolist()
                ok, msg = _export_records_to_watchlist(live_df, selected_ids, target_group)
                if ok:
                    st.session_state[_k(f"sticky_{editor_key}_匯入自選_ids")] = []
                _set_status(msg, "success" if ok else "warning")
                st.rerun()
            detail_msg = _safe_str(st.session_state.get(_k("watchlist_import_detail"), ""))
            if detail_msg:
                st.caption(detail_msg)
        with action_cols[2]:
            if st.button("✅ 套用編輯", use_container_width=True):
                master = live_df.copy()
                edit_map = {str(r["record_id"]): dict(r) for _, r in edited_df.iterrows()}
                for idx in master.index:
                    rec_id = _safe_str(master.at[idx, "record_id"])
                    if rec_id not in edit_map:
                        continue
                    src = edit_map[rec_id]
                    for c in [c for c in master.columns if c in src]:
                        if c in ["record_id", "股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分", "族群資金流分數", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "技術結構分數", "起漲前兆分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明",  "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", "股神決策分數", "股神建議動作", "股神信心", "股神進場區間", "股神推論", "最新價", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "3日績效%", "5日績效%", "10日績效%", "20日績效%", "推薦日期", "推薦時間", "推薦理由摘要"]:
                            continue
                        master.at[idx, c] = src.get(c)
                    recalc = _recalc_row(master.loc[idx].to_dict())
                    for k2, v2 in recalc.items():
                        if k2 in master.columns:
                            master.at[idx, k2] = v2
                master = _apply_mode_labels(master)
                _save_state_df(master)
                st.success("已套用，尚未同步")
        with action_cols[3]:
            if st.button("🗑️ 刪除勾選", use_container_width=True):
                if "刪除" not in edited_df.columns:
                    st.warning("目前表格缺少刪除欄位，請重新載入後再試。")
                else:
                    checked_df = edited_df[edited_df["刪除"].fillna(False).astype(bool)].copy()

                    delete_ids = []
                    if not checked_df.empty and "record_id" in checked_df.columns:
                        delete_ids = [_safe_str(x) for x in checked_df["record_id"].astype(str).tolist() if _safe_str(x)]

                    if not delete_ids:
                        st.warning("請先勾選要刪除的紀錄。")
                    else:
                        before_n = len(live_df)
                        new_df = _delete_records_by_ids(live_df, delete_ids)
                        after_n = len(new_df)
                        deleted_n = max(before_n - after_n, 0)

                        _save_state_df(new_df)
                        st.session_state[_k(f"sticky_{editor_key}_刪除_ids")] = []
                        st.session_state[_k("last_delete_msg")] = f"已刪除 {deleted_n} 筆，尚未同步；若要永久寫回，請按「儲存同步」。"
                        st.rerun()
        with action_cols[4]:
            if st.button("🧼 清空目前篩選", use_container_width=True):
                source_df = view_df if not truncated else view_df.head(int(st.session_state.get(_k("visible_limit"), FAST_VISIBLE_LIMIT)))
                if source_df.empty:
                    st.warning("目前篩選結果沒有資料可清空。")
                else:
                    new_df = _clear_filtered_records(live_df, source_df)
                    _save_state_df(new_df)
                    st.success(f"已清空 {len(source_df)} 筆，尚未同步")
        with action_cols[5]:
            st.caption("流程：篩選 → 欄位順序調整 → 編輯 / 匯入自選 → 更新價格 / 更新績效 → 儲存同步")

    with tabs[1]:
        render_pro_section("股神模式進出場決策", "將 7_股神推薦 的分數欄位，結合最新價、停損距離、歷史績效與模式標籤，轉成可操作建議。")
        god_df = live_df.copy()
        if god_df.empty:
            st.info("目前沒有推薦紀錄可分析。")
        else:
            topk = st.columns(4)
            decision_counts = god_df["股神建議動作"].fillna("未判定").value_counts()
            with topk[0]:
                render_pro_info_card("可進場 / 布局", [("筆數", int(decision_counts.get("可進場", 0) + decision_counts.get("拉回可布局", 0)), "股神模式")], chips=["進場"])
            with topk[1]:
                render_pro_info_card("續抱 / 續抱觀察", [("筆數", int(decision_counts.get("續抱", 0) + decision_counts.get("續抱觀察", 0)), "股神模式")], chips=["持有"])
            with topk[2]:
                render_pro_info_card("減碼 / 出場", [("筆數", int(decision_counts.get("減碼觀察", 0) + decision_counts.get("轉弱出場", 0) + decision_counts.get("立即出場", 0) + decision_counts.get("分批停利", 0)), "股神模式")], chips=["風險"])
            with topk[3]:
                avg_god = pd.to_numeric(god_df["股神決策分數"], errors="coerce").dropna().mean()
                render_pro_info_card("平均決策分數", [("分數", "-" if pd.isna(avg_god) else f"{avg_god:.2f}", "0~100")], chips=["綜合"])

            decision_filter_cols = st.columns([1.2, 1.2, 1.2])
            with decision_filter_cols[0]:
                act_filter = st.selectbox("股神建議動作", ["全部"] + sorted([x for x in god_df["股神建議動作"].dropna().astype(str).unique().tolist() if x]), key=_k("god_action_filter"))
            with decision_filter_cols[1]:
                conf_filter = st.selectbox("股神信心", ["全部"] + sorted([x for x in god_df["股神信心"].dropna().astype(str).unique().tolist() if x]), key=_k("god_conf_filter"))
            with decision_filter_cols[2]:
                min_score = st.slider("最低決策分數", 0, 100, 60, 1, key=_k("god_min_score"))

            show_god = god_df.copy()
            if act_filter != "全部":
                show_god = show_god[show_god["股神建議動作"].astype(str) == act_filter].copy()
            if conf_filter != "全部":
                show_god = show_god[show_god["股神信心"].astype(str) == conf_filter].copy()
            show_god = show_god[pd.to_numeric(show_god["股神決策分數"], errors="coerce").fillna(0) >= min_score].copy()
            show_god = show_god.sort_values(["股神決策分數", "推薦總分", "20日績效%"], ascending=[False, False, False], na_position="last")

            st.dataframe(
                show_god[[c for c in [
                    "股票代號", "股票名稱", "類別", "推薦模式", "推薦總分", "買點分級", "風險說明", "股神推論邏輯",
                    "股神決策分數", "股神建議動作", "股神信心", "股神進場區間", "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "近端支撐", "近端壓力", "突破確認價", "停損參考", "追高風險等級", "是否建議追價", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "最新價", "停損價", "賣出目標1", "賣出目標2", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "3日績效%", "5日績效%", "10日績效%", "20日績效%", "模式績效標籤", "股神推論"
                ] if c in show_god.columns]],
                use_container_width=True,
                hide_index=True,
            )

    with tabs[2]:
        render_pro_section("手動新增推薦紀錄")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            manual_code = st.text_input("股票代號", value="", key=_k("manual_code"))
        with c2:
            manual_name = st.text_input("股票名稱", value="", key=_k("manual_name"))
        with c3:
            manual_market = st.selectbox("市場別", ["上市", "上櫃", "興櫃"], index=0, key=_k("manual_market"))
        with c4:
            manual_category = st.text_input("類別", value="", key=_k("manual_category"))
        c5, c6, c7, c8 = st.columns(4)
        with c5:
            manual_mode = st.text_input("推薦模式", value="手動新增", key=_k("manual_mode"))
        with c6:
            manual_grade = st.selectbox("推薦等級", ["", "S", "A", "B", "C", "股神級", "強烈關注", "優先觀察", "可列追蹤", "觀察"], index=1, key=_k("manual_grade"))
        with c7:
            manual_total = st.number_input("推薦總分", min_value=0.0, max_value=1000.0, value=85.0, step=0.1, key=_k("manual_total"))
        with c8:
            manual_price = st.number_input("推薦價格", min_value=0.0, value=0.0, step=0.01, key=_k("manual_price"))
        c9, c10, c11, c12 = st.columns(4)
        with c9:
            manual_stop = st.number_input("停損價", min_value=0.0, value=0.0, step=0.01, key=_k("manual_stop"))
        with c10:
            manual_t1 = st.number_input("賣出目標1", min_value=0.0, value=0.0, step=0.01, key=_k("manual_t1"))
        with c11:
            manual_t2 = st.number_input("賣出目標2", min_value=0.0, value=0.0, step=0.01, key=_k("manual_t2"))
        with c12:
            manual_status = st.selectbox("目前狀態", STATUS_OPTIONS, index=0, key=_k("manual_status"))
        manual_reason = st.text_area("推薦理由摘要", value="", height=90, key=_k("manual_reason"))
        manual_tag = st.text_input("推薦標籤", value="", key=_k("manual_tag"))
        if st.button("➕ 新增並同步", use_container_width=True, type="primary"):
            if not _normalize_code(manual_code):
                st.warning("請輸入股票代號")
            else:
                rec_date = _now_date_text()
                rec_time = _now_time_text()
                row = {
                    "record_id": _create_record_id(_normalize_code(manual_code), rec_date, rec_time, manual_mode),
                    "股票代號": _normalize_code(manual_code),
                    "股票名稱": _safe_str(manual_name) or _normalize_code(manual_code),
                    "市場別": manual_market,
                    "類別": manual_category,
                    "推薦模式": manual_mode,
                    "推薦等級": manual_grade,
                    "推薦總分": manual_total,
                    "推薦價格": manual_price if manual_price > 0 else None,
                    "停損價": manual_stop if manual_stop > 0 else None,
                    "賣出目標1": manual_t1 if manual_t1 > 0 else None,
                    "賣出目標2": manual_t2 if manual_t2 > 0 else None,
                    "推薦日期": rec_date,
                    "推薦時間": rec_time,
                    "建立時間": _now_text(),
                    "更新時間": _now_text(),
                    "目前狀態": manual_status,
                    "推薦標籤": manual_tag,
                    "推薦理由摘要": manual_reason,
                }
                new_df = _append_records_dedup_by_business_key(_get_state_df(), pd.DataFrame([row]))
                new_df = _backfill_perf_columns(new_df)
                new_df = _apply_mode_labels(new_df)
                _save_state_df(new_df)
                ok = _save_records_dual(new_df)
                if ok:
                    st.success("已加入並同步成功")
                    st.rerun()

    with tabs[3]:
        render_pro_section("系統推薦績效分析", "以推薦價格對照最新價與推薦後 1/3/5/10/20 日、最大漲幅、最大回撤做回測校正")
        valid_sys = pd.to_numeric(live_df["損益幅%"], errors="coerce").dropna()
        win_rate_sys = float((valid_sys > 0).mean() * 100) if not valid_sys.empty else 0.0
        avg_sys_ret = float(valid_sys.mean()) if not valid_sys.empty else 0.0
        valid_20 = pd.to_numeric(live_df.get("推薦後20日%", live_df.get("20日績效%")), errors="coerce").dropna()
        avg_20_v = float(valid_20.mean()) if not valid_20.empty else 0.0
        win_20 = float((valid_20 > 0).mean() * 100) if not valid_20.empty else 0.0
        target_rate = float(live_df["是否達目標1"].fillna(False).map(_normalize_bool).mean() * 100) if len(live_df) else 0.0
        stop_rate = float(live_df["是否達停損"].fillna(False).map(_normalize_bool).mean() * 100) if len(live_df) else 0.0

        render_pro_kpi_row([
            {"label": "系統樣本數", "value": format_number(len(live_df)), "delta": "", "delta_class": "pro-kpi-delta-flat"},
            {"label": "系統勝率", "value": f"{win_rate_sys:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
            {"label": "平均系統報酬%", "value": f"{avg_sys_ret:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
            {"label": "20日勝率", "value": f"{win_20:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
            {"label": "平均20日績效%", "value": f"{avg_20_v:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
            {"label": "達目標1比率", "value": f"{target_rate:.2f}%", "delta": f"停損率 {stop_rate:.2f}%", "delta_class": "pro-kpi-delta-flat"},
        ])
        best_cols = st.columns(2)
        with best_cols[0]:
            if not ana_tables["best_mode"].empty:
                top_mode = ana_tables["best_mode"].iloc[0]
                st.info(f"最強模式：{_safe_str(top_mode.get('推薦模式'))} ｜ 平均20日績效 {(_safe_float(top_mode.get('平均20日績效'), 0) or 0):.2f}% ｜ 20日勝率 {(_safe_float(top_mode.get('20日勝率'), 0) or 0):.2f}%")
            else:
                st.info("最強模式：暫無資料")
        with best_cols[1]:
            if not ana_tables["best_category"].empty:
                top_cat = ana_tables["best_category"].iloc[0]
                st.info(f"最強類別：{_safe_str(top_cat.get('類別'))} ｜ 平均20日績效 {(_safe_float(top_cat.get('平均20日績效'), 0) or 0):.2f}% ｜ 20日勝率 {(_safe_float(top_cat.get('20日勝率'), 0) or 0):.2f}%")
            else:
                st.info("最強類別：暫無資料")
        sub_tabs = st.tabs(["模式分析", "類別分析", "等級分析", "明細表"])
        with sub_tabs[0]:
            st.dataframe(ana_tables["mode"], use_container_width=True, hide_index=True)
        with sub_tabs[1]:
            st.dataframe(ana_tables["category"], use_container_width=True, hide_index=True)
        with sub_tabs[2]:
            st.dataframe(ana_tables["grade"], use_container_width=True, hide_index=True)
        with sub_tabs[3]:
            detail_cols = [c for c in [
                "股票代號", "股票名稱", "類別", "推薦模式", "推薦等級", "模式績效標籤",
                "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "近端支撐", "近端壓力", "突破確認價", "停損參考", "追高風險等級", "是否建議追價", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "最新價", "損益金額", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
                "是否達停損", "是否達目標1", "是否達目標2", "推薦日期", "持有天數", "推薦理由摘要"
            ] if c in live_df.columns]
            st.dataframe(_format_df(live_df[detail_cols]), use_container_width=True, hide_index=True)

        st.divider()
        _render_v15_auto_tune_panel(live_df)

    with tabs[4]:
        render_pro_section("實際交易分析", "只統計有實際買進資料的紀錄")
        trade_df = live_df[live_df["是否已實際買進"].fillna(False).map(_normalize_bool)].copy()
        if trade_df.empty:
            st.info("目前沒有實際交易資料。")
        else:
            valid_real = pd.to_numeric(trade_df["實際報酬%"], errors="coerce").dropna()
            real_win = float((valid_real > 0).mean() * 100) if not valid_real.empty else 0.0
            real_avg = float(valid_real.mean()) if not valid_real.empty else 0.0
            render_pro_kpi_row([
                {"label": "實際交易筆數", "value": len(trade_df), "delta": "", "delta_class": "pro-kpi-delta-flat"},
                {"label": "實際勝率", "value": f"{real_win:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
                {"label": "平均實際報酬%", "value": f"{real_avg:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
            ])
            st.dataframe(trade_df[[c for c in ["股票代號", "股票名稱", "推薦模式", "推薦價格", "實際買進價", "實際賣出價", "實際報酬%", "目前狀態", "備註"] if c in trade_df.columns]], use_container_width=True, hide_index=True)
            st.dataframe(ana_tables["trade_mode"], use_container_width=True, hide_index=True)

    with tabs[5]:
        render_pro_section("Excel 匯出")
        excel_bytes = _build_export_bytes(live_df, ana_tables)
        st.download_button(
            "📥 下載 Excel（推薦紀錄 / 模式分析 / 類別分析 / 等級分析 / 實際交易分析 / 最強模式 / 最強類別）",
            data=excel_bytes,
            file_name=f"股神推薦紀錄_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    with tabs[6]:
        render_pro_info_card(
            "同步 / 欄位完整性",
            [
                ("主要來源", "godpick_records.json + Firestore", "雙寫"),
                ("自選股匯入", "可將勾選紀錄匯入 watchlist.json", "已整合"),
                ("匯入位置", _watchlist_github_config()["path"], "沿用4_自選股中心"),
                ("UI 設定", _ui_config_github_config()["path"], "永久記錄"),
                ("刪除 / 清空", "支援", "總表管理內"),
                ("批次更新", "支援表格編輯 / 刪除 / 清空 / 更新", "已保留"),
                ("推薦後績效", "1/3/5/10/20 日績效% + 最大漲幅/回撤 + 命中結果", "V12已整合"),
                ("模式績效標籤", "依模式歷史表現自動標記", "已整合"),
                ("V15權重回饋", "依推薦後績效、勝率、回撤提出調權建議", "只建議不自動改"),
                ("最強模式 / 類別", "依20日績效 + 勝率綜合排序", "已整合"),
                ("Excel 匯出", "推薦紀錄 / 分析表 / 最強榜", "已整合"),
            ],
            chips=["完整版", "不可缺功能", "雙寫同步", "匯入自選股", "推薦後績效", "回測校正", "最強模式", "最強類別", "權重回饋V15", "UI永久記錄"],
        )


if __name__ == "__main__":
    main()


