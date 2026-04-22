# 請直接將以下內容覆蓋到 pages/0_大盤走勢.py

# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Any
import base64
import hashlib
import io
import json
import math

import pandas as pd
import requests
import streamlit as st

from utils import (
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
)

try:
    from utils import get_normalized_watchlist
except Exception:
    get_normalized_watchlist = None

try:
    from utils import get_history_data
except Exception:
    get_history_data = None

try:
    from utils import get_realtime_stock_info
except Exception:
    get_realtime_stock_info = None

PAGE_TITLE = "大盤走勢｜股神Pro因子強化版"
PFX = "macro_godpro_factor_"

RECORD_COLUMNS = [
    "record_id",
    "推估日期",
    "建立時間",
    "更新時間",
    "模式名稱",
    "市場情境",
    "推估方向",
    "方向強度",
    "是否適合進場",
    "是否適合續抱",
    "是否適合減碼",
    "是否適合出場",
    "建議動作",
    "股神模式分數",
    "股神信心度",
    "預估漲跌點",
    "預估高點",
    "預估低點",
    "預估區間寬度",
    "風險等級",
    "國際新聞風險分",
    "美股因子分",
    "夜盤因子分",
    "技術面因子分",
    "籌碼面因子分",
    "事件因子分",
    "結構面因子分",
    "風險面因子分",
    "大盤基準點",
    "加權收盤",
    "加權漲跌%",
    "成交量估分",
    "VIX",
    "美元台幣",
    "NASDAQ漲跌%",
    "SOX漲跌%",
    "SP500漲跌%",
    "台積電ADR漲跌%",
    "ES夜盤漲跌%",
    "NQ夜盤漲跌%",
    "外資買賣超估分",
    "期貨選擇權估分",
    "類股輪動估分",
    "外資買賣超(億)",
    "三大法人合計(億)",
    "外資期貨淨單",
    "PCR",
    "融資增減(億)",
    "融券增減張",
    "強勢族群",
    "弱勢族群",
    "重大事件清單",
    "因子來源狀態",
    "加權資料日期",
    "美股資料日期",
    "夜盤資料日期",
    "法人資料日期",
    "期權資料日期",
    "融資券資料日期",
    "新聞資料區間",
    "股神推論邏輯",
    "進場確認條件",
    "出場警訊",
    "主要風險",
    "建議倉位",
    "實際方向",
    "實際漲跌點",
    "實際高點",
    "實際低點",
    "方向是否命中",
    "區間是否命中",
    "點數誤差",
    "建議動作是否合適",
    "進場建議績效分",
    "出場建議績效分",
    "整體檢討分",
    "誤判主因類別",
    "誤判主因",
    "收盤檢討",
    "備註",
]

MODEL_NAMES = [
    "股神平衡版",
    "技術面優先",
    "美夜盤優先",
    "新聞風險優先",
    "籌碼事件優先",
]

DIRECTION_OPTIONS = ["偏多", "偏空", "震盪"]
RISK_OPTIONS = ["低", "中", "高", "極高"]
ERROR_CAUSES = ["", "新聞", "夜盤", "美股", "技術", "籌碼", "事件", "風險控管", "點數高估", "點數低估", "其他"]


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


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_text() -> str:
    return date.today().strftime("%Y-%m-%d")


def _create_record_id(pred_date: str, model_name: str) -> str:
    raw = f"{pred_date}|{model_name}"
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
        "path": _safe_str(st.secrets.get("MACRO_TREND_RECORDS_GITHUB_PATH", "macro_trend_records.json")) or "macro_trend_records.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _get_file_sha() -> tuple[str, str]:
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
        return "", f"讀取 GitHub SHA 失敗：{resp.status_code} / {resp.text[:200]}"
    except Exception as e:
        return "", f"讀取 GitHub SHA 例外：{e}"


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=RECORD_COLUMNS)
    x = df.copy()
    for c in RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = None
    numeric_cols = [
        "股神模式分數", "股神信心度", "預估漲跌點", "預估高點", "預估低點", "預估區間寬度",
        "國際新聞風險分", "美股因子分", "夜盤因子分", "技術面因子分", "籌碼面因子分", "事件因子分",
        "結構面因子分", "風險面因子分", "大盤基準點", "加權收盤", "加權漲跌%", "成交量估分",
        "VIX", "美元台幣", "NASDAQ漲跌%", "SOX漲跌%", "SP500漲跌%", "台積電ADR漲跌%", "ES夜盤漲跌%",
        "NQ夜盤漲跌%", "外資買賣超估分", "期貨選擇權估分", "類股輪動估分",
        "外資買賣超(億)", "三大法人合計(億)", "外資期貨淨單", "PCR", "融資增減(億)", "融券增減張",
        "實際漲跌點", "實際高點", "實際低點", "點數誤差", "進場建議績效分", "出場建議績效分", "整體檢討分",
    ]
    for c in numeric_cols:
        x[c] = pd.to_numeric(x[c], errors="coerce")
    bool_cols = ["方向是否命中", "區間是否命中", "建議動作是否合適"]
    for c in bool_cols:
        x[c] = x[c].fillna(False).map(lambda v: str(v).lower() in {"true", "1", "yes", "y", "是"})
    for c in [
        "推估日期", "建立時間", "更新時間", "模式名稱", "市場情境", "推估方向", "方向強度", "是否適合進場", "是否適合續抱",
        "是否適合減碼", "是否適合出場", "建議動作", "風險等級", "股神推論邏輯", "進場確認條件", "出場警訊", "主要風險",
        "建議倉位", "強勢族群", "弱勢族群", "重大事件清單", "因子來源狀態",
        "實際方向", "誤判主因類別", "誤判主因", "收盤檢討", "備註"
    ]:
        x[c] = x[c].fillna("").astype(str)
    return x[RECORD_COLUMNS].copy()


def _read_records_from_github() -> tuple[pd.DataFrame, str]:
    cfg = _github_config()
    if not cfg["token"]:
        return pd.DataFrame(columns=RECORD_COLUMNS), "未設定 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(cfg["token"]),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return pd.DataFrame(columns=RECORD_COLUMNS), ""
        if resp.status_code != 200:
            return pd.DataFrame(columns=RECORD_COLUMNS), f"GitHub 讀取失敗：{resp.status_code} / {resp.text[:200]}"
        content = resp.json().get("content", "")
        if not content:
            return pd.DataFrame(columns=RECORD_COLUMNS), ""
        payload = json.loads(base64.b64decode(content).decode("utf-8"))
        if isinstance(payload, list):
            return _ensure_columns(pd.DataFrame(payload)), ""
        return pd.DataFrame(columns=RECORD_COLUMNS), ""
    except Exception as e:
        return pd.DataFrame(columns=RECORD_COLUMNS), f"GitHub 讀取例外：{e}"


def _write_records_to_github(df: pd.DataFrame) -> tuple[bool, str]:
    cfg = _github_config()
    if not cfg["token"]:
        return False, "未設定 GITHUB_TOKEN"
    sha, err = _get_file_sha()
    if err:
        return False, err
    body: dict[str, Any] = {
        "message": f"update macro trend records at {_now_text()}",
        "content": base64.b64encode(json.dumps(_ensure_columns(df).to_dict(orient="records"), ensure_ascii=False, indent=2).encode("utf-8")).decode("utf-8"),
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha
    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(cfg["token"]),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已同步 GitHub：{cfg['path']}"
        return False, f"GitHub 寫入失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return False, f"GitHub 寫入例外：{e}"


def _save_state_df(df: pd.DataFrame):
    st.session_state[_k("records_df")] = _ensure_columns(df)
    st.session_state[_k("saved_at")] = _now_text()


def _get_state_df() -> pd.DataFrame:
    df = st.session_state.get(_k("records_df"))
    if isinstance(df, pd.DataFrame):
        return _ensure_columns(df)
    return pd.DataFrame(columns=RECORD_COLUMNS)


@st.cache_data(ttl=900, show_spinner=False)
def _iter_recent_dates(pred_date_text: str, lookback_days: int = 10) -> list[pd.Timestamp]:
    dt = pd.to_datetime(pred_date_text, errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp.today().normalize()
    return [dt - pd.Timedelta(days=i) for i in range(lookback_days + 1)]


@st.cache_data(ttl=900, show_spinner=False)
def _fetch_stooq(symbol: str, pred_date_text: str) -> dict[str, Any]:
    pred_dt = pd.to_datetime(pred_date_text, errors="coerce")
    if pd.isna(pred_dt):
        pred_dt = pd.Timestamp.today().normalize()
    start_dt = pred_dt - pd.Timedelta(days=80)
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    try:
        df = pd.read_csv(url)
        if df.empty:
            return {}
        df.columns = [str(c).strip().lower() for c in df.columns]
        if "date" not in df.columns:
            return {}
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
        df = df[(df["date"] >= start_dt) & (df["date"] <= pred_dt)]
        if df.empty:
            return {}
        valid_close = df.dropna(subset=["close"]).copy()
        if valid_close.empty:
            return {}
        row = valid_close.iloc[-1]
        prev_close = valid_close.iloc[-2]["close"] if len(valid_close) >= 2 else None
        close = _safe_float(row.get("close"))
        pct = ((close - prev_close) / prev_close * 100) if (close is not None and prev_close not in [None, 0]) else None
        ma5 = valid_close["close"].tail(5).mean() if "close" in valid_close.columns else None
        ma20 = valid_close["close"].tail(20).mean() if "close" in valid_close.columns else None
        return {
            "date": pd.to_datetime(row.get("date")).strftime("%Y-%m-%d") if row.get("date") is not None else "",
            "open": _safe_float(row.get("open")),
            "high": _safe_float(row.get("high")),
            "low": _safe_float(row.get("low")),
            "close": close,
            "volume": _safe_float(row.get("volume")),
            "pct": pct,
            "ma5": _safe_float(ma5),
            "ma20": _safe_float(ma20),
            "source": "stooq",
        }
    except Exception:
        return {}


@st.cache_data(ttl=900, show_spinner=False)
def _price_on_or_before(symbol: str, pred_date_text: str, lookback_days: int = 15) -> dict[str, Any]:
    symbol_map = {
        "TSM": "tsm.us",
        "^TWII": "^twii",
        "^IXIC": "^ixic",
        "^SOX": "^sox",
        "^GSPC": "^spx",
        "ES.F": "es.f",
        "NQ.F": "nq.f",
        "^VIX": "^vix",
        "USDTWD": "usdtwd",
    }
    s = symbol_map.get(_safe_str(symbol), _safe_str(symbol)).lower()
    pred_dt = pd.to_datetime(pred_date_text, errors="coerce")
    if pd.isna(pred_dt):
        pred_dt = pd.Timestamp.today().normalize()

    row = _fetch_stooq(s, pred_dt.strftime("%Y-%m-%d"))
    if row:
        row["used_date"] = _safe_str(row.get("date"))
        row["symbol"] = s
        return row

    for i in range(1, lookback_days + 1):
        dt = (pred_dt - pd.Timedelta(days=i)).strftime("%Y-%m-%d")
        row = _fetch_stooq(s, dt)
        if row:
            row["used_date"] = _safe_str(row.get("date"))
            row["symbol"] = s
            return row

    return {
        "symbol": s,
        "used_date": "",
        "date": "",
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "volume": None,
        "pct": None,
        "ma5": None,
        "ma20": None,
        "source": "empty",
    }

# 其餘內容已放在附件文字檔，避免這裡過長。請直接使用旁邊畫布中的完整檔案內容或下方文字檔。
