# -*- coding: utf-8 -*-
"""
utils.py｜股票爬蟲 Streamlit 共用工具恢復版
版本：2026-04-23 串聯修正版重建
目的：
1. 補齊各頁常用 import，避免模組串聯整個壞掉
2. 統一自選股 watchlist.json 讀寫
3. 提供即時資料 / 歷史資料安全抓取函式
4. 提供股神版 UI 元件函式
"""

from __future__ import annotations

from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import time
import math

import pandas as pd
import requests
import streamlit as st


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

WATCHLIST_FILE = ROOT_DIR / "watchlist.json"
RECOMMEND_RECORD_FILE = DATA_DIR / "god_recommend_records.json"
STOCK_MASTER_CACHE = DATA_DIR / "stock_master_cache.json"


# =========================================================
# UI：股神 Pro 風格
# =========================================================

def inject_pro_theme() -> None:
    st.markdown(
        """
        <style>
        .block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
        .god-hero {
            padding: 18px 22px;
            border-radius: 18px;
            background: linear-gradient(135deg, #101828 0%, #1D2939 55%, #344054 100%);
            color: white;
            box-shadow: 0 8px 28px rgba(16,24,40,.18);
            margin-bottom: 16px;
        }
        .god-hero h1 {font-size: 30px; margin: 0 0 6px 0;}
        .god-hero p {font-size: 14px; margin: 0; color: #D0D5DD;}
        .pro-section {
            border: 1px solid #EAECF0;
            border-radius: 16px;
            padding: 14px 16px;
            background: #FFFFFF;
            margin: 12px 0;
            box-shadow: 0 2px 12px rgba(16,24,40,.05);
        }
        .pro-card {
            border: 1px solid #EAECF0;
            border-radius: 15px;
            padding: 14px;
            background: #FFFFFF;
            box-shadow: 0 2px 10px rgba(16,24,40,.05);
        }
        .kpi-box {
            border-radius: 16px;
            padding: 14px 16px;
            background: #F9FAFB;
            border: 1px solid #EAECF0;
        }
        .kpi-title {font-size: 13px; color: #667085;}
        .kpi-value {font-size: 24px; font-weight: 800; color: #101828;}
        .small-muted {font-size: 12px; color: #667085;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_pro_hero(title: str, subtitle: str = "", badge: str = "股神 Pro") -> None:
    st.markdown(
        f"""
        <div class="god-hero">
            <div style="font-size:12px;opacity:.82;margin-bottom:5px;">{badge}</div>
            <h1>{title}</h1>
            <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_pro_section(title: str, subtitle: str = "") -> None:
    st.markdown(
        f"""
        <div class="pro-section">
            <b>{title}</b><br>
            <span class="small-muted">{subtitle}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_pro_info_card(title: str, value: Any, help_text: str = "") -> None:
    st.markdown(
        f"""
        <div class="pro-card">
            <div class="kpi-title">{title}</div>
            <div class="kpi-value">{value}</div>
            <div class="small-muted">{help_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_pro_kpi_row(items: List[Tuple[str, Any, str]]) -> None:
    cols = st.columns(len(items))
    for col, (title, value, desc) in zip(cols, items):
        with col:
            st.markdown(
                f"""
                <div class="kpi-box">
                    <div class="kpi-title">{title}</div>
                    <div class="kpi-value">{value}</div>
                    <div class="small-muted">{desc}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


# =========================================================
# JSON 安全讀寫
# =========================================================

def load_json_file(path: str | Path, default: Any) -> Any:
    p = Path(path)
    try:
        if not p.exists():
            return default
        txt = p.read_text(encoding="utf-8-sig")
        if not txt.strip():
            return default
        return json.loads(txt)
    except Exception:
        return default


def save_json_file(path: str | Path, data: Any) -> bool:
    p = Path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(p)
        return True
    except Exception as e:
        st.error(f"JSON 寫入失敗：{p}｜{e}")
        return False


# =========================================================
# 自選股 Watchlist
# 格式：
# {
#   "預設": [{"code":"2330","name":"台積電"}, ...]
# }
# =========================================================

def _default_watchlist() -> Dict[str, List[Dict[str, str]]]:
    return {
        "預設": [
            {"code": "2330", "name": "台積電"},
            {"code": "2317", "name": "鴻海"},
            {"code": "2454", "name": "聯發科"},
        ]
    }


def load_watchlist() -> Dict[str, List[Dict[str, str]]]:
    data = load_json_file(WATCHLIST_FILE, _default_watchlist())
    if not isinstance(data, dict):
        data = _default_watchlist()
    cleaned: Dict[str, List[Dict[str, str]]] = {}
    for group, rows in data.items():
        if isinstance(rows, list):
            cleaned[str(group)] = []
            for x in rows:
                if isinstance(x, dict):
                    code = str(x.get("code", "")).strip()
                    name = str(x.get("name", "")).strip()
                else:
                    code = str(x).strip()
                    name = ""
                if code:
                    cleaned[str(group)].append({"code": code, "name": name})
    if not cleaned:
        cleaned = _default_watchlist()
    return cleaned


def save_watchlist(data: Dict[str, List[Dict[str, str]]]) -> bool:
    return save_json_file(WATCHLIST_FILE, data)


def get_normalized_watchlist() -> Dict[str, List[str]]:
    wl = load_watchlist()
    return {g: [str(x.get("code", "")).strip() for x in rows if str(x.get("code", "")).strip()] for g, rows in wl.items()}


def add_stock_to_watchlist(group: str, code: str, name: str = "") -> bool:
    group = group or "預設"
    code = str(code).strip()
    if not code:
        return False
    wl = load_watchlist()
    wl.setdefault(group, [])
    exists = any(str(x.get("code")) == code for x in wl[group])
    if not exists:
        wl[group].append({"code": code, "name": name})
    return save_watchlist(wl)


def remove_stock_from_watchlist(group: str, code: str) -> bool:
    wl = load_watchlist()
    if group in wl:
        wl[group] = [x for x in wl[group] if str(x.get("code")) != str(code)]
    return save_watchlist(wl)


# =========================================================
# 股票主檔共用包裝
# =========================================================

def load_stock_master_safe() -> pd.DataFrame:
    try:
        from stock_master_service import load_stock_master
        return load_stock_master()
    except Exception:
        data = load_json_file(STOCK_MASTER_CACHE, [])
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()


def find_stock_name(code: str, master_df: Optional[pd.DataFrame] = None) -> str:
    code = str(code).strip()
    if master_df is None:
        master_df = load_stock_master_safe()
    if master_df is not None and not master_df.empty and "code" in master_df.columns:
        hit = master_df[master_df["code"].astype(str) == code]
        if not hit.empty:
            return str(hit.iloc[0].get("name", ""))
    return ""


def search_stock_master(keyword: str, limit: int = 50) -> pd.DataFrame:
    df = load_stock_master_safe()
    if df.empty or not keyword:
        return df.head(limit)
    kw = str(keyword).strip()
    mask = pd.Series(False, index=df.index)
    for col in ["code", "name", "market", "industry", "category"]:
        if col in df.columns:
            mask = mask | df[col].astype(str).str.contains(kw, case=False, na=False)
    return df[mask].head(limit)


# =========================================================
# 台股資料抓取
# =========================================================

def _twse_date(d: date) -> str:
    return d.strftime("%Y%m%d")


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).replace(",", "").replace("--", "").strip()
        if s in ("", "-", "nan", "None"):
            return None
        return float(s)
    except Exception:
        return None


@st.cache_data(ttl=60 * 30, show_spinner=False)
def get_realtime_stock_info(code: str) -> Dict[str, Any]:
    """
    即時資訊恢復版：
    優先 Yahoo tw.quote API，失敗則回傳空值，不讓頁面壞掉。
    """
    code = str(code).strip()
    result = {
        "code": code,
        "name": "",
        "price": None,
        "change": None,
        "change_pct": None,
        "volume": None,
        "time": "",
        "source": "none",
        "ok": False,
        "error": "",
    }
    try:
        url = f"https://tw.quote.finance.yahoo.net/quote/q?type=tick&sym={code}"
        r = requests.get(url, timeout=8)
        if r.ok:
            js = r.json()
            if isinstance(js, dict):
                # Yahoo 格式可能變動，採寬鬆解析
                mem = js.get("mem", {}) or {}
                quote = js.get("quote", {}) or {}
                price = _safe_float(mem.get("price") or quote.get("price") or quote.get("z"))
                result.update({
                    "name": str(mem.get("name") or quote.get("name") or ""),
                    "price": price,
                    "change": _safe_float(mem.get("change") or quote.get("change") or quote.get("c")),
                    "change_pct": _safe_float(mem.get("changePercent") or quote.get("changePercent")),
                    "volume": _safe_float(mem.get("volume") or quote.get("volume") or quote.get("v")),
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "source": "Yahoo",
                    "ok": price is not None,
                })
                return result
    except Exception as e:
        result["error"] = str(e)
    return result


@st.cache_data(ttl=60 * 60 * 12, show_spinner=False)
def get_history_data(code: str, days: int = 260, end_date: Optional[date] = None) -> pd.DataFrame:
    """
    歷史K線恢復版：
    透過 TWSE / TPEX 月資料往回抓，回傳欄位：
    date, open, high, low, close, volume
    """
    code = str(code).strip()
    if end_date is None:
        end_date = date.today()

    rows: List[Dict[str, Any]] = []
    months = max(3, math.ceil(days / 20) + 2)

    def month_iter(end: date, n: int):
        y, m = end.year, end.month
        for _ in range(n):
            yield date(y, m, 1)
            m -= 1
            if m == 0:
                y -= 1
                m = 12

    for md in month_iter(end_date, months):
        # TWSE
        try:
            url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
            params = {"response": "json", "date": md.strftime("%Y%m%d"), "stockNo": code}
            r = requests.get(url, params=params, timeout=10)
            js = r.json()
            if js.get("stat") == "OK" and js.get("data"):
                for x in js["data"]:
                    roc_y, mm, dd = x[0].split("/")
                    dt = date(int(roc_y) + 1911, int(mm), int(dd))
                    rows.append({
                        "date": pd.Timestamp(dt),
                        "open": _safe_float(x[3]),
                        "high": _safe_float(x[4]),
                        "low": _safe_float(x[5]),
                        "close": _safe_float(x[6]),
                        "volume": _safe_float(x[1]),
                    })
        except Exception:
            pass

        # TPEX
        try:
            url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
            params = {"code": code, "date": md.strftime("%Y/%m/%d"), "response": "json"}
            r = requests.get(url, params=params, timeout=10)
            js = r.json()
            data = js.get("tables", [{}])[0].get("data", []) if isinstance(js, dict) else []
            for x in data:
                roc_y, mm, dd = x[0].split("/")
                dt = date(int(roc_y) + 1911, int(mm), int(dd))
                rows.append({
                    "date": pd.Timestamp(dt),
                    "open": _safe_float(x[3]),
                    "high": _safe_float(x[4]),
                    "low": _safe_float(x[5]),
                    "close": _safe_float(x[6]),
                    "volume": _safe_float(x[1]),
                })
        except Exception:
            pass

        time.sleep(0.03)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.dropna(subset=["date", "close"]).drop_duplicates("date").sort_values("date")
    return df.tail(days).reset_index(drop=True)


# =========================================================
# 技術指標 / 股神分數
# =========================================================

def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    d = df.copy()
    for n in [5, 10, 20, 60, 120, 240]:
        d[f"ma{n}"] = d["close"].rolling(n).mean()
    d["vol_ma20"] = d["volume"].rolling(20).mean()

    low9 = d["low"].rolling(9).min()
    high9 = d["high"].rolling(9).max()
    rsv = (d["close"] - low9) / (high9 - low9) * 100
    d["k"] = rsv.ewm(com=2).mean()
    d["d"] = d["k"].ewm(com=2).mean()

    ema12 = d["close"].ewm(span=12, adjust=False).mean()
    ema26 = d["close"].ewm(span=26, adjust=False).mean()
    d["dif"] = ema12 - ema26
    d["macd"] = d["dif"].ewm(span=9, adjust=False).mean()
    d["hist"] = d["dif"] - d["macd"]
    return d


def score_stock_god_mode(df: pd.DataFrame, weights: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    weights = weights or {
        "trend": 25,
        "momentum": 25,
        "volume": 20,
        "kd_macd": 20,
        "risk": 10,
    }
    if df is None or df.empty or len(df) < 30:
        return {"score": 0, "level": "資料不足", "reason": "歷史資料不足", "detail": {}}

    d = add_technical_indicators(df)
    last = d.iloc[-1]
    prev = d.iloc[-2]

    trend = 0
    if last["close"] > last.get("ma20", 10**9): trend += 35
    if last["ma20"] > last.get("ma60", 10**9): trend += 35
    if last["close"] > last.get("ma60", 10**9): trend += 30

    pct5 = (last["close"] / d.iloc[-6]["close"] - 1) * 100 if len(d) >= 6 else 0
    momentum = max(0, min(100, 50 + pct5 * 8))

    vr = last["volume"] / last["vol_ma20"] if last.get("vol_ma20") else 0
    volume = max(0, min(100, vr * 45))

    kd_macd = 0
    if last["k"] > last["d"]: kd_macd += 35
    if last["dif"] > last["macd"]: kd_macd += 35
    if last["hist"] > prev["hist"]: kd_macd += 30

    drawdown20 = (last["close"] / d["close"].tail(20).max() - 1) * 100
    risk = 100 if drawdown20 > -8 else 70 if drawdown20 > -15 else 40

    raw = {
        "trend": trend,
        "momentum": momentum,
        "volume": volume,
        "kd_macd": kd_macd,
        "risk": risk,
    }
    score = sum(raw[k] * weights.get(k, 0) / 100 for k in raw)

    if score >= 85:
        level = "A+ 強勢起漲觀察"
    elif score >= 75:
        level = "A 可列入買點觀察"
    elif score >= 65:
        level = "B 轉強觀察"
    elif score >= 55:
        level = "C 弱轉強初篩"
    else:
        level = "D 暫不建議"

    reason = f"趨勢{trend:.0f} / 動能{momentum:.0f} / 量能{volume:.0f} / KD-MACD{kd_macd:.0f} / 風險{risk:.0f}"
    return {"score": round(float(score), 2), "level": level, "reason": reason, "detail": raw}


# =========================================================
# 推薦紀錄
# =========================================================

def load_recommend_records() -> List[Dict[str, Any]]:
    data = load_json_file(RECOMMEND_RECORD_FILE, [])
    return data if isinstance(data, list) else []


def save_recommend_records(records: List[Dict[str, Any]]) -> bool:
    return save_json_file(RECOMMEND_RECORD_FILE, records)


def append_recommend_records(records: List[Dict[str, Any]]) -> bool:
    old = load_recommend_records()
    old.extend(records)
    return save_recommend_records(old)
