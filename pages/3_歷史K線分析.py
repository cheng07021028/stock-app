# pages/3_歷史K線分析.py
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from utils import (
    compute_radar_scores,
    compute_signal_snapshot,
    compute_support_resistance_snapshot,
    get_all_code_name_map,
    get_history_data,
    get_normalized_watchlist,
    get_stock_name_and_market,
    inject_pro_theme,
    load_last_query_state,
    parse_date_safe,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
    save_last_query_state,
    score_to_badge,
)

PAGE_TITLE = "歷史K線分析"
STATE_PREFIX = "hist_"


# =========================================================
# session state
# =========================================================
def _k(key: str) -> str:
    return f"{STATE_PREFIX}{key}"


def _ss(key: str, default: Any = None):
    return st.session_state.get(_k(key), default)


def _set_ss(key: str, value: Any):
    st.session_state[_k(key)] = value


# =========================================================
# 基礎工具
# =========================================================
def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _is_stock_code(text: str) -> bool:
    text = _safe_str(text)
    return text.isdigit() and 4 <= len(text) <= 6


def _split_code_name(text: str) -> tuple[str, str]:
    raw = _safe_str(text)
    if not raw:
        return "", ""

    if "(" in raw and ")" in raw:
        left = raw.split("(")[0].strip()
        mid = raw.split("(")[1].split(")")[0].strip()
        if _is_stock_code(mid):
            return mid, left

    for sep in [" ", "-", "_", "/"]:
        if sep in raw:
            parts = [p.strip() for p in raw.split(sep) if p.strip()]
            if parts and _is_stock_code(parts[0]):
                return parts[0], " ".join(parts[1:]).strip()

    if _is_stock_code(raw):
        return raw, ""

    return "", raw


def _display_stock(code: str, name: str) -> str:
    code = _safe_str(code)
    name = _safe_str(name)
    if code and name:
        return f"{code} {name}"
    return code or name or ""


def _first_existing(d: dict, keys: list[str], default: Any = None):
    for k in keys:
        if k in d:
            val = d[k]
            try:
                if pd.notna(val):
                    return val
            except Exception:
                if val is not None:
                    return val
    return default


def _to_pydate(v: Any, fallback: date) -> date:
    if v is None:
        return fallback

    if isinstance(v, date) and not isinstance(v, datetime):
        return v

    if isinstance(v, datetime):
        return v.date()

    if isinstance(v, pd.Timestamp):
        if pd.isna(v):
            return fallback
        return v.date()

    if isinstance(v, np.datetime64):
        try:
            return pd.Timestamp(v).date()
        except Exception:
            return fallback

    if isinstance(v, (list, tuple)):
        if len(v) >= 1:
            return _to_pydate(v[0], fallback)
        return fallback

    try:
        x = parse_date_safe(v)
        if isinstance(x, date):
            return x
        if isinstance(x, datetime):
            return x.date()
        if isinstance(x, pd.Timestamp) and not pd.isna(x):
            return x.date()
    except Exception:
        pass

    try:
        x = pd.to_datetime(v, errors="coerce")
        if pd.notna(x):
            return x.date()
    except Exception:
        pass

    return fallback


# =========================================================
# utils 安全包裝
# =========================================================
def _safe_load_last_query_state(page_key: str) -> dict[str, Any]:
    try:
        raw = load_last_query_state(page_key)
    except Exception:
        return {}

    if raw is None:
        return {}

    if isinstance(raw, dict):
        return raw

    if isinstance(raw, pd.Series):
        try:
            return raw.to_dict()
        except Exception:
            return {}

    if isinstance(raw, pd.DataFrame):
        try:
            if raw.empty:
                return {}
            return raw.iloc[0].to_dict()
        except Exception:
            return {}

    if isinstance(raw, (list, tuple)):
        if len(raw) == 1 and isinstance(raw[0], dict):
            return raw[0]

    return {}


def _safe_save_last_query_state(page_key: str, payload: dict[str, Any]) -> None:
    try:
        if isinstance(payload, dict):
            save_last_query_state(page_key, payload)
    except Exception:
        pass


def _safe_score_to_badge(score: Any) -> str:
    try:
        return str(score_to_badge(score))
    except Exception:
        try:
            s = float(score)
            if s >= 80:
                return "A"
            if s >= 60:
                return "B"
            if s >= 40:
                return "C"
            if s >= 20:
                return "D"
            return "E"
        except Exception:
            return "-"


def _safe_get_code_name_map() -> dict[str, str]:
    try:
        raw = get_all_code_name_map()
    except Exception:
        return {}

    if raw is None:
        return {}

    if isinstance(raw, dict):
        return {_safe_str(k): _safe_str(v) for k, v in raw.items() if _safe_str(k)}

    if isinstance(raw, pd.Series):
        try:
            raw = raw.to_dict()
            return {_safe_str(k): _safe_str(v) for k, v in raw.items() if _safe_str(k)}
        except Exception:
            return {}

    if isinstance(raw, pd.DataFrame):
        if raw.empty:
            return {}

        df = raw.copy()
        cols = {str(c).lower(): c for c in df.columns}

        code_col = None
        name_col = None

        for c in ["code", "stock_code", "symbol", "ticker", "id"]:
            if c in cols:
                code_col = cols[c]
                break

        for c in ["name", "stock_name", "company", "label", "title"]:
            if c in cols:
                name_col = cols[c]
                break

        if code_col is None and len(df.columns) >= 1:
            code_col = df.columns[0]
        if name_col is None and len(df.columns) >= 2:
            name_col = df.columns[1]

        out = {}
        for _, row in df.iterrows():
            code = _safe_str(row.get(code_col)) if code_col is not None else ""
            name = _safe_str(row.get(name_col)) if name_col is not None else ""
            if not code:
                code2, name2 = _split_code_name(_safe_str(row.iloc[0]) if len(row) > 0 else "")
                code = code2
                name = name or name2
            if code:
                out[code] = name
        return out

    if isinstance(raw, (list, tuple)):
        out = {}
        for item in raw:
            if isinstance(item, dict):
                code = _safe_str(_first_existing(item, ["code", "stock_code", "symbol", "ticker", "id"], ""))
                name = _safe_str(_first_existing(item, ["name", "stock_name", "company", "label", "title"], ""))
                if not code:
                    code, parsed_name = _split_code_name(_safe_str(item))
                    name = name or parsed_name
                if code:
                    out[code] = name
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                code = _safe_str(item[0])
                name = _safe_str(item[1])
                if code:
                    out[code] = name
            else:
                code, name = _split_code_name(_safe_str(item))
                if code:
                    out[code] = name
        return out

    return {}


def _safe_get_watchlist() -> Any:
    try:
        return get_normalized_watchlist()
    except Exception:
        return None


# =========================================================
# 歷史資料整理
# =========================================================
def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()

    df = df.copy()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date")
    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).set_index("date")
    else:
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[~df.index.isna()]

    return df.sort_index()


def _find_col(df: pd.DataFrame, names: list[str]) -> str | None:
    lower_map = {str(c).lower(): c for c in df.columns}
    for name in names:
        if name.lower() in lower_map:
            return lower_map[name.lower()]
    return None


def _normalize_history_df(df: pd.DataFrame) -> pd.DataFrame:
    df = _ensure_datetime_index(df)
    if df.empty:
        return df

    out = pd.DataFrame(index=df.index)

    col_open = _find_col(df, ["Open", "open", "開盤", "開盤價"])
    col_high = _find_col(df, ["High", "high", "最高", "最高價"])
    col_low = _find_col(df, ["Low", "low", "最低", "最低價"])
    col_close = _find_col(df, ["Close", "close", "收盤", "收盤價", "Adj Close", "adj close"])
    col_volume = _find_col(df, ["Volume", "volume", "成交量"])

    if col_open is not None:
        out["Open"] = pd.to_numeric(df[col_open], errors="coerce")
    if col_high is not None:
        out["High"] = pd.to_numeric(df[col_high], errors="coerce")
    if col_low is not None:
        out["Low"] = pd.to_numeric(df[col_low], errors="coerce")
    if col_close is not None:
        out["Close"] = pd.to_numeric(df[col_close], errors="coerce")
    if col_volume is not None:
        out["Volume"] = pd.to_numeric(df[col_volume], errors="coerce").fillna(0)

    out = out.dropna(subset=["Close"])

    if "Open" not in out.columns:
        out["Open"] = out["Close"]
    if "High" not in out.columns:
        out["High"] = out[["Open", "Close"]].max(axis=1)
    if "Low" not in out.columns:
        out["Low"] = out[["Open", "Close"]].min(axis=1)
    if "Volume" not in out.columns:
        out["Volume"] = 0

    return out


# =========================================================
# 群組 / 股票
# =========================================================
def _normalize_stock_item(item: Any, code_name_map: dict[str, str]) -> tuple[str, str] | None:
    if item is None:
        return None

    if isinstance(item, pd.Series):
        item = item.to_dict()

    if isinstance(item, dict):
        code = _safe_str(_first_existing(item, ["code", "stock_code", "symbol", "ticker", "id"], ""))
        name = _safe_str(_first_existing(item, ["name", "stock_name", "company", "label", "title"], ""))
        display = _safe_str(_first_existing(item, ["display", "text"], ""))

        if not code and display:
            code, parsed_name = _split_code_name(display)
            name = name or parsed_name

        if not code:
            return None

        if not name:
            name = _safe_str(code_name_map.get(code))
        return code, name

    if isinstance(item, (list, tuple)):
        if len(item) >= 2:
            code = _safe_str(item[0])
            name = _safe_str(item[1])
            if code:
                return code, name or _safe_str(code_name_map.get(code))
        elif len(item) == 1:
            return _normalize_stock_item(item[0], code_name_map)
        return None

    raw = _safe_str(item)
    if not raw:
        return None

    code, name = _split_code_name(raw)
    if code:
        if not name:
            name = _safe_str(code_name_map.get(code))
        return code, name

    reverse_map = {v: k for k, v in code_name_map.items() if _safe_str(v)}
    if raw in reverse_map:
        return reverse_map[raw], raw

    return None


def _build_group_stock_map() -> dict[str, list[str]]:
    code_name_map = _safe_get_code_name_map()
    watchlist = _safe_get_watchlist()
    group_map: dict[str, list[str]] = {}

    def add_item(group_name: str, item: Any):
        parsed = _normalize_stock_item(item, code_name_map)
        if not parsed:
            return
        code, name = parsed
        label = _display_stock(code, name)
        if not label:
            return
        group_map.setdefault(group_name, [])
        if label not in group_map[group_name]:
            group_map[group_name].append(label)

    if isinstance(watchlist, dict):
        for group, items in watchlist.items():
            group_name = _safe_str(group) or "未分組"

            if isinstance(items, pd.DataFrame):
                for _, row in items.iterrows():
                    add_item(group_name, row.to_dict())

            elif isinstance(items, dict):
                for bucket in ["stocks", "items"]:
                    if bucket in items:
                        stocks = items[bucket]
                        if isinstance(stocks, pd.DataFrame):
                            for _, row in stocks.iterrows():
                                add_item(group_name, row.to_dict())
                        elif isinstance(stocks, (list, tuple)):
                            for item in stocks:
                                add_item(group_name, item)
                        else:
                            add_item(group_name, stocks)
                        break
                else:
                    for k, v in items.items():
                        if isinstance(v, (str, int, float)):
                            code = _safe_str(k)
                            name = _safe_str(v)
                            if _is_stock_code(code):
                                add_item(group_name, {"code": code, "name": name})
                            else:
                                add_item(group_name, v)
                        else:
                            add_item(group_name, v)

            elif isinstance(items, (list, tuple)):
                for item in items:
                    add_item(group_name, item)

            else:
                add_item(group_name, items)

    elif isinstance(watchlist, pd.DataFrame):
        for _, row in watchlist.iterrows():
            row_dict = row.to_dict()
            group_name = _safe_str(_first_existing(row_dict, ["group", "category", "sector", "group_name"], "未分組"))
            add_item(group_name, row_dict)

    elif isinstance(watchlist, (list, tuple)):
        for row in watchlist:
            group_name = "未分組"
            if isinstance(row, dict):
                group_name = _safe_str(_first_existing(row, ["group", "category", "sector", "group_name"], "未分組"))
            add_item(group_name, row)

    if not group_map:
        fallback = _safe_get_code_name_map()
        codes = list(fallback.keys())[:100]
        if codes:
            group_map["全部股票"] = [_display_stock(code, fallback.get(code, "")) for code in codes]

    for g in list(group_map.keys()):
        group_map[g] = sorted(set(group_map[g]), key=lambda x: (_split_code_name(x)[0], x))

    return group_map


def _build_stock_lookup(group_map: dict[str, list[str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for group, stocks in group_map.items():
        for stock in stocks:
            code, name = _split_code_name(stock)
            rows.append(
                {
                    "group": group,
                    "stock": stock,
                    "code": code,
                    "name": name,
                    "search_blob": f"{group} {stock} {code} {name}".lower(),
                }
            )
    return rows


def _find_search_target(keyword: str, stock_rows: list[dict[str, str]]) -> dict[str, str] | None:
    q = _safe_str(keyword).lower()
    if not q:
        return None

    for row in stock_rows:
        if q == row["code"].lower():
            return row
    for row in stock_rows:
        if q == row["stock"].lower():
            return row
    for row in stock_rows:
        if q == row["name"].lower():
            return row

    hits = [row for row in stock_rows if row["code"].lower().startswith(q) or row["name"].lower().startswith(q)]
    if hits:
        return hits[0]

    hits = [row for row in stock_rows if q in row["search_blob"]]
    if hits:
        return hits[0]

    return None


# =========================================================
# 指標 / 事件
# =========================================================
def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    close = df["Close"]
    high = df["High"]
    low = df["Low"]

    for n in [5, 10, 20, 60, 120, 240]:
        df[f"MA{n}"] = close.rolling(n).mean()

    low_n = low.rolling(9).min()
    high_n = high.rolling(9).max()
    rsv = (close - low_n) / (high_n - low_n).replace(0, np.nan) * 100
    df["K"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    df["D"] = df["K"].ewm(alpha=1 / 3, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["MACD"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["OSC"] = df["DIF"] - df["MACD"]

    df["PctChg"] = close.pct_change() * 100
    return df


def _cross_up(a_prev, a_now, b_prev, b_now) -> bool:
    return pd.notna(a_prev) and pd.notna(a_now) and pd.notna(b_prev) and pd.notna(b_now) and a_prev <= b_prev and a_now > b_now


def _cross_down(a_prev, a_now, b_prev, b_now) -> bool:
    return pd.notna(a_prev) and pd.notna(a_now) and pd.notna(b_prev) and pd.notna(b_now) and a_prev >= b_prev and a_now < b_now


def _detect_pivots(df: pd.DataFrame, window: int = 3) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
    if df.empty or len(df) < window * 2 + 1:
        return [], []

    highs = df["High"].values
    lows = df["Low"].values
    idx = df.index.to_list()

    peak_dates = []
    trough_dates = []

    for i in range(window, len(df) - window):
        h = highs[i]
        l = lows[i]
        if np.isfinite(h) and h == np.max(highs[i - window:i + window + 1]):
            peak_dates.append(idx[i])
        if np.isfinite(l) and l == np.min(lows[i - window:i + window + 1]):
            trough_dates.append(idx[i])

    return peak_dates, trough_dates


def _build_event_log(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 3:
        return pd.DataFrame(columns=["日期", "事件類型", "事件名稱", "說明", "等級"])

    events: list[dict[str, Any]] = []
    peaks, troughs = _detect_pivots(df, window=3)
    peak_set = set(peaks)
    trough_set = set(troughs)

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        dt = df.index[i]

        if "MA20" in df.columns and "MA60" in df.columns:
            if _cross_up(prev["MA20"], cur["MA20"], prev["MA60"], cur["MA60"]):
                events.append({"日期": dt, "事件類型": "MA交叉", "事件名稱": "MA20 黃金交叉 MA60", "說明": "中期均線向上突破，偏多。", "等級": "強多"})
            elif _cross_down(prev["MA20"], cur["MA20"], prev["MA60"], cur["MA60"]):
                events.append({"日期": dt, "事件類型": "MA交叉", "事件名稱": "MA20 死亡交叉 MA60", "說明": "中期均線向下跌破，偏空。", "等級": "強空"})

        if _cross_up(prev["K"], cur["K"], prev["D"], cur["D"]):
            events.append({"日期": dt, "事件類型": "KD交叉", "事件名稱": "KD 黃金交叉", "說明": "短線動能轉強。", "等級": "偏多"})
        elif _cross_down(prev["K"], cur["K"], prev["D"], cur["D"]):
            events.append({"日期": dt, "事件類型": "KD交叉", "事件名稱": "KD 死亡交叉", "說明": "短線動能轉弱。", "等級": "偏空"})

        if _cross_up(prev["DIF"], cur["DIF"], prev["MACD"], cur["MACD"]):
            events.append({"日期": dt, "事件類型": "MACD交叉", "事件名稱": "MACD 黃金交叉", "說明": "波段趨勢有轉強跡象。", "等級": "偏多"})
        elif _cross_down(prev["DIF"], cur["DIF"], prev["MACD"], cur["MACD"]):
            events.append({"日期": dt, "事件類型": "MACD交叉", "事件名稱": "MACD 死亡交叉", "說明": "波段趨勢有轉弱跡象。", "等級": "偏空"})

        if dt in trough_set:
            events.append({"日期": dt, "事件類型": "轉折點", "事件名稱": "起漲點", "說明": "局部低點轉折，後續留意量價配合。", "等級": "觀察"})
        if dt in peak_set:
            events.append({"日期": dt, "事件類型": "轉折點", "事件名稱": "起跌點", "說明": "局部高點轉折，後續留意回檔風險。", "等級": "觀察"})

    ev = pd.DataFrame(events)
    if ev.empty:
        return pd.DataFrame(columns=["日期", "事件類型", "事件名稱", "說明", "等級"])
    return ev.sort_values("日期", ascending=False).reset_index(drop=True)


def _build_signal_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {}

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last

    price = float(last["Close"])
    ma20 = float(last["MA20"]) if pd.notna(last["MA20"]) else np.nan
    ma60 = float(last["MA60"]) if pd.notna(last["MA60"]) else np.nan

    score = 0
    if pd.notna(ma20) and price > ma20:
        score += 20
    if pd.notna(ma60) and price > ma60:
        score += 20
    if pd.notna(ma20) and pd.notna(ma60) and ma20 > ma60:
        score += 20
    if pd.notna(last["K"]) and pd.notna(last["D"]) and last["K"] > last["D"]:
        score += 20
    if pd.notna(last["DIF"]) and pd.notna(last["MACD"]) and last["DIF"] > last["MACD"]:
        score += 20

    level = "中性"
    if score >= 80:
        level = "強多"
    elif score >= 60:
        level = "偏多"
    elif score <= 20:
        level = "強空"
    elif score <= 40:
        level = "偏空"

    tags = []
    if _cross_up(prev["K"], last["K"], prev["D"], last["D"]):
        tags.append("KD黃金交叉")
    if _cross_down(prev["K"], last["K"], prev["D"], last["D"]):
        tags.append("KD死亡交叉")
    if _cross_up(prev["DIF"], last["DIF"], prev["MACD"], last["MACD"]):
        tags.append("MACD黃金交叉")
    if _cross_down(prev["DIF"], last["DIF"], prev["MACD"], last["MACD"]):
        tags.append("MACD死亡交叉")
    if _cross_up(prev["MA20"], last["MA20"], prev["MA60"], last["MA60"]):
        tags.append("MA黃金交叉")
    if _cross_down(prev["MA20"], last["MA20"], prev["MA60"], last["MA60"]):
        tags.append("MA死亡交叉")

    return {"signal_score": int(round(score)), "signal_level": level, "cross_tags": tags}


def _safe_compute_signal_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    try:
        snap = compute_signal_snapshot(df)
        if isinstance(snap, dict):
            return snap
    except Exception:
        pass
    return _build_signal_summary(df)


def _safe_compute_radar_scores(df: pd.DataFrame) -> dict[str, float]:
    try:
        scores = compute_radar_scores(df)
        if isinstance(scores, dict):
            return {str(k): float(v) for k, v in scores.items()}
    except Exception:
        pass

    if df.empty:
        return {"趨勢": 0, "動能": 0, "量價": 0, "風險": 0, "位置": 0}

    last = df.iloc[-1]
    price = float(last["Close"])

    trend = 80 if pd.notna(last.get("MA60", np.nan)) and price > last.get("MA60", np.nan) else 40
    momentum = 80 if last.get("DIF", 0) > last.get("MACD", 0) else 40
    volume = 60
    risk = 80 if pd.notna(last.get("MA20", np.nan)) and price > last.get("MA20", np.nan) else 40
    pos = 70

    return {"趨勢": float(trend), "動能": float(momentum), "量價": float(volume), "風險": float(risk), "位置": float(pos)}


def _safe_compute_support_resistance(df: pd.DataFrame) -> dict[str, Any]:
    try:
        sr = compute_support_resistance_snapshot(df)
        if isinstance(sr, dict):
            return sr
    except Exception:
        pass

    if df.empty:
        return {}

    recent = df.tail(60)
    return {
        "support_1": float(recent["Low"].quantile(0.2)),
        "support_2": float(recent["Low"].quantile(0.1)),
        "resistance_1": float(recent["High"].quantile(0.8)),
        "resistance_2": float(recent["High"].quantile(0.9)),
    }


# =========================================================
# 圖表
# =========================================================
def _plot_candlestick(df: pd.DataFrame, stock_label: str) -> go.Figure:
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.60, 0.20, 0.20],
        specs=[[{"secondary_y": False}], [{"secondary_y": False}], [{"secondary_y": False}]],
    )

    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name="K線",
        ),
        row=1,
        col=1,
    )

    for ma in [5, 10, 20, 60, 120, 240]:
        col = f"MA{ma}"
        if col in df.columns:
            fig.add_trace(go.Scatter(x=df.index, y=df[col], mode="lines", name=col), row=1, col=1)

    fig.add_trace(go.Bar(x=df.index, y=df["Volume"], name="成交量"), row=2, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["K"], mode="lines", name="K"), row=3, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["D"], mode="lines", name="D"), row=3, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["DIF"], mode="lines", name="DIF"), row=3, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["MACD"], mode="lines", name="MACD"), row=3, col=1)
    fig.add_trace(go.Bar(x=df.index, y=df["OSC"], name="OSC"), row=3, col=1)

    fig.update_layout(
        title=f"{stock_label}｜歷史K線分析",
        xaxis_rangeslider_visible=False,
        height=950,
        legend=dict(orientation="h", y=1.02, x=0),
        margin=dict(l=20, r=20, t=60, b=20),
    )
    return fig


def _plot_radar(radar_scores: dict[str, float], stock_label: str) -> go.Figure:
    labels = list(radar_scores.keys())
    values = [float(radar_scores[k]) for k in labels]
    if labels:
        labels = labels + [labels[0]]
        values = values + [values[0]]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(r=values, theta=labels, fill="toself", name=stock_label))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=False,
        height=420,
        margin=dict(l=20, r=20, t=40, b=20),
    )
    return fig


# =========================================================
# 狀態同步
# =========================================================
def _repair_group_stock_state(group_map: dict[str, list[str]]):
    groups = list(group_map.keys())
    current_group = _ss("group", "")

    if current_group not in group_map:
        current_group = groups[0] if groups else ""
        _set_ss("group", current_group)

    stock_options = group_map.get(current_group, [])
    current_stock = _ss("stock", "")
    if current_stock not in stock_options:
        _set_ss("stock", stock_options[0] if stock_options else "")


def _sanitize_date_state():
    today = date.today()
    default_start = today - timedelta(days=365)
    default_end = today

    _set_ss("start_date", _to_pydate(_ss("start_date"), default_start))
    _set_ss("end_date", _to_pydate(_ss("end_date"), default_end))


def _init_state(group_map: dict[str, list[str]]):
    groups = list(group_map.keys())

    if _ss("group") is None:
        _set_ss("group", groups[0] if groups else "")

    if _ss("stock") is None:
        default_stocks = group_map.get(_ss("group"), [])
        _set_ss("stock", default_stocks[0] if default_stocks else "")

    if _ss("search_input") is None:
        _set_ss("search_input", "")

    today = date.today()
    default_start = today - timedelta(days=365)
    default_end = today

    saved = _safe_load_last_query_state("history_kline_page")

    if _ss("start_date") is None:
        _set_ss("start_date", _to_pydate(saved.get("start_date"), default_start))
    if _ss("end_date") is None:
        _set_ss("end_date", _to_pydate(saved.get("end_date"), default_end))

    saved_group = _safe_str(saved.get("group"))
    saved_stock = _safe_str(saved.get("stock"))

    if saved_group in group_map:
        _set_ss("group", saved_group)
        if saved_stock in group_map.get(saved_group, []):
            _set_ss("stock", saved_stock)

    _repair_group_stock_state(group_map)
    _sanitize_date_state()


def _apply_search_sync(keyword: str, group_map: dict[str, list[str]], stock_rows: list[dict[str, str]]) -> bool:
    target = _find_search_target(keyword, stock_rows)
    if not target:
        return False

    _set_ss("group", target["group"])
    _set_ss("stock", target["stock"])
    _set_ss("search_input", target["stock"])
    _repair_group_stock_state(group_map)
    return True


def _on_group_change(group_map: dict[str, list[str]]):
    current_group = _ss("group", "")
    stock_options = group_map.get(current_group, [])
    current_stock = _ss("stock", "")
    if current_stock not in stock_options:
        _set_ss("stock", stock_options[0] if stock_options else "")


# =========================================================
# 主頁
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    group_map = _build_group_stock_map()
    stock_rows = _build_stock_lookup(group_map)
    _init_state(group_map)

    render_pro_hero(
        title="歷史K線分析",
        subtitle="K線・MA・KD・MACD・雷達評分・支撐壓力・起漲起跌點・交叉事件",
    )

    render_pro_section("快速搜尋股票")
    s1, s2 = st.columns([5, 1])

    with s1:
        st.text_input(
            "輸入股票代碼或名稱",
            key=_k("search_input"),
            placeholder="例如：2330、台積電、2454 聯發科",
            label_visibility="collapsed",
        )

    with s2:
        search_clicked = st.button("帶入", use_container_width=True, type="primary")

    if search_clicked:
        ok = _apply_search_sync(_ss("search_input", ""), group_map, stock_rows)
        if ok:
            st.success(f"已同步到：{_ss('group')} / {_ss('stock')}")
        else:
            st.warning("找不到對應股票，請重新輸入代碼或名稱。")

    render_pro_section("查詢條件")
    _repair_group_stock_state(group_map)
    _sanitize_date_state()

    groups = list(group_map.keys())
    c1, c2, c3, c4 = st.columns([2, 3, 2, 2])

    with c1:
        st.selectbox(
            "選擇群組",
            options=groups,
            key=_k("group"),
            on_change=_on_group_change,
            args=(group_map,),
        )

    current_group = _ss("group", "")
    stock_options = group_map.get(current_group, [])
    if _ss("stock", "") not in stock_options:
        _set_ss("stock", stock_options[0] if stock_options else "")

    with c2:
        st.selectbox(
            "群組股票",
            options=stock_options,
            key=_k("stock"),
        )

    # 這裡先把 state 強制洗成 py date，再丟給 date_input
    _sanitize_date_state()

    with c3:
        st.date_input("開始日期", key=_k("start_date"))

    with c4:
        st.date_input("結束日期", key=_k("end_date"))

    # 再洗一次，避免使用者操作後回來是奇怪型別
    _sanitize_date_state()

    selected_group = _ss("group", "")
    selected_stock = _ss("stock", "")
    start_date = _to_pydate(_ss("start_date"), date.today() - timedelta(days=365))
    end_date = _to_pydate(_ss("end_date"), date.today())

    st.caption(f"目前實際查詢值：群組【{selected_group}】 / 股票【{selected_stock}】")

    if not selected_stock:
        st.error("目前沒有可查詢股票。")
        st.stop()

    code, name = _split_code_name(selected_stock)
    if not code:
        st.error("股票代碼解析失敗，請重新選擇。")
        st.stop()

    if start_date > end_date:
        st.error("開始日期不可大於結束日期。")
        st.stop()

    _safe_save_last_query_state(
        "history_kline_page",
        {
            "group": selected_group,
            "stock": selected_stock,
            "start_date": str(start_date),
            "end_date": str(end_date),
        },
    )

    with st.spinner("讀取歷史資料中..."):
        raw_df = get_history_data(code, start_date, end_date)

    hist_df = _normalize_history_df(raw_df)
    if hist_df.empty:
        st.error("查無歷史資料，請更換股票或日期區間。")
        st.stop()

    hist_df = _compute_indicators(hist_df)

    market = ""
    try:
        real_name, market = get_stock_name_and_market(code)
        if real_name:
            name = real_name
    except Exception:
        pass

    stock_label = _display_stock(code, name)
    signal_snapshot = _safe_compute_signal_snapshot(hist_df)
    radar_scores = _safe_compute_radar_scores(hist_df)
    sr_snapshot = _safe_compute_support_resistance(hist_df)
    event_df = _build_event_log(hist_df)

    last = hist_df.iloc[-1]
    prev_close = hist_df["Close"].iloc[-2] if len(hist_df) >= 2 else last["Close"]
    pct = ((last["Close"] / prev_close) - 1) * 100 if prev_close else 0
    radar_avg = float(np.mean(list(radar_scores.values()))) if radar_scores else 0
    signal_score = float(signal_snapshot.get("signal_score", 0))
    signal_level = _safe_str(signal_snapshot.get("signal_level", "中性"))

    sr1 = sr_snapshot.get("support_1")
    rr1 = sr_snapshot.get("resistance_1")

    render_pro_kpi_row(
        [
            {"label": "股票", "value": stock_label},
            {"label": "最新收盤", "value": f"{last['Close']:.2f}"},
            {"label": "單日漲跌", "value": f"{pct:+.2f}%"},
            {"label": "訊號燈號", "value": f"{signal_level} / {int(signal_score)}"},
            {"label": "雷達均分", "value": f"{radar_avg:.1f}"},
        ]
    )

    info_cols = st.columns(3)
    with info_cols[0]:
        render_pro_info_card(
            "支撐區",
            f"{sr1:.2f}" if isinstance(sr1, (int, float)) and not math.isnan(sr1) else "—",
            "近60日推估",
        )
    with info_cols[1]:
        render_pro_info_card(
            "壓力區",
            f"{rr1:.2f}" if isinstance(rr1, (int, float)) and not math.isnan(rr1) else "—",
            "近60日推估",
        )
    with info_cols[2]:
        render_pro_info_card("評級", _safe_score_to_badge(radar_avg), market or "台股")

    render_pro_section("K線 / 均線 / 成交量 / KD / MACD")
    st.plotly_chart(_plot_candlestick(hist_df, stock_label), use_container_width=True)

    render_pro_section("最近事件摘要")
    summary_cols = st.columns(4)
    recent_events = event_df.head(6).copy()
    cross_tags = signal_snapshot.get("cross_tags", []) if isinstance(signal_snapshot, dict) else []

    with summary_cols[0]:
        latest_event = recent_events["事件名稱"].iloc[0] if not recent_events.empty else "無"
        render_pro_info_card("最新事件", latest_event, "最近觸發")
    with summary_cols[1]:
        render_pro_info_card("交叉訊號", "、".join(cross_tags) if cross_tags else "無", "當前偵測")
    with summary_cols[2]:
        trend_text = "站上 MA60" if pd.notna(last["MA60"]) and last["Close"] > last["MA60"] else "跌破 MA60"
        render_pro_info_card("波段趨勢", trend_text, "以 MA60 觀察")
    with summary_cols[3]:
        render_pro_info_card("KD 狀態", f"K={last['K']:.1f} / D={last['D']:.1f}", "短線動能")

    left, right = st.columns([1, 1])

    with left:
        render_pro_section("雷達評分")
        st.plotly_chart(_plot_radar(radar_scores, stock_label), use_container_width=True)
        radar_df = pd.DataFrame({"指標": list(radar_scores.keys()), "分數": [round(float(v), 2) for v in radar_scores.values()]})
        st.dataframe(radar_df, use_container_width=True, hide_index=True)

    with right:
        render_pro_section("支撐 / 壓力")
        sr_rows = []
        for k, v in sr_snapshot.items():
            if isinstance(v, (int, float)) and not math.isnan(v):
                sr_rows.append({"項目": k, "數值": round(float(v), 2)})
        if sr_rows:
            st.dataframe(pd.DataFrame(sr_rows), use_container_width=True, hide_index=True)
        else:
            st.info("目前無支撐壓力資料。")

    render_pro_section("事件說明與篩選")
    event_types = ["全部"] + sorted(event_df["事件類型"].dropna().unique().tolist()) if not event_df.empty else ["全部"]

    f1, f2 = st.columns([2, 5])
    with f1:
        st.selectbox("事件篩選", options=event_types, key=_k("event_type_filter"))
    with f2:
        st.markdown(
            """
            <div style="padding-top: 28px;">
            起漲點 / 起跌點：局部高低轉折。MA / KD / MACD 交叉：用來判讀短中期方向變化。
            </div>
            """,
            unsafe_allow_html=True,
        )

    show_event_df = event_df.copy()
    event_type_selected = _ss("event_type_filter", "全部")
    if event_type_selected != "全部" and not show_event_df.empty:
        show_event_df = show_event_df[show_event_df["事件類型"] == event_type_selected].copy()

    if not show_event_df.empty:
        show_event_df["日期"] = pd.to_datetime(show_event_df["日期"]).dt.strftime("%Y-%m-%d")
        st.dataframe(show_event_df, use_container_width=True, hide_index=True)
    else:
        st.info("目前沒有符合條件的事件。")

    render_pro_section("技術指標明細")
    detail_cols = [
        "Open", "High", "Low", "Close", "Volume",
        "MA5", "MA10", "MA20", "MA60", "MA120", "MA240",
        "K", "D", "J", "DIF", "MACD", "OSC", "PctChg"
    ]
    detail_df = hist_df[detail_cols].tail(120).copy()
    detail_df = detail_df.reset_index()
    detail_df = detail_df.rename(columns={detail_df.columns[0]: "Date"})
    detail_df["Date"] = pd.to_datetime(detail_df["Date"]).dt.strftime("%Y-%m-%d")
    st.dataframe(detail_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
