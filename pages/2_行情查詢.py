# pages/2_行情查詢.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import requests
import streamlit as st

from utils import (
    compute_radar_scores,
    compute_signal_snapshot,
    compute_support_resistance_snapshot,
    format_number,
    get_all_code_name_map,
    get_history_data,
    get_normalized_watchlist,
    get_realtime_stock_info,
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

PAGE_TITLE = "行情查詢"
PFX = "quote_"


# =========================================================
# 基礎工具
# =========================================================
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


def _to_date(v: Any, fallback: date) -> date:
    if v is None:
        return fallback
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        x = pd.to_datetime(v, errors="coerce")
        if pd.notna(x):
            return x.date()
    except Exception:
        pass
    return fallback


# =========================================================
# 群組 / 搜尋
# =========================================================
def _build_group_stock_map() -> dict[str, list[dict[str, str]]]:
    watchlist = get_normalized_watchlist()
    group_map: dict[str, list[dict[str, str]]] = {}

    if isinstance(watchlist, dict):
        for group_name, items in watchlist.items():
            g = _safe_str(group_name) or "未分組"
            group_map[g] = []

            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    code = _safe_str(item.get("code"))
                    name = _safe_str(item.get("name")) or code
                    market = _safe_str(item.get("market")) or "上市"
                    if code:
                        group_map[g].append(
                            {
                                "code": code,
                                "name": name,
                                "market": market,
                                "label": f"{code} {name}",
                            }
                        )

    if not group_map:
        try:
            all_df = get_all_code_name_map("")
            if isinstance(all_df, pd.DataFrame) and not all_df.empty:
                rows = []
                for _, row in all_df.head(120).iterrows():
                    code = _safe_str(row.get("code"))
                    name = _safe_str(row.get("name")) or code
                    market = _safe_str(row.get("market")) or "上市"
                    if code:
                        rows.append(
                            {
                                "code": code,
                                "name": name,
                                "market": market,
                                "label": f"{code} {name}",
                            }
                        )
                if rows:
                    group_map["全部股票"] = rows
        except Exception:
            pass

    return group_map


def _flatten_group_map(group_map: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    rows = []
    for group_name, items in group_map.items():
        for item in items:
            rows.append(
                {
                    "group": group_name,
                    "code": _safe_str(item.get("code")),
                    "name": _safe_str(item.get("name")),
                    "market": _safe_str(item.get("market")),
                    "label": _safe_str(item.get("label")),
                }
            )
    return rows


def _find_search_target(keyword: str, flat_rows: list[dict[str, str]]) -> dict[str, str] | None:
    q = _safe_str(keyword).lower()
    if not q:
        return None

    for row in flat_rows:
        if q == row["code"].lower():
            return row
    for row in flat_rows:
        if q == row["name"].lower():
            return row
    for row in flat_rows:
        if q == row["label"].lower():
            return row

    prefix_hits = [r for r in flat_rows if r["code"].lower().startswith(q) or r["name"].lower().startswith(q)]
    if prefix_hits:
        return prefix_hits[0]

    contain_hits = [
        r for r in flat_rows
        if q in f"{r['group']} {r['code']} {r['name']} {r['label']}".lower()
    ]
    if contain_hits:
        return contain_hits[0]

    return None


# =========================================================
# State
# =========================================================
def _init_state(group_map: dict[str, list[dict[str, str]]]):
    saved = load_last_query_state()
    today = date.today()
    default_start = today - timedelta(days=365)
    default_end = today

    groups = list(group_map.keys())

    if _k("group") not in st.session_state:
        saved_group = _safe_str(saved.get("quick_group", ""))
        st.session_state[_k("group")] = saved_group if saved_group in groups else (groups[0] if groups else "")

    if _k("stock_code") not in st.session_state:
        st.session_state[_k("stock_code")] = _safe_str(saved.get("quick_stock_code", ""))

    if _k("search_input") not in st.session_state:
        st.session_state[_k("search_input")] = ""

    if _k("start_date") not in st.session_state:
        st.session_state[_k("start_date")] = parse_date_safe(saved.get("home_start"), default_start)

    if _k("end_date") not in st.session_state:
        st.session_state[_k("end_date")] = parse_date_safe(saved.get("home_end"), default_end)

    st.session_state[_k("start_date")] = _to_date(st.session_state.get(_k("start_date")), default_start)
    st.session_state[_k("end_date")] = _to_date(st.session_state.get(_k("end_date")), default_end)

    _repair_state(group_map)


def _repair_state(group_map: dict[str, list[dict[str, str]]]):
    groups = list(group_map.keys())
    current_group = _safe_str(st.session_state.get(_k("group"), ""))

    if current_group not in group_map:
        st.session_state[_k("group")] = groups[0] if groups else ""
        current_group = st.session_state[_k("group")]

    items = group_map.get(current_group, [])
    valid_codes = [x["code"] for x in items]
    current_code = _safe_str(st.session_state.get(_k("stock_code"), ""))

    if valid_codes:
        if current_code not in valid_codes:
            st.session_state[_k("stock_code")] = valid_codes[0]
    else:
        st.session_state[_k("stock_code")] = ""


def _on_group_change(group_map: dict[str, list[dict[str, str]]]):
    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    st.session_state[_k("stock_code")] = items[0]["code"] if items else ""


# =========================================================
# 歷史資料 smart fallback
# =========================================================
def _prepare_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "日期" not in df.columns:
        return pd.DataFrame()

    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    numeric_cols = ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "收盤價" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["收盤價"]).copy()
    if df.empty:
        return pd.DataFrame()

    close = df["收盤價"]
    high = df["最高價"] if "最高價" in df.columns else close
    low = df["最低價"] if "最低價" in df.columns else close

    for n in [5, 10, 20, 60, 120, 240]:
        df[f"MA{n}"] = close.rolling(n).mean()

    low_9 = low.rolling(9).min()
    high_9 = high.rolling(9).max()
    rsv = (close - low_9) / (high_9 - low_9).replace(0, pd.NA) * 100
    df["K"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    df["D"] = df["K"].ewm(alpha=1 / 3, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["DIF"] - df["DEA"]

    df["漲跌幅(%)"] = close.pct_change() * 100

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["ATR14"] = tr.rolling(14).mean()

    return df


@st.cache_data(ttl=1800, show_spinner=False)
def _get_tpex_history_data(stock_no: str, start_date: date, end_date: date) -> pd.DataFrame:
    stock_no = _safe_str(stock_no)
    if not stock_no:
        return pd.DataFrame()

    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    if end_ts < start_ts:
        return pd.DataFrame()

    month_starts = pd.date_range(start=start_ts.replace(day=1), end=end_ts, freq="MS")
    frames = []
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.tpex.org.tw/"}

    for dt in month_starts:
        roc_year = dt.year - 1911
        month = dt.month
        roc_date = f"{roc_year}/{month:02d}"

        try:
            r = requests.get(
                "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php",
                params={"l": "zh-tw", "d": roc_date, "stkno": stock_no},
                headers=headers,
                timeout=20,
                verify=False,
            )
            r.raise_for_status()
            data = r.json()

            aa_data = data.get("aaData", [])
            fields = data.get("fields", [])
            if not aa_data:
                continue

            temp = pd.DataFrame(aa_data, columns=fields if fields and len(fields) == len(aa_data[0]) else None)
            frames.append(temp)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    rename_map = {}
    for col in df.columns:
        c = _safe_str(col)
        if c in ["日期", "日 期"]:
            rename_map[col] = "日期"
        elif "成交仟股" in c or "成交股數" in c:
            rename_map[col] = "成交股數"
        elif "成交仟元" in c or "成交金額" in c:
            rename_map[col] = "成交金額"
        elif "開盤" in c:
            rename_map[col] = "開盤價"
        elif "最高" in c:
            rename_map[col] = "最高價"
        elif "最低" in c:
            rename_map[col] = "最低價"
        elif "收盤" in c:
            rename_map[col] = "收盤價"
        elif "成交筆數" in c:
            rename_map[col] = "成交筆數"

    df = df.rename(columns=rename_map)

    if "日期" not in df.columns:
        return pd.DataFrame()

    def convert_roc_date(x):
        x = _safe_str(x)
        if not x:
            return pd.NaT
        if "/" in x:
            parts = x.split("/")
            if len(parts) == 3:
                try:
                    year = int(parts[0]) + 1911
                    month = int(parts[1])
                    day = int(parts[2])
                    return pd.Timestamp(year=year, month=month, day=day)
                except Exception:
                    return pd.NaT
        try:
            return pd.to_datetime(x)
        except Exception:
            return pd.NaT

    df["日期"] = df["日期"].apply(convert_roc_date)
    df = df.dropna(subset=["日期"])

    for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
                .replace(["--", "---", "", "----"], pd.NA)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "成交股數" in df.columns:
        try:
            med = df["成交股數"].dropna().median()
            if pd.notna(med) and med < 100000:
                df["成交股數"] = df["成交股數"] * 1000
        except Exception:
            pass

    df = df[(df["日期"] >= start_ts) & (df["日期"] <= end_ts)]
    df = df.sort_values("日期").drop_duplicates(subset=["日期"], keep="last").reset_index(drop=True)
    return df


@st.cache_data(ttl=1800, show_spinner=False)
def _get_history_data_smart(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date) -> pd.DataFrame:
    df = get_history_data(
        stock_no=stock_no,
        stock_name=stock_name,
        market_type=market_type,
        start_date=start_date,
        end_date=end_date,
    )
    df = _prepare_history_df(df)
    if not df.empty:
        return df

    if _safe_str(market_type) in ["上櫃", "興櫃"]:
        df2 = _get_tpex_history_data(stock_no, start_date, end_date)
        df2 = _prepare_history_df(df2)
        if not df2.empty:
            return df2

    return pd.DataFrame()


# =========================================================
# 事件 / 股神觀點
# =========================================================
def _detect_pivots_smart(df: pd.DataFrame, window: int = 4, min_gap: int = 6, atr_ratio: float = 0.8):
    if df is None or df.empty or len(df) < window * 2 + 3:
        return [], []

    highs = df["最高價"].tolist()
    lows = df["最低價"].tolist()
    atr = df["ATR14"] if "ATR14" in df.columns else pd.Series([None] * len(df))

    peak_idx = []
    trough_idx = []

    for i in range(window, len(df) - window):
        cur_high = highs[i]
        cur_low = lows[i]
        if pd.isna(cur_high) or pd.isna(cur_low):
            continue

        left_high = highs[i - window:i]
        right_high = highs[i + 1:i + 1 + window]
        left_low = lows[i - window:i]
        right_low = lows[i + 1:i + 1 + window]

        is_peak = all(cur_high >= x for x in left_high + right_high if pd.notna(x))
        is_trough = all(cur_low <= x for x in left_low + right_low if pd.notna(x))

        if is_peak:
            if not peak_idx or (i - peak_idx[-1] >= min_gap):
                peak_idx.append(i)
            else:
                old_i = peak_idx[-1]
                if cur_high > highs[old_i]:
                    peak_idx[-1] = i

        if is_trough:
            if not trough_idx or (i - trough_idx[-1] >= min_gap):
                trough_idx.append(i)
            else:
                old_i = trough_idx[-1]
                if cur_low < lows[old_i]:
                    trough_idx[-1] = i

    filtered_peak = []
    filtered_trough = []

    for i in peak_idx:
        if not filtered_peak:
            filtered_peak.append(i)
            continue
        prev_i = filtered_peak[-1]
        move = abs(highs[i] - highs[prev_i])
        atr_ref = max(_safe_float(df.iloc[i].get("ATR14"), 0), _safe_float(df.iloc[prev_i].get("ATR14"), 0))
        if atr_ref == 0 or move >= atr_ref * atr_ratio:
            filtered_peak.append(i)

    for i in trough_idx:
        if not filtered_trough:
            filtered_trough.append(i)
            continue
        prev_i = filtered_trough[-1]
        move = abs(lows[i] - lows[prev_i])
        atr_ref = max(_safe_float(df.iloc[i].get("ATR14"), 0), _safe_float(df.iloc[prev_i].get("ATR14"), 0))
        if atr_ref == 0 or move >= atr_ref * atr_ratio:
            filtered_trough.append(i)

    return filtered_peak, filtered_trough


def _build_event_df(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if df is None or df.empty or len(df) < 3:
        return pd.DataFrame(columns=["日期", "事件分類", "事件", "說明"])

    peak_idx, trough_idx = _detect_pivots_smart(df, window=4, min_gap=6, atr_ratio=0.8)

    for i in trough_idx:
        r = df.iloc[i]
        rows.append({
            "日期": r["日期"],
            "事件分類": "起漲點",
            "事件": "起漲點",
            "說明": f"局部低點形成，低點約 {format_number(r.get('最低價'), 2)}。",
        })

    for i in peak_idx:
        r = df.iloc[i]
        rows.append({
            "日期": r["日期"],
            "事件分類": "起跌點",
            "事件": "起跌點",
            "說明": f"局部高點形成，高點約 {format_number(r.get('最高價'), 2)}。",
        })

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        d = cur["日期"]

        if all(c in df.columns for c in ["MA5", "MA10"]):
            if pd.notna(prev["MA5"]) and pd.notna(prev["MA10"]) and pd.notna(cur["MA5"]) and pd.notna(cur["MA10"]):
                if prev["MA5"] <= prev["MA10"] and cur["MA5"] > cur["MA10"]:
                    rows.append({"日期": d, "事件分類": "MA", "事件": "MA黃金交叉", "說明": "MA5 上穿 MA10，短線偏強。"})
                elif prev["MA5"] >= prev["MA10"] and cur["MA5"] < cur["MA10"]:
                    rows.append({"日期": d, "事件分類": "MA", "事件": "MA死亡交叉", "說明": "MA5 下破 MA10，短線偏弱。"})

        if all(c in df.columns for c in ["K", "D"]):
            if pd.notna(prev["K"]) and pd.notna(prev["D"]) and pd.notna(cur["K"]) and pd.notna(cur["D"]):
                if prev["K"] <= prev["D"] and cur["K"] > cur["D"]:
                    rows.append({"日期": d, "事件分類": "KD", "事件": "KD黃金交叉", "說明": "KD 轉強。"})
                elif prev["K"] >= prev["D"] and cur["K"] < cur["D"]:
                    rows.append({"日期": d, "事件分類": "KD", "事件": "KD死亡交叉", "說明": "KD 轉弱。"})

        if all(c in df.columns for c in ["DIF", "DEA"]):
            if pd.notna(prev["DIF"]) and pd.notna(prev["DEA"]) and pd.notna(cur["DIF"]) and pd.notna(cur["DEA"]):
                if prev["DIF"] <= prev["DEA"] and cur["DIF"] > cur["DEA"]:
                    rows.append({"日期": d, "事件分類": "MACD", "事件": "MACD黃金交叉", "說明": "DIF 上穿 DEA，動能轉強。"})
                elif prev["DIF"] >= prev["DEA"] and cur["DIF"] < cur["DEA"]:
                    rows.append({"日期": d, "事件分類": "MACD", "事件": "MACD死亡交叉", "說明": "DIF 下破 DEA，動能轉弱。"})

        if i >= 19 and all(c in df.columns for c in ["最高價", "最低價", "收盤價"]):
            recent = df.iloc[max(0, i - 19): i + 1]
            high20 = recent["最高價"].max()
            low20 = recent["最低價"].min()
            close_price = cur["收盤價"]

            if pd.notna(close_price) and pd.notna(high20) and close_price >= high20:
                rows.append({"日期": d, "事件分類": "突破", "事件": "突破20日高", "說明": "股價創 20 日新高。"})
            if pd.notna(close_price) and pd.notna(low20) and close_price <= low20:
                rows.append({"日期": d, "事件分類": "跌破", "事件": "跌破20日低", "說明": "股價創 20 日新低。"})

    if not rows:
        return pd.DataFrame(columns=["日期", "事件分類", "事件", "說明"])

    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["日期", "事件", "說明"])
        .sort_values("日期", ascending=False)
        .reset_index(drop=True)
    )


def _build_master_commentary(df: pd.DataFrame, signal_snapshot: dict, sr_snapshot: dict, radar: dict, event_df: pd.DataFrame):
    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"))
    ma20 = _safe_float(last.get("MA20"))
    ma60 = _safe_float(last.get("MA60"))
    k_val = _safe_float(last.get("K"))
    d_val = _safe_float(last.get("D"))
    dif = _safe_float(last.get("DIF"))
    dea = _safe_float(last.get("DEA"))

    views = []

    if close_now is not None and ma20 is not None and ma60 is not None:
        if close_now > ma20 and close_now > ma60:
            views.append(("趨勢觀點", "股價位於 MA20 與 MA60 之上，中期結構偏多。", ""))
        elif close_now < ma20 and close_now < ma60:
            views.append(("趨勢觀點", "股價位於 MA20 與 MA60 之下，中期結構偏弱。", ""))
        else:
            views.append(("趨勢觀點", "股價位在中期均線交界區，屬整理與等待方向選擇。", ""))

    if k_val is not None and d_val is not None and dif is not None and dea is not None:
        if k_val > d_val and dif > dea:
            views.append(("動能觀點", "KD 與 MACD 同步偏多，短線攻擊動能較佳。", ""))
        elif k_val < d_val and dif < dea:
            views.append(("動能觀點", "KD 與 MACD 同步偏弱，反彈宜防再轉弱。", ""))
        else:
            views.append(("動能觀點", "擺盪動能與趨勢動能未完全共振，走勢容易反覆。", ""))

    pressure_text = sr_snapshot.get("pressure_signal", ("—", ""))[0]
    support_text = sr_snapshot.get("support_signal", ("—", ""))[0]
    break_text = sr_snapshot.get("break_signal", ("—", ""))[0]

    if "突破" in break_text:
        views.append(("結構觀點", "目前屬突破結構，關鍵在突破後是否守住，不是只看站上那一刻。", ""))
    elif "跌破" in break_text:
        views.append(("結構觀點", "目前屬跌破結構，若無法快速站回，弱勢延續機率較高。", ""))
    else:
        if "接近20日壓力" in pressure_text:
            views.append(("結構觀點", "股價逼近短壓，沒有量就容易變成假突破或震盪。", ""))
        elif "接近20日支撐" in support_text:
            views.append(("結構觀點", "股價接近短撐，重點看是否出現防守量與止跌K棒。", ""))
        else:
            views.append(("結構觀點", "目前位於區間內部，較適合等待明確突破或跌破再提高把握度。", ""))

    radar_avg = round(sum([
        _safe_float(radar.get("trend"), 50),
        _safe_float(radar.get("momentum"), 50),
        _safe_float(radar.get("volume"), 50),
        _safe_float(radar.get("position"), 50),
        _safe_float(radar.get("structure"), 50),
    ]) / 5, 1)
    views.append(("雷達總評", f"五維均分約 {radar_avg}，{radar.get('summary', '—')}", ""))

    if event_df is not None and not event_df.empty:
        last_event = event_df.iloc[0]
        views.append(("最近關鍵事件", f"{_safe_str(last_event.get('事件'))}：{_safe_str(last_event.get('說明'))}", ""))

    score = _safe_float(signal_snapshot.get("score"), 0)
    if score >= 4:
        action_text = "偏多架構，但在壓力區不建議無量追價，較佳節奏是等拉回不破或突破後續強。"
    elif score >= 2:
        action_text = "偏多但未到全面強攻，宜觀察回測支撐是否守穩。"
    elif score <= -4:
        action_text = "偏空結構明確，風險控管應優先於抄底預設。"
    elif score <= -2:
        action_text = "弱勢整理機率高，除非出現止跌與量能改善，否則先保守。"
    else:
        action_text = "多空混合，最佳策略通常不是猜，而是等關鍵位表態後再跟。"
    views.append(("股神操作觀點", action_text, ""))

    return views


def _build_recent_event_summary(event_df: pd.DataFrame):
    if event_df is None or event_df.empty:
        return [("最近事件", "無明確新事件", "")]

    out = []
    for _, row in event_df.head(8).iterrows():
        try:
            d = pd.to_datetime(row["日期"]).strftime("%Y-%m-%d")
        except Exception:
            d = _safe_str(row["日期"])
        out.append((d, _safe_str(row["事件"]), ""))
    return out


# =========================================================
# 主頁
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    group_map = _build_group_stock_map()
    flat_rows = _flatten_group_map(group_map)
    _init_state(group_map)

    render_pro_hero(
        title="行情查詢｜股神版",
        subtitle="即時行情、訊號燈號、支撐壓力、最近事件、股神分析觀點，支援上櫃歷史 fallback。",
    )

    render_pro_section("快速搜尋股票")

    s1, s2 = st.columns([5, 1])
    with s1:
        st.text_input(
            "輸入股票代碼或名稱",
            key=_k("search_input"),
            placeholder="例如：2330、台積電、3548 兆利",
            label_visibility="collapsed",
        )
    with s2:
        if st.button("帶入", use_container_width=True, type="primary"):
            target = _find_search_target(st.session_state.get(_k("search_input"), ""), flat_rows)
            if target:
                st.session_state[_k("group")] = target["group"]
                st.session_state[_k("stock_code")] = target["code"]
                save_last_query_state(
                    quick_group=target["group"],
                    quick_stock_code=target["code"],
                    home_start=st.session_state.get(_k("start_date")),
                    home_end=st.session_state.get(_k("end_date")),
                )
                st.rerun()
            else:
                st.warning("找不到對應股票。")

    render_pro_section("查詢條件")
    _repair_state(group_map)

    groups = list(group_map.keys())
    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    code_to_item = {x["code"]: x for x in items}
    code_options = [x["code"] for x in items]

    c1, c2, c3, c4 = st.columns([2, 3, 2, 2])

    with c1:
        st.selectbox(
            "選擇群組",
            options=groups,
            key=_k("group"),
            on_change=_on_group_change,
            args=(group_map,),
        )

    with c2:
        st.selectbox(
            "群組股票",
            options=code_options if code_options else [""],
            key=_k("stock_code"),
            format_func=lambda code: code_to_item.get(code, {}).get("label", code),
        )

    with c3:
        st.date_input("開始日期", key=_k("start_date"))

    with c4:
        st.date_input("結束日期", key=_k("end_date"))

    selected_group = _safe_str(st.session_state.get(_k("group"), ""))
    selected_code = _safe_str(st.session_state.get(_k("stock_code"), ""))
    start_date = _to_date(st.session_state.get(_k("start_date")), date.today() - timedelta(days=365))
    end_date = _to_date(st.session_state.get(_k("end_date")), date.today())

    if start_date > end_date:
        st.error("開始日期不可大於結束日期。")
        st.stop()

    if not selected_code or selected_code not in code_to_item:
        st.warning("請先選擇股票。")
        st.stop()

    selected_item = code_to_item[selected_code]
    stock_name = _safe_str(selected_item.get("name"))
    market_type = _safe_str(selected_item.get("market")) or "上市"
    stock_label = f"{selected_code} {stock_name}"

    st.caption(f"目前實際查詢值：群組【{selected_group}】 / 股票【{stock_label}】 / 市場【{market_type}】")

    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code=selected_code,
        home_start=start_date,
        home_end=end_date,
    )

    realtime = get_realtime_stock_info(selected_code, stock_name, market_type)

    with st.spinner("載入股神資料中..."):
        df = _get_history_data_smart(
            stock_no=selected_code,
            stock_name=stock_name,
            market_type=market_type,
            start_date=start_date,
            end_date=end_date,
        )

    signal_snapshot = None
    sr_snapshot = None
    radar = None
    event_df = pd.DataFrame(columns=["日期", "事件分類", "事件", "說明"])
    badge_text = "查無燈號"

    if not df.empty:
        signal_snapshot = compute_signal_snapshot(df)
        sr_snapshot = compute_support_resistance_snapshot(df)
        radar = compute_radar_scores(df)
        badge_text, _ = score_to_badge(signal_snapshot.get("score", 0))
        event_df = _build_event_df(df)

    price = realtime.get("price")
    prev_close = realtime.get("prev_close")
    change = realtime.get("change")
    change_pct = realtime.get("change_pct")
    total_volume = realtime.get("total_volume")

    render_pro_kpi_row(
        [
            {
                "label": "現價",
                "value": format_number(price, 2),
                "delta": f"{format_number(change, 2)} / {format_number(change_pct, 2)}%",
                "delta_class": "pro-kpi-delta-up" if _safe_float(change, 0) > 0 else ("pro-kpi-delta-down" if _safe_float(change, 0) < 0 else "pro-kpi-delta-flat"),
            },
            {
                "label": "昨收",
                "value": format_number(prev_close, 2),
                "delta": market_type,
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "總量",
                "value": format_number(total_volume, 0),
                "delta": "即時量",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "訊號燈號",
                "value": badge_text,
                "delta": stock_label,
                "delta_class": "pro-kpi-delta-flat",
            },
        ]
    )

    render_pro_info_card(
        "即時摘要",
        [
            ("股票", stock_label, ""),
            ("市場別", market_type, ""),
            ("開盤", format_number(realtime.get("open"), 2), ""),
            ("最高", format_number(realtime.get("high"), 2), ""),
            ("最低", format_number(realtime.get("low"), 2), ""),
            ("更新時間", _safe_str(realtime.get("update_time")) or "—", ""),
        ],
        chips=[badge_text],
    )

    if df.empty:
        st.warning("即時資料可查，但歷史資料不足，因此無法產出完整股神分析。這通常不是上櫃不能查，而是該區間歷史來源回傳空值。")
        st.stop()

    render_pro_section("訊號燈號 / 支撐壓力 / 最近事件")

    left, right = st.columns([1, 1])

    with left:
        render_pro_info_card(
            "股神訊號燈號",
            [
                ("均線趨勢", signal_snapshot.get("ma_trend", ("—", ""))[0], signal_snapshot.get("ma_trend", ("", "pro-flat"))[1]),
                ("KD交叉", signal_snapshot.get("kd_cross", ("—", ""))[0], signal_snapshot.get("kd_cross", ("", "pro-flat"))[1]),
                ("MACD趨勢", signal_snapshot.get("macd_trend", ("—", ""))[0], signal_snapshot.get("macd_trend", ("", "pro-flat"))[1]),
                ("價位狀態", signal_snapshot.get("price_vs_ma20", ("—", ""))[0], signal_snapshot.get("price_vs_ma20", ("", "pro-flat"))[1]),
                ("突破狀態", signal_snapshot.get("breakout_20d", ("—", ""))[0], signal_snapshot.get("breakout_20d", ("", "pro-flat"))[1]),
                ("量能狀態", signal_snapshot.get("volume_state", ("—", ""))[0], signal_snapshot.get("volume_state", ("", "pro-flat"))[1]),
            ],
            chips=[badge_text, f"分數 {signal_snapshot.get('score', 0)}"],
        )

        render_pro_info_card(
            "最近事件摘要",
            _build_recent_event_summary(event_df),
            chips=[market_type],
        )

    with right:
        render_pro_info_card(
            "支撐壓力",
            [
                ("20日壓力", format_number(sr_snapshot.get("res_20"), 2), ""),
                ("20日支撐", format_number(sr_snapshot.get("sup_20"), 2), ""),
                ("60日壓力", format_number(sr_snapshot.get("res_60"), 2), ""),
                ("60日支撐", format_number(sr_snapshot.get("sup_60"), 2), ""),
                ("壓力訊號", sr_snapshot.get("pressure_signal", ("—", ""))[0], sr_snapshot.get("pressure_signal", ("", "pro-flat"))[1]),
                ("支撐訊號", sr_snapshot.get("support_signal", ("—", ""))[0], sr_snapshot.get("support_signal", ("", "pro-flat"))[1]),
                ("區間判斷", sr_snapshot.get("break_signal", ("—", ""))[0], sr_snapshot.get("break_signal", ("", "pro-flat"))[1]),
            ],
        )

        render_pro_info_card(
            "股神分析觀點",
            _build_master_commentary(df, signal_snapshot, sr_snapshot, radar, event_df),
            chips=[badge_text, market_type],
        )

    render_pro_section("雷達評分")

    render_pro_info_card(
        "五維雷達評分",
        [
            ("趨勢", radar.get("trend", 50), ""),
            ("動能", radar.get("momentum", 50), ""),
            ("量能", radar.get("volume", 50), ""),
            ("位置", radar.get("position", 50), ""),
            ("結構", radar.get("structure", 50), ""),
            ("摘要", radar.get("summary", "—"), ""),
        ],
        chips=[stock_label],
    )

    render_pro_section("最近事件明細")
    if event_df.empty:
        st.info("近期沒有明確新事件。")
    else:
        st.dataframe(event_df.head(20), use_container_width=True, hide_index=True)

    with st.expander("效能說明"):
        st.write("這版已做 cache、搜尋同步修正、上櫃 smart history fallback。")
        st.write("維持完整分析頁思維，不走簡化展示版。")


if __name__ == "__main__":
    main()
