# pages/3_歷史K線分析.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

from utils import (
    compute_radar_scores,
    compute_support_resistance_snapshot,
    compute_signal_snapshot,
    format_number,
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
PFX = "hk_"


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
                for _, row in all_df.head(100).iterrows():
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

    if _k("event_filter") not in st.session_state:
        st.session_state[_k("event_filter")] = "全部"

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
# 歷史資料處理
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
# 事件
# =========================================================
def _build_event_df(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if df is None or df.empty or len(df) < 3:
        return pd.DataFrame(columns=["日期", "事件分類", "事件", "說明"])

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        d = cur["日期"]

        if all(c in df.columns for c in ["MA5", "MA10"]) and pd.notna(prev["MA5"]) and pd.notna(prev["MA10"]) and pd.notna(cur["MA5"]) and pd.notna(cur["MA10"]):
            if prev["MA5"] <= prev["MA10"] and cur["MA5"] > cur["MA10"]:
                rows.append({"日期": d, "事件分類": "MA", "事件": "MA黃金交叉", "說明": "MA5 上穿 MA10。"})
            elif prev["MA5"] >= prev["MA10"] and cur["MA5"] < cur["MA10"]:
                rows.append({"日期": d, "事件分類": "MA", "事件": "MA死亡交叉", "說明": "MA5 下破 MA10。"})

        if all(c in df.columns for c in ["K", "D"]) and pd.notna(prev["K"]) and pd.notna(prev["D"]) and pd.notna(cur["K"]) and pd.notna(cur["D"]):
            if prev["K"] <= prev["D"] and cur["K"] > cur["D"]:
                rows.append({"日期": d, "事件分類": "KD", "事件": "KD黃金交叉", "說明": "KD 轉強。"})
            elif prev["K"] >= prev["D"] and cur["K"] < cur["D"]:
                rows.append({"日期": d, "事件分類": "KD", "事件": "KD死亡交叉", "說明": "KD 轉弱。"})

        if all(c in df.columns for c in ["DIF", "DEA"]) and pd.notna(prev["DIF"]) and pd.notna(prev["DEA"]) and pd.notna(cur["DIF"]) and pd.notna(cur["DEA"]):
            if prev["DIF"] <= prev["DEA"] and cur["DIF"] > cur["DEA"]:
                rows.append({"日期": d, "事件分類": "MACD", "事件": "MACD黃金交叉", "說明": "DIF 上穿 DEA。"})
            elif prev["DIF"] >= prev["DEA"] and cur["DIF"] < cur["DEA"]:
                rows.append({"日期": d, "事件分類": "MACD", "事件": "MACD死亡交叉", "說明": "DIF 下破 DEA。"})

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

    event_df = pd.DataFrame(rows).sort_values("日期", ascending=False).reset_index(drop=True)
    return event_df


# =========================================================
# 圖表
# =========================================================
def _build_candlestick_chart(df: pd.DataFrame, stock_label: str) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=df["日期"],
            open=df["開盤價"],
            high=df["最高價"],
            low=df["最低價"],
            close=df["收盤價"],
            name="K線",
        )
    )

    for n in [5, 10, 20, 60, 120, 240]:
        col = f"MA{n}"
        if col in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df[col],
                    mode="lines",
                    name=col,
                )
            )

    fig.update_layout(
        title=f"{stock_label}｜歷史K線",
        height=620,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="日期",
        yaxis_title="價格",
        xaxis_rangeslider_visible=False,
    )
    return fig


def _build_kd_chart(df: pd.DataFrame, stock_label: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["日期"], y=df["K"], mode="lines", name="K"))
    fig.add_trace(go.Scatter(x=df["日期"], y=df["D"], mode="lines", name="D"))
    fig.update_layout(
        title=f"{stock_label}｜KD",
        height=300,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def _build_macd_chart(df: pd.DataFrame, stock_label: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["日期"], y=df["DIF"], mode="lines", name="DIF"))
    fig.add_trace(go.Scatter(x=df["日期"], y=df["DEA"], mode="lines", name="DEA"))
    fig.add_trace(go.Bar(x=df["日期"], y=df["MACD_HIST"], name="MACD柱"))
    fig.update_layout(
        title=f"{stock_label}｜MACD",
        height=320,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


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
        title="歷史K線分析｜股神版",
        subtitle="K線、MA、KD、MACD、雷達評分、支撐壓力、起漲起跌事件、最近事件摘要。",
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

    with st.spinner("載入股神資料中..."):
        df = _get_history_data_smart(
            stock_no=selected_code,
            stock_name=stock_name,
            market_type=market_type,
            start_date=start_date,
            end_date=end_date,
        )

    if df.empty:
        st.error("查無歷史資料，請更換股票或日期區間。")
        st.stop()

    signal_snapshot = compute_signal_snapshot(df)
    sr_snapshot = compute_support_resistance_snapshot(df)
    radar = compute_radar_scores(df)
    badge_text, badge_class = score_to_badge(signal_snapshot.get("score", 0))
    event_df = _build_event_df(df)

    last = df.iloc[-1]
    first = df.iloc[0]
    close_now = _safe_float(last.get("收盤價"))
    close_first = _safe_float(first.get("收盤價"))
    interval_pct = ((close_now / close_first) - 1) * 100 if close_first not in [None, 0] else None

    render_pro_kpi_row(
        [
            {
                "label": "最新收盤",
                "value": format_number(close_now, 2),
                "delta": format_number(interval_pct, 2) + "%",
                "delta_class": "pro-kpi-delta-up" if _safe_float(interval_pct, 0) > 0 else ("pro-kpi-delta-down" if _safe_float(interval_pct, 0) < 0 else "pro-kpi-delta-flat"),
            },
            {
                "label": "訊號燈號",
                "value": badge_text,
                "delta": f"分數 {signal_snapshot.get('score', 0)}",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "資料筆數",
                "value": len(df),
                "delta": market_type,
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "區間",
                "value": f"{start_date} ~ {end_date}",
                "delta": stock_label,
                "delta_class": "pro-kpi-delta-flat",
            },
        ]
    )

    render_pro_section("歷史K線")
    st.plotly_chart(_build_candlestick_chart(df, stock_label), use_container_width=True)

    g1, g2 = st.columns(2)
    with g1:
        st.plotly_chart(_build_kd_chart(df, stock_label), use_container_width=True)
    with g2:
        st.plotly_chart(_build_macd_chart(df, stock_label), use_container_width=True)

    render_pro_section("雷達評分 / 訊號 / 支撐壓力")

    left, right = st.columns([1, 1])

    with left:
        render_pro_info_card(
            "股神雷達評分",
            [
                ("趨勢", radar.get("trend", 50), ""),
                ("動能", radar.get("momentum", 50), ""),
                ("量能", radar.get("volume", 50), ""),
                ("位置", radar.get("position", 50), ""),
                ("結構", radar.get("structure", 50), ""),
                ("摘要", radar.get("summary", "—"), ""),
            ],
            chips=[badge_text],
        )

        render_pro_info_card(
            "訊號燈號",
            [
                ("均線趨勢", signal_snapshot.get("ma_trend", ("—", ""))[0], signal_snapshot.get("ma_trend", ("", "pro-flat"))[1]),
                ("KD交叉", signal_snapshot.get("kd_cross", ("—", ""))[0], signal_snapshot.get("kd_cross", ("", "pro-flat"))[1]),
                ("MACD趨勢", signal_snapshot.get("macd_trend", ("—", ""))[0], signal_snapshot.get("macd_trend", ("", "pro-flat"))[1]),
                ("價位狀態", signal_snapshot.get("price_vs_ma20", ("—", ""))[0], signal_snapshot.get("price_vs_ma20", ("", "pro-flat"))[1]),
                ("突破狀態", signal_snapshot.get("breakout_20d", ("—", ""))[0], signal_snapshot.get("breakout_20d", ("", "pro-flat"))[1]),
                ("量能狀態", signal_snapshot.get("volume_state", ("—", ""))[0], signal_snapshot.get("volume_state", ("", "pro-flat"))[1]),
            ],
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
            "策略摘要",
            [
                ("多空評語", signal_snapshot.get("comment", "—"), ""),
                ("趨勢解讀", sr_snapshot.get("comment_trend", "—"), ""),
                ("風險提醒", sr_snapshot.get("comment_risk", "—"), ""),
                ("觀察重點", sr_snapshot.get("comment_focus", "—"), ""),
                ("操作建議", sr_snapshot.get("comment_action", "—"), ""),
            ],
            chips=[market_type, badge_text],
        )

    render_pro_section("最近事件")

    event_options = ["全部", "MA", "KD", "MACD", "突破", "跌破"]
    cur_filter = _safe_str(st.session_state.get(_k("event_filter"), "全部"))
    if cur_filter not in event_options:
        st.session_state[_k("event_filter")] = "全部"

    st.selectbox("事件篩選", options=event_options, key=_k("event_filter"))

    filtered_event_df = event_df.copy()
    if not filtered_event_df.empty and st.session_state.get(_k("event_filter")) != "全部":
        filtered_event_df = filtered_event_df[filtered_event_df["事件分類"] == st.session_state.get(_k("event_filter"))].reset_index(drop=True)

    if filtered_event_df.empty:
        st.info("目前沒有符合條件的事件。")
    else:
        st.dataframe(filtered_event_df, use_container_width=True, hide_index=True)

    with st.expander("效能說明"):
        st.write("這版已做 cache、群組同步修正、上櫃歷史 fallback。")
        st.write("如果你還要更快，下一步我建議把排行榜與多股比較也共用這套 smart history。")


if __name__ == "__main__":
    main()
