# pages/3_歷史K線分析.py
from __future__ import annotations

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
PFX = "hist_"


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
    try:
        x = pd.to_datetime(v, errors="coerce")
        if pd.notna(x):
            return x.date()
    except Exception:
        pass
    return fallback


def _fmt_num(v: Any, digits: int = 2) -> str:
    return format_number(v, digits)


def _fmt_pct(v: Any, digits: int = 2) -> str:
    try:
        if pd.isna(v):
            return "—"
    except Exception:
        pass
    try:
        return f"{float(v):+,.{digits}f}%"
    except Exception:
        return "—"


def _css_by_value(v: Any) -> str:
    try:
        x = float(v)
    except Exception:
        return "pro-flat"
    if x > 0:
        return "pro-up"
    if x < 0:
        return "pro-down"
    return "pro-flat"


def _css_by_diff(a: Any, b: Any) -> str:
    try:
        if pd.isna(a) or pd.isna(b):
            return "pro-flat"
    except Exception:
        pass
    try:
        d = float(a) - float(b)
    except Exception:
        return "pro-flat"
    return _css_by_value(d)


# =========================================================
# watchlist / 搜尋 / 同步
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
                    name = _safe_str(item.get("name"))
                    market = _safe_str(item.get("market")) or "上市"
                    if code:
                        group_map[g].append(
                            {
                                "code": code,
                                "name": name or code,
                                "market": market,
                                "label": f"{code} {name or code}",
                            }
                        )

    if not group_map:
        all_df = get_all_code_name_map("")
        if isinstance(all_df, pd.DataFrame) and not all_df.empty:
            rows = []
            for _, row in all_df.head(100).iterrows():
                code = _safe_str(row.get("code"))
                name = _safe_str(row.get("name"))
                market = _safe_str(row.get("market")) or "上市"
                if code:
                    rows.append(
                        {
                            "code": code,
                            "name": name or code,
                            "market": market,
                            "label": f"{code} {name or code}",
                        }
                    )
            if rows:
                group_map["全部股票"] = rows

    return group_map


def _build_search_rows(group_map: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for group_name, items in group_map.items():
        for item in items:
            rows.append(
                {
                    "group": group_name,
                    "code": _safe_str(item.get("code")),
                    "name": _safe_str(item.get("name")),
                    "market": _safe_str(item.get("market")),
                    "label": _safe_str(item.get("label")),
                    "blob": f"{group_name} {item.get('code','')} {item.get('name','')} {item.get('label','')}".lower(),
                }
            )
    return rows


def _find_search_target(keyword: str, search_rows: list[dict[str, str]]) -> dict[str, str] | None:
    q = _safe_str(keyword).lower()
    if not q:
        return None

    for row in search_rows:
        if q == row["code"].lower():
            return row
    for row in search_rows:
        if q == row["name"].lower():
            return row
    for row in search_rows:
        if q == row["label"].lower():
            return row

    prefix_hits = [r for r in search_rows if r["code"].lower().startswith(q) or r["name"].lower().startswith(q)]
    if prefix_hits:
        return prefix_hits[0]

    contain_hits = [r for r in search_rows if q in r["blob"]]
    if contain_hits:
        return contain_hits[0]

    return None


def _repair_state(group_map: dict[str, list[dict[str, str]]]):
    groups = list(group_map.keys())
    if not groups:
        st.session_state[_k("group")] = ""
        st.session_state[_k("stock_code")] = ""
        return

    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    if current_group not in group_map:
        current_group = groups[0]
        st.session_state[_k("group")] = current_group

    codes = [x["code"] for x in group_map.get(current_group, [])]
    current_code = _safe_str(st.session_state.get(_k("stock_code"), ""))
    if current_code not in codes:
        st.session_state[_k("stock_code")] = codes[0] if codes else ""


def _on_group_change(group_map: dict[str, list[dict[str, str]]]):
    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    st.session_state[_k("stock_code")] = items[0]["code"] if items else ""


def _init_state(group_map: dict[str, list[dict[str, str]]]):
    saved = load_last_query_state()
    today = date.today()
    default_start = today - timedelta(days=365)
    default_end = today

    if _k("group") not in st.session_state:
        st.session_state[_k("group")] = _safe_str(saved.get("quick_group", ""))

    if _k("stock_code") not in st.session_state:
        st.session_state[_k("stock_code")] = _safe_str(saved.get("quick_stock_code", ""))

    if _k("search_input") not in st.session_state:
        st.session_state[_k("search_input")] = ""

    if _k("start_date_input") not in st.session_state:
        st.session_state[_k("start_date_input")] = parse_date_safe(saved.get("home_start"), default_start)

    if _k("end_date_input") not in st.session_state:
        st.session_state[_k("end_date_input")] = parse_date_safe(saved.get("home_end"), default_end)

    if _k("event_types") not in st.session_state:
        st.session_state[_k("event_types")] = []

    st.session_state[_k("start_date_input")] = _to_pydate(
        st.session_state.get(_k("start_date_input")), default_start
    )
    st.session_state[_k("end_date_input")] = _to_pydate(
        st.session_state.get(_k("end_date_input")), default_end
    )

    _repair_state(group_map)


# =========================================================
# 歷史資料與指標
# =========================================================
def _prepare_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "日期" not in df.columns:
        return pd.DataFrame()

    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    numeric_cols = ["成交股數", "開盤價", "最高價", "最低價", "收盤價"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["收盤價"]).copy()

    close = df["收盤價"]
    high = df["最高價"]
    low = df["最低價"]

    for n in [5, 10, 20, 60, 120, 240]:
        df[f"MA{n}"] = close.rolling(n).mean()

    low_9 = low.rolling(9).min()
    high_9 = high.rolling(9).max()
    rsv = (close - low_9) / (high_9 - low_9).replace(0, np.nan) * 100
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


def _cross_up(a1, a2, b1, b2) -> bool:
    return pd.notna(a1) and pd.notna(a2) and pd.notna(b1) and pd.notna(b2) and a1 <= b1 and a2 > b2


def _cross_down(a1, a2, b1, b2) -> bool:
    return pd.notna(a1) and pd.notna(a2) and pd.notna(b1) and pd.notna(b2) and a1 >= b1 and a2 < b2


def _detect_pivots(df: pd.DataFrame, window: int = 3) -> tuple[list[int], list[int]]:
    if df.empty or len(df) < window * 2 + 1:
        return [], []

    highs = df["最高價"].values
    lows = df["最低價"].values

    peaks_idx = []
    troughs_idx = []

    for i in range(window, len(df) - window):
        h = highs[i]
        l = lows[i]
        if np.isfinite(h) and h == np.max(highs[i - window:i + window + 1]):
            peaks_idx.append(i)
        if np.isfinite(l) and l == np.min(lows[i - window:i + window + 1]):
            troughs_idx.append(i)

    return peaks_idx, troughs_idx


def _build_event_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 3:
        return pd.DataFrame(columns=["日期", "事件類型", "事件名稱", "說明", "等級", "idx"])

    events = []
    peak_idx, trough_idx = _detect_pivots(df, window=3)
    peak_set = set(peak_idx)
    trough_set = set(trough_idx)

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        dt = pd.to_datetime(cur["日期"])

        if _cross_up(prev["MA20"], cur["MA20"], prev["MA60"], cur["MA60"]):
            events.append(
                {"日期": dt, "事件類型": "MA交叉", "事件名稱": "MA20 黃金交叉 MA60", "說明": "中期均線轉強。", "等級": "強多", "idx": i}
            )
        elif _cross_down(prev["MA20"], cur["MA20"], prev["MA60"], cur["MA60"]):
            events.append(
                {"日期": dt, "事件類型": "MA交叉", "事件名稱": "MA20 死亡交叉 MA60", "說明": "中期均線轉弱。", "等級": "強空", "idx": i}
            )

        if _cross_up(prev["K"], cur["K"], prev["D"], cur["D"]):
            events.append(
                {"日期": dt, "事件類型": "KD交叉", "事件名稱": "KD 黃金交叉", "說明": "短線動能轉強。", "等級": "偏多", "idx": i}
            )
        elif _cross_down(prev["K"], cur["K"], prev["D"], cur["D"]):
            events.append(
                {"日期": dt, "事件類型": "KD交叉", "事件名稱": "KD 死亡交叉", "說明": "短線動能轉弱。", "等級": "偏空", "idx": i}
            )

        if _cross_up(prev["DIF"], cur["DIF"], prev["DEA"], cur["DEA"]):
            events.append(
                {"日期": dt, "事件類型": "MACD交叉", "事件名稱": "MACD 黃金交叉", "說明": "波段趨勢轉強。", "等級": "偏多", "idx": i}
            )
        elif _cross_down(prev["DIF"], cur["DIF"], prev["DEA"], cur["DEA"]):
            events.append(
                {"日期": dt, "事件類型": "MACD交叉", "事件名稱": "MACD 死亡交叉", "說明": "波段趨勢轉弱。", "等級": "偏空", "idx": i}
            )

        if i in trough_set:
            events.append(
                {"日期": dt, "事件類型": "轉折點", "事件名稱": "起漲點", "說明": "局部低點轉折。", "等級": "觀察", "idx": i}
            )
        if i in peak_set:
            events.append(
                {"日期": dt, "事件類型": "轉折點", "事件名稱": "起跌點", "說明": "局部高點轉折。", "等級": "觀察", "idx": i}
            )

    ev = pd.DataFrame(events)
    if ev.empty:
        return pd.DataFrame(columns=["日期", "事件類型", "事件名稱", "說明", "等級", "idx"])
    return ev.sort_values("日期", ascending=False).reset_index(drop=True)


# =========================================================
# 圖表
# =========================================================
def _plot_kline(df: pd.DataFrame, stock_label: str, events: pd.DataFrame) -> go.Figure:
    x = pd.to_datetime(df["日期"])

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.50, 0.15, 0.17, 0.18],
    )

    fig.add_trace(
        go.Candlestick(
            x=x,
            open=df["開盤價"],
            high=df["最高價"],
            low=df["最低價"],
            close=df["收盤價"],
            name="K線",
        ),
        row=1,
        col=1,
    )

    for ma in [5, 10, 20, 60, 120, 240]:
        col = f"MA{ma}"
        if col in df.columns:
            fig.add_trace(
                go.Scatter(x=x, y=df[col], mode="lines", name=col),
                row=1,
                col=1,
            )

    peak_idx, trough_idx = _detect_pivots(df, window=3)
    if trough_idx:
        fig.add_trace(
            go.Scatter(
                x=df.loc[trough_idx, "日期"],
                y=df.loc[trough_idx, "最低價"] * 0.995,
                mode="markers",
                name="起漲點",
                marker=dict(symbol="triangle-up", size=10),
                text=["起漲點"] * len(trough_idx),
                hovertemplate="%{x}<br>%{text}<extra></extra>",
            ),
            row=1,
            col=1,
        )
    if peak_idx:
        fig.add_trace(
            go.Scatter(
                x=df.loc[peak_idx, "日期"],
                y=df.loc[peak_idx, "最高價"] * 1.005,
                mode="markers",
                name="起跌點",
                marker=dict(symbol="triangle-down", size=10),
                text=["起跌點"] * len(peak_idx),
                hovertemplate="%{x}<br>%{text}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    if events is not None and not events.empty:
        plot_events = events[events["事件類型"].isin(["MA交叉", "KD交叉", "MACD交叉"])].copy()
        if not plot_events.empty:
            mark_x = []
            mark_y = []
            mark_text = []
            for _, row in plot_events.iterrows():
                idx = int(row["idx"])
                if 0 <= idx < len(df):
                    mark_x.append(df.iloc[idx]["日期"])
                    mark_y.append(df.iloc[idx]["收盤價"])
                    mark_text.append(row["事件名稱"])
            if mark_x:
                fig.add_trace(
                    go.Scatter(
                        x=mark_x,
                        y=mark_y,
                        mode="markers",
                        name="交叉事件",
                        marker=dict(symbol="diamond", size=8),
                        text=mark_text,
                        hovertemplate="%{x}<br>%{text}<extra></extra>",
                    ),
                    row=1,
                    col=1,
                )

    fig.add_trace(go.Bar(x=x, y=df["成交股數"], name="成交股數"), row=2, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["K"], mode="lines", name="K"), row=3, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["D"], mode="lines", name="D"), row=3, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["DIF"], mode="lines", name="DIF"), row=4, col=1)
    fig.add_trace(go.Scatter(x=x, y=df["DEA"], mode="lines", name="DEA"), row=4, col=1)
    fig.add_trace(go.Bar(x=x, y=df["MACD_HIST"], name="MACD_HIST"), row=4, col=1)

    fig.update_layout(
        title=f"{stock_label}｜歷史K線分析",
        xaxis_rangeslider_visible=False,
        height=1040,
        margin=dict(l=20, r=20, t=60, b=20),
        legend=dict(orientation="h", y=1.03, x=0),
    )
    return fig


def _plot_radar(radar: dict[str, Any], stock_label: str) -> go.Figure:
    labels_map = {
        "trend": "趨勢",
        "momentum": "動能",
        "volume": "量能",
        "position": "位置",
        "structure": "結構",
    }
    keys = ["trend", "momentum", "volume", "position", "structure"]
    theta = [labels_map[k] for k in keys]
    r = [float(radar.get(k, 50)) for k in keys]

    theta = theta + [theta[0]]
    r = r + [r[0]]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(r=r, theta=theta, fill="toself", name=stock_label))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=False,
        height=420,
        margin=dict(l=20, r=20, t=40, b=20),
    )
    return fig


# =========================================================
# 主頁
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    group_map = _build_group_stock_map()
    search_rows = _build_search_rows(group_map)
    _init_state(group_map)

    render_pro_hero(
        title="歷史K線分析",
        subtitle="K線・MA・KD・MACD・雷達評分・支撐壓力・起漲起跌點・交叉事件・事件篩選",
    )

    # 快速搜尋
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
        if st.button("帶入", use_container_width=True, type="primary"):
            target = _find_search_target(st.session_state.get(_k("search_input"), ""), search_rows)
            if target:
                st.session_state[_k("group")] = target["group"]
                st.session_state[_k("stock_code")] = target["code"]
                st.rerun()
            else:
                st.warning("找不到對應股票。")

    # 查詢條件
    render_pro_section("查詢條件")
    _repair_state(group_map)

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

    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    code_to_item = {x["code"]: x for x in items}
    code_options = [x["code"] for x in items]

    if st.session_state.get(_k("stock_code"), "") not in code_options:
        st.session_state[_k("stock_code")] = code_options[0] if code_options else ""

    with c2:
        st.selectbox(
            "群組股票",
            options=code_options,
            key=_k("stock_code"),
            format_func=lambda code: code_to_item.get(code, {}).get("label", code),
        )

    with c3:
        st.date_input("開始日期", key=_k("start_date_input"))

    with c4:
        st.date_input("結束日期", key=_k("end_date_input"))

    selected_group = _safe_str(st.session_state.get(_k("group"), ""))
    selected_code = _safe_str(st.session_state.get(_k("stock_code"), ""))
    start_date = _to_pydate(st.session_state.get(_k("start_date_input")), date.today() - timedelta(days=365))
    end_date = _to_pydate(st.session_state.get(_k("end_date_input")), date.today())

    if not selected_code:
        st.error("目前沒有可查詢股票。")
        st.stop()

    selected_item = code_to_item.get(selected_code, {})
    manual_name = _safe_str(selected_item.get("name"))
    manual_market = _safe_str(selected_item.get("market")) or "上市"

    st.caption(f"目前實際查詢值：群組【{selected_group}】 / 股票【{selected_code} {manual_name}】")

    if start_date > end_date:
        st.error("開始日期不可大於結束日期。")
        st.stop()

    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code=selected_code,
        home_start=start_date,
        home_end=end_date,
    )

    all_code_name_df = get_all_code_name_map("")
    stock_name, market_type = get_stock_name_and_market(selected_code, all_code_name_df, manual_name)
    market_type = manual_market or market_type or "上市"
    stock_label = f"{selected_code} {stock_name}"

    with st.spinner("讀取歷史資料中..."):
        raw_df = get_history_data(
            stock_no=selected_code,
            stock_name=stock_name,
            market_type=market_type,
            start_date=start_date,
            end_date=end_date,
        )

    df = _prepare_history_df(raw_df)
    if df.empty:
        st.error("查無歷史資料，請更換股票或日期區間。")
        st.stop()

    signal = compute_signal_snapshot(df)
    sr = compute_support_resistance_snapshot(df)
    radar = compute_radar_scores(df)
    events = _build_event_df(df)

    last = df.iloc[-1]
    prev_close = df["收盤價"].iloc[-2] if len(df) >= 2 else last["收盤價"]
    chg = (last["收盤價"] - prev_close) if pd.notna(prev_close) else None
    chg_pct = ((last["收盤價"] / prev_close) - 1) * 100 if pd.notna(prev_close) and prev_close not in [0, None] else None

    badge_text, badge_css = score_to_badge(signal.get("score", 0))
    radar_avg = np.mean(
        [
            radar.get("trend", 50),
            radar.get("momentum", 50),
            radar.get("volume", 50),
            radar.get("position", 50),
            radar.get("structure", 50),
        ]
    )

    # KPI
    render_pro_kpi_row(
        [
            {"label": "股票", "value": stock_label, "delta": market_type, "delta_class": "pro-kpi-delta-flat"},
            {
                "label": "最新收盤",
                "value": _fmt_num(last["收盤價"], 2),
                "delta": f"{_fmt_num(chg, 2)} / {_fmt_pct(chg_pct)}" if chg is not None else "—",
                "delta_class": "pro-kpi-delta-flat",
            },
            {"label": "訊號評級", "value": badge_text, "delta": f"分數 {signal.get('score', 0)}", "delta_class": "pro-kpi-delta-flat"},
            {"label": "雷達均分", "value": f"{float(radar_avg):.1f}", "delta": radar.get("summary", ""), "delta_class": "pro-kpi-delta-flat"},
        ]
    )

    # 摘要卡
    info1, info2 = st.columns(2)
    with info1:
        render_pro_info_card(
            "支撐 / 壓力",
            [
                ("20日壓力", _fmt_num(sr.get("res_20"), 2), "pro-down"),
                ("20日支撐", _fmt_num(sr.get("sup_20"), 2), "pro-up"),
                ("60日壓力", _fmt_num(sr.get("res_60"), 2), "pro-down"),
                ("60日支撐", _fmt_num(sr.get("sup_60"), 2), "pro-up"),
            ],
            chips=[
                sr.get("pressure_signal", ("中性", ""))[0],
                sr.get("support_signal", ("中性", ""))[0],
                sr.get("break_signal", ("區間內", ""))[0],
            ],
        )

    with info2:
        render_pro_info_card(
            "訊號摘要",
            [
                ("均線趨勢", signal.get("ma_trend", ("中性", "pro-flat"))[0], signal.get("ma_trend", ("中性", "pro-flat"))[1]),
                ("KD 交叉", signal.get("kd_cross", ("中性", "pro-flat"))[0], signal.get("kd_cross", ("中性", "pro-flat"))[1]),
                ("MACD", signal.get("macd_trend", ("中性", "pro-flat"))[0], signal.get("macd_trend", ("中性", "pro-flat"))[1]),
                ("價位 vs MA20", signal.get("price_vs_ma20", ("中性", "pro-flat"))[0], signal.get("price_vs_ma20", ("中性", "pro-flat"))[1]),
            ],
            chips=[badge_text, f"雷達均分 {radar_avg:.1f}"],
        )

    # K線圖
    render_pro_section("K線 / 均線 / 成交量 / KD / MACD")
    st.plotly_chart(_plot_kline(df, stock_label, events), use_container_width=True)

    # 事件摘要
    recent_events = events.head(8).copy()
    cross_events = recent_events[recent_events["事件類型"].isin(["MA交叉", "KD交叉", "MACD交叉"])].copy()

    s1, s2, s3, s4 = st.columns(4)
    with s1:
        latest_event = recent_events["事件名稱"].iloc[0] if not recent_events.empty else "無"
        render_pro_info_card("最新事件", [("最近觸發", latest_event, "")])
    with s2:
        render_pro_info_card("壓力狀態", [(sr.get("pressure_signal", ("中性", ""))[0], sr.get("comment_risk", "—"), "")])
    with s3:
        render_pro_info_card("支撐狀態", [(sr.get("support_signal", ("中性", ""))[0], sr.get("comment_focus", "—"), "")])
    with s4:
        render_pro_info_card("操作提醒", [(sr.get("break_signal", ("區間內", ""))[0], sr.get("comment_action", "—"), "")])

    # 雷達 / 事件說明
    left, right = st.columns([1, 1])

    with left:
        render_pro_section("雷達評分")
        st.plotly_chart(_plot_radar(radar, stock_label), use_container_width=True)
        radar_df = pd.DataFrame(
            {
                "項目": ["趨勢", "動能", "量能", "位置", "結構"],
                "分數": [
                    radar.get("trend", 50),
                    radar.get("momentum", 50),
                    radar.get("volume", 50),
                    radar.get("position", 50),
                    radar.get("structure", 50),
                ],
            }
        )
        st.dataframe(radar_df, use_container_width=True, hide_index=True)

    with right:
        render_pro_section("事件 / 結構解讀")
        render_pro_info_card(
            "區間解讀",
            [
                ("趨勢判讀", sr.get("comment_trend", "—"), ""),
                ("風險提醒", sr.get("comment_risk", "—"), ""),
                ("觀察重點", sr.get("comment_focus", "—"), ""),
                ("操作提醒", sr.get("comment_action", "—"), ""),
            ],
        )

    # 事件篩選
    render_pro_section("事件說明與篩選", "可依事件類型查看起漲點 / 起跌點 / MA交叉 / KD交叉 / MACD交叉")

    event_type_options = ["全部"]
    if not events.empty:
        event_type_options += sorted(events["事件類型"].dropna().unique().tolist())

    f1, f2 = st.columns([2, 5])
    with f1:
        selected_event_type = st.selectbox("事件篩選", event_type_options, key=_k("event_type_filter"))
    with f2:
        st.markdown(
            """
            <div style="padding-top: 28px;">
            起漲點 / 起跌點：局部轉折。MA / KD / MACD 交叉：用來觀察短中期方向與動能切換。
            </div>
            """,
            unsafe_allow_html=True,
        )

    if events.empty:
        st.info("目前沒有偵測到事件。")
    else:
        show_events = events.copy()
        if selected_event_type != "全部":
            show_events = show_events[show_events["事件類型"] == selected_event_type].copy()
        show_events["日期"] = pd.to_datetime(show_events["日期"]).dt.strftime("%Y-%m-%d")
        st.dataframe(show_events[["日期", "事件類型", "事件名稱", "說明", "等級"]], use_container_width=True, hide_index=True)

    # 最近事件摘要表
    render_pro_section("最近事件摘要")
    if recent_events.empty:
        st.info("目前沒有最近事件。")
    else:
        temp = recent_events.copy()
        temp["日期"] = pd.to_datetime(temp["日期"]).dt.strftime("%Y-%m-%d")
        st.dataframe(temp[["日期", "事件類型", "事件名稱", "說明", "等級"]], use_container_width=True, hide_index=True)

    # 技術指標明細
    render_pro_section("技術指標明細")
    detail_cols = [
        "日期",
        "開盤價",
        "最高價",
        "最低價",
        "收盤價",
        "成交股數",
        "MA5",
        "MA10",
        "MA20",
        "MA60",
        "MA120",
        "MA240",
        "K",
        "D",
        "J",
        "DIF",
        "DEA",
        "MACD_HIST",
        "漲跌幅(%)",
    ]
    show_cols = [c for c in detail_cols if c in df.columns]
    detail_df = df[show_cols].tail(120).copy()
    detail_df["日期"] = pd.to_datetime(detail_df["日期"]).dt.strftime("%Y-%m-%d")
    st.dataframe(detail_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
