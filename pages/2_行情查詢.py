# pages/2_行情查詢.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
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


def _safe_float(v: Any, default: float = np.nan) -> float:
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default


# =========================================================
# 自選股 / 搜尋 / 同步
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
    default_start = today - timedelta(days=180)
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

    st.session_state[_k("start_date_input")] = _to_pydate(
        st.session_state.get(_k("start_date_input")), default_start
    )
    st.session_state[_k("end_date_input")] = _to_pydate(
        st.session_state.get(_k("end_date_input")), default_end
    )

    _repair_state(group_map)


# =========================================================
# 資料整理
# =========================================================
def _prepare_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "日期" not in df.columns:
        return pd.DataFrame()

    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    for col in ["成交股數", "開盤價", "最高價", "最低價", "收盤價"]:
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


def _build_recent_events(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 3:
        return pd.DataFrame(columns=["日期", "事件類型", "事件名稱", "說明", "等級"])

    events = []

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        dt = pd.to_datetime(cur["日期"])

        if pd.notna(prev.get("MA20")) and pd.notna(cur.get("MA20")) and pd.notna(prev.get("MA60")) and pd.notna(cur.get("MA60")):
            if _cross_up(prev["MA20"], cur["MA20"], prev["MA60"], cur["MA60"]):
                events.append({"日期": dt, "事件類型": "MA交叉", "事件名稱": "MA20 黃金交叉 MA60", "說明": "中期均線轉強。", "等級": "強多"})
            elif _cross_down(prev["MA20"], cur["MA20"], prev["MA60"], cur["MA60"]):
                events.append({"日期": dt, "事件類型": "MA交叉", "事件名稱": "MA20 死亡交叉 MA60", "說明": "中期均線轉弱。", "等級": "強空"})

        if pd.notna(prev.get("K")) and pd.notna(cur.get("K")) and pd.notna(prev.get("D")) and pd.notna(cur.get("D")):
            if _cross_up(prev["K"], cur["K"], prev["D"], cur["D"]):
                events.append({"日期": dt, "事件類型": "KD交叉", "事件名稱": "KD 黃金交叉", "說明": "短線動能轉強。", "等級": "偏多"})
            elif _cross_down(prev["K"], cur["K"], prev["D"], cur["D"]):
                events.append({"日期": dt, "事件類型": "KD交叉", "事件名稱": "KD 死亡交叉", "說明": "短線動能轉弱。", "等級": "偏空"})

        if pd.notna(prev.get("DIF")) and pd.notna(cur.get("DIF")) and pd.notna(prev.get("DEA")) and pd.notna(cur.get("DEA")):
            if _cross_up(prev["DIF"], cur["DIF"], prev["DEA"], cur["DEA"]):
                events.append({"日期": dt, "事件類型": "MACD交叉", "事件名稱": "MACD 黃金交叉", "說明": "波段趨勢轉強。", "等級": "偏多"})
            elif _cross_down(prev["DIF"], cur["DIF"], prev["DEA"], cur["DEA"]):
                events.append({"日期": dt, "事件類型": "MACD交叉", "事件名稱": "MACD 死亡交叉", "說明": "波段趨勢轉弱。", "等級": "偏空"})

    ev = pd.DataFrame(events)
    if ev.empty:
        return pd.DataFrame(columns=["日期", "事件類型", "事件名稱", "說明", "等級"])
    return ev.sort_values("日期", ascending=False).reset_index(drop=True)


def _build_quote_strategy_summary(info: dict, signal: dict, sr: dict, radar: dict, events: pd.DataFrame) -> dict[str, str]:
    price = _safe_float(info.get("price"))
    prev_close = _safe_float(info.get("prev_close"))
    chg_pct = ((price / prev_close) - 1) * 100 if pd.notna(price) and pd.notna(prev_close) and prev_close != 0 else np.nan
    radar_avg = np.mean(
        [
            radar.get("trend", 50),
            radar.get("momentum", 50),
            radar.get("volume", 50),
            radar.get("position", 50),
            radar.get("structure", 50),
        ]
    )

    badge_text, _ = score_to_badge(signal.get("score", 0))
    latest_event = events.iloc[0]["事件名稱"] if events is not None and not events.empty else "近期無明確交叉事件"

    if pd.notna(chg_pct):
        if chg_pct >= 3:
            market_state = "今日強勢上攻，短線資金偏多。"
        elif chg_pct >= 0:
            market_state = "今日偏強震盪，仍維持相對優勢。"
        elif chg_pct <= -3:
            market_state = "今日明顯轉弱，短線壓力較大。"
        else:
            market_state = "今日偏弱震盪，宜觀察支撐防守。"
    else:
        market_state = "即時漲跌資訊不足。"

    sr_text = (
        f"短壓 {_fmt_num(sr.get('res_20'), 2)}、短撐 {_fmt_num(sr.get('sup_20'), 2)}；"
        f"波段壓力 {_fmt_num(sr.get('res_60'), 2)}、波段支撐 {_fmt_num(sr.get('sup_60'), 2)}。"
    )

    if radar_avg >= 75 and signal.get("score", 0) >= 3:
        action = "策略偏多，可優先觀察拉回不破支撐後的續強。"
    elif radar_avg >= 60 and signal.get("score", 0) >= 1:
        action = "策略中偏多，宜等量能放大或壓力突破確認。"
    elif radar_avg <= 35 or signal.get("score", 0) <= -3:
        action = "策略保守，未止跌前不宜搶短。"
    else:
        action = "策略中性，先看區間突破方向。"

    return {
        "定位": f"目前燈號【{badge_text}】；{market_state}",
        "關鍵事件": latest_event,
        "壓力支撐": sr_text,
        "策略": action,
    }


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
        title="行情查詢｜股神版",
        subtitle="即時行情、訊號燈號、支撐壓力、最近事件、策略摘要，維持你現有專案架構。",
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
        if st.button("帶入", use_container_width=True, type="primary"):
            target = _find_search_target(st.session_state.get(_k("search_input"), ""), search_rows)
            if target:
                st.session_state[_k("group")] = target["group"]
                st.session_state[_k("stock_code")] = target["code"]
                st.rerun()
            else:
                st.warning("找不到對應股票。")

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
    start_date = _to_pydate(st.session_state.get(_k("start_date_input")), date.today() - timedelta(days=180))
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

    with st.spinner("讀取即時與歷史資料中..."):
        info = get_realtime_stock_info(selected_code, stock_name, market_type)
        raw_df = get_history_data(
            stock_no=selected_code,
            stock_name=stock_name,
            market_type=market_type,
            start_date=start_date,
            end_date=end_date,
        )

    hist_df = _prepare_history_df(raw_df)
    if hist_df.empty:
        st.error("查無歷史資料，請更換股票或日期區間。")
        st.stop()

    signal = compute_signal_snapshot(hist_df)
    sr = compute_support_resistance_snapshot(hist_df)
    radar = compute_radar_scores(hist_df)
    events = _build_recent_events(hist_df)
    strategy = _build_quote_strategy_summary(info, signal, sr, radar, events)

    price = info.get("price")
    prev_close = info.get("prev_close")
    change = info.get("change")
    change_pct = info.get("change_pct")
    open_price = info.get("open")
    high_price = info.get("high")
    low_price = info.get("low")
    total_volume = info.get("total_volume")
    update_time = _safe_str(info.get("update_time"))

    badge_text, _ = score_to_badge(signal.get("score", 0))
    radar_avg = np.mean(
        [
            radar.get("trend", 50),
            radar.get("momentum", 50),
            radar.get("volume", 50),
            radar.get("position", 50),
            radar.get("structure", 50),
        ]
    )

    render_pro_kpi_row(
        [
            {"label": "股票", "value": stock_label, "delta": market_type, "delta_class": "pro-kpi-delta-flat"},
            {
                "label": "現價",
                "value": _fmt_num(price, 2),
                "delta": f"{_fmt_num(change, 2)} / {_fmt_pct(change_pct)}",
                "delta_class": "pro-kpi-delta-flat",
            },
            {"label": "訊號評級", "value": badge_text, "delta": f"分數 {signal.get('score', 0)}", "delta_class": "pro-kpi-delta-flat"},
            {"label": "雷達均分", "value": f"{float(radar_avg):.1f}", "delta": radar.get("summary", ""), "delta_class": "pro-kpi-delta-flat"},
        ]
    )

    top1, top2 = st.columns(2)

    with top1:
        render_pro_info_card(
            "即時行情",
            [
                ("更新時間", update_time or "—", ""),
                ("開盤", _fmt_num(open_price, 2), ""),
                ("最高", _fmt_num(high_price, 2), "pro-up"),
                ("最低", _fmt_num(low_price, 2), "pro-down"),
                ("昨收", _fmt_num(prev_close, 2), ""),
                ("總量", _fmt_num(total_volume, 0), ""),
            ],
            chips=["即時資料" if info.get("ok") else "資料異常"],
        )

    with top2:
        render_pro_info_card(
            "訊號燈號",
            [
                ("均線趨勢", signal.get("ma_trend", ("中性", "pro-flat"))[0], signal.get("ma_trend", ("中性", "pro-flat"))[1]),
                ("KD 交叉", signal.get("kd_cross", ("中性", "pro-flat"))[0], signal.get("kd_cross", ("中性", "pro-flat"))[1]),
                ("MACD", signal.get("macd_trend", ("中性", "pro-flat"))[0], signal.get("macd_trend", ("中性", "pro-flat"))[1]),
                ("價位 vs MA20", signal.get("price_vs_ma20", ("中性", "pro-flat"))[0], signal.get("price_vs_ma20", ("中性", "pro-flat"))[1]),
                ("20日突破", signal.get("breakout_20d", ("中性", "pro-flat"))[0], signal.get("breakout_20d", ("中性", "pro-flat"))[1]),
                ("量能狀態", signal.get("volume_state", ("中性", "pro-flat"))[0], signal.get("volume_state", ("中性", "pro-flat"))[1]),
            ],
            chips=[badge_text],
        )

    mid1, mid2 = st.columns(2)

    with mid1:
        render_pro_info_card(
            "支撐 / 壓力",
            [
                ("20日壓力", _fmt_num(sr.get("res_20"), 2), "pro-down"),
                ("20日支撐", _fmt_num(sr.get("sup_20"), 2), "pro-up"),
                ("60日壓力", _fmt_num(sr.get("res_60"), 2), "pro-down"),
                ("60日支撐", _fmt_num(sr.get("sup_60"), 2), "pro-up"),
                ("距20日壓力", _fmt_pct(sr.get("dist_res_20_pct"), 2), ""),
                ("距20日支撐", _fmt_pct(sr.get("dist_sup_20_pct"), 2), ""),
            ],
            chips=[
                sr.get("pressure_signal", ("中性", ""))[0],
                sr.get("support_signal", ("中性", ""))[0],
                sr.get("break_signal", ("區間內", ""))[0],
            ],
        )

    with mid2:
        render_pro_info_card(
            "策略摘要",
            [
                ("目前定位", strategy["定位"], ""),
                ("關鍵事件", strategy["關鍵事件"], ""),
                ("壓力支撐", strategy["壓力支撐"], ""),
                ("策略建議", strategy["策略"], ""),
            ],
        )

    render_pro_section("最近事件摘要")

    if events.empty:
        st.info("目前沒有最近事件。")
    else:
        recent = events.head(8).copy()
        recent["日期"] = pd.to_datetime(recent["日期"]).dt.strftime("%Y-%m-%d")
        st.dataframe(
            recent[["日期", "事件類型", "事件名稱", "說明", "等級"]],
            use_container_width=True,
            hide_index=True,
        )

    bottom1, bottom2 = st.columns(2)

    with bottom1:
        render_pro_section("雷達評分")
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

    with bottom2:
        render_pro_section("支撐壓力說明")
        render_pro_info_card(
            "區間解讀",
            [
                ("趨勢判讀", sr.get("comment_trend", "—"), ""),
                ("風險提醒", sr.get("comment_risk", "—"), ""),
                ("觀察重點", sr.get("comment_focus", "—"), ""),
                ("操作提醒", sr.get("comment_action", "—"), ""),
            ],
        )

    render_pro_section("近期日線摘要")
    tail_cols = [
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
        "K",
        "D",
        "DIF",
        "DEA",
        "MACD_HIST",
        "漲跌幅(%)",
    ]
    show_cols = [c for c in tail_cols if c in hist_df.columns]
    detail_df = hist_df[show_cols].tail(60).copy()
    detail_df["日期"] = pd.to_datetime(detail_df["日期"]).dt.strftime("%Y-%m-%d")
    st.dataframe(detail_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
