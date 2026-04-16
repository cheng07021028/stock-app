# pages/5_排行榜.py
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

PAGE_TITLE = "排行榜"
PFX = "rank_"


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
# watchlist / state
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


def _init_state(group_map: dict[str, list[dict[str, str]]]):
    saved = load_last_query_state()
    today = date.today()
    default_start = today - timedelta(days=90)
    default_end = today

    groups = list(group_map.keys())

    if _k("group") not in st.session_state:
        saved_group = _safe_str(saved.get("quick_group", ""))
        st.session_state[_k("group")] = saved_group if saved_group in groups else (groups[0] if groups else "全部股票")

    if _k("start_date") not in st.session_state:
        st.session_state[_k("start_date")] = parse_date_safe(saved.get("home_start"), default_start)

    if _k("end_date") not in st.session_state:
        st.session_state[_k("end_date")] = parse_date_safe(saved.get("home_end"), default_end)

    if _k("sort_by") not in st.session_state:
        st.session_state[_k("sort_by")] = "綜合評級"

    if _k("sort_asc") not in st.session_state:
        st.session_state[_k("sort_asc")] = False

    if _k("top_n") not in st.session_state:
        st.session_state[_k("top_n")] = 30

    if _k("min_days") not in st.session_state:
        st.session_state[_k("min_days")] = 20

    st.session_state[_k("start_date")] = _to_pydate(st.session_state.get(_k("start_date")), default_start)
    st.session_state[_k("end_date")] = _to_pydate(st.session_state.get(_k("end_date")), default_end)


# =========================================================
# 歷史資料整理
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
    df["J"] = 3 * df["K"] - 2 * df["D"]

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["DIF"] - df["DEA"]

    df["漲跌幅(%)"] = close.pct_change() * 100
    return df


# =========================================================
# 排行計算
# =========================================================
@st.cache_data(ttl=600, show_spinner=False)
def _get_stock_rank_snapshot(code: str, manual_name: str, market_type: str, start_date: date, end_date: date) -> dict[str, Any]:
    all_code_name_df = get_all_code_name_map("")
    stock_name, market_type2 = get_stock_name_and_market(code, all_code_name_df, manual_name)
    market = market_type or market_type2 or "上市"

    raw_df = get_history_data(
        stock_no=code,
        stock_name=stock_name,
        market_type=market,
        start_date=start_date,
        end_date=end_date,
    )
    df = _prepare_history_df(raw_df)
    if df.empty or len(df) < 2:
        return {"ok": False, "code": code, "name": stock_name, "market": market}

    last = df.iloc[-1]
    first = df.iloc[0]

    signal = compute_signal_snapshot(df)
    sr = compute_support_resistance_snapshot(df)
    radar = compute_radar_scores(df)

    last_close = _safe_float(last.get("收盤價"))
    first_close = _safe_float(first.get("收盤價"))
    interval_pct = ((last_close / first_close) - 1) * 100 if pd.notna(last_close) and pd.notna(first_close) and first_close != 0 else np.nan

    radar_avg = float(
        np.mean(
            [
                radar.get("trend", 50),
                radar.get("momentum", 50),
                radar.get("volume", 50),
                radar.get("position", 50),
                radar.get("structure", 50),
            ]
        )
    )

    signal_score = _safe_float(signal.get("score"), 0.0)
    # 綜合評級：訊號偏向方向 + 雷達均分 + 區間漲跌
    composite = radar_avg + signal_score * 8 + (0 if pd.isna(interval_pct) else interval_pct * 0.6)

    dist_res_20 = _safe_float(sr.get("dist_res_20_pct"))
    dist_sup_20 = _safe_float(sr.get("dist_sup_20_pct"))

    return {
        "ok": True,
        "code": code,
        "name": stock_name,
        "market": market,
        "days": len(df),
        "最新收盤": last_close,
        "區間漲跌幅(%)": interval_pct,
        "訊號分數": signal_score,
        "綜合評級": composite,
        "雷達均分": radar_avg,
        "趨勢": radar.get("trend", 50),
        "動能": radar.get("momentum", 50),
        "量能": radar.get("volume", 50),
        "位置": radar.get("position", 50),
        "結構": radar.get("structure", 50),
        "燈號": score_to_badge(signal_score)[0],
        "訊號說明": signal.get("comment", "—"),
        "壓力訊號": sr.get("pressure_signal", ("中性", "pro-flat"))[0],
        "支撐訊號": sr.get("support_signal", ("中性", "pro-flat"))[0],
        "區間訊號": sr.get("break_signal", ("區間內", "pro-flat"))[0],
        "20日壓力": sr.get("res_20"),
        "20日支撐": sr.get("sup_20"),
        "60日壓力": sr.get("res_60"),
        "60日支撐": sr.get("sup_60"),
        "距20日壓力(%)": dist_res_20,
        "距20日支撐(%)": dist_sup_20,
    }


def _build_rank_df(group_map: dict[str, list[dict[str, str]]], selected_group: str, start_date: date, end_date: date, min_days: int) -> pd.DataFrame:
    rows = []
    target_groups = [selected_group] if selected_group != "全部群組" else list(group_map.keys())

    for group in target_groups:
        for item in group_map.get(group, []):
            snap = _get_stock_rank_snapshot(
                code=_safe_str(item.get("code")),
                manual_name=_safe_str(item.get("name")),
                market_type=_safe_str(item.get("market")) or "上市",
                start_date=start_date,
                end_date=end_date,
            )
            if not snap.get("ok"):
                continue
            if int(snap.get("days", 0)) < int(min_days):
                continue
            snap["群組"] = group
            snap["股票"] = f"{snap['code']} {snap['name']}"
            rows.append(snap)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    numeric_cols = [
        "最新收盤",
        "區間漲跌幅(%)",
        "訊號分數",
        "綜合評級",
        "雷達均分",
        "趨勢",
        "動能",
        "量能",
        "位置",
        "結構",
        "20日壓力",
        "20日支撐",
        "60日壓力",
        "60日支撐",
        "距20日壓力(%)",
        "距20日支撐(%)",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# =========================================================
# 主頁
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    group_map = _build_group_stock_map()
    _init_state(group_map)

    render_pro_hero(
        title="排行榜｜股神版",
        subtitle="綜合評級、訊號分數、雷達均分、支撐壓力、區間漲跌幅，一頁排序比較。",
    )

    render_pro_section("查詢條件")

    groups = ["全部群組"] + list(group_map.keys())
    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2])

    with c1:
        st.selectbox("群組", groups, key=_k("group"))
    with c2:
        st.date_input("開始日期", key=_k("start_date"))
    with c3:
        st.date_input("結束日期", key=_k("end_date"))
    with c4:
        st.selectbox(
            "排序欄位",
            ["綜合評級", "區間漲跌幅(%)", "訊號分數", "雷達均分", "距20日壓力(%)", "距20日支撐(%)"],
            key=_k("sort_by"),
        )
    with c5:
        st.number_input("顯示筆數", min_value=5, max_value=100, step=5, key=_k("top_n"))

    d1, d2, d3 = st.columns([2, 2, 2])
    with d1:
        st.number_input("最少交易天數", min_value=2, max_value=120, step=1, key=_k("min_days"))
    with d2:
        st.checkbox("升冪排序", key=_k("sort_asc"))
    with d3:
        run_clicked = st.button("更新排行榜", use_container_width=True, type="primary")

    selected_group = _safe_str(st.session_state.get(_k("group"), "全部群組"))
    start_date = _to_pydate(st.session_state.get(_k("start_date")), date.today() - timedelta(days=90))
    end_date = _to_pydate(st.session_state.get(_k("end_date")), date.today())
    sort_by = _safe_str(st.session_state.get(_k("sort_by"), "綜合評級"))
    sort_asc = bool(st.session_state.get(_k("sort_asc"), False))
    top_n = int(st.session_state.get(_k("top_n"), 30))
    min_days = int(st.session_state.get(_k("min_days"), 20))

    if start_date > end_date:
        st.error("開始日期不可大於結束日期。")
        st.stop()

    save_last_query_state(
        quick_group="" if selected_group == "全部群組" else selected_group,
        quick_stock_code="",
        home_start=start_date,
        home_end=end_date,
    )

    if run_clicked or True:
        with st.spinner("計算排行榜中..."):
            rank_df = _build_rank_df(group_map, selected_group, start_date, end_date, min_days)

    if rank_df.empty:
        st.warning("沒有可顯示的排行資料。")
        st.stop()

    rank_df = rank_df.sort_values(sort_by, ascending=sort_asc, na_position="last").head(top_n).reset_index(drop=True)

    strong_long = int((rank_df["燈號"] == "強多").sum()) if "燈號" in rank_df.columns else 0
    strong_short = int((rank_df["燈號"] == "強空").sum()) if "燈號" in rank_df.columns else 0
    avg_return = float(rank_df["區間漲跌幅(%)"].mean()) if "區間漲跌幅(%)" in rank_df.columns else np.nan
    avg_radar = float(rank_df["雷達均分"].mean()) if "雷達均分" in rank_df.columns else np.nan

    render_pro_kpi_row(
        [
            {"label": "排行群組", "value": selected_group, "delta": f"{len(rank_df)} 檔", "delta_class": "pro-kpi-delta-flat"},
            {"label": "強多數量", "value": strong_long, "delta": "燈號統計", "delta_class": "pro-kpi-delta-flat"},
            {"label": "強空數量", "value": strong_short, "delta": "燈號統計", "delta_class": "pro-kpi-delta-flat"},
            {"label": "平均區間漲跌", "value": _fmt_pct(avg_return), "delta": f"雷達均分 {avg_radar:.1f}" if pd.notna(avg_radar) else "—", "delta_class": "pro-kpi-delta-flat"},
        ]
    )

    top1, top2 = st.columns(2)
    with top1:
        top_row = rank_df.iloc[0]
        render_pro_info_card(
            "排行榜冠軍",
            [
                ("股票", _safe_str(top_row.get("股票")), ""),
                ("綜合評級", _fmt_num(top_row.get("綜合評級"), 1), ""),
                ("區間漲跌幅", _fmt_pct(top_row.get("區間漲跌幅(%)")), "pro-up" if _safe_float(top_row.get("區間漲跌幅(%)"), 0) >= 0 else "pro-down"),
                ("燈號", _safe_str(top_row.get("燈號")), ""),
                ("雷達均分", _fmt_num(top_row.get("雷達均分"), 1), ""),
                ("訊號說明", _safe_str(top_row.get("訊號說明")), ""),
            ],
            chips=[_safe_str(top_row.get("群組")), _safe_str(top_row.get("壓力訊號")), _safe_str(top_row.get("支撐訊號"))],
        )

    with top2:
        worst_row = rank_df.iloc[-1]
        render_pro_info_card(
            "尾端觀察",
            [
                ("股票", _safe_str(worst_row.get("股票")), ""),
                ("綜合評級", _fmt_num(worst_row.get("綜合評級"), 1), ""),
                ("區間漲跌幅", _fmt_pct(worst_row.get("區間漲跌幅(%)")), "pro-up" if _safe_float(worst_row.get("區間漲跌幅(%)"), 0) >= 0 else "pro-down"),
                ("燈號", _safe_str(worst_row.get("燈號")), ""),
                ("雷達均分", _fmt_num(worst_row.get("雷達均分"), 1), ""),
                ("訊號說明", _safe_str(worst_row.get("訊號說明")), ""),
            ],
            chips=[_safe_str(worst_row.get("群組")), _safe_str(worst_row.get("壓力訊號")), _safe_str(worst_row.get("支撐訊號"))],
        )

    render_pro_section("排行榜明細")

    show_df = rank_df[
        [
            "群組",
            "股票",
            "最新收盤",
            "區間漲跌幅(%)",
            "燈號",
            "訊號分數",
            "雷達均分",
            "綜合評級",
            "壓力訊號",
            "支撐訊號",
            "區間訊號",
            "20日壓力",
            "20日支撐",
            "60日壓力",
            "60日支撐",
            "距20日壓力(%)",
            "距20日支撐(%)",
        ]
    ].copy()

    st.dataframe(show_df, use_container_width=True, hide_index=True)

    render_pro_section("前十名摘要")

    top10 = rank_df.head(10).copy()
    summary_df = top10[["股票", "綜合評級", "區間漲跌幅(%)", "燈號", "雷達均分", "壓力訊號", "支撐訊號"]].copy()
    st.dataframe(summary_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
