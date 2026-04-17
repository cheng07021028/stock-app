# pages/5_排行榜.py
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
# 群組
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


# =========================================================
# State
# =========================================================
def _init_state(group_map: dict[str, list[dict[str, str]]]):
    saved = load_last_query_state()
    today = date.today()
    default_start = today - timedelta(days=180)
    default_end = today

    groups = list(group_map.keys())

    if _k("group") not in st.session_state:
        saved_group = _safe_str(saved.get("quick_group", ""))
        st.session_state[_k("group")] = saved_group if saved_group in groups else (groups[0] if groups else "")

    if _k("start_date") not in st.session_state:
        st.session_state[_k("start_date")] = parse_date_safe(saved.get("home_start"), default_start)

    if _k("end_date") not in st.session_state:
        st.session_state[_k("end_date")] = parse_date_safe(saved.get("home_end"), default_end)

    if _k("sort_by") not in st.session_state:
        st.session_state[_k("sort_by")] = "綜合評級"

    if _k("ascending") not in st.session_state:
        st.session_state[_k("ascending")] = False

    if _k("top_n") not in st.session_state:
        st.session_state[_k("top_n")] = 30

    st.session_state[_k("start_date")] = _to_date(st.session_state.get(_k("start_date")), default_start)
    st.session_state[_k("end_date")] = _to_date(st.session_state.get(_k("end_date")), default_end)

    _repair_state(group_map)


def _repair_state(group_map: dict[str, list[dict[str, str]]]):
    groups = list(group_map.keys())
    current_group = _safe_str(st.session_state.get(_k("group"), ""))

    if current_group not in group_map:
        st.session_state[_k("group")] = groups[0] if groups else ""


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
# 排行榜計算
# =========================================================
@st.cache_data(ttl=900, show_spinner=False)
def _calc_one_rank_row(code: str, name: str, market: str, start_date: date, end_date: date) -> dict[str, Any]:
    df = _get_history_data_smart(
        stock_no=code,
        stock_name=name,
        market_type=market,
        start_date=start_date,
        end_date=end_date,
    )

    if df.empty or len(df) < 2:
        return {
            "股票代號": code,
            "股票名稱": name,
            "市場別": market,
            "股票": f"{code} {name}",
            "資料筆數": 0,
            "綜合評級": None,
            "燈號": "查無資料",
            "訊號分數": None,
            "雷達均分": None,
            "趨勢": None,
            "動能": None,
            "量能": None,
            "位置": None,
            "結構": None,
            "20日壓力": None,
            "20日支撐": None,
            "60日壓力": None,
            "60日支撐": None,
            "壓力訊號": "—",
            "支撐訊號": "—",
            "區間訊號": "—",
            "區間漲跌幅(%)": None,
            "最新收盤": None,
        }

    signal = compute_signal_snapshot(df)
    sr = compute_support_resistance_snapshot(df)
    radar = compute_radar_scores(df)

    last = df.iloc[-1]
    first = df.iloc[0]

    close_now = _safe_float(last.get("收盤價"))
    close_first = _safe_float(first.get("收盤價"))
    interval_pct = ((close_now / close_first) - 1) * 100 if close_first not in [None, 0] else None

    radar_avg = None
    radar_values = [
        _safe_float(radar.get("trend")),
        _safe_float(radar.get("momentum")),
        _safe_float(radar.get("volume")),
        _safe_float(radar.get("position")),
        _safe_float(radar.get("structure")),
    ]
    radar_values = [x for x in radar_values if x is not None]
    if radar_values:
        radar_avg = sum(radar_values) / len(radar_values)

    signal_score = _safe_float(signal.get("score"), 0)
    total_score = None
    if radar_avg is not None:
        total_score = round(signal_score * 10 + radar_avg, 2)

    badge_text, _ = score_to_badge(signal.get("score", 0))

    return {
        "股票代號": code,
        "股票名稱": name,
        "市場別": market,
        "股票": f"{code} {name}",
        "資料筆數": len(df),
        "綜合評級": total_score,
        "燈號": badge_text,
        "訊號分數": signal_score,
        "雷達均分": radar_avg,
        "趨勢": radar.get("trend"),
        "動能": radar.get("momentum"),
        "量能": radar.get("volume"),
        "位置": radar.get("position"),
        "結構": radar.get("structure"),
        "20日壓力": sr.get("res_20"),
        "20日支撐": sr.get("sup_20"),
        "60日壓力": sr.get("res_60"),
        "60日支撐": sr.get("sup_60"),
        "壓力訊號": sr.get("pressure_signal", ("—", ""))[0],
        "支撐訊號": sr.get("support_signal", ("—", ""))[0],
        "區間訊號": sr.get("break_signal", ("—", ""))[0],
        "區間漲跌幅(%)": interval_pct,
        "最新收盤": close_now,
    }


def _build_rank_df(group_map: dict[str, list[dict[str, str]]], selected_group: str, start_date: date, end_date: date) -> pd.DataFrame:
    items = group_map.get(selected_group, [])
    rows = []

    for item in items:
        code = _safe_str(item.get("code"))
        name = _safe_str(item.get("name"))
        market = _safe_str(item.get("market")) or "上市"
        if not code:
            continue
        rows.append(_calc_one_rank_row(code, name, market, start_date, end_date))

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df


def _format_rank_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    numeric_cols = [
        "綜合評級", "訊號分數", "雷達均分", "趨勢", "動能", "量能", "位置", "結構",
        "20日壓力", "20日支撐", "60日壓力", "60日支撐", "區間漲跌幅(%)", "最新收盤"
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    return out


# =========================================================
# 主頁
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    group_map = _build_group_stock_map()
    _init_state(group_map)
    _repair_state(group_map)

    render_pro_hero(
        title="排行榜｜股神版",
        subtitle="綜合評級、訊號分數、雷達分數、支撐壓力、區間漲跌幅，可排序，支援上櫃 smart history。",
    )

    groups = list(group_map.keys())
    selected_group = _safe_str(st.session_state.get(_k("group"), ""))
    start_date = _to_date(st.session_state.get(_k("start_date")), date.today() - timedelta(days=180))
    end_date = _to_date(st.session_state.get(_k("end_date")), date.today())

    render_pro_section("查詢條件")

    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2])

    with c1:
        st.selectbox("群組", options=groups if groups else [""], key=_k("group"))

    with c2:
        st.date_input("開始日期", key=_k("start_date"))

    with c3:
        st.date_input("結束日期", key=_k("end_date"))

    with c4:
        st.selectbox(
            "排序欄位",
            options=["綜合評級", "訊號分數", "雷達均分", "區間漲跌幅(%)", "最新收盤"],
            key=_k("sort_by"),
        )

    with c5:
        st.selectbox(
            "排序方向",
            options=["由高到低", "由低到高"],
            index=0 if not st.session_state.get(_k("ascending"), False) else 1,
            key=_k("sort_order_text"),
        )
        st.session_state[_k("ascending")] = st.session_state.get(_k("sort_order_text")) == "由低到高"

    d1, d2 = st.columns([2, 2])
    with d1:
        st.slider("顯示筆數", min_value=5, max_value=100, value=st.session_state.get(_k("top_n"), 30), step=5, key=_k("top_n"))
    with d2:
        st.caption(f"目前群組：{selected_group}")

    if start_date > end_date:
        st.error("開始日期不可大於結束日期。")
        st.stop()

    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code="",
        home_start=start_date,
        home_end=end_date,
    )

    with st.spinner("計算排行榜中..."):
        rank_df = _build_rank_df(group_map, selected_group, start_date, end_date)
        rank_df = _format_rank_df(rank_df)

    if rank_df.empty:
        st.error("查無排行榜資料。")
        st.stop()

    sort_by = _safe_str(st.session_state.get(_k("sort_by"), "綜合評級"))
    ascending = bool(st.session_state.get(_k("ascending"), False))
    top_n = int(st.session_state.get(_k("top_n"), 30))

    if sort_by in rank_df.columns:
        rank_df = rank_df.sort_values(sort_by, ascending=ascending, na_position="last").reset_index(drop=True)

    show_df = rank_df.head(top_n).copy()

    valid_rating_df = rank_df.dropna(subset=["綜合評級"])
    best_row = valid_rating_df.iloc[0] if not valid_rating_df.empty else None
    worst_row = valid_rating_df.iloc[-1] if not valid_rating_df.empty else None

    strong_bull_count = int((rank_df["燈號"] == "強多").sum()) if "燈號" in rank_df.columns else 0
    strong_bear_count = int((rank_df["燈號"] == "強空").sum()) if "燈號" in rank_df.columns else 0

    render_pro_kpi_row(
        [
            {
                "label": "群組股票數",
                "value": len(rank_df),
                "delta": selected_group,
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "最強股",
                "value": _safe_str(best_row["股票"]) if best_row is not None else "—",
                "delta": format_number(best_row["綜合評級"], 2) if best_row is not None else "—",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "最弱股",
                "value": _safe_str(worst_row["股票"]) if worst_row is not None else "—",
                "delta": format_number(worst_row["綜合評級"], 2) if worst_row is not None else "—",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "強多 / 強空",
                "value": f"{strong_bull_count} / {strong_bear_count}",
                "delta": "燈號統計",
                "delta_class": "pro-kpi-delta-flat",
            },
        ]
    )

    left, right = st.columns([1, 1])

    with left:
        if best_row is not None:
            render_pro_info_card(
                "最強股摘要",
                [
                    ("股票", _safe_str(best_row["股票"]), ""),
                    ("綜合評級", format_number(best_row["綜合評級"], 2), ""),
                    ("燈號", _safe_str(best_row["燈號"]), ""),
                    ("訊號分數", format_number(best_row["訊號分數"], 2), ""),
                    ("雷達均分", format_number(best_row["雷達均分"], 2), ""),
                    ("區間漲跌幅", format_number(best_row["區間漲跌幅(%)"], 2) + "%", ""),
                ],
            )

    with right:
        render_pro_info_card(
            "排行榜說明",
            [
                ("綜合評級", "訊號分數 × 10 + 雷達均分。", ""),
                ("排序", "可切換依綜合評級、訊號、雷達、漲跌幅排序。", ""),
                ("資料來源", "先走既有 history，再對上櫃走 fallback。", ""),
                ("用途", "先抓最強最弱，再往下看支撐壓力與區間訊號。", ""),
            ],
        )

    render_pro_section("排行榜明細")

    display_cols = [
        "股票",
        "市場別",
        "綜合評級",
        "燈號",
        "訊號分數",
        "雷達均分",
        "趨勢",
        "動能",
        "量能",
        "位置",
        "結構",
        "最新收盤",
        "區間漲跌幅(%)",
        "20日壓力",
        "20日支撐",
        "60日壓力",
        "60日支撐",
        "壓力訊號",
        "支撐訊號",
        "區間訊號",
        "資料筆數",
    ]
    display_cols = [c for c in display_cols if c in show_df.columns]
    st.dataframe(show_df[display_cols], use_container_width=True, hide_index=True)

    render_pro_section("強弱摘要")

    if not valid_rating_df.empty:
        top5 = valid_rating_df.head(5)[["股票", "綜合評級", "燈號", "區間漲跌幅(%)"]].copy()
        bottom5 = valid_rating_df.tail(5)[["股票", "綜合評級", "燈號", "區間漲跌幅(%)"]].copy()

        g1, g2 = st.columns(2)
        with g1:
            st.markdown("#### 前 5 強")
            st.dataframe(top5, use_container_width=True, hide_index=True)
        with g2:
            st.markdown("#### 後 5 弱")
            st.dataframe(bottom5, use_container_width=True, hide_index=True)

    with st.expander("效能說明"):
        st.write("這版已做 cache，並對上櫃 / 興櫃補 smart history fallback。")
        st.write("如果你還要更快，下一步我建議把首頁也接排行榜摘要快取。")


if __name__ == "__main__":
    main()
