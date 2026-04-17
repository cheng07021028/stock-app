# pages/6_多股比較.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import plotly.graph_objects as go
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

PAGE_TITLE = "多股比較"
PFX = "cmp_"


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
                    group_map["全部股票(前120)"] = rows
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
    groups = list(group_map.keys())
    today = date.today()

    if _k("group") not in st.session_state:
        saved_group = _safe_str(saved.get("quick_group", ""))
        st.session_state[_k("group")] = saved_group if saved_group in groups else (groups[0] if groups else "")

    if _k("search_input") not in st.session_state:
        st.session_state[_k("search_input")] = ""

    if _k("selected_codes") not in st.session_state:
        st.session_state[_k("selected_codes")] = []

    if _k("metric") not in st.session_state:
        st.session_state[_k("metric")] = "報酬率比較"

    if _k("start_date") not in st.session_state:
        st.session_state[_k("start_date")] = today - timedelta(days=180)

    if _k("end_date") not in st.session_state:
        st.session_state[_k("end_date")] = today


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
    volume = pd.to_numeric(df["成交股數"], errors="coerce") if "成交股數" in df.columns else pd.Series(index=df.index, dtype=float)

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

    df["VOL5"] = volume.rolling(5).mean()
    df["VOL20"] = volume.rolling(20).mean()
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
        roc_date = f"{roc_year}/{dt.month:02d}"
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
                    return pd.Timestamp(year=int(parts[0]) + 1911, month=int(parts[1]), day=int(parts[2]))
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
# 單股比較摘要
# =========================================================
@st.cache_data(ttl=1800, show_spinner=False)
def _build_compare_row(code: str, name: str, market: str, start_date: date, end_date: date) -> dict[str, Any] | None:
    df = _get_history_data_smart(code, name, market, start_date, end_date)
    if df.empty or len(df) < 2:
        return None

    first = df.iloc[0]
    last = df.iloc[-1]

    first_close = _safe_float(first.get("收盤價"))
    last_close = _safe_float(last.get("收盤價"))
    ma20 = _safe_float(last.get("MA20"))
    ma60 = _safe_float(last.get("MA60"))
    vol5 = _safe_float(last.get("VOL5"))
    vol20 = _safe_float(last.get("VOL20"))

    if first_close in [None, 0] or last_close is None:
        return None

    return_pct = ((last_close / first_close) - 1) * 100
    signal_snapshot = compute_signal_snapshot(df)
    sr_snapshot = compute_support_resistance_snapshot(df)
    radar = compute_radar_scores(df)

    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))
    dist_res20 = ((res20 - last_close) / res20 * 100) if res20 not in [None, 0] else None
    dist_sup20 = ((last_close - sup20) / sup20 * 100) if sup20 not in [None, 0] else None
    vol_ratio = (vol5 / vol20) if vol5 not in [None, 0] and vol20 not in [None, 0] else None

    series = pd.DataFrame(
        {
            "日期": df["日期"],
            "收盤價": df["收盤價"],
        }
    ).copy()
    series["報酬率%"] = (series["收盤價"] / series["收盤價"].iloc[0] - 1) * 100
    series["股票"] = f"{code} {name}"

    return {
        "code": code,
        "name": name,
        "market": market,
        "label": f"{code} {name}",
        "last_close": last_close,
        "return_pct": return_pct,
        "signal_score": _safe_float(signal_snapshot.get("score"), 0),
        "badge": score_to_badge(signal_snapshot.get("score", 0))[0],
        "ma_trend": _safe_str(signal_snapshot.get("ma_trend", ("—", ""))[0]),
        "break_signal": _safe_str(sr_snapshot.get("break_signal", ("—", ""))[0]),
        "res20": res20,
        "sup20": sup20,
        "dist_res20": dist_res20,
        "dist_sup20": dist_sup20,
        "vol_ratio": vol_ratio,
        "trend_score": _safe_float(radar.get("trend"), 50),
        "momentum_score": _safe_float(radar.get("momentum"), 50),
        "structure_score": _safe_float(radar.get("structure"), 50),
        "series": series,
    }


def _build_compare_dataset(selected_items: list[dict[str, str]], start_date: date, end_date: date):
    rows = []
    series_list = []

    for item in selected_items:
        try:
            row = _build_compare_row(
                code=_safe_str(item.get("code")),
                name=_safe_str(item.get("name")),
                market=_safe_str(item.get("market")) or "上市",
                start_date=start_date,
                end_date=end_date,
            )
            if row:
                rows.append(row)
                series_list.append(row["series"])
        except Exception:
            continue

    compare_df = pd.DataFrame(
        [
            {
                "股票代號": r["code"],
                "股票名稱": r["name"],
                "市場別": r["market"],
                "最新收盤": r["last_close"],
                "區間報酬(%)": r["return_pct"],
                "訊號分數": r["signal_score"],
                "燈號": r["badge"],
                "均線趨勢": r["ma_trend"],
                "突破判斷": r["break_signal"],
                "20日壓力": r["res20"],
                "20日支撐": r["sup20"],
                "距20日壓力(%)": r["dist_res20"],
                "距20日支撐(%)": r["dist_sup20"],
                "量比(VOL5/VOL20)": r["vol_ratio"],
                "趨勢分數": r["trend_score"],
                "動能分數": r["momentum_score"],
                "結構分數": r["structure_score"],
            }
            for r in rows
        ]
    )

    merged_series = pd.concat(series_list, ignore_index=True) if series_list else pd.DataFrame()
    return compare_df, merged_series


# =========================================================
# 圖表
# =========================================================
def _build_compare_chart(series_df: pd.DataFrame, metric: str) -> go.Figure:
    fig = go.Figure()
    if series_df is None or series_df.empty:
        return fig

    value_col = "報酬率%" if metric == "報酬率比較" else "收盤價"

    for stock_label, sub in series_df.groupby("股票"):
        fig.add_trace(
            go.Scatter(
                x=sub["日期"],
                y=sub[value_col],
                mode="lines",
                name=stock_label,
            )
        )

    fig.update_layout(
        title=metric,
        height=620,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="日期",
        yaxis_title=value_col,
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
        title="多股比較｜股神版",
        subtitle="同群組多檔一起比較，快速看誰最強、誰最熱、誰最接近突破。",
    )

    render_pro_section("選股條件")

    groups = list(group_map.keys())
    c1, c2 = st.columns([2, 4])
    with c1:
        st.selectbox("選擇群組", options=groups, key=_k("group"))
    with c2:
        st.multiselect(
            "群組股票",
            options=[x["code"] for x in group_map.get(_safe_str(st.session_state.get(_k("group"), "")), [])],
            default=st.session_state.get(_k("selected_codes"), []),
            key=_k("selected_codes"),
            format_func=lambda code: next(
                (
                    x["label"]
                    for x in group_map.get(_safe_str(st.session_state.get(_k("group"), "")), [])
                    if x["code"] == code
                ),
                code,
            ),
        )

    s1, s2 = st.columns([5, 1])
    with s1:
        st.text_input(
            "快速加入股票（輸入代碼或名稱）",
            key=_k("search_input"),
            placeholder="例如：2330、台積電、3548 兆利",
            label_visibility="collapsed",
        )
    with s2:
        if st.button("加入", use_container_width=True):
            target = _find_search_target(st.session_state.get(_k("search_input"), ""), flat_rows)
            if target:
                selected_codes = list(st.session_state.get(_k("selected_codes"), []))
                if target["code"] not in selected_codes:
                    selected_codes.append(target["code"])
                    st.session_state[_k("selected_codes")] = selected_codes
                    st.rerun()
            else:
                st.warning("找不到對應股票。")

    d1, d2, d3 = st.columns([2, 2, 2])
    with d1:
        st.selectbox("比較模式", options=["報酬率比較", "收盤價比較"], key=_k("metric"))
    with d2:
        st.date_input("起始日期", key=_k("start_date"))
    with d3:
        st.date_input("結束日期", key=_k("end_date"))

    selected_group = _safe_str(st.session_state.get(_k("group"), ""))
    selected_codes = list(st.session_state.get(_k("selected_codes"), []))
    metric = _safe_str(st.session_state.get(_k("metric"), "報酬率比較"))
    start_date = _to_date(st.session_state.get(_k("start_date")), date.today() - timedelta(days=180))
    end_date = _to_date(st.session_state.get(_k("end_date")), date.today())

    if start_date > end_date:
        st.error("起始日期不可大於結束日期。")
        st.stop()

    group_items = group_map.get(selected_group, [])
    group_code_map = {x["code"]: x for x in group_items}

    selected_items = []
    for code in selected_codes:
        if code in group_code_map:
            selected_items.append(group_code_map[code])
        else:
            found = next((r for r in flat_rows if r["code"] == code), None)
            if found:
                selected_items.append(
                    {
                        "code": found["code"],
                        "name": found["name"],
                        "market": found["market"],
                        "label": found["label"],
                    }
                )

    if len(selected_items) < 2:
        st.info("請至少選擇 2 檔股票做比較。")
        st.stop()

    save_last_query_state(
        quick_group=selected_group,
        home_start=start_date,
        home_end=end_date,
    )

    with st.spinner("建立多股比較資料中..."):
        compare_df, series_df = _build_compare_dataset(selected_items, start_date, end_date)

    if compare_df.empty:
        st.error("目前選股在區間內沒有足夠資料可比較。")
        st.stop()

    best_return_row = compare_df.sort_values("區間報酬(%)", ascending=False).iloc[0]
    best_signal_row = compare_df.sort_values("訊號分數", ascending=False).iloc[0]
    hottest_row = compare_df.sort_values("量比(VOL5/VOL20)", ascending=False).iloc[0]

    render_pro_kpi_row(
        [
            {
                "label": "最強報酬",
                "value": f"{_safe_str(best_return_row['股票代號'])} {_safe_str(best_return_row['股票名稱'])}",
                "delta": f"{format_number(best_return_row.get('區間報酬(%)'), 2)}%",
                "delta_class": "pro-kpi-delta-up",
            },
            {
                "label": "最強訊號",
                "value": f"{_safe_str(best_signal_row['股票代號'])} {_safe_str(best_signal_row['股票名稱'])}",
                "delta": f"分數 {format_number(best_signal_row.get('訊號分數'), 2)}",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "最熱量比",
                "value": f"{_safe_str(hottest_row['股票代號'])} {_safe_str(hottest_row['股票名稱'])}",
                "delta": f"{format_number(hottest_row.get('量比(VOL5/VOL20)'), 2)}",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "比較檔數",
                "value": len(compare_df),
                "delta": selected_group,
                "delta_class": "pro-kpi-delta-flat",
            },
        ]
    )

    st.plotly_chart(_build_compare_chart(series_df, metric), use_container_width=True)

    left, right = st.columns(2)

    with left:
        strong_df = compare_df.sort_values("區間報酬(%)", ascending=False).head(5)
        render_pro_info_card(
            "報酬率前段班",
            [
                (
                    f"{i+1}. {_safe_str(r['股票代號'])} {_safe_str(r['股票名稱'])}",
                    f"報酬 {format_number(r.get('區間報酬(%)'), 2)}%",
                    ""
                )
                for i, (_, r) in enumerate(strong_df.iterrows())
            ],
            chips=["強勢股"],
        )

        signal_df = compare_df.sort_values("訊號分數", ascending=False).head(5)
        render_pro_info_card(
            "訊號最強",
            [
                (
                    f"{i+1}. {_safe_str(r['股票代號'])} {_safe_str(r['股票名稱'])}",
                    f"分數 {format_number(r.get('訊號分數'), 2)} / {_safe_str(r.get('燈號'))}",
                    ""
                )
                for i, (_, r) in enumerate(signal_df.iterrows())
            ],
            chips=["燈號比較"],
        )

    with right:
        pressure_df = compare_df.sort_values("距20日壓力(%)", ascending=True).head(5)
        render_pro_info_card(
            "最接近突破",
            [
                (
                    f"{i+1}. {_safe_str(r['股票代號'])} {_safe_str(r['股票名稱'])}",
                    f"距20壓力 {format_number(r.get('距20日壓力(%)'), 2)}%",
                    ""
                )
                for i, (_, r) in enumerate(pressure_df.iterrows())
            ],
            chips=["突破候選"],
        )

        support_df = compare_df.sort_values("距20日支撐(%)", ascending=True).head(5)
        render_pro_info_card(
            "最接近支撐",
            [
                (
                    f"{i+1}. {_safe_str(r['股票代號'])} {_safe_str(r['股票名稱'])}",
                    f"距20支撐 {format_number(r.get('距20日支撐(%)'), 2)}%",
                    ""
                )
                for i, (_, r) in enumerate(support_df.iterrows())
            ],
            chips=["支撐觀察"],
        )

    tabs = st.tabs(["比較總表", "強弱排序", "結構分數"])

    with tabs[0]:
        show_cols = [
            "股票代號", "股票名稱", "市場別", "最新收盤",
            "區間報酬(%)", "訊號分數", "燈號",
            "均線趨勢", "突破判斷",
            "20日壓力", "20日支撐",
            "距20日壓力(%)", "距20日支撐(%)",
            "量比(VOL5/VOL20)",
        ]
        show_cols = [c for c in show_cols if c in compare_df.columns]
        st.dataframe(compare_df[show_cols], use_container_width=True, hide_index=True)

    with tabs[1]:
        sorted_df = compare_df.sort_values("區間報酬(%)", ascending=False).copy()
        show_cols = [
            "股票代號", "股票名稱", "區間報酬(%)",
            "訊號分數", "燈號", "均線趨勢", "突破判斷",
        ]
        show_cols = [c for c in show_cols if c in sorted_df.columns]
        st.dataframe(sorted_df[show_cols], use_container_width=True, hide_index=True)

    with tabs[2]:
        score_df = compare_df.sort_values(["趨勢分數", "動能分數", "結構分數"], ascending=False).copy()
        show_cols = [
            "股票代號", "股票名稱",
            "趨勢分數", "動能分數", "結構分數",
            "訊號分數", "燈號",
        ]
        show_cols = [c for c in show_cols if c in score_df.columns]
        st.dataframe(score_df[show_cols], use_container_width=True, hide_index=True)

    with st.expander("效能說明"):
        st.write("1. 多股比較以選取股票為 universe，避免全市場過慢。")
        st.write("2. 歷史資料與上櫃 fallback 使用 cache。")
        st.write("3. 每檔比較資料只算一次，再做圖表與表格。")
        st.write("4. 若後續要更快，可再做批次共用資料版。")


if __name__ == "__main__":
    main()
