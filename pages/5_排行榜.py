from datetime import date, timedelta
import pandas as pd
import streamlit as st

from utils import (
    get_normalized_watchlist,
    get_realtime_watchlist_df,
    get_history_data,
    apply_font_scale,
    get_font_scale,
    inject_pro_theme,
    render_pro_hero,
    render_pro_section,
    render_pro_kpi_row,
    compute_signal_snapshot,
    score_to_badge,
    compute_support_resistance_snapshot,
    compute_radar_scores,
    format_number,
)


def add_rank_indicators(df: pd.DataFrame):
    if df.empty or "收盤價" not in df.columns:
        return df

    df = df.copy()

    df["MA5"] = df["收盤價"].rolling(window=5, min_periods=1).mean()
    df["MA10"] = df["收盤價"].rolling(window=10, min_periods=1).mean()
    df["MA20"] = df["收盤價"].rolling(window=20, min_periods=1).mean()

    if all(col in df.columns for col in ["最高價", "最低價", "收盤價"]):
        low_n = df["最低價"].rolling(window=9, min_periods=1).min()
        high_n = df["最高價"].rolling(window=9, min_periods=1).max()
        denom = (high_n - low_n).replace(0, pd.NA)
        rsv = ((df["收盤價"] - low_n) / denom * 100).fillna(0)

        k_values = []
        d_values = []
        k_prev = 50.0
        d_prev = 50.0

        for val in rsv:
            val = float(val)
            k_now = (2 / 3) * k_prev + (1 / 3) * val
            d_now = (2 / 3) * d_prev + (1 / 3) * k_now
            k_values.append(k_now)
            d_values.append(d_now)
            k_prev = k_now
            d_prev = d_now

        df["K"] = k_values
        df["D"] = d_values

    ema12 = df["收盤價"].ewm(span=12, adjust=False).mean()
    ema26 = df["收盤價"].ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["DIF"] - df["DEA"]

    return df


@st.cache_data(ttl=900, show_spinner=False)
def build_rank_dataset(watchlist_dict: dict, query_date: str):
    realtime_df = get_realtime_watchlist_df(watchlist_dict, query_date)

    if realtime_df is None or realtime_df.empty:
        return pd.DataFrame()

    rows = []
    today_dt = date.today()

    for _, row in realtime_df.iterrows():
        code = str(row.get("股票代號", "")).strip()
        name = str(row.get("股票名稱", "")).strip()
        market = str(row.get("市場別", "")).strip() or "上市"
        group = str(row.get("群組", "")).strip()

        history_df = get_history_data(
            code,
            name,
            market,
            today_dt - timedelta(days=120),
            today_dt
        )

        if history_df is None or history_df.empty:
            rows.append({
                "群組": group,
                "股票代號": code,
                "股票名稱": name,
                "市場別": market,
                "現價": row.get("現價"),
                "漲跌": row.get("漲跌"),
                "漲跌幅(%)": row.get("漲跌幅(%)"),
                "總量": row.get("總量"),
                "更新時間": row.get("更新時間"),
                "綜合評級": "無資料",
                "訊號分數": None,
                "均線結構": "無資料",
                "MACD狀態": "無資料",
                "20日壓力": None,
                "20日支撐": None,
                "趨勢分數": None,
                "動能分數": None,
                "量能分數": None,
                "位置分數": None,
                "結構分數": None,
                "區間漲跌幅(%)": None,
            })
            continue

        history_df = add_rank_indicators(history_df)
        signal = compute_signal_snapshot(history_df)
        sr = compute_support_resistance_snapshot(history_df)
        radar = compute_radar_scores(history_df)
        badge_text, _ = score_to_badge(signal.get("score", 0))

        latest_close = history_df["收盤價"].iloc[-1] if "收盤價" in history_df.columns else None
        first_close = history_df["收盤價"].iloc[0] if "收盤價" in history_df.columns else None

        range_pct = None
        if latest_close is not None and first_close not in [None, 0]:
            range_pct = (latest_close - first_close) / first_close * 100

        rows.append({
            "群組": group,
            "股票代號": code,
            "股票名稱": name,
            "市場別": market,
            "現價": row.get("現價"),
            "漲跌": row.get("漲跌"),
            "漲跌幅(%)": row.get("漲跌幅(%)"),
            "總量": row.get("總量"),
            "更新時間": row.get("更新時間"),
            "綜合評級": badge_text,
            "訊號分數": signal.get("score"),
            "均線結構": signal["ma_trend"][0],
            "MACD狀態": signal["macd_trend"][0],
            "20日壓力": sr.get("res_20"),
            "20日支撐": sr.get("sup_20"),
            "趨勢分數": radar["trend"],
            "動能分數": radar["momentum"],
            "量能分數": radar["volume"],
            "位置分數": radar["position"],
            "結構分數": radar["structure"],
            "區間漲跌幅(%)": range_pct,
        })

    return pd.DataFrame(rows)


def render_rank_table(df: pd.DataFrame, height: int = 780):
    if df is None or df.empty:
        st.info("目前沒有資料。")
        return

    display_df = df.copy()

    format_dict = {}
    for col in ["現價", "漲跌", "漲跌幅(%)", "20日壓力", "20日支撐", "區間漲跌幅(%)"]:
        if col in display_df.columns:
            format_dict[col] = "{:,.2f}"
    for col in ["總量", "訊號分數", "趨勢分數", "動能分數", "量能分數", "位置分數", "結構分數"]:
        if col in display_df.columns:
            format_dict[col] = "{:,.0f}"

    def color_change(val):
        if pd.isna(val):
            return ""
        try:
            v = float(val)
        except Exception:
            return ""
        if v > 0:
            return "color: #dc2626; font-weight: 800;"
        elif v < 0:
            return "color: #059669; font-weight: 800;"
        return "color: #64748b; font-weight: 700;"

    def color_badge(val):
        text = str(val)
        if text in ["強多", "偏多"]:
            return "color: #dc2626; font-weight: 800;"
        if text in ["強空", "偏空"]:
            return "color: #059669; font-weight: 800;"
        if text == "整理":
            return "color: #64748b; font-weight: 800;"
        return ""

    styler = display_df.style.format(format_dict, na_rep="—")
    if "漲跌" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌"])
    if "漲跌幅(%)" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌幅(%)"])
    if "區間漲跌幅(%)" in display_df.columns:
        styler = styler.map(color_change, subset=["區間漲跌幅(%)"])
    if "訊號分數" in display_df.columns:
        styler = styler.map(color_change, subset=["訊號分數"])
    if "綜合評級" in display_df.columns:
        styler = styler.map(color_badge, subset=["綜合評級"])

    st.dataframe(styler, use_container_width=True, hide_index=True, height=height)


st.set_page_config(page_title="排行榜", page_icon="🏆", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)
inject_pro_theme()

render_pro_hero(
    "排行榜｜升級版",
    "不只看漲跌幅，加入綜合評級、訊號燈號、雷達分數與支撐壓力，做真正的掃盤排行。"
)

watchlist_dict = get_normalized_watchlist()
if not watchlist_dict:
    st.warning("目前沒有自選股群組。")
    st.stop()

query_date = date.today().strftime("%Y%m%d")

sort_col = st.selectbox(
    "排序方式",
    [
        "漲跌幅(%)",
        "區間漲跌幅(%)",
        "訊號分數",
        "趨勢分數",
        "動能分數",
        "量能分數",
        "位置分數",
        "結構分數",
        "總量",
        "現價",
    ],
    index=0
)

ascending = st.toggle("升冪排序", value=False)

if st.button("更新排行榜", type="primary", use_container_width=True):
    get_realtime_watchlist_df.clear()
    build_rank_dataset.clear()

with st.spinner("正在讀取排行榜資料..."):
    rank_df = build_rank_dataset(watchlist_dict, query_date)

if rank_df.empty:
    st.info("目前沒有資料。")
    st.stop()

if sort_col in rank_df.columns:
    rank_df = rank_df.sort_values(by=sort_col, ascending=ascending, na_position="last").reset_index(drop=True)

top_name = "—"
top_metric = "—"
if not rank_df.empty and sort_col in rank_df.columns:
    top_row = rank_df.iloc[0]
    top_name = f"{top_row.get('股票名稱', '—')}（{top_row.get('股票代號', '—')}）"
    value = top_row.get(sort_col)
    if pd.notna(value):
        if sort_col == "總量":
            top_metric = f"{value:,.0f}"
        elif sort_col in ["訊號分數", "趨勢分數", "動能分數", "量能分數", "位置分數", "結構分數"]:
            top_metric = f"{value:,.0f}"
        else:
            top_metric = f"{value:,.2f}"

strong_count = int((rank_df["綜合評級"] == "強多").sum()) if "綜合評級" in rank_df.columns else 0
weak_count = int((rank_df["綜合評級"] == "強空").sum()) if "綜合評級" in rank_df.columns else 0

render_pro_kpi_row([
    {"label": "排序欄位", "value": sort_col, "delta": "Ranking Factor", "delta_class": "pro-kpi-delta-flat"},
    {"label": "榜首標的", "value": top_name, "delta": f"{sort_col}：{top_metric}", "delta_class": "pro-kpi-delta-flat"},
    {"label": "強多 / 強空", "value": f"{strong_count} / {weak_count}", "delta": "Signal Distribution", "delta_class": "pro-kpi-delta-flat"},
])

show_cols = [
    "群組", "股票代號", "股票名稱", "市場別",
    "現價", "漲跌", "漲跌幅(%)", "區間漲跌幅(%)",
    "綜合評級", "訊號分數", "均線結構", "MACD狀態",
    "20日壓力", "20日支撐",
    "趨勢分數", "動能分數", "量能分數", "位置分數", "結構分數",
    "總量", "更新時間"
]
show_cols = [c for c in show_cols if c in rank_df.columns]

render_pro_section("排行結果", "可快速篩出最強、最弱、最有量、訊號最完整的標的，再進一步切到單股頁深看")
render_rank_table(rank_df[show_cols], height=800)
