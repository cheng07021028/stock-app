from datetime import date, timedelta
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils import (
    get_normalized_watchlist,
    apply_font_scale,
    get_font_scale,
    get_all_code_name_map,
    get_stock_name_and_market,
    get_history_data,
)

from query_state import load_last_query_state, save_last_query_state, parse_date_safe


@st.cache_data(ttl=600, show_spinner=False)
def build_stock_options(items, lookup_date):
    all_code_name_df = get_all_code_name_map(lookup_date)
    stock_options = []

    for item in items:
        code = str(item.get("code", "")).strip()
        manual_name = str(item.get("name", "")).strip()

        if not code:
            continue

        stock_name, market_type = get_stock_name_and_market(code, all_code_name_df, manual_name)
        stock_options.append({
            "label": f"{stock_name} ({code}) [{market_type}]",
            "code": code,
            "name": stock_name,
            "market": market_type,
        })

    return stock_options


def prepare_display_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = pd.DataFrame(df).copy()

    if "日期" in df.columns:
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        df = df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    if "收盤價" in df.columns:
        df["MA5"] = df["收盤價"].rolling(window=5, min_periods=1).mean()
        df["MA10"] = df["收盤價"].rolling(window=10, min_periods=1).mean()
        df["MA20"] = df["收盤價"].rolling(window=20, min_periods=1).mean()

    return df


def render_summary_metrics(df: pd.DataFrame):
    if df.empty or "收盤價" not in df.columns:
        return

    latest_close = df["收盤價"].iloc[-1]
    first_close = df["收盤價"].iloc[0]
    price_change = latest_close - first_close
    price_change_pct = (price_change / first_close * 100) if first_close not in [0, None] else 0

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("最新收盤價", f"{latest_close:,.2f}")
    with m2:
        st.metric("區間漲跌", f"{price_change:,.2f}")
    with m3:
        st.metric("區間漲跌幅", f"{price_change_pct:,.2f}%")


def render_table(df: pd.DataFrame):
    show_cols = [
        c for c in [
            "日期", "開盤價", "最高價", "最低價", "收盤價",
            "MA5", "MA10", "MA20", "成交股數", "成交金額", "成交筆數"
        ] if c in df.columns
    ]

    if show_cols:
        st.dataframe(df[show_cols], use_container_width=True, hide_index=True)
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def render_chart(df: pd.DataFrame):
    if df.empty:
        return

    need_price_cols = all(col in df.columns for col in ["日期", "開盤價", "最高價", "最低價", "收盤價"])
    has_volume = "成交股數" in df.columns

    if need_price_cols:
        st.subheader("K線趨勢圖")

        if has_volume:
            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.06,
                row_heights=[0.72, 0.28]
            )
        else:
            fig = make_subplots(rows=1, cols=1)

        fig.add_trace(
            go.Candlestick(
                x=df["日期"],
                open=df["開盤價"],
                high=df["最高價"],
                low=df["最低價"],
                close=df["收盤價"],
                name="K線",
                hovertemplate=(
                    "日期: %{x|%Y-%m-%d}<br>"
                    "開盤: %{open:.2f}<br>"
                    "最高: %{high:.2f}<br>"
                    "最低: %{low:.2f}<br>"
                    "收盤: %{close:.2f}<extra></extra>"
                )
            ),
            row=1,
            col=1
        )

        if "MA5" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df["MA5"],
                    mode="lines",
                    name="MA5",
                    line=dict(width=1.8),
                    hovertemplate="日期: %{x|%Y-%m-%d}<br>MA5: %{y:.2f}<extra></extra>"
                ),
                row=1,
                col=1
            )

        if "MA10" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df["MA10"],
                    mode="lines",
                    name="MA10",
                    line=dict(width=2.2),
                    hovertemplate="日期: %{x|%Y-%m-%d}<br>MA10: %{y:.2f}<extra></extra>"
                ),
                row=1,
                col=1
            )

        if "MA20" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df["MA20"],
                    mode="lines",
                    name="MA20",
                    line=dict(width=2.2),
                    hovertemplate="日期: %{x|%Y-%m-%d}<br>MA20: %{y:.2f}<extra></extra>"
                ),
                row=1,
                col=1
            )

        if has_volume:
            volume_colors = []
            for _, row_data in df.iterrows():
                open_p = row_data.get("開盤價")
                close_p = row_data.get("收盤價")
                if pd.notna(close_p) and pd.notna(open_p) and close_p >= open_p:
                    volume_colors.append("#ef5350")
                else:
                    volume_colors.append("#26a69a")

            fig.add_trace(
                go.Bar(
                    x=df["日期"],
                    y=df["成交股數"],
                    name="成交量",
                    marker_color=volume_colors,
                    hovertemplate="日期: %{x|%Y-%m-%d}<br>成交量: %{y:,.0f}<extra></extra>"
                ),
                row=2,
                col=1
            )

        fig.update_layout(
            height=760 if has_volume else 540,
            xaxis_rangeslider_visible=False,
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="left",
                x=0
            ),
            margin=dict(l=20, r=20, t=20, b=20)
        )

        fig.update_yaxes(title_text="股價", row=1, col=1)
        if has_volume:
            fig.update_yaxes(title_text="成交量", row=2, col=1)

        st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})
        return

    if "收盤價" in df.columns and "日期" in df.columns:
        st.subheader("收盤價趨勢圖")

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df["日期"],
                y=df["收盤價"],
                mode="lines+markers",
                name="收盤價",
                hovertemplate="日期: %{x|%Y-%m-%d}<br>收盤價: %{y:.2f}<extra></extra>"
            )
        )

        if "MA5" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df["MA5"],
                    mode="lines",
                    name="MA5",
                    hovertemplate="日期: %{x|%Y-%m-%d}<br>MA5: %{y:.2f}<extra></extra>"
                )
            )

        if "MA10" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df["MA10"],
                    mode="lines",
                    name="MA10",
                    hovertemplate="日期: %{x|%Y-%m-%d}<br>MA10: %{y:.2f}<extra></extra>"
                )
            )

        if "MA20" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df["MA20"],
                    mode="lines",
                    name="MA20",
                    hovertemplate="日期: %{x|%Y-%m-%d}<br>MA20: %{y:.2f}<extra></extra>"
                )
            )

        fig.update_layout(
            height=520,
            hovermode="x unified",
            margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="left",
                x=0
            )
        )

        st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})


st.set_page_config(page_title="歷史K線分析", page_icon="📊", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)

if "kline_state_loaded" not in st.session_state:
    last_state = load_last_query_state()
    today_dt = date.today()

    st.session_state.kline_group = last_state.get("quick_group", "")
    st.session_state.kline_stock_code = last_state.get("quick_stock_code", "")
    st.session_state.kline_start = parse_date_safe(
        last_state.get("home_start", ""),
        today_dt - timedelta(days=90)
    )
    st.session_state.kline_end = parse_date_safe(
        last_state.get("home_end", ""),
        today_dt
    )
    st.session_state.kline_state_loaded = True

if "kline_result_df" not in st.session_state:
    st.session_state.kline_result_df = None

if "kline_selected_stock" not in st.session_state:
    st.session_state.kline_selected_stock = None

if "kline_selected_group" not in st.session_state:
    st.session_state.kline_selected_group = None

st.title("📊 歷史K線分析")
st.caption("依群組、股票與日期區間查詢歷史K線資料")

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")

watchlist_dict = get_normalized_watchlist()
group_names = list(watchlist_dict.keys())

if not group_names:
    st.warning("目前沒有自選股群組，請先到「自選股中心」建立群組與股票。")
    st.stop()

saved_group = st.session_state.get("kline_group", "")
group_index = group_names.index(saved_group) if saved_group in group_names else 0

with st.form("kline_query_form", clear_on_submit=False):
    c1, c2 = st.columns(2)

    with c1:
        selected_group = st.selectbox(
            "選擇群組",
            group_names,
            index=group_index
        )

    items = watchlist_dict.get(selected_group, [])
    stock_options = build_stock_options(items, lookup_date)

    with c2:
        if stock_options:
            saved_stock_code = st.session_state.get("kline_stock_code", "")
            stock_codes = [x["code"] for x in stock_options]
            stock_index = stock_codes.index(saved_stock_code) if saved_stock_code in stock_codes else 0

            selected_stock_label = st.selectbox(
                "選擇股票",
                [x["label"] for x in stock_options],
                index=stock_index
            )
            selected_stock = next(x for x in stock_options if x["label"] == selected_stock_label)
        else:
            selected_stock = None
            st.selectbox("選擇股票", ["此群組目前沒有股票"], index=0)

    d1, d2 = st.columns(2)

    with d1:
        start_date = st.date_input(
            "開始日期",
            value=st.session_state.get("kline_start", today_dt - timedelta(days=90))
        )

    with d2:
        end_date = st.date_input(
            "結束日期",
            value=st.session_state.get("kline_end", today_dt)
        )

    query_btn = st.form_submit_button("開始查詢", type="primary", use_container_width=True)

if query_btn:
    if start_date > end_date:
        st.error("開始日期不能大於結束日期")
        st.stop()

    if selected_stock is None:
        st.warning("此群組目前沒有可查詢股票。")
        st.stop()

    st.session_state.kline_group = selected_group
    st.session_state.kline_stock_code = selected_stock["code"]
    st.session_state.kline_start = start_date
    st.session_state.kline_end = end_date

    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code=selected_stock["code"],
        home_start=start_date,
        home_end=end_date
    )

    with st.spinner("正在查詢歷史資料..."):
        df = get_history_data(
            selected_stock["code"],
            selected_stock["name"],
            selected_stock["market"],
            start_date,
            end_date
        )

    st.session_state.kline_result_df = df
    st.session_state.kline_selected_stock = selected_stock
    st.session_state.kline_selected_group = selected_group

result_df = st.session_state.get("kline_result_df", None)
result_stock = st.session_state.get("kline_selected_stock", None)
result_group = st.session_state.get("kline_selected_group", None)

if result_df is not None and result_stock is not None:
    st.markdown(
        f"""
**目前查詢條件：**  
群組：{result_group}  
股票：{result_stock['name']}（{result_stock['code']}）  
市場別：{result_stock['market']}  
日期區間：{st.session_state.kline_start} ~ {st.session_state.kline_end}
"""
    )

    df = prepare_display_df(result_df)

    if df.empty:
        st.warning("查無歷史資料。")
        st.stop()

    if len(df) < 10:
        st.info("目前資料筆數不足 10 筆，MA10 以現有資料平均顯示。")
    if len(df) < 20:
        st.info("目前資料筆數不足 20 筆，MA20 以現有資料平均顯示。")

    render_summary_metrics(df)

    st.markdown("---")
    st.subheader("歷史資料")
    render_table(df)
    render_chart(df)

    st.success(f"查詢完成，共 {len(df)} 筆資料。")
else:
    st.info("請先選擇條件後按「開始查詢」。")
