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
    get_realtime_stock_info,
    render_realtime_info_card,
    to_excel_bytes,
    load_last_query_state,
    save_last_query_state,
    parse_date_safe,
)


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


def add_indicators(df: pd.DataFrame, selected_indicators: list):
    if df.empty or "收盤價" not in df.columns:
        return df

    ma_map = {
        "MA5": 5,
        "MA10": 10,
        "MA20": 20,
        "MA60": 60,
        "MA120": 120,
        "MA240": 240,
    }

    for ma_name, window in ma_map.items():
        if ma_name in selected_indicators:
            df[ma_name] = df["收盤價"].rolling(window=window, min_periods=1).mean()

    if "KD" in selected_indicators and all(col in df.columns for col in ["最高價", "最低價", "收盤價"]):
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

    if "MACD" in selected_indicators:
        ema12 = df["收盤價"].ewm(span=12, adjust=False).mean()
        ema26 = df["收盤價"].ewm(span=26, adjust=False).mean()
        df["DIF"] = ema12 - ema26
        df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
        df["MACD_HIST"] = df["DIF"] - df["DEA"]

    return df


def render_table(df: pd.DataFrame):
    if df.empty:
        st.info("目前沒有可顯示的資料。")
        return

    base_cols = ["日期", "開盤價", "最高價", "最低價", "收盤價", "成交股數", "成交金額", "成交筆數"]
    indicator_cols = ["MA5", "MA10", "MA20", "MA60", "MA120", "MA240", "K", "D", "DIF", "DEA", "MACD_HIST"]
    show_cols = [c for c in base_cols + indicator_cols if c in df.columns]

    display_df = df[show_cols].copy()

    if "日期" in display_df.columns:
        display_df["日期"] = pd.to_datetime(display_df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")

    if "收盤價" in display_df.columns:
        display_df["漲跌"] = display_df["收盤價"].diff()
        cols = list(display_df.columns)
        insert_at = cols.index("收盤價") + 1
        ordered_cols = cols[:insert_at] + ["漲跌"] + [c for c in cols[insert_at:] if c != "漲跌"]
        display_df = display_df[ordered_cols]

    format_dict = {}
    for col in ["開盤價", "最高價", "最低價", "收盤價", "漲跌", "MA5", "MA10", "MA20", "MA60", "MA120", "MA240", "K", "D", "DIF", "DEA", "MACD_HIST"]:
        if col in display_df.columns:
            format_dict[col] = "{:,.2f}"
    for col in ["成交股數", "成交金額", "成交筆數"]:
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
            return "color: #d32f2f; font-weight: 700;"
        elif v < 0:
            return "color: #00897b; font-weight: 700;"
        return "color: #666;"

    styler = display_df.style.format(format_dict, na_rep="—")
    if "漲跌" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌"])

    st.dataframe(styler, use_container_width=True, hide_index=True, height=680)


def render_chart(df: pd.DataFrame, selected_indicators: list):
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
                vertical_spacing=0.04,
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
            ),
            row=1,
            col=1
        )

        ma_colors = {
            "MA5": "#4A90E2",
            "MA10": "#F44336",
            "MA20": "#F5A623",
            "MA60": "#8E44AD",
            "MA120": "#7F8C8D",
            "MA240": "#2C3E50",
        }

        for ma_name, color in ma_colors.items():
            if ma_name in selected_indicators and ma_name in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df["日期"],
                        y=df[ma_name],
                        mode="lines",
                        name=ma_name,
                        line=dict(width=2.2, color=color),
                    ),
                    row=1,
                    col=1
                )

        if has_volume:
            fig.add_trace(
                go.Bar(
                    x=df["日期"],
                    y=df["成交股數"],
                    name="成交量",
                ),
                row=2,
                col=1
            )

        fig.update_layout(
            height=780 if has_volume else 560,
            xaxis_rangeslider_visible=False,
            hovermode="x unified",
        )

        st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

    if "KD" in selected_indicators and all(col in df.columns for col in ["日期", "K", "D"]):
        with st.expander("查看 KD 指標", expanded=False):
            kd_fig = go.Figure()
            kd_fig.add_trace(go.Scatter(x=df["日期"], y=df["K"], mode="lines", name="K"))
            kd_fig.add_trace(go.Scatter(x=df["日期"], y=df["D"], mode="lines", name="D"))
            kd_fig.update_layout(height=320, hovermode="x unified")
            st.plotly_chart(kd_fig, use_container_width=True, config={"displaylogo": False})

    if "MACD" in selected_indicators and all(col in df.columns for col in ["日期", "DIF", "DEA", "MACD_HIST"]):
        with st.expander("查看 MACD 指標", expanded=False):
            macd_fig = go.Figure()
            macd_fig.add_trace(go.Bar(x=df["日期"], y=df["MACD_HIST"], name="MACD柱"))
            macd_fig.add_trace(go.Scatter(x=df["日期"], y=df["DIF"], mode="lines", name="DIF"))
            macd_fig.add_trace(go.Scatter(x=df["日期"], y=df["DEA"], mode="lines", name="DEA"))
            macd_fig.update_layout(height=360, hovermode="x unified")
            st.plotly_chart(macd_fig, use_container_width=True, config={"displaylogo": False})


st.set_page_config(page_title="歷史K線分析", page_icon="📊", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)

watchlist_dict = get_normalized_watchlist()
group_names = list(watchlist_dict.keys())

st.title("📊 歷史K線分析")
st.caption("正式整合版｜查詢歷史資料與技術指標")

if not group_names:
    st.warning("目前沒有自選股群組，請先到「自選股中心」建立群組與股票。")
    st.stop()

last_state = load_last_query_state()
today_dt = date.today()

default_group = last_state.get("quick_group", "")
default_code = last_state.get("quick_stock_code", "")
default_start = parse_date_safe(last_state.get("home_start", ""), today_dt - timedelta(days=90))
default_end = parse_date_safe(last_state.get("home_end", ""), today_dt)

group_index = group_names.index(default_group) if default_group in group_names else 0
lookup_date = today_dt.strftime("%Y%m%d")

with st.form("history_query_form", clear_on_submit=False):
    c1, c2 = st.columns(2)

    with c1:
        selected_group = st.selectbox("選擇群組", group_names, index=group_index)

    items = watchlist_dict.get(selected_group, [])
    stock_options = build_stock_options(items, lookup_date)

    with c2:
        if stock_options:
            code_list = [x["code"] for x in stock_options]
            stock_index = code_list.index(default_code) if default_code in code_list else 0
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
        start_date = st.date_input("開始日期", value=default_start)
    with d2:
        end_date = st.date_input("結束日期", value=default_end)

    selected_indicators = st.multiselect(
        "技術指標",
        ["MA5", "MA10", "MA20", "MA60", "MA120", "MA240", "KD", "MACD"],
        default=["MA5", "MA10", "MA20"]
    )

    query_btn = st.form_submit_button("開始查詢", type="primary", use_container_width=True)

if not selected_stock:
    st.warning("此群組目前沒有可查詢股票。")
    st.stop()

if start_date > end_date:
    st.error("開始日期不能大於結束日期")
    st.stop()

if query_btn or selected_stock:
    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code=selected_stock["code"],
        home_start=start_date,
        home_end=end_date
    )

    realtime_info = get_realtime_stock_info(
        selected_stock["code"],
        selected_stock["name"],
        selected_stock["market"]
    )
    render_realtime_info_card(realtime_info, title="今日即時資訊")

    with st.spinner("正在查詢歷史資料..."):
        df = get_history_data(
            selected_stock["code"],
            selected_stock["name"],
            selected_stock["market"],
            start_date,
            end_date
        )

    if df.empty:
        st.warning("查無歷史資料。")
        st.stop()

    df = add_indicators(df, selected_indicators)

    st.markdown(
        f"""
**目前查詢條件：**  
群組：{selected_group}  
股票：{selected_stock['name']}（{selected_stock['code']}）  
市場別：{selected_stock['market']}  
日期區間：{start_date} ~ {end_date}  
技術指標：{", ".join(selected_indicators) if selected_indicators else "無"}
"""
    )

    download_df = df.copy()
    if "日期" in download_df.columns:
        download_df["日期"] = pd.to_datetime(download_df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")

    excel_bytes = to_excel_bytes({"歷史K線資料": download_df})

    d1, d2 = st.columns([1, 5])
    with d1:
        st.download_button(
            label="下載 Excel",
            data=excel_bytes,
            file_name=f"{selected_stock['code']}_{selected_stock['name']}_歷史K線分析.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    st.markdown("---")
    st.subheader("歷史資料")
    render_table(df)
    render_chart(df, selected_indicators)

    st.success(f"查詢完成，共 {len(df)} 筆資料。")
