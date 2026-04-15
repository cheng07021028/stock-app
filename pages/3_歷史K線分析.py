from datetime import date, timedelta
import pandas as pd
import streamlit as st

from utils import (
    get_normalized_watchlist,
    get_all_code_name_map,
    get_history_data,
    apply_font_scale,
    get_font_scale,
    format_number,
    get_stock_name_and_market,
)

st.set_page_config(page_title="歷史K線分析", page_icon="📈", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

with st.sidebar:
    st.markdown("## 顯示設定")
    st.session_state.font_scale = st.slider("字體大小 (%)", 100, 220, st.session_state.font_scale, 10)

apply_font_scale(st.session_state.font_scale)

st.title("📈 歷史K線分析")
st.caption("可依自選股群組選擇股票與日期區間查詢歷史資料")

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")

watchlist_dict = get_normalized_watchlist()
if not watchlist_dict:
    st.warning("目前沒有自選股群組，請先到自選股中心建立清單。")
    st.stop()

all_code_name_df = get_all_code_name_map(lookup_date)
if all_code_name_df.empty:
    st.info("目前使用備援模式，部分股票名稱可能以內建對照或代號顯示。")


def get_stock_display_options(group_name: str):
    items = watchlist_dict.get(group_name, [])
    options = []

    for item in items:
        code = str(item.get("code", "")).strip()
        manual_name = str(item.get("name", "")).strip()
        stock_name, market_type = get_stock_name_and_market(code, all_code_name_df, manual_name)

        options.append({
            "label": f"{stock_name} ({code}) [{market_type}]",
            "code": code,
            "name": stock_name,
            "market": market_type
        })

    return options


group_names = list(watchlist_dict.keys())

if "history_start_dt" not in st.session_state:
    st.session_state.history_start_dt = today_dt - timedelta(days=90)
if "history_end_dt" not in st.session_state:
    st.session_state.history_end_dt = today_dt
if "history_result_df" not in st.session_state:
    st.session_state.history_result_df = None
if "history_selected_stock" not in st.session_state:
    st.session_state.history_selected_stock = None

with st.form("history_form"):
    c1, c2 = st.columns(2)

    with c1:
        selected_group = st.selectbox("選擇群組", group_names, index=0)

    stock_options = get_stock_display_options(selected_group)

    if not stock_options:
        st.warning("此群組目前沒有股票。")
        st.stop()

    with c2:
        selected_label = st.selectbox(
            "選擇股票",
            [x["label"] for x in stock_options],
            index=0
        )

    selected_stock = next(x for x in stock_options if x["label"] == selected_label)

    d1, d2, d3 = st.columns([1, 1, 1])

    with d1:
        start_dt = st.date_input(
            "開始日期",
            value=st.session_state.history_start_dt,
            key="history_start_dt"
        )

    with d2:
        end_dt = st.date_input(
            "結束日期",
            value=st.session_state.history_end_dt,
            key="history_end_dt"
        )

    with d3:
        st.markdown("<br>", unsafe_allow_html=True)
        query_btn = st.form_submit_button("開始查詢", use_container_width=True)

if start_dt > end_dt:
    st.error("開始日期不能大於結束日期")
    st.stop()

st.markdown("---")
st.write(f"**目前選擇：** {selected_stock['name']}（{selected_stock['code']}） / {selected_stock['market']}")
st.write(f"**查詢區間：** {start_dt} ~ {end_dt}")

if query_btn:
    with st.spinner("正在抓取歷史資料..."):
        hist_df = get_history_data(
            stock_no=selected_stock["code"],
            stock_name=selected_stock["name"],
            market_type=selected_stock["market"],
            start_dt=start_dt,
            end_dt=end_dt
        )

    st.session_state.history_result_df = hist_df
    st.session_state.history_selected_stock = selected_stock

hist_df = st.session_state.history_result_df
saved_stock = st.session_state.history_selected_stock

if hist_df is None or saved_stock is None:
    st.info("請先選擇條件後按『開始查詢』。")
    st.stop()

if hist_df.empty:
    st.warning("查無歷史資料。可能是資料來源暫時無回應，或該股票目前不支援此抓取方式。")
    st.stop()

show_df = hist_df.copy()

if "日期" in show_df.columns:
    show_df["日期"] = pd.to_datetime(show_df["日期"]).dt.strftime("%Y-%m-%d")

numeric_cols = ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌價差", "成交筆數"]
for col in numeric_cols:
    if col in show_df.columns:
        digits = 0 if col in ["成交股數", "成交金額", "成交筆數"] else 2
        show_df[col] = show_df[col].apply(lambda x: format_number(x, digits) if pd.notna(x) else "")

st.subheader("歷史資料明細")
st.dataframe(show_df, use_container_width=True, hide_index=True)

chart_df = hist_df.copy()
chart_df = chart_df.dropna(subset=["日期", "收盤價"])

if not chart_df.empty:
    chart_df = chart_df.sort_values("日期")
    chart_df = chart_df.set_index("日期")

    st.subheader("收盤價走勢")
    st.line_chart(chart_df["收盤價"], use_container_width=True)

    latest_row = hist_df.sort_values("日期").iloc[-1]
    mc1, mc2, mc3, mc4 = st.columns(4)
    with mc1:
        st.metric("最新收盤價", format_number(latest_row.get("收盤價")))
    with mc2:
        st.metric("最高價", format_number(latest_row.get("最高價")))
    with mc3:
        st.metric("最低價", format_number(latest_row.get("最低價")))
    with mc4:
        st.metric("成交股數", format_number(latest_row.get("成交股數"), 0))
