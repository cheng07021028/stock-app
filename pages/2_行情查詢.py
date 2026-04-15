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

st.set_page_config(page_title="行情查詢", page_icon="📌", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

with st.sidebar:
    st.markdown("## 顯示設定")
    st.session_state.font_scale = st.slider("字體大小 (%)", 100, 220, st.session_state.font_scale, 10)

apply_font_scale(st.session_state.font_scale)

st.title("📌 行情查詢")
st.caption("可從自選股群組選擇股票，或直接輸入股票代號查詢最新行情")

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")

watchlist_dict = get_normalized_watchlist()
all_code_name_df = get_all_code_name_map(lookup_date)

if all_code_name_df.empty:
    st.info("目前使用備援模式，部分股票名稱可能以內建對照或代號顯示。")


def get_group_stock_options():
    result = {}
    for group_name, items in watchlist_dict.items():
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
        result[group_name] = options
    return result


group_stock_map = get_group_stock_options()
group_names = list(group_stock_map.keys())

if "quote_result_df" not in st.session_state:
    st.session_state.quote_result_df = None
if "quote_selected_code" not in st.session_state:
    st.session_state.quote_selected_code = None
if "quote_selected_name" not in st.session_state:
    st.session_state.quote_selected_name = None
if "quote_selected_market" not in st.session_state:
    st.session_state.quote_selected_market = None

with st.form("quote_form"):
    tab1, tab2 = st.tabs(["從自選股選擇", "直接輸入代號"])

    selected_code = None
    selected_name = None
    selected_market = None

    with tab1:
        if not group_names:
            st.warning("目前沒有自選股群組，請先到自選股中心建立。")
        else:
            c1, c2 = st.columns(2)

            with c1:
                selected_group = st.selectbox("選擇群組", group_names, index=0)

            stock_options = group_stock_map.get(selected_group, [])

            with c2:
                if stock_options:
                    selected_label = st.selectbox(
                        "選擇股票",
                        [x["label"] for x in stock_options],
                        index=0
                    )
                    selected_item = next(x for x in stock_options if x["label"] == selected_label)
                    selected_code = selected_item["code"]
                    selected_name = selected_item["name"]
                    selected_market = selected_item["market"]
                else:
                    st.warning("此群組目前沒有股票。")

    with tab2:
        manual_code = st.text_input("輸入股票代號", placeholder="例如：2330")
        if manual_code.strip():
            code = manual_code.strip()
            name, market = get_stock_name_and_market(code, all_code_name_df, "")
            selected_code = code
            selected_name = name
            selected_market = market

    query_btn = st.form_submit_button("開始查詢", use_container_width=True)

if query_btn:
    if not selected_code:
        st.warning("請先選擇股票或輸入股票代號。")
        st.stop()

    with st.spinner("正在抓取行情資料..."):
        start_dt = today_dt - timedelta(days=40)
        end_dt = today_dt
        hist_df = get_history_data(
            stock_no=selected_code,
            stock_name=selected_name,
            market_type=selected_market,
            start_dt=start_dt,
            end_dt=end_dt
        )

    st.session_state.quote_result_df = hist_df
    st.session_state.quote_selected_code = selected_code
    st.session_state.quote_selected_name = selected_name
    st.session_state.quote_selected_market = selected_market

hist_df = st.session_state.quote_result_df
selected_code = st.session_state.quote_selected_code
selected_name = st.session_state.quote_selected_name
selected_market = st.session_state.quote_selected_market

st.markdown("---")

if hist_df is None or not selected_code:
    st.info("請先選擇條件後按『開始查詢』。")
    st.stop()

st.subheader(f"{selected_name} ({selected_code})")
st.caption(f"市場別：{selected_market}")

if hist_df.empty:
    st.warning("查無行情資料。可能是資料來源暫時無回應，或此股票目前不支援此抓取方式。")
    st.stop()

hist_df = hist_df.sort_values("日期").reset_index(drop=True)
latest = hist_df.iloc[-1]

prev_close = None
if len(hist_df) >= 2:
    prev_close = hist_df.iloc[-2].get("收盤價")

latest_close = latest.get("收盤價")
price_change = None
pct_change = None

if prev_close is not None and latest_close is not None:
    price_change = latest_close - prev_close
    if prev_close != 0:
        pct_change = (price_change / prev_close) * 100

delta_text = ""
if price_change is not None and pct_change is not None:
    sign = "+" if price_change >= 0 else ""
    delta_text = f"{sign}{price_change:.2f} ({sign}{pct_change:.2f}%)"

m1, m2, m3, m4 = st.columns(4)
with m1:
    st.metric("最新價", format_number(latest.get("收盤價")), delta=delta_text)
with m2:
    st.metric("開盤價", format_number(latest.get("開盤價")))
with m3:
    st.metric("最高價", format_number(latest.get("最高價")))
with m4:
    st.metric("最低價", format_number(latest.get("最低價")))

m5, m6, m7 = st.columns(3)
with m5:
    st.metric("成交股數", format_number(latest.get("成交股數"), 0))
with m6:
    st.metric("成交金額", format_number(latest.get("成交金額"), 0))
with m7:
    st.metric("成交筆數", format_number(latest.get("成交筆數"), 0))

st.markdown("---")

chart_df = hist_df.dropna(subset=["日期", "收盤價"]).copy()
if not chart_df.empty:
    chart_df = chart_df.sort_values("日期").set_index("日期")
    st.subheader("近 30 天收盤走勢")
    st.line_chart(chart_df["收盤價"], use_container_width=True)

st.markdown("---")

show_df = hist_df.copy()
if "日期" in show_df.columns:
    show_df["日期"] = pd.to_datetime(show_df["日期"]).dt.strftime("%Y-%m-%d")

for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌價差", "成交筆數"]:
    if col in show_df.columns:
        digits = 0 if col in ["成交股數", "成交金額", "成交筆數"] else 2
        show_df[col] = show_df[col].apply(lambda x: format_number(x, digits) if pd.notna(x) else "")

st.subheader("歷史明細")
st.dataframe(show_df, use_container_width=True, hide_index=True)
