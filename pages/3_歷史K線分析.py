from datetime import date, timedelta
import pandas as pd
import streamlit as st

from utils import (
    get_normalized_watchlist,
    apply_font_scale,
    get_font_scale,
    get_all_code_name_map,
    get_stock_name_and_market,
    get_history_data,
)

from query_state import load_last_query_state, save_last_query_state, parse_date_safe

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

st.title("📊 歷史K線分析")
st.caption("依群組、股票與日期區間查詢歷史K線資料")

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")

watchlist_dict = get_normalized_watchlist()
all_code_name_df = get_all_code_name_map(lookup_date)

group_names = list(watchlist_dict.keys())

if not group_names:
    st.warning("目前沒有自選股群組，請先到「自選股中心」建立群組與股票。")
    st.stop()

saved_group = st.session_state.get("kline_group", "")
group_index = group_names.index(saved_group) if saved_group in group_names else 0

c1, c2 = st.columns(2)

with c1:
    selected_group = st.selectbox(
        "選擇群組",
        group_names,
        index=group_index,
        key="kline_group_selectbox"
    )

items = watchlist_dict.get(selected_group, [])
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

with c2:
    if stock_options:
        saved_stock_code = st.session_state.get("kline_stock_code", "")
        stock_codes = [x["code"] for x in stock_options]
        stock_index = stock_codes.index(saved_stock_code) if saved_stock_code in stock_codes else 0

        selected_stock_label = st.selectbox(
            "選擇股票",
            [x["label"] for x in stock_options],
            index=stock_index,
            key="kline_stock_selectbox"
        )
        selected_stock = next(x for x in stock_options if x["label"] == selected_stock_label)
    else:
        selected_stock = None
        st.selectbox("選擇股票", ["此群組目前沒有股票"], index=0, key="kline_stock_empty")

d1, d2 = st.columns(2)

with d1:
    start_date = st.date_input(
        "開始日期",
        value=st.session_state.kline_start,
        key="kline_start"
    )

with d2:
    end_date = st.date_input(
        "結束日期",
        value=st.session_state.kline_end,
        key="kline_end"
    )

st.session_state.kline_group = selected_group
st.session_state.kline_stock_code = selected_stock["code"] if selected_stock is not None else ""

save_last_query_state(
    quick_group=selected_group,
    quick_stock_code=selected_stock["code"] if selected_stock is not None else "",
    home_start=start_date,
    home_end=end_date
)

if start_date > end_date:
    st.error("開始日期不能大於結束日期")
    st.stop()

if selected_stock is None:
    st.warning("此群組目前沒有可查詢股票。")
    st.stop()

st.markdown(
    f"""
**目前查詢條件：**  
群組：{selected_group}  
股票：{selected_stock['name']}（{selected_stock['code']}）  
市場別：{selected_stock['market']}  
日期區間：{start_date} ~ {end_date}
"""
)

query_btn = st.button("開始查詢", type="primary", use_container_width=True)

if query_btn:
    with st.spinner("正在查詢歷史資料..."):
        try:
            df = get_history_data(
                selected_stock["code"],
                selected_stock["name"],
                selected_stock["market"],
                start_date,
                end_date
            )
        except Exception as e:
            st.error(f"歷史資料查詢失敗：{e}")
            st.stop()

    if df is None or len(df) == 0:
        st.warning("查無歷史資料。")
        st.stop()

    df = pd.DataFrame(df).copy()

    st.markdown("---")
    st.subheader("歷史資料")

    show_cols = [c for c in ["日期", "開盤價", "最高價", "最低價", "收盤價", "成交股數", "成交金額", "成交筆數"] if c in df.columns]
    if show_cols:
        st.dataframe(df[show_cols], use_container_width=True, hide_index=True)
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    if "收盤價" in df.columns and "日期" in df.columns:
        chart_df = df[["日期", "收盤價"]].copy().set_index("日期")
        st.subheader("收盤價走勢圖")
        st.line_chart(chart_df, use_container_width=True)

    if "成交股數" in df.columns and "日期" in df.columns:
        vol_df = df[["日期", "成交股數"]].copy().set_index("日期")
        st.subheader("成交量走勢圖")
        st.bar_chart(vol_df, use_container_width=True)

    st.success(f"查詢完成，共 {len(df)} 筆資料。")
