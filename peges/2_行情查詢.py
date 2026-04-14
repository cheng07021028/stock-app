from datetime import date, timedelta
import pandas as pd
import streamlit as st

from utils import (
    get_all_code_name_map,
    search_stocks,
    get_history_data,
    format_number,
    load_watchlist,
    save_watchlist,
)

st.set_page_config(page_title="行情查詢", page_icon="💹", layout="wide")
st.title("💹 行情查詢")
st.caption("可先搜尋並加入分組自選股，再從群組中選擇股票查詢。")

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")
all_code_name_df = get_all_code_name_map(lookup_date)

if all_code_name_df.empty:
    st.error("無法取得股票清單。")
    st.stop()

if "watchlist" not in st.session_state:
    st.session_state.watchlist = load_watchlist()

all_code_name_df["顯示"] = (
    all_code_name_df["證券名稱"] +
    " (" +
    all_code_name_df["證券代號"] +
    ") [" +
    all_code_name_df["市場別"] +
    "]"
)

st.markdown("### 搜尋並加入分組自選股")

top_left, top_mid, top_right = st.columns([2, 1, 1])

with top_left:
    search_text = st.text_input("搜尋股票（代號或名稱）", value="")
    filtered_df = search_stocks(all_code_name_df, search_text, top_n=50)

    selected_to_add = st.multiselect(
        "搜尋結果",
        options=filtered_df["顯示"].tolist() if not filtered_df.empty else []
    )

with top_mid:
    existing_groups = list(st.session_state.watchlist.keys())
    selected_group = st.selectbox("加入到群組", options=existing_groups)

with top_right:
    new_group_name = st.text_input("新增群組名稱", value="")
    if st.button("建立新群組", use_container_width=True):
        new_group_name = new_group_name.strip()
        if not new_group_name:
            st.warning("請輸入群組名稱。")
        elif new_group_name in st.session_state.watchlist:
            st.warning("這個群組已存在。")
        else:
            st.session_state.watchlist[new_group_name] = []
            save_watchlist(st.session_state.watchlist)
            st.success(f"已建立群組：{new_group_name}")
            st.rerun()

if st.button("加入自選股"):
    add_codes = []
    for item in selected_to_add:
        code = item.split("(")[-1].split(")")[0].strip()
        add_codes.append(code)

    group_codes = st.session_state.watchlist.get(selected_group, [])
    merged = list(dict.fromkeys(group_codes + add_codes))
    st.session_state.watchlist[selected_group] = merged
    save_watchlist(st.session_state.watchlist)
    st.success(f"已加入群組：{selected_group}")
    st.rerun()

st.markdown("### 從分組自選股查詢")

group_names = list(st.session_state.watchlist.keys())
if not group_names:
    st.warning("目前沒有任何自選股群組。")
    st.stop()

left, right = st.columns([2, 1])

with left:
    query_group = st.selectbox("選擇群組", options=group_names)

group_codes = st.session_state.watchlist.get(query_group, [])

group_watchlist_df = all_code_name_df[
    all_code_name_df["證券代號"].isin(group_codes)
].copy()

if group_watchlist_df.empty:
    st.warning(f"群組「{query_group}」目前沒有股票。")
    st.stop()

group_watchlist_df["顯示"] = (
    group_watchlist_df["證券名稱"] +
    " (" +
    group_watchlist_df["證券代號"] +
    ") [" +
    group_watchlist_df["市場別"] +
    "]"
)

with left:
    selected = st.selectbox(
        "選擇股票（僅顯示該群組）",
        options=group_watchlist_df["顯示"].tolist()
    )

with right:
    auto_refresh = st.toggle("自動刷新", value=False)
    refresh_seconds = st.selectbox("刷新秒數", options=[10, 15, 30, 60], index=2)

if not selected:
    st.stop()

stock_code = selected.split("(")[-1].split(")")[0].strip()
stock_row = group_watchlist_df[group_watchlist_df["證券代號"] == stock_code].iloc[0]
stock_name = stock_row["證券名稱"]
market_type = stock_row["市場別"]


@st.cache_data(ttl=15, show_spinner=False)
def get_latest_quote_snapshot(stock_code: str, stock_name: str, market_type: str) -> tuple[pd.DataFrame, dict]:
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=40)

    hist_df = get_history_data(stock_code, stock_name, market_type, start_dt, end_dt)

    if hist_df.empty:
        return pd.DataFrame(), {}

    hist_df = hist_df.sort_values("日期").reset_index(drop=True)
    latest = hist_df.iloc[-1].to_dict()

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

    snapshot = {
        "股票代號": stock_code,
        "股票名稱": stock_name,
        "市場別": market_type,
        "日期": latest.get("日期"),
        "開盤價": latest.get("開盤價"),
        "最高價": latest.get("最高價"),
        "最低價": latest.get("最低價"),
        "最新價": latest_close,
        "成交股數": latest.get("成交股數"),
        "成交金額": latest.get("成交金額"),
        "成交筆數": latest.get("成交筆數"),
        "漲跌": price_change,
        "漲跌幅%": pct_change,
    }

    return hist_df, snapshot


def render_quote(snapshot: dict, hist_df: pd.DataFrame):
    if not snapshot:
        st.warning("查無資料。")
        return

    latest_price = snapshot.get("最新價")
    change = snapshot.get("漲跌")
    pct = snapshot.get("漲跌幅%")

    delta_text = None
    if change is not None and pct is not None:
        sign = "+" if change >= 0 else ""
        delta_text = f"{sign}{change:,.2f} ({sign}{pct:,.2f}%)"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("股票", f"{snapshot['股票代號']} {snapshot['股票名稱']}")
    with c2:
        st.metric("最新價", format_number(latest_price), delta=delta_text)
    with c3:
        st.metric("最高價", format_number(snapshot.get("最高價")))
    with c4:
        st.metric("最低價", format_number(snapshot.get("最低價")))

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        st.metric("開盤價", format_number(snapshot.get("開盤價")))
    with c6:
        st.metric("成交股數", format_number(snapshot.get("成交股數"), 0))
    with c7:
        st.metric("成交金額", format_number(snapshot.get("成交金額"), 0))
    with c8:
        st.metric("成交筆數", format_number(snapshot.get("成交筆數"), 0))

    st.markdown("### 最近資料")
    show_df = hist_df.copy()

    if "日期" in show_df.columns:
        show_df["日期"] = pd.to_datetime(show_df["日期"]).dt.strftime("%Y-%m-%d")

    for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌價差", "成交筆數"]:
        if col in show_df.columns:
            show_df[col] = show_df[col].apply(format_number)

    st.dataframe(show_df.tail(10), use_container_width=True, hide_index=True)


if auto_refresh:
    st.info("目前為延遲/近即時模式。切換自動刷新後會週期更新頁面。")

hist_df, snapshot = get_latest_quote_snapshot(stock_code, stock_name, market_type)
render_quote(snapshot, hist_df)