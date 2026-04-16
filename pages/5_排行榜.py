from datetime import date
import streamlit as st

from utils import (
    get_normalized_watchlist,
    get_realtime_watchlist_df,
    render_realtime_table,
    apply_font_scale,
    get_font_scale,
)

st.set_page_config(page_title="排行榜", page_icon="🏆", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)

st.title("🏆 排行榜")
st.caption("正式整合版｜依即時漲跌幅或成交量排序")

watchlist_dict = get_normalized_watchlist()
if not watchlist_dict:
    st.warning("目前沒有自選股群組。")
    st.stop()

query_date = date.today().strftime("%Y%m%d")

sort_col = st.selectbox(
    "排序方式",
    ["漲跌幅(%)", "漲跌", "總量", "現價"],
    index=0
)

ascending = st.toggle("升冪排序", value=False)

if st.button("更新排行榜", type="primary", use_container_width=True):
    get_realtime_watchlist_df.clear()

with st.spinner("正在讀取排行榜資料..."):
    realtime_df = get_realtime_watchlist_df(watchlist_dict, query_date)

if realtime_df.empty:
    st.info("目前沒有資料。")
    st.stop()

rank_df = realtime_df.copy()
if sort_col in rank_df.columns:
    rank_df = rank_df.sort_values(by=sort_col, ascending=ascending, na_position="last").reset_index(drop=True)

render_realtime_table(rank_df, height=700)
