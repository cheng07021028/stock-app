from datetime import date
import streamlit as st

from utils import (
    get_normalized_watchlist,
    get_realtime_watchlist_df,
    render_realtime_table,
    apply_font_scale,
    get_font_scale,
)

st.set_page_config(page_title="儀表板", page_icon="📊", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)

st.title("📊 儀表板")
st.caption("正式整合版｜顯示自選股群組即時摘要")

watchlist_dict = get_normalized_watchlist()
if not watchlist_dict:
    st.warning("目前沒有自選股群組。")
    st.stop()

query_date = date.today().strftime("%Y%m%d")

c1, c2, c3 = st.columns(3)
with c1:
    st.metric("群組數量", len(watchlist_dict))
with c2:
    st.metric("自選股總數", sum(len(v) for v in watchlist_dict.values()))
with c3:
    st.metric("今日日期", date.today().strftime("%Y-%m-%d"))

if st.button("更新即時資料", type="primary", use_container_width=True):
    get_realtime_watchlist_df.clear()

with st.spinner("正在讀取即時資料..."):
    realtime_df = get_realtime_watchlist_df(watchlist_dict, query_date)

if realtime_df.empty:
    st.info("目前沒有可顯示的即時資料。")
else:
    render_realtime_table(realtime_df, height=680)
