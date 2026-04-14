from datetime import date
import streamlit as st
from utils import load_watchlist

st.set_page_config(page_title="股市專家系統", page_icon="📈", layout="wide")

st.title("📈 股市專家系統")
st.caption("專業版多頁系統")

watchlist_dict = load_watchlist()

group_count = len(watchlist_dict)
stock_count = sum(len(v) for v in watchlist_dict.values())

c1, c2, c3 = st.columns(3)
with c1:
    st.metric("群組數量", group_count)
with c2:
    st.metric("自選股總數", stock_count)
with c3:
    st.metric("今日日期", date.today().strftime("%Y-%m-%d"))

st.markdown("## 系統功能")
st.markdown("""
請從左側選單進入各功能頁：

- 儀表板
- 行情查詢
- 歷史 K 線分析
- 自選股中心
- 排行榜
""")

st.info("建議先到「自選股中心」建立你的股票群組，再到「儀表板」或「行情查詢」查看。")