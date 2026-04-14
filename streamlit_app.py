from datetime import date
import streamlit as st
from utils import load_watchlist

st.set_page_config(page_title="股市專家系統", page_icon="📈", layout="wide")

st.title("📈 股市專家系統")
st.caption("多頁專業版：儀表板、行情查詢、歷史K線分析、自選股中心、排行榜")

watchlist = load_watchlist()

c1, c2, c3 = st.columns(3)
with c1:
    st.metric("自選股數量", len(watchlist))
with c2:
    st.metric("今日日期", date.today().strftime("%Y-%m-%d"))
with c3:
    st.metric("系統模式", "專業版")

st.markdown("### 使用方式")
st.write("請從左側功能大綱切換到不同頁面。")

st.markdown("""
目前頁面建議配置：

- 儀表板：總覽
- 行情查詢：延遲 / 近即時行情
- 歷史K線分析：K線、均線、成交量
- 自選股中心：新增/刪除/管理
- 排行榜：成交值、成交量、價格排行
""")