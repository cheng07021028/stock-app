from datetime import date
import streamlit as st
from utils import load_watchlist, apply_font_scale, get_font_scale

st.set_page_config(page_title="股市專家系統", page_icon="📈", layout="wide")

with st.sidebar:
    st.markdown("## 顯示設定")
    st.session_state.font_scale = st.slider("字體大小 (%)", 80, 160, get_font_scale(), 5)

apply_font_scale(st.session_state.font_scale)

st.title("📈 股市專家系統")
st.caption("專業版多頁系統首頁")

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
- 歷史K線分析
- 自選股中心
- 排行榜
""")

st.info("先到左側頁面操作。建議流程：自選股中心 → 儀表板 → 行情查詢 → 歷史K線分析")