import streamlit as st
from stock_master_service import (
    load_stock_master,
    refresh_stock_master,
    search_stock_master,
    get_stock_master_categories,
    get_stock_master_diagnostics,
)

st.set_page_config(page_title="股票主檔更新", layout="wide")
st.title("股票主檔更新")

master_df = load_stock_master()
all_categories = ["全部"] + get_stock_master_categories(master_df)

c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
with c1:
    kw = st.text_input("搜尋股票代號 / 名稱 / 正式產業別 / 主題類別")
with c2:
    market_filter = st.selectbox("市場別", ["全部", "上市", "上櫃", "興櫃"])
with c3:
    category_filter = st.selectbox("類別 / 產業篩選", all_categories)
with c4:
    st.write("")
    st.write("")
    refresh_btn = st.button("更新股票主檔", use_container_width=True, type="primary")

if refresh_btn:
    with st.spinner("更新股票主檔中..."):
        master_df, logs = refresh_stock_master()
    st.success("股票主檔已更新")
    for line in logs:
        st.caption(line)

with st.expander("主檔診斷訊息", expanded=False):
    for line in get_stock_master_diagnostics():
        st.write(f"- {line}")

found_df = search_stock_master(
    master_df,
    keyword=kw,
    market_filter=market_filter,
    category_filter=category_filter,
)

st.dataframe(found_df, use_container_width=True, height=700)
