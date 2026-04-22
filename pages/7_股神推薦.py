# -*- coding: utf-8 -*-
from __future__ import annotations

import traceback
import streamlit as st
import pandas as pd

st.set_page_config(page_title="股神推薦", layout="wide")

def main():
    st.title("股神推薦")
    st.caption("診斷版：先確認頁面能正常顯示，再接回完整推薦功能")

    try:
        from stock_master_service import (
            load_stock_master,
            search_stock_master,
            get_stock_master_categories,
            get_stock_master_diagnostics,
        )
        st.success("stock_master_service 匯入成功")
    except Exception as e:
        st.error(f"stock_master_service 匯入失敗：{type(e).__name__}: {e}")
        st.code(traceback.format_exc())
        return

    try:
        master_df = load_stock_master()
        st.success(f"主檔載入成功：{len(master_df) if isinstance(master_df, pd.DataFrame) else 0} 筆")
    except Exception as e:
        st.error(f"load_stock_master() 執行失敗：{type(e).__name__}: {e}")
        st.code(traceback.format_exc())
        return

    try:
        all_categories = get_stock_master_categories(master_df)
    except Exception as e:
        st.error(f"get_stock_master_categories() 失敗：{type(e).__name__}: {e}")
        st.code(traceback.format_exc())
        all_categories = []

    c1, c2, c3 = st.columns([3, 2, 2])
    with c1:
        kw = st.text_input("搜尋股票代號 / 名稱 / 正式產業別 / 主題類別")
    with c2:
        market_filter = st.selectbox("市場別篩選", ["全部", "上市", "上櫃", "興櫃"])
    with c3:
        category_filter = st.selectbox("類別 / 產業篩選", ["全部"] + all_categories)

    try:
        found_df = search_stock_master(
            master_df,
            keyword=kw,
            market_filter=market_filter,
            category_filter=category_filter,
        )
    except Exception as e:
        st.error(f"search_stock_master() 失敗：{type(e).__name__}: {e}")
        st.code(traceback.format_exc())
        found_df = pd.DataFrame()

    with st.expander("主檔診斷訊息", expanded=True):
        try:
            for line in get_stock_master_diagnostics(master_df):
                st.write(f"- {line}")
        except Exception as e:
            st.error(f"get_stock_master_diagnostics() 失敗：{type(e).__name__}: {e}")
            st.code(traceback.format_exc())

    st.dataframe(found_df, use_container_width=True, height=600)

if __name__ == "__main__":
    main()
