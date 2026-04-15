from datetime import date, timedelta
import pandas as pd
import streamlit as st
from utils import get_all_code_name_map, get_history_data, format_number

st.set_page_config(page_title="排行榜", page_icon="🏆", layout="wide")
st.title("🏆 排行榜")

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")
all_code_name_df = get_all_code_name_map(lookup_date)

if all_code_name_df.empty:
    st.error("無法取得股票清單。")
    st.stop()

sample_df = all_code_name_df.head(30).copy()
rows = []
start_dt = today_dt - timedelta(days=40)
end_dt = today_dt

with st.spinner("正在整理排行資料..."):
    for _, row in sample_df.iterrows():
        hist_df = get_history_data(row["證券代號"], row["證券名稱"], row["市場別"], start_dt, end_dt)
        if hist_df.empty:
            continue
        latest = hist_df.iloc[-1]
        rows.append({
            "證券代號": row["證券代號"],
            "證券名稱": row["證券名稱"],
            "市場別": row["市場別"],
            "最新收盤價": latest.get("收盤價"),
            "成交股數": latest.get("成交股數"),
            "成交金額": latest.get("成交金額"),
        })

rank_df = pd.DataFrame(rows)

if rank_df.empty:
    st.warning("目前沒有可排行資料。")
    st.stop()

sort_col = st.selectbox("排序欄位", ["成交金額", "成交股數", "最新收盤價"])
rank_df = rank_df.sort_values(sort_col, ascending=False).reset_index(drop=True)

show_df = rank_df.copy()
for col in ["最新收盤價", "成交股數", "成交金額"]:
    show_df[col] = show_df[col].apply(format_number)

st.dataframe(show_df, use_container_width=True, hide_index=True)