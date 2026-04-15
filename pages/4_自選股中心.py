from datetime import date
import streamlit as st
from utils import get_all_code_name_map, search_stocks, load_watchlist, save_watchlist

st.set_page_config(page_title="自選股中心", page_icon="⭐", layout="wide")
st.title("⭐ 自選股中心")

if "watchlist" not in st.session_state:
    st.session_state.watchlist = load_watchlist()

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")
all_code_name_df = get_all_code_name_map(lookup_date)

if all_code_name_df.empty:
    st.error("無法取得股票清單。")
    st.stop()

all_code_name_df["顯示"] = (
    all_code_name_df["證券名稱"] +
    " (" +
    all_code_name_df["證券代號"] +
    ") [" +
    all_code_name_df["市場別"] +
    "]"
)

st.markdown("### 建立新群組")
new_group_name = st.text_input("群組名稱", value="")
if st.button("建立群組"):
    new_group_name = new_group_name.strip()
    if not new_group_name:
        st.warning("請輸入群組名稱。")
    elif new_group_name in st.session_state.watchlist:
        st.warning("群組已存在。")
    else:
        st.session_state.watchlist[new_group_name] = []
        save_watchlist(st.session_state.watchlist)
        st.success(f"已建立群組：{new_group_name}")
        st.rerun()

st.markdown("### 搜尋股票加入群組")
search_text = st.text_input("搜尋股票（代號或名稱）", value="")
filtered_df = search_stocks(all_code_name_df, search_text, top_n=50)

selected_to_add = st.multiselect(
    "搜尋結果",
    options=filtered_df["顯示"].tolist() if not filtered_df.empty else []
)

group_names = list(st.session_state.watchlist.keys())
selected_group = st.selectbox("加入到群組", options=group_names)

if st.button("加入股票到群組"):
    add_codes = [item.split("(")[-1].split(")")[0].strip() for item in selected_to_add]
    current_codes = st.session_state.watchlist.get(selected_group, [])
    st.session_state.watchlist[selected_group] = list(dict.fromkeys(current_codes + add_codes))
    save_watchlist(st.session_state.watchlist)
    st.success(f"已加入群組：{selected_group}")
    st.rerun()

st.markdown("### 目前群組內容")
for group_name, codes in st.session_state.watchlist.items():
    st.markdown(f"#### {group_name}")
    if not codes:
        st.info("此群組目前沒有股票。")
        continue

    group_df = all_code_name_df[all_code_name_df["證券代號"].isin(codes)].copy()
    st.dataframe(group_df[["證券代號", "證券名稱", "市場別"]], use_container_width=True, hide_index=True)

    remove_targets = st.multiselect(
        f"移除 {group_name} 的股票",
        options=group_df["顯示"].tolist(),
        key=f"remove_{group_name}"
    )

    if st.button(f"確認移除：{group_name}", key=f"btn_remove_{group_name}"):
        remove_codes = [item.split("(")[-1].split(")")[0].strip() for item in remove_targets]
        st.session_state.watchlist[group_name] = [x for x in codes if x not in remove_codes]
        save_watchlist(st.session_state.watchlist)
        st.success(f"已更新群組：{group_name}")
        st.rerun()