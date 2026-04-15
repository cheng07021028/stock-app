import streamlit as st
import pandas as pd

from utils import (
    load_watchlist,
    save_watchlist,
    get_all_code_name_map,
    apply_font_scale,
    get_font_scale,
)

st.set_page_config(page_title="自選股中心", page_icon="⭐", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

with st.sidebar:
    st.markdown("## 顯示設定")
    st.session_state.font_scale = st.slider("字體大小 (%)", 100, 220, st.session_state.font_scale, 10)

apply_font_scale(st.session_state.font_scale)

st.title("⭐ 自選股中心")
st.caption("可管理自選股群組、手動新增股票代號、刪除股票與群組")

watchlist_dict = load_watchlist()

lookup_df = get_all_code_name_map(pd.Timestamp.today().strftime("%Y%m%d"))
api_ok = not lookup_df.empty

if not api_ok:
    st.info("目前使用手動代號模式，仍可新增與管理自選股。")

FALLBACK_NAME_MAP = {
    "2330": "台積電",
    "2454": "聯發科",
    "3711": "日月光投控",
    "2317": "鴻海",
    "2382": "廣達",
    "0050": "元大台灣50",
    "0056": "元大高股息",
    "2881": "富邦金",
    "2882": "國泰金"
}


def get_stock_name(code: str) -> str:
    code = str(code).strip()

    if api_ok:
        match = lookup_df[lookup_df["證券代號"] == code]
        if not match.empty:
            return str(match.iloc[0]["證券名稱"]).strip()

    return FALLBACK_NAME_MAP.get(code, f"股票{code}")


def get_market_type(code: str) -> str:
    code = str(code).strip()

    if api_ok:
        match = lookup_df[lookup_df["證券代號"] == code]
        if not match.empty:
            return str(match.iloc[0]["市場別"]).strip()

    if code.startswith("00"):
        return "上市"
    return "上市"


st.markdown("---")
st.subheader("新增群組")

c1, c2 = st.columns([3, 1])
with c1:
    new_group_name = st.text_input("新群組名稱", placeholder="例如：AI概念股")
with c2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("新增群組", use_container_width=True):
        group_name = new_group_name.strip()
        if not group_name:
            st.warning("請輸入群組名稱")
        elif group_name in watchlist_dict:
            st.warning("群組已存在")
        else:
            watchlist_dict[group_name] = []
            save_watchlist(watchlist_dict)
            st.success(f"已新增群組：{group_name}")
            st.rerun()

st.markdown("---")
st.subheader("新增股票")

group_names = list(watchlist_dict.keys())
if not group_names:
    st.warning("目前沒有群組，請先新增群組。")
    st.stop()

a1, a2 = st.columns(2)
with a1:
    selected_group = st.selectbox("選擇群組", group_names, index=0)
with a2:
    stock_code_input = st.text_input("股票代號", placeholder="例如：2330")

if st.button("加入自選股", use_container_width=True):
    code = stock_code_input.strip()

    if not code:
        st.warning("請輸入股票代號")
    else:
        current_codes = watchlist_dict.get(selected_group, [])
        if code in current_codes:
            st.warning(f"{code} 已存在於群組「{selected_group}」")
        else:
            current_codes.append(code)
            watchlist_dict[selected_group] = list(dict.fromkeys(current_codes))
            save_watchlist(watchlist_dict)
            st.success(f"已加入 {get_stock_name(code)} ({code}) 到群組「{selected_group}」")
            st.rerun()

st.markdown("---")
st.subheader("目前自選股清單")

all_rows = []
for group_name, codes in watchlist_dict.items():
    if not codes:
        all_rows.append({
            "群組": group_name,
            "證券代號": "",
            "證券名稱": "",
            "市場別": ""
        })
        continue

    for code in codes:
        all_rows.append({
            "群組": group_name,
            "證券代號": code,
            "證券名稱": get_stock_name(code),
            "市場別": get_market_type(code)
        })

if all_rows:
    df = pd.DataFrame(all_rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("目前沒有任何自選股資料。")

st.markdown("---")
st.subheader("刪除股票")

d1, d2 = st.columns(2)
with d1:
    delete_group = st.selectbox("選擇要刪除股票的群組", group_names, index=0, key="delete_group")

delete_options = watchlist_dict.get(delete_group, [])
delete_labels = [f"{get_stock_name(code)} ({code})" for code in delete_options]

with d2:
    if delete_labels:
        delete_stock_label = st.selectbox("選擇股票", delete_labels, index=0, key="delete_stock")
    else:
        delete_stock_label = None
        st.selectbox("選擇股票", ["此群組無股票"], index=0, key="delete_stock_empty")

if st.button("刪除選定股票", use_container_width=True):
    if not delete_options:
        st.warning("此群組沒有可刪除的股票")
    else:
        delete_code = delete_stock_label.split("(")[-1].replace(")", "").strip()
        watchlist_dict[delete_group] = [x for x in watchlist_dict[delete_group] if str(x).strip() != delete_code]
        save_watchlist(watchlist_dict)
        st.success(f"已刪除 {delete_stock_label}")
        st.rerun()

st.markdown("---")
st.subheader("刪除群組")

g1, g2 = st.columns([3, 1])
with g1:
    delete_group_name = st.selectbox("選擇要刪除的群組", group_names, index=0, key="drop_group")
with g2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("刪除群組", use_container_width=True):
        if delete_group_name in watchlist_dict:
            del watchlist_dict[delete_group_name]
            save_watchlist(watchlist_dict)
            st.success(f"已刪除群組：{delete_group_name}")
            st.rerun()
