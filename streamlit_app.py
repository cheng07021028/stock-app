from datetime import date, timedelta
from urllib.parse import quote

import pandas as pd
import streamlit as st

from utils import load_watchlist, apply_font_scale, get_font_scale, get_all_code_name_map

st.set_page_config(page_title="股市專家系統", page_icon="📈", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

with st.sidebar:
    st.markdown("## 顯示設定")
    st.session_state.font_scale = st.slider("字體大小 (%)", 100, 220, st.session_state.font_scale, 10)

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

st.markdown("---")
st.subheader("快速查詢入口")
st.caption("可在首頁快速選擇群組、股票與日期區間，再切換到『歷史K線分析』頁面使用")

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

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")
all_code_name_df = get_all_code_name_map(lookup_date)

if all_code_name_df.empty:
    st.info("目前使用備援模式，部分股票名稱可能以內建對照或代號顯示。")


def guess_market_type(code: str) -> str:
    code = str(code).strip()
    if code.startswith("00"):
        return "上市"
    if code in ["3711"]:
        return "上市"
    return "上市"


def get_stock_name_and_market(code: str):
    code = str(code).strip()

    if not all_code_name_df.empty:
        match = all_code_name_df[all_code_name_df["證券代號"] == code]
        if not match.empty:
            row = match.iloc[0]
            return str(row["證券名稱"]).strip(), str(row["市場別"]).strip()

    return FALLBACK_NAME_MAP.get(code, f"股票{code}"), guess_market_type(code)


group_names = list(watchlist_dict.keys())

if group_names:
    q1, q2 = st.columns(2)

    with q1:
        quick_group = st.selectbox("選擇群組", group_names, index=0)

    codes = watchlist_dict.get(quick_group, [])
    stock_options = []

    for code in codes:
        stock_name, market_type = get_stock_name_and_market(code)
        stock_options.append({
            "label": f"{stock_name} ({code}) [{market_type}]",
            "code": str(code).strip(),
            "name": stock_name,
            "market": market_type,
        })

    with q2:
        if stock_options:
            quick_stock_label = st.selectbox(
                "選擇股票",
                [x["label"] for x in stock_options],
                index=0
            )
            quick_stock = next(x for x in stock_options if x["label"] == quick_stock_label)
        else:
            quick_stock = None
            st.selectbox("選擇股票", ["此群組目前沒有股票"], index=0)

    d1, d2 = st.columns(2)
    with d1:
        quick_start = st.date_input("開始日期", today_dt - timedelta(days=90), key="home_start")
    with d2:
        quick_end = st.date_input("結束日期", today_dt, key="home_end")

    if quick_start > quick_end:
        st.error("開始日期不能大於結束日期")
    else:
        if quick_stock is not None:
            st.markdown(
                f"""
                **目前快速查詢條件：**  
                群組：{quick_group}  
                股票：{quick_stock['name']}（{quick_stock['code']}）  
                市場別：{quick_stock['market']}  
                日期區間：{quick_start} ~ {quick_end}
                """
            )

            st.info("首頁提供快速選擇；實際查詢請切換到左側『歷史K線分析』頁面。")
else:
    st.warning("目前沒有自選股群組，請先到『自選股中心』建立群組與股票。")

st.markdown("---")
st.subheader("系統功能")

st.markdown("""
- 儀表板：查看各群組最新行情摘要
- 行情查詢：單支股票最新行情與近 30 天走勢
- 歷史K線分析：依股票與日期區間查詢歷史資料
- 自選股中心：建立群組、新增與刪除股票
- 排行榜：查看股票排行資訊
""")

st.info("建議流程：自選股中心 → 儀表板 → 行情查詢 → 歷史K線分析")
