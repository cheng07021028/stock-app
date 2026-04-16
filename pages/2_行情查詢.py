from datetime import date
import streamlit as st

from utils import (
    get_normalized_watchlist,
    get_all_code_name_map,
    get_stock_name_and_market,
    get_realtime_stock_info,
    render_realtime_info_card,
    apply_font_scale,
    get_font_scale,
)


@st.cache_data(ttl=600, show_spinner=False)
def build_stock_options(items, lookup_date):
    all_code_name_df = get_all_code_name_map(lookup_date)
    stock_options = []

    for item in items:
        code = str(item.get("code", "")).strip()
        manual_name = str(item.get("name", "")).strip()

        if not code:
            continue

        stock_name, market_type = get_stock_name_and_market(code, all_code_name_df, manual_name)
        stock_options.append({
            "label": f"{stock_name} ({code}) [{market_type}]",
            "code": code,
            "name": stock_name,
            "market": market_type,
        })

    return stock_options


st.set_page_config(page_title="行情查詢", page_icon="📈", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)

st.title("📈 行情查詢")
st.caption("正式整合版｜查詢單一股票即時資訊")

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")

watchlist_dict = get_normalized_watchlist()
group_names = list(watchlist_dict.keys())

if not group_names:
    st.warning("目前沒有自選股群組，請先到「自選股中心」建立群組與股票。")
    st.stop()

c1, c2 = st.columns(2)

with c1:
    selected_group = st.selectbox("選擇群組", group_names, index=0)

items = watchlist_dict.get(selected_group, [])
stock_options = build_stock_options(items, lookup_date)

with c2:
    if stock_options:
        selected_stock_label = st.selectbox(
            "選擇股票",
            [x["label"] for x in stock_options],
            index=0
        )
        selected_stock = next(x for x in stock_options if x["label"] == selected_stock_label)
    else:
        selected_stock = None
        st.selectbox("選擇股票", ["此群組目前沒有股票"], index=0)

if selected_stock is None:
    st.warning("此群組目前沒有可查詢股票。")
    st.stop()

if st.button("查詢即時資訊", type="primary", use_container_width=True):
    with st.spinner("正在查詢即時資訊..."):
        info = get_realtime_stock_info(
            selected_stock["code"],
            selected_stock["name"],
            selected_stock["market"]
        )
    render_realtime_info_card(info, title="即時行情")
else:
    info = get_realtime_stock_info(
        selected_stock["code"],
        selected_stock["name"],
        selected_stock["market"]
    )
    render_realtime_info_card(info, title="即時行情")
