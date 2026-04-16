from datetime import date, timedelta
import streamlit as st

from utils import (
    get_normalized_watchlist,
    get_all_code_name_map,
    get_stock_name_and_market,
    get_realtime_stock_info,
    get_history_data,
    apply_font_scale,
    get_font_scale,
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_section,
    format_number,
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


def render_quote_summary(stock_info: dict, history_df):
    price = stock_info.get("price")
    prev_close = stock_info.get("prev_close")
    change = stock_info.get("change")
    change_pct = stock_info.get("change_pct")

    change_text = "—"
    change_class = ""
    if change is not None and change_pct is not None:
        change_text = f"{change:+.2f} / {change_pct:+.2f}%"
        change_class = "pro-up" if change > 0 else "pro-down" if change < 0 else "pro-flat"

    high_30 = history_df["最高價"].max() if history_df is not None and not history_df.empty and "最高價" in history_df.columns else None
    low_30 = history_df["最低價"].min() if history_df is not None and not history_df.empty and "最低價" in history_df.columns else None
    avg_30 = history_df["收盤價"].mean() if history_df is not None and not history_df.empty and "收盤價" in history_df.columns else None

    render_pro_info_card(
        "單股即時總覽",
        [
            ("股票", f"{stock_info.get('name', '—')}（{stock_info.get('code', '—')}）", ""),
            ("市場別", stock_info.get("market", "—"), ""),
            ("現價", format_number(price, 2), ""),
            ("昨收", format_number(prev_close, 2), ""),
            ("即時漲跌", change_text, change_class),
            ("開盤", format_number(stock_info.get("open"), 2), ""),
            ("最高", format_number(stock_info.get("high"), 2), ""),
            ("最低", format_number(stock_info.get("low"), 2), ""),
            ("總量", format_number(stock_info.get("total_volume"), 0), ""),
            ("近30日最高", format_number(high_30, 2), ""),
            ("近30日最低", format_number(low_30, 2), ""),
            ("近30日均價", format_number(avg_30, 2), ""),
            ("更新時間", stock_info.get("update_time", "—"), ""),
        ],
        chips=["即時行情", "單股監控", "短週期檢視"]
    )


st.set_page_config(page_title="行情查詢", page_icon="📈", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)
inject_pro_theme()

render_pro_hero(
    "行情查詢",
    "單股工作站｜聚焦單一股票的即時狀態、短週期高低點與盤中位置"
)

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

with st.spinner("正在查詢即時資訊..."):
    info = get_realtime_stock_info(
        selected_stock["code"],
        selected_stock["name"],
        selected_stock["market"]
    )

with st.spinner("正在讀取近 30 日資料..."):
    history_df = get_history_data(
        selected_stock["code"],
        selected_stock["name"],
        selected_stock["market"],
        today_dt - timedelta(days=30),
        today_dt
    )

render_quote_summary(info, history_df)

render_pro_section("盤中觀察提示", "這一頁重點是快速判讀單一標的的當下位置，不取代完整 K 線分析頁")

st.markdown("""
- 看 **現價 vs 昨收**：先判斷方向  
- 看 **開高走低 / 開低走高**：觀察盤中強弱  
- 看 **近 30 日最高 / 最低**：判斷是否接近區間邊界  
- 看 **總量**：確認波動是否有量能支持  
""")
