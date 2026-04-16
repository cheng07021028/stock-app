from datetime import date, timedelta
import streamlit as st

from utils import (
    get_normalized_watchlist,
    apply_font_scale,
    get_font_scale,
    get_all_code_name_map,
    get_stock_name_and_market,
    get_realtime_stock_info,
    load_last_query_state,
    save_last_query_state,
    parse_date_safe,
    inject_pro_theme,
    render_pro_hero,
    render_pro_section,
    render_pro_kpi_row,
    render_pro_info_card,
    format_number,
)


@st.cache_data(ttl=600, show_spinner=False)
def build_group_stock_options(watchlist_dict, lookup_date):
    all_code_name_df = get_all_code_name_map(lookup_date)
    result = {}

    for group_name, items in watchlist_dict.items():
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

        result[group_name] = stock_options

    return result


st.set_page_config(page_title="股市專家系統", page_icon="📈", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

if "home_state_loaded" not in st.session_state:
    last_state = load_last_query_state()
    today_dt = date.today()

    st.session_state.last_quick_group = last_state.get("quick_group", "")
    st.session_state.last_quick_stock_code = last_state.get("quick_stock_code", "")
    st.session_state.home_start = parse_date_safe(
        last_state.get("home_start", ""),
        today_dt - timedelta(days=90)
    )
    st.session_state.home_end = parse_date_safe(
        last_state.get("home_end", ""),
        today_dt
    )
    st.session_state.home_state_loaded = True

with st.sidebar:
    st.markdown("## 顯示設定")
    st.session_state.font_scale = st.slider(
        "字體大小 (%)",
        100,
        220,
        st.session_state.font_scale,
        10
    )

apply_font_scale(st.session_state.font_scale)
inject_pro_theme()

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")

watchlist_dict = get_normalized_watchlist()
group_stock_options = build_group_stock_options(watchlist_dict, lookup_date)
all_code_name_df = get_all_code_name_map(lookup_date)

render_pro_hero(
    "股市專家系統",
    "專業盤面首頁｜整合自選股、即時資訊、查詢條件與歷史分析入口"
)

group_count = len(watchlist_dict)
stock_count = sum(len(v) for v in watchlist_dict.values())

render_pro_kpi_row([
    {"label": "群組數量", "value": f"{group_count:,}", "delta": "Watchlist Groups", "delta_class": "pro-kpi-delta-flat"},
    {"label": "自選股總數", "value": f"{stock_count:,}", "delta": "Tracked Symbols", "delta_class": "pro-kpi-delta-flat"},
    {"label": "今日日期", "value": today_dt.strftime("%Y-%m-%d"), "delta": "Trading Dashboard", "delta_class": "pro-kpi-delta-flat"},
])

render_pro_section("快速查詢入口", "先在首頁選條件，再切換到歷史K線分析頁直接使用")

group_names = list(watchlist_dict.keys())

if group_names:
    saved_group = st.session_state.get("last_quick_group", "")
    group_index = group_names.index(saved_group) if saved_group in group_names else 0

    with st.form("home_quick_query_form", clear_on_submit=False):
        q1, q2 = st.columns(2)

        with q1:
            quick_group = st.selectbox(
                "選擇群組",
                group_names,
                index=group_index
            )

        stock_options = group_stock_options.get(quick_group, [])

        with q2:
            if stock_options:
                saved_stock_code = st.session_state.get("last_quick_stock_code", "")
                stock_codes = [x["code"] for x in stock_options]
                stock_index = stock_codes.index(saved_stock_code) if saved_stock_code in stock_codes else 0

                quick_stock_label = st.selectbox(
                    "選擇股票",
                    [x["label"] for x in stock_options],
                    index=stock_index
                )
                quick_stock = next(x for x in stock_options if x["label"] == quick_stock_label)
            else:
                quick_stock = None
                st.selectbox("選擇股票", ["此群組目前沒有股票"], index=0)

        d1, d2 = st.columns(2)
        with d1:
            quick_start = st.date_input(
                "開始日期",
                value=st.session_state.get("home_start", today_dt - timedelta(days=90))
            )
        with d2:
            quick_end = st.date_input(
                "結束日期",
                value=st.session_state.get("home_end", today_dt)
            )

        save_btn = st.form_submit_button("套用查詢條件", type="primary", use_container_width=True)

    if save_btn:
        if quick_start > quick_end:
            st.error("開始日期不能大於結束日期")
            st.stop()

        st.session_state.last_quick_group = quick_group
        st.session_state.last_quick_stock_code = quick_stock["code"] if quick_stock is not None else ""
        st.session_state.home_start = quick_start
        st.session_state.home_end = quick_end

        save_last_query_state(
            quick_group=quick_group,
            quick_stock_code=quick_stock["code"] if quick_stock is not None else "",
            home_start=quick_start,
            home_end=quick_end
        )

        st.success("查詢條件已更新，切換到左側『歷史K線分析』頁面即可直接使用。")

    current_group = st.session_state.get("last_quick_group", group_names[0])
    current_start = st.session_state.get("home_start", today_dt - timedelta(days=90))
    current_end = st.session_state.get("home_end", today_dt)

    current_stock_code = st.session_state.get("last_quick_stock_code", "")
    current_stock = None

    current_group_options = group_stock_options.get(current_group, [])
    for item in current_group_options:
        if item["code"] == current_stock_code:
            current_stock = item
            break

    if current_start > current_end:
        st.error("開始日期不能大於結束日期")
    else:
        if current_stock is not None:
            info = get_realtime_stock_info(
                current_stock["code"],
                current_stock["name"],
                current_stock["market"]
            )

            price = info.get("price")
            change = info.get("change")
            change_pct = info.get("change_pct")

            if change is not None and change_pct is not None:
                delta_text = f"{change:+.2f} / {change_pct:+.2f}%"
                delta_class = "pro-up" if change > 0 else "pro-down" if change < 0 else "pro-flat"
            else:
                delta_text = "Real-time Snapshot"
                delta_class = "pro-flat"

            render_pro_info_card(
                "今日即時總覽",
                [
                    ("群組", current_group, ""),
                    ("股票", f"{current_stock['name']}（{current_stock['code']}）", ""),
                    ("市場別", current_stock["market"], ""),
                    ("日期區間", f"{current_start} ~ {current_end}", ""),
                    ("現價", format_number(price, 2), ""),
                    ("漲跌 / 幅度", delta_text, delta_class),
                    ("更新時間", info.get("update_time", "—"), ""),
                    ("資料狀態", "即時連線成功" if info.get("ok") else "即時資料異常", ""),
                ],
                chips=["首頁條件", "即時資訊", "歷史分析入口"]
            )

        st.info("首頁提供快速選擇；實際查詢請切換到左側『歷史K線分析』頁面。")
else:
    st.warning("目前沒有自選股群組，請先到『自選股中心』建立群組與股票。")

render_pro_section("系統功能", "目前已完成共用版整合，接下來可進一步做更高階盤面與分析功能")

st.markdown("""
- 儀表板：查看各群組最新行情摘要  
- 行情查詢：單支股票最新行情與近況查詢  
- 歷史K線分析：依股票與日期區間查詢歷史資料  
- 自選股中心：建立群組、新增與刪除股票  
- 排行榜：查看股票排行資訊  
""")
