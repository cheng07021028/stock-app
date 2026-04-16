from datetime import date, timedelta
import pandas as pd
import streamlit as st

from utils import (
    get_normalized_watchlist,
    apply_font_scale,
    get_font_scale,
    get_all_code_name_map,
    get_stock_name_and_market,
    get_realtime_stock_info,
    get_realtime_watchlist_df,
    get_history_data,
    load_last_query_state,
    save_last_query_state,
    parse_date_safe,
    inject_pro_theme,
    render_pro_hero,
    render_pro_section,
    render_pro_kpi_row,
    render_pro_info_card,
    format_number,
    compute_signal_snapshot,
    score_to_badge,
    compute_support_resistance_snapshot,
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
                "group": group_name,
                "label": f"{stock_name} ({code}) [{market_type}]",
                "code": code,
                "name": stock_name,
                "market": market_type,
            })

        result[group_name] = stock_options

    return result


@st.cache_data(ttl=600, show_spinner=False)
def build_all_stock_options(watchlist_dict, lookup_date):
    grouped = build_group_stock_options(watchlist_dict, lookup_date)
    all_options = []
    for group_name, items in grouped.items():
        for item in items:
            all_options.append({
                **item,
                "search_label": f"{item['name']} ({item['code']}) [{item['market']}]｜{group_name}"
            })
    return all_options


def filter_stock_options(all_options, keyword):
    keyword = str(keyword).strip().lower()
    if not keyword:
        return all_options[:30]

    matched = []
    for item in all_options:
        pool = " ".join([
            str(item.get("name", "")),
            str(item.get("code", "")),
            str(item.get("market", "")),
            str(item.get("group", "")),
            str(item.get("search_label", "")),
        ]).lower()
        if keyword in pool:
            matched.append(item)
    return matched[:50]


def add_home_indicators(df: pd.DataFrame):
    if df is None or df.empty or "收盤價" not in df.columns:
        return df

    df = df.copy()
    df["MA5"] = df["收盤價"].rolling(window=5, min_periods=1).mean()
    df["MA10"] = df["收盤價"].rolling(window=10, min_periods=1).mean()
    df["MA20"] = df["收盤價"].rolling(window=20, min_periods=1).mean()

    if all(col in df.columns for col in ["最高價", "最低價", "收盤價"]):
        low_n = df["最低價"].rolling(window=9, min_periods=1).min()
        high_n = df["最高價"].rolling(window=9, min_periods=1).max()
        denom = (high_n - low_n).replace(0, pd.NA)
        rsv = ((df["收盤價"] - low_n) / denom * 100).fillna(0)

        k_values, d_values = [], []
        k_prev, d_prev = 50.0, 50.0
        for val in rsv:
            val = float(val)
            k_now = (2 / 3) * k_prev + (1 / 3) * val
            d_now = (2 / 3) * d_prev + (1 / 3) * k_now
            k_values.append(k_now)
            d_values.append(d_now)
            k_prev, d_prev = k_now, d_now

        df["K"] = k_values
        df["D"] = d_values

    ema12 = df["收盤價"].ewm(span=12, adjust=False).mean()
    ema26 = df["收盤價"].ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["DIF"] - df["DEA"]
    return df


@st.cache_data(ttl=900, show_spinner=False)
def build_home_market_snapshot(watchlist_dict, query_date):
    realtime_df = get_realtime_watchlist_df(watchlist_dict, query_date)
    if realtime_df is None or realtime_df.empty:
        return realtime_df, None, None, 0, 0, pd.DataFrame()

    rows = []
    strong_count = 0
    weak_count = 0

    for _, row in realtime_df.iterrows():
        code = str(row.get("股票代號", "")).strip()
        name = str(row.get("股票名稱", "")).strip()
        market = str(row.get("市場別", "")).strip() or "上市"
        group = str(row.get("群組", "")).strip()

        hist = get_history_data(
            code,
            name,
            market,
            date.today() - timedelta(days=120),
            date.today()
        )

        if hist is None or hist.empty:
            rows.append({
                "群組": group,
                "股票代號": code,
                "股票名稱": name,
                "市場別": market,
                "現價": row.get("現價"),
                "漲跌": row.get("漲跌"),
                "漲跌幅(%)": row.get("漲跌幅(%)"),
                "總量": row.get("總量"),
                "更新時間": row.get("更新時間"),
                "綜合評級": "無資料",
                "訊號分數": None,
                "均線結構": "無資料",
                "突破狀態": "無資料",
                "20日壓力": None,
                "20日支撐": None,
                "距20壓力(%)": None,
                "距20支撐(%)": None,
                "推薦類型": "",
                "推薦原因": "",
            })
            continue

        hist = add_home_indicators(hist)
        signal = compute_signal_snapshot(hist)
        sr = compute_support_resistance_snapshot(hist)
        badge_text, _ = score_to_badge(signal.get("score", 0))

        if badge_text == "強多":
            strong_count += 1
        elif badge_text == "強空":
            weak_count += 1

        change_pct = row.get("漲跌幅(%)")
        signal_score = signal.get("score", 0)
        dist_res_20 = sr.get("dist_res_20_pct")
        dist_sup_20 = sr.get("dist_sup_20_pct")
        breakout = signal.get("breakout_20d", ("", ""))[0]

        rec_type = ""
        rec_reason = ""

        if signal_score >= 3 and change_pct is not None and pd.notna(change_pct) and change_pct > 0:
            rec_type = "今日優先觀察"
            rec_reason = f"訊號分數 {signal_score:+d}，且即時漲跌幅 {change_pct:+.2f}%"
            if "突破20日高" in breakout:
                rec_reason += "，並出現 20 日突破"

        if dist_sup_20 is not None and 0 <= dist_sup_20 <= 2.0:
            if not rec_type:
                rec_type = "接近支撐可觀察"
                rec_reason = f"距 20 日支撐僅 {dist_sup_20:.2f}%"

        if dist_res_20 is not None and 0 <= dist_res_20 <= 2.0:
            if not rec_type:
                rec_type = "接近壓力要小心"
                rec_reason = f"距 20 日壓力僅 {dist_res_20:.2f}%"

        if signal_score <= -3:
            rec_type = "今日偏弱避開"
            rec_reason = f"訊號分數 {signal_score:+d}，偏弱訊號明顯"

        rows.append({
            "群組": group,
            "股票代號": code,
            "股票名稱": name,
            "市場別": market,
            "現價": row.get("現價"),
            "漲跌": row.get("漲跌"),
            "漲跌幅(%)": row.get("漲跌幅(%)"),
            "總量": row.get("總量"),
            "更新時間": row.get("更新時間"),
            "綜合評級": badge_text,
            "訊號分數": signal_score,
            "均線結構": signal["ma_trend"][0],
            "突破狀態": breakout,
            "20日壓力": sr.get("res_20"),
            "20日支撐": sr.get("sup_20"),
            "距20壓力(%)": dist_res_20,
            "距20支撐(%)": dist_sup_20,
            "推薦類型": rec_type,
            "推薦原因": rec_reason,
        })

    analysis_df = pd.DataFrame(rows)

    ranked = realtime_df.sort_values(by="漲跌幅(%)", ascending=False, na_position="last").reset_index(drop=True)
    top_gainer = ranked.iloc[0].to_dict() if not ranked.empty else None
    top_loser = ranked.sort_values(by="漲跌幅(%)", ascending=True, na_position="last").iloc[0].to_dict() if not ranked.empty else None

    return realtime_df, top_gainer, top_loser, strong_count, weak_count, analysis_df


def render_recommendation_block(df: pd.DataFrame, rec_type: str, title: str, chip: str):
    sub = df[df["推薦類型"] == rec_type].copy() if df is not None and not df.empty else pd.DataFrame()

    render_pro_section(title, "由訊號分數、支撐壓力距離與突破狀態自動整理")

    if sub.empty:
        render_pro_info_card(
            title,
            [("結果", "目前沒有符合條件的標的", "")],
            chips=[chip]
        )
        return

    sub = sub.sort_values(by=["訊號分數", "漲跌幅(%)"], ascending=[False, False], na_position="last").head(3)

    info_pairs = []
    for _, row in sub.iterrows():
        name = f"{row['股票名稱']}（{row['股票代號']}）"
        reason = row.get("推薦原因", "—")
        extra = f"現價 {format_number(row.get('現價'), 2)}｜評級 {row.get('綜合評級', '—')}"
        info_pairs.append((name, f"{reason}｜{extra}", ""))

    render_pro_info_card(
        title,
        info_pairs,
        chips=[chip]
    )


st.set_page_config(page_title="股市專家系統", page_icon="📈", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

if "home_state_loaded" not in st.session_state:
    last_state = load_last_query_state()
    today_dt = date.today()

    st.session_state.last_quick_group = last_state.get("quick_group", "")
    st.session_state.last_quick_stock_code = last_state.get("quick_stock_code", "")
    st.session_state.home_start = parse_date_safe(last_state.get("home_start", ""), today_dt - timedelta(days=90))
    st.session_state.home_end = parse_date_safe(last_state.get("home_end", ""), today_dt)
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
all_stock_options = build_all_stock_options(watchlist_dict, lookup_date)

render_pro_hero(
    "股市專家系統｜自動推薦版",
    "整合盤面快照、今日強弱、自動推薦、快速搜尋與歷史分析入口。"
)

group_count = len(watchlist_dict)
stock_count = sum(len(v) for v in watchlist_dict.values())

realtime_df, top_gainer, top_loser, strong_count, weak_count, analysis_df = build_home_market_snapshot(watchlist_dict, lookup_date)

avg_change_pct = None
if realtime_df is not None and not realtime_df.empty and "漲跌幅(%)" in realtime_df.columns:
    avg_change_pct = realtime_df["漲跌幅(%)"].mean()

avg_text = f"{avg_change_pct:+.2f}%" if avg_change_pct is not None and pd.notna(avg_change_pct) else "—"

render_pro_kpi_row([
    {"label": "群組數量", "value": f"{group_count:,}", "delta": "Watchlist Groups", "delta_class": "pro-kpi-delta-flat"},
    {"label": "自選股總數", "value": f"{stock_count:,}", "delta": "Tracked Symbols", "delta_class": "pro-kpi-delta-flat"},
    {"label": "平均漲跌幅", "value": avg_text, "delta": "Market Breadth", "delta_class": "pro-kpi-delta-flat"},
    {"label": "強多 / 強空", "value": f"{strong_count} / {weak_count}", "delta": today_dt.strftime("%Y-%m-%d"), "delta_class": "pro-kpi-delta-flat"},
])

render_pro_section("今日焦點", "先看今日最強與最弱，再決定要往哪一檔深入分析")

gainer_text = "—"
gainer_class = ""
if top_gainer is not None:
    pct = top_gainer.get("漲跌幅(%)")
    if pct is not None and pd.notna(pct):
        gainer_text = f"{top_gainer.get('股票名稱', '—')}（{top_gainer.get('股票代號', '—')}） / {pct:+.2f}%"
        gainer_class = "pro-up"

loser_text = "—"
loser_class = ""
if top_loser is not None:
    pct = top_loser.get("漲跌幅(%)")
    if pct is not None and pd.notna(pct):
        loser_text = f"{top_loser.get('股票名稱', '—')}（{top_loser.get('股票代號', '—')}） / {pct:+.2f}%"
        loser_class = "pro-down"

render_pro_info_card(
    "今日最強 / 最弱",
    [
        ("今日最強股", gainer_text, gainer_class),
        ("今日最弱股", loser_text, loser_class),
        ("盤面平均", avg_text, "pro-up" if avg_change_pct and avg_change_pct > 0 else "pro-down" if avg_change_pct and avg_change_pct < 0 else ""),
        ("資料筆數", str(len(realtime_df)) if realtime_df is not None else "0", ""),
    ],
    chips=["Top Gainer", "Top Loser", "Breadth"]
)

render_recommendation_block(analysis_df, "今日優先觀察", "今日優先觀察", "Watch First")
render_recommendation_block(analysis_df, "接近支撐可觀察", "接近支撐可觀察", "Near Support")
render_recommendation_block(analysis_df, "接近壓力要小心", "接近壓力要小心", "Near Resistance")
render_recommendation_block(analysis_df, "今日偏弱避開", "今日偏弱避開", "Weak Today")

render_pro_section("快速搜尋股票", "可直接輸入股票名稱或代號，再自動帶入首頁快速查詢條件")
search_keyword = st.text_input(
    "輸入股票名稱或代號",
    placeholder="例如：台積電 / 2330 / 鴻海 / 聯發科",
    key="home_search_keyword"
)

matched_options = filter_stock_options(all_stock_options, search_keyword)
searched_stock = None

if matched_options:
    quick_pick = st.selectbox(
        "搜尋結果",
        [x["search_label"] for x in matched_options],
        index=0,
        key="home_search_result"
    )
    searched_stock = next(x for x in matched_options if x["search_label"] == quick_pick)
else:
    st.info("找不到符合的股票，請改用下方群組與股票選單。")

render_pro_section("快速查詢入口", "先在首頁選條件，再切換到『歷史K線分析』頁面使用")

group_names = list(watchlist_dict.keys())

if group_names:
    default_group = st.session_state.get("last_quick_group", group_names[0])
    group_index = group_names.index(default_group) if default_group in group_names else 0

    with st.form("home_quick_query_form", clear_on_submit=False):
        q1, q2 = st.columns(2)

        with q1:
            quick_group = st.selectbox("選擇群組", group_names, index=group_index)

        stock_options = group_stock_options.get(quick_group, [])

        with q2:
            if stock_options:
                saved_code = st.session_state.get("last_quick_stock_code", "")
                stock_codes = [x["code"] for x in stock_options]
                stock_index = stock_codes.index(saved_code) if saved_code in stock_codes else 0

                quick_stock_label = st.selectbox(
                    "選擇股票",
                    [x["label"] for x in stock_options],
                    index=stock_index
                )
                quick_stock = next(x for x in stock_options if x["label"] == quick_stock_label)
            else:
                quick_stock = None
                st.selectbox("選擇股票", ["此群組目前沒有股票"], index=0)

        if searched_stock is not None:
            quick_group = searched_stock["group"]
            quick_stock = searched_stock

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
    for item in all_stock_options:
        if item["code"] == current_stock_code and item["group"] == current_group:
            current_stock = item
            break

    if current_stock is not None:
        info = get_realtime_stock_info(
            current_stock["code"],
            current_stock["name"],
            current_stock["market"]
        )

        price = info.get("price")
        change = info.get("change")
        change_pct = info.get("change_pct")

        delta_text = "—"
        delta_class = ""
        if change is not None and change_pct is not None:
            delta_text = f"{change:+.2f} / {change_pct:+.2f}%"
            delta_class = "pro-up" if change > 0 else "pro-down" if change < 0 else "pro-flat"

        render_pro_info_card(
            "目前快速查詢條件",
            [
                ("群組", current_group, ""),
                ("股票", f"{current_stock['name']}（{current_stock['code']}）", ""),
                ("市場別", current_stock["market"], ""),
                ("日期區間", f"{current_start} ~ {current_end}", ""),
                ("即時現價", format_number(price, 2), ""),
                ("漲跌 / 幅度", delta_text, delta_class),
                ("更新時間", info.get("update_time", "—"), ""),
                ("使用說明", "切到『歷史K線分析』頁可直接延續這組條件", ""),
            ],
            chips=["Home Query", "Realtime Snapshot", "Analysis Entry"]
        )
else:
    st.warning("目前沒有自選股群組，請先到『自選股中心』建立群組與股票。")

render_pro_section("系統功能", "首頁已整合自動推薦、快速搜尋與盤面概況，後續可再擴充自動跳轉與提醒")
st.markdown("""
- 儀表板：查看各群組最新行情摘要  
- 行情查詢：單支股票最新行情、訊號、支撐壓力與最近事件  
- 歷史K線分析：完整K線、事件標記、雷達評分與支撐壓力  
- 自選股中心：建立群組、新增與刪除股票  
- 排行榜：用綜合評級、雷達分數與漲跌做掃盤排行  
- 多股比較：同時比較 2 到 4 檔股票的強弱輪廓  
""")
