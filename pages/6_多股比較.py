from datetime import date, timedelta
import pandas as pd
import plotly.graph_objects as go
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
    render_pro_section,
    render_pro_info_card,
    format_number,
    compute_signal_snapshot,
    score_to_badge,
    compute_support_resistance_snapshot,
    compute_radar_scores,
)


@st.cache_data(ttl=600, show_spinner=False)
def build_all_stock_options(watchlist_dict, lookup_date):
    all_code_name_df = get_all_code_name_map(lookup_date)
    result = []
    seen = set()

    for group_name, items in watchlist_dict.items():
        for item in items:
            code = str(item.get("code", "")).strip()
            manual_name = str(item.get("name", "")).strip()
            if not code:
                continue

            stock_name, market_type = get_stock_name_and_market(code, all_code_name_df, manual_name)
            key = (code, market_type)
            if key in seen:
                continue
            seen.add(key)

            result.append({
                "group": group_name,
                "label": f"{stock_name} ({code}) [{market_type}]｜{group_name}",
                "code": code,
                "name": stock_name,
                "market": market_type,
            })

    return result


def filter_stock_options(all_options, keyword):
    keyword = str(keyword).strip().lower()
    if not keyword:
        return all_options[:100]

    matched = []
    for item in all_options:
        text_pool = " ".join([
            str(item.get("name", "")),
            str(item.get("code", "")),
            str(item.get("market", "")),
            str(item.get("group", "")),
            str(item.get("label", "")),
        ]).lower()

        if keyword in text_pool:
            matched.append(item)

    return matched[:100]


def add_compare_indicators(df: pd.DataFrame):
    if df.empty or "收盤價" not in df.columns:
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

        k_values = []
        d_values = []
        k_prev = 50.0
        d_prev = 50.0

        for val in rsv:
            val = float(val)
            k_now = (2 / 3) * k_prev + (1 / 3) * val
            d_now = (2 / 3) * d_prev + (1 / 3) * k_now
            k_values.append(k_now)
            d_values.append(d_now)
            k_prev = k_now
            d_prev = d_now

        df["K"] = k_values
        df["D"] = d_values

    ema12 = df["收盤價"].ewm(span=12, adjust=False).mean()
    ema26 = df["收盤價"].ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["DIF"] - df["DEA"]

    return df


def build_compare_snapshot(stock_item, start_date, end_date):
    info = get_realtime_stock_info(
        stock_item["code"],
        stock_item["name"],
        stock_item["market"]
    )

    history_df = get_history_data(
        stock_item["code"],
        stock_item["name"],
        stock_item["market"],
        start_date,
        end_date
    )

    if history_df is None or history_df.empty:
        return {
            "stock": stock_item,
            "info": info,
            "history": pd.DataFrame(),
            "signal": None,
            "sr": None,
            "radar": None,
            "badge_text": "無資料",
            "badge_class": "pro-flat",
            "range_change": None,
            "range_change_pct": None,
        }

    history_df = add_compare_indicators(history_df)
    signal = compute_signal_snapshot(history_df)
    sr = compute_support_resistance_snapshot(history_df)
    radar = compute_radar_scores(history_df)
    badge_text, badge_class = score_to_badge(signal.get("score", 0))

    latest_close = history_df["收盤價"].iloc[-1] if "收盤價" in history_df.columns else None
    first_close = history_df["收盤價"].iloc[0] if "收盤價" in history_df.columns else None

    range_change = None
    range_change_pct = None
    if latest_close is not None and first_close not in [None, 0]:
        range_change = latest_close - first_close
        range_change_pct = range_change / first_close * 100

    return {
        "stock": stock_item,
        "info": info,
        "history": history_df,
        "signal": signal,
        "sr": sr,
        "radar": radar,
        "badge_text": badge_text,
        "badge_class": badge_class,
        "range_change": range_change,
        "range_change_pct": range_change_pct,
    }


def render_compare_cards(snapshots):
    for snap in snapshots:
        stock = snap["stock"]
        info = snap["info"]
        signal = snap["signal"]
        sr = snap["sr"]
        radar = snap["radar"]

        if signal is None or sr is None or radar is None:
            render_pro_info_card(
                f"{stock['name']}（{stock['code']}）",
                [
                    ("市場別", stock["market"], ""),
                    ("資料狀態", "歷史資料不足", "pro-down"),
                ],
                chips=["No Data"]
            )
            continue

        range_text = "—"
        range_class = ""
        if snap["range_change"] is not None and snap["range_change_pct"] is not None:
            range_text = f"{snap['range_change']:+.2f} / {snap['range_change_pct']:+.2f}%"
            range_class = "pro-up" if snap["range_change"] > 0 else "pro-down" if snap["range_change"] < 0 else "pro-flat"

        render_pro_info_card(
            f"{stock['name']}（{stock['code']}）",
            [
                ("群組", stock.get("group", "—"), ""),
                ("市場別", stock["market"], ""),
                ("即時現價", format_number(info.get("price"), 2), ""),
                ("即時漲跌", f"{info.get('change', 0):+.2f} / {info.get('change_pct', 0):+.2f}%" if info.get("change") is not None and info.get("change_pct") is not None else "—", "pro-up" if (info.get("change") or 0) > 0 else "pro-down" if (info.get("change") or 0) < 0 else ""),
                ("區間漲跌", range_text, range_class),
                ("綜合評級", snap["badge_text"], snap["badge_class"]),
                ("均線結構", signal["ma_trend"][0], signal["ma_trend"][1]),
                ("MACD", signal["macd_trend"][0], signal["macd_trend"][1]),
                ("20日壓力", format_number(sr.get("res_20"), 2), ""),
                ("20日支撐", format_number(sr.get("sup_20"), 2), ""),
                ("趨勢分數", f"{radar['trend']} / 100", ""),
                ("動能分數", f"{radar['momentum']} / 100", ""),
                ("量能分數", f"{radar['volume']} / 100", ""),
                ("結構分數", f"{radar['structure']} / 100", ""),
            ],
            chips=["Compare", "Signal", "Radar"]
        )


def render_compare_table(snapshots):
    rows = []
    for snap in snapshots:
        stock = snap["stock"]
        info = snap["info"]
        signal = snap["signal"]
        sr = snap["sr"]
        radar = snap["radar"]

        if signal is None or sr is None or radar is None:
            rows.append({
                "股票": f"{stock['name']} ({stock['code']})",
                "群組": stock.get("group", "—"),
                "現價": None,
                "即時漲跌幅(%)": None,
                "區間漲跌幅(%)": None,
                "評級": "無資料",
                "均線結構": "無資料",
                "MACD": "無資料",
                "20日壓力": None,
                "20日支撐": None,
                "趨勢": None,
                "動能": None,
                "量能": None,
                "位置": None,
                "結構": None,
            })
            continue

        rows.append({
            "股票": f"{stock['name']} ({stock['code']})",
            "群組": stock.get("group", "—"),
            "現價": info.get("price"),
            "即時漲跌幅(%)": info.get("change_pct"),
            "區間漲跌幅(%)": snap["range_change_pct"],
            "評級": snap["badge_text"],
            "均線結構": signal["ma_trend"][0],
            "MACD": signal["macd_trend"][0],
            "20日壓力": sr.get("res_20"),
            "20日支撐": sr.get("sup_20"),
            "趨勢": radar["trend"],
            "動能": radar["momentum"],
            "量能": radar["volume"],
            "位置": radar["position"],
            "結構": radar["structure"],
        })

    df = pd.DataFrame(rows)

    format_dict = {
        "現價": "{:,.2f}",
        "即時漲跌幅(%)": "{:,.2f}",
        "區間漲跌幅(%)": "{:,.2f}",
        "20日壓力": "{:,.2f}",
        "20日支撐": "{:,.2f}",
        "趨勢": "{:,.0f}",
        "動能": "{:,.0f}",
        "量能": "{:,.0f}",
        "位置": "{:,.0f}",
        "結構": "{:,.0f}",
    }

    def color_pct(val):
        if pd.isna(val):
            return ""
        try:
            v = float(val)
        except Exception:
            return ""
        if v > 0:
            return "color: #dc2626; font-weight: 800;"
        if v < 0:
            return "color: #059669; font-weight: 800;"
        return "color: #64748b; font-weight: 700;"

    styler = df.style.format(format_dict, na_rep="—")
    if "即時漲跌幅(%)" in df.columns:
        styler = styler.map(color_pct, subset=["即時漲跌幅(%)"])
    if "區間漲跌幅(%)" in df.columns:
        styler = styler.map(color_pct, subset=["區間漲跌幅(%)"])

    st.dataframe(styler, use_container_width=True, hide_index=True, height=520)


def render_radar_compare(snapshots):
    render_pro_section("雷達比較", "同時比較多檔股票的趨勢、動能、量能、位置、結構")

    fig = go.Figure()
    categories = ["趨勢", "動能", "量能", "位置", "結構"]

    for snap in snapshots:
        radar = snap["radar"]
        stock = snap["stock"]
        if radar is None:
            continue

        values = [
            radar["trend"],
            radar["momentum"],
            radar["volume"],
            radar["position"],
            radar["structure"],
        ]

        fig.add_trace(go.Scatterpolar(
            r=values + [values[0]],
            theta=categories + [categories[0]],
            fill="toself",
            name=f"{stock['name']}({stock['code']})",
        ))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        height=560,
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(orientation="h", y=1.08, x=0),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})


st.set_page_config(page_title="多股比較", page_icon="🧭", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)
inject_pro_theme()

render_pro_hero(
    "多股比較版",
    "同時比較 2 到 4 檔股票的雷達分數、訊號燈號、支撐壓力與漲跌表現。"
)

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")

watchlist_dict = get_normalized_watchlist()
if not watchlist_dict:
    st.warning("目前沒有自選股群組，請先到「自選股中心」建立群組與股票。")
    st.stop()

all_stock_options = build_all_stock_options(watchlist_dict, lookup_date)

search_keyword = st.text_input(
    "輸入股票名稱或代號",
    placeholder="例如：台積電 / 2330 / 鴻海 / 聯發科",
    key="compare_search_keyword"
)

matched_options = filter_stock_options(all_stock_options, search_keyword)
option_labels = [x["label"] for x in matched_options]

selected_labels = st.multiselect(
    "選擇 2 到 4 檔股票比較",
    option_labels,
    default=option_labels[:2] if len(option_labels) >= 2 else option_labels[:1],
    max_selections=4,
)

d1, d2 = st.columns(2)
with d1:
    start_date = st.date_input("開始日期", value=today_dt - timedelta(days=120), key="compare_start")
with d2:
    end_date = st.date_input("結束日期", value=today_dt, key="compare_end")

if start_date > end_date:
    st.error("開始日期不能大於結束日期")
    st.stop()

selected_stocks = [x for x in matched_options if x["label"] in selected_labels]

if len(selected_stocks) < 2:
    st.info("請至少選擇 2 檔股票進行比較。")
    st.stop()

with st.spinner("正在整理多股比較資料..."):
    snapshots = [build_compare_snapshot(stock, start_date, end_date) for stock in selected_stocks]

render_pro_section("比較總覽", "先看卡片，再看雷達與表格，最適合找出相對強弱")
render_compare_cards(snapshots)
render_radar_compare(snapshots)
render_pro_section("比較表", "用表格快速看出誰比較強、誰比較弱、誰更接近壓力或支撐")
render_compare_table(snapshots)
