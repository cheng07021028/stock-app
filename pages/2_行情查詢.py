from datetime import date, timedelta
import pandas as pd
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
    render_pro_kpi_row,
    format_number,
    compute_signal_snapshot,
    score_to_badge,
    compute_support_resistance_snapshot,
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


def add_basic_indicators(df: pd.DataFrame):
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


def add_event_points(df: pd.DataFrame):
    if df.empty:
        return df

    df = df.copy()

    df["起漲點"] = False
    df["起跌點"] = False
    df["MA黃金交叉"] = False
    df["MA死亡交叉"] = False
    df["KD黃金交叉"] = False
    df["KD死亡交叉"] = False
    df["MACD黃金交叉"] = False
    df["MACD死亡交叉"] = False

    if all(col in df.columns for col in ["最低價", "最高價", "收盤價"]) and len(df) >= 5:
        for i in range(2, len(df) - 2):
            low_now = df.iloc[i]["最低價"]
            high_now = df.iloc[i]["最高價"]

            prev_lows = [df.iloc[i - 2]["最低價"], df.iloc[i - 1]["最低價"]]
            next_lows = [df.iloc[i + 1]["最低價"], df.iloc[i + 2]["最低價"]]

            prev_highs = [df.iloc[i - 2]["最高價"], df.iloc[i - 1]["最高價"]]
            next_highs = [df.iloc[i + 1]["最高價"], df.iloc[i + 2]["最高價"]]

            if pd.notna(low_now) and all(pd.notna(x) for x in prev_lows + next_lows):
                if low_now <= min(prev_lows + next_lows):
                    if df.iloc[i + 1]["收盤價"] > df.iloc[i]["收盤價"]:
                        df.at[df.index[i], "起漲點"] = True

            if pd.notna(high_now) and all(pd.notna(x) for x in prev_highs + next_highs):
                if high_now >= max(prev_highs + next_highs):
                    if df.iloc[i + 1]["收盤價"] < df.iloc[i]["收盤價"]:
                        df.at[df.index[i], "起跌點"] = True

    if all(col in df.columns for col in ["MA5", "MA10"]) and len(df) >= 2:
        for i in range(1, len(df)):
            prev_ma5, prev_ma10 = df.iloc[i - 1]["MA5"], df.iloc[i - 1]["MA10"]
            now_ma5, now_ma10 = df.iloc[i]["MA5"], df.iloc[i]["MA10"]

            if all(pd.notna(x) for x in [prev_ma5, prev_ma10, now_ma5, now_ma10]):
                if prev_ma5 <= prev_ma10 and now_ma5 > now_ma10:
                    df.at[df.index[i], "MA黃金交叉"] = True
                elif prev_ma5 >= prev_ma10 and now_ma5 < now_ma10:
                    df.at[df.index[i], "MA死亡交叉"] = True

    if all(col in df.columns for col in ["K", "D"]) and len(df) >= 2:
        for i in range(1, len(df)):
            prev_k, prev_d = df.iloc[i - 1]["K"], df.iloc[i - 1]["D"]
            now_k, now_d = df.iloc[i]["K"], df.iloc[i]["D"]

            if all(pd.notna(x) for x in [prev_k, prev_d, now_k, now_d]):
                if prev_k <= prev_d and now_k > now_d:
                    df.at[df.index[i], "KD黃金交叉"] = True
                elif prev_k >= prev_d and now_k < now_d:
                    df.at[df.index[i], "KD死亡交叉"] = True

    if all(col in df.columns for col in ["DIF", "DEA"]) and len(df) >= 2:
        for i in range(1, len(df)):
            prev_dif, prev_dea = df.iloc[i - 1]["DIF"], df.iloc[i - 1]["DEA"]
            now_dif, now_dea = df.iloc[i]["DIF"], df.iloc[i]["DEA"]

            if all(pd.notna(x) for x in [prev_dif, prev_dea, now_dif, now_dea]):
                if prev_dif <= prev_dea and now_dif > now_dea:
                    df.at[df.index[i], "MACD黃金交叉"] = True
                elif prev_dif >= prev_dea and now_dif < now_dea:
                    df.at[df.index[i], "MACD死亡交叉"] = True

    return df


def get_latest_event_date(df: pd.DataFrame, flag_col: str):
    if df is None or df.empty or flag_col not in df.columns or "日期" not in df.columns:
        return "—"

    sub = df[df[flag_col] == True]
    if sub.empty:
        return "—"

    dt = pd.to_datetime(sub["日期"].iloc[-1], errors="coerce")
    if pd.isna(dt):
        return "—"
    return dt.strftime("%Y-%m-%d")


def render_signal_summary(info, signal, sr):
    score = signal.get("score", 0)
    badge_text, _ = score_to_badge(score)

    change_value = info.get("change")
    change_pct = info.get("change_pct")
    change_text = f"{change_value:+.2f}" if change_value is not None else "—"
    change_pct_text = f"{change_pct:+.2f}%" if change_pct is not None else "—"

    render_pro_kpi_row([
        {"label": "現價", "value": format_number(info.get("price"), 2), "delta": info.get("update_time", "—"), "delta_class": "pro-kpi-delta-flat"},
        {"label": "即時漲跌", "value": change_text, "delta": change_pct_text, "delta_class": "pro-kpi-delta-flat"},
        {"label": "綜合評級", "value": badge_text, "delta": f"訊號分數 {score:+d}", "delta_class": "pro-kpi-delta-flat"},
    ])

    render_pro_info_card(
        "單股訊號面板",
        [
            ("股票", f"{info.get('name', '—')}（{info.get('code', '—')}）", ""),
            ("市場別", info.get("market", "—"), ""),
            ("均線排列", signal["ma_trend"][0], signal["ma_trend"][1]),
            ("KD交叉", signal["kd_cross"][0], signal["kd_cross"][1]),
            ("MACD狀態", signal["macd_trend"][0], signal["macd_trend"][1]),
            ("價格 vs MA20", signal["price_vs_ma20"][0], signal["price_vs_ma20"][1]),
            ("20日突破", signal["breakout_20d"][0], signal["breakout_20d"][1]),
            ("量能", signal["volume_state"][0], signal["volume_state"][1]),
            ("20日壓力", format_number(sr.get("res_20"), 2), ""),
            ("20日支撐", format_number(sr.get("sup_20"), 2), ""),
            ("壓力訊號", sr["pressure_signal"][0], sr["pressure_signal"][1]),
            ("支撐訊號", sr["support_signal"][0], sr["support_signal"][1]),
            ("突破狀態", sr["break_signal"][0], sr["break_signal"][1]),
            ("操作建議", sr["comment_action"], ""),
        ],
        chips=["單股訊號", "支撐壓力", "短線判讀"]
    )


def render_recent_events(df: pd.DataFrame):
    render_pro_section("最近事件摘要", "快速查看最近一次起漲點、起跌點與各類交叉點，不用切到完整K線頁也能先判讀")

    render_pro_info_card(
        "最近事件",
        [
            ("最近起漲點", get_latest_event_date(df, "起漲點"), "pro-up" if get_latest_event_date(df, "起漲點") != "—" else ""),
            ("最近起跌點", get_latest_event_date(df, "起跌點"), "pro-down" if get_latest_event_date(df, "起跌點") != "—" else ""),
            ("最近 MA 黃金交叉", get_latest_event_date(df, "MA黃金交叉"), "pro-up" if get_latest_event_date(df, "MA黃金交叉") != "—" else ""),
            ("最近 MA 死亡交叉", get_latest_event_date(df, "MA死亡交叉"), "pro-down" if get_latest_event_date(df, "MA死亡交叉") != "—" else ""),
            ("最近 KD 黃金交叉", get_latest_event_date(df, "KD黃金交叉"), "pro-up" if get_latest_event_date(df, "KD黃金交叉") != "—" else ""),
            ("最近 KD 死亡交叉", get_latest_event_date(df, "KD死亡交叉"), "pro-down" if get_latest_event_date(df, "KD死亡交叉") != "—" else "),
            ("最近 MACD 黃金交叉", get_latest_event_date(df, "MACD黃金交叉"), "pro-up" if get_latest_event_date(df, "MACD黃金交叉") != "—" else ""),
            ("最近 MACD 死亡交叉", get_latest_event_date(df, "MACD死亡交叉"), "pro-down" if get_latest_event_date(df, "MACD死亡交叉") != "—" else ""),
        ],
        chips=["Recent Events", "Turning Points", "Cross Signals"]
    )


st.set_page_config(page_title="行情查詢", page_icon="📈", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)
inject_pro_theme()

render_pro_hero(
    "行情查詢｜事件摘要版",
    "單股工作站進階版｜即時資訊 + 訊號燈號 + 支撐壓力 + 最近事件摘要。"
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

with st.spinner("正在讀取近 120 日資料..."):
    history_df = get_history_data(
        selected_stock["code"],
        selected_stock["name"],
        selected_stock["market"],
        today_dt - timedelta(days=120),
        today_dt
    )

if history_df is None or history_df.empty:
    st.info("目前沒有足夠的歷史資料可產生訊號。")
else:
    history_df = add_basic_indicators(history_df)
    history_df = add_event_points(history_df)

    signal = compute_signal_snapshot(history_df)
    sr = compute_support_resistance_snapshot(history_df)

    render_signal_summary(info, signal, sr)
    render_recent_events(history_df)

render_pro_section("觀察建議", "這一頁適合快速單股判讀；若要深入看完整圖上事件標記，請切到歷史K線分析頁")
