from datetime import date, timedelta
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils import (
    get_normalized_watchlist,
    apply_font_scale,
    get_font_scale,
    get_all_code_name_map,
    get_stock_name_and_market,
    get_history_data,
    get_realtime_stock_info,
    to_excel_bytes,
    load_last_query_state,
    save_last_query_state,
    parse_date_safe,
    inject_pro_theme,
    render_pro_hero,
    render_pro_section,
    render_pro_info_card,
    render_pro_kpi_row,
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
            key = (group_name, code, market_type)
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


@st.cache_data(ttl=600, show_spinner=False)
def build_group_stock_options(items, lookup_date):
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
            str(item.get("label", "")),
        ]).lower()

        if keyword in pool:
            matched.append(item)

    return matched[:50]


def add_indicators(df: pd.DataFrame, selected_indicators: list):
    if df.empty or "收盤價" not in df.columns:
        return df

    df = df.copy()

    ma_map = {
        "MA5": 5,
        "MA10": 10,
        "MA20": 20,
        "MA60": 60,
        "MA120": 120,
        "MA240": 240,
    }

    for ma_name, window in ma_map.items():
        if ma_name in selected_indicators:
            df[ma_name] = df["收盤價"].rolling(window=window, min_periods=1).mean()

    if "KD" in selected_indicators and all(col in df.columns for col in ["最高價", "最低價", "收盤價"]):
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

    if "MACD" in selected_indicators:
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


def get_recent_events(df: pd.DataFrame, flag_col: str, event_name: str, price_col: str = "收盤價", tail_n: int = 3):
    if df is None or df.empty or flag_col not in df.columns or "日期" not in df.columns:
        return []

    sub = df[df[flag_col] == True].copy()
    if sub.empty:
        return []

    sub["日期"] = pd.to_datetime(sub["日期"], errors="coerce")
    sub = sub.dropna(subset=["日期"]).sort_values("日期", ascending=False).head(tail_n)

    events = []
    for _, row in sub.iterrows():
        price_val = row.get(price_col)
        events.append({
            "事件": event_name,
            "日期": row["日期"].strftime("%Y-%m-%d"),
            "價格": format_number(price_val, 2),
        })
    return events


def render_signal_board(signal: dict):
    score = signal.get("score", 0)
    badge_text, badge_class = score_to_badge(score)

    render_pro_kpi_row([
        {"label": "訊號分數", "value": f"{score:+d}", "delta": f"綜合評級：{badge_text}", "delta_class": "pro-kpi-delta-flat"},
        {"label": "均線結構", "value": signal["ma_trend"][0], "delta": "MA Structure", "delta_class": "pro-kpi-delta-flat"},
        {"label": "動能訊號", "value": signal["macd_trend"][0], "delta": "MACD Momentum", "delta_class": "pro-kpi-delta-flat"},
    ])

    render_pro_info_card(
        "訊號燈號面板",
        [
            ("均線排列", signal["ma_trend"][0], signal["ma_trend"][1]),
            ("KD交叉", signal["kd_cross"][0], signal["kd_cross"][1]),
            ("MACD狀態", signal["macd_trend"][0], signal["macd_trend"][1]),
            ("價格相對MA20", signal["price_vs_ma20"][0], signal["price_vs_ma20"][1]),
            ("20日突破狀態", signal["breakout_20d"][0], signal["breakout_20d"][1]),
            ("量能狀態", signal["volume_state"][0], signal["volume_state"][1]),
            ("綜合評級", badge_text, badge_class),
            ("盤勢評語", signal["comment"], ""),
        ],
        chips=["訊號燈號", "規則引擎", "多空判讀"]
    )


def render_sr_board(sr: dict):
    dist_res_20 = f"{sr.get('dist_res_20_pct'):.2f}%" if sr.get("dist_res_20_pct") is not None else "—"
    dist_sup_20 = f"{sr.get('dist_sup_20_pct'):.2f}%" if sr.get("dist_sup_20_pct") is not None else "—"
    dist_res_60 = f"{sr.get('dist_res_60_pct'):.2f}%" if sr.get("dist_res_60_pct") is not None else "—"
    dist_sup_60 = f"{sr.get('dist_sup_60_pct'):.2f}%" if sr.get("dist_sup_60_pct") is not None else "—"

    render_pro_info_card(
        "支撐 / 壓力總覽",
        [
            ("20日壓力", format_number(sr.get("res_20"), 2), ""),
            ("20日支撐", format_number(sr.get("sup_20"), 2), ""),
            ("60日壓力", format_number(sr.get("res_60"), 2), ""),
            ("60日支撐", format_number(sr.get("sup_60"), 2), ""),
            ("距20壓力", dist_res_20, sr["pressure_signal"][1]),
            ("距20支撐", dist_sup_20, sr["support_signal"][1]),
            ("距60壓力", dist_res_60, ""),
            ("距60支撐", dist_sup_60, ""),
            ("壓力訊號", sr["pressure_signal"][0], sr["pressure_signal"][1]),
            ("支撐訊號", sr["support_signal"][0], sr["support_signal"][1]),
            ("突破狀態", sr["break_signal"][0], sr["break_signal"][1]),
        ],
        chips=["Support", "Resistance", "Breakout"]
    )

    render_pro_info_card(
        "盤勢評語",
        [
            ("趨勢判讀", sr["comment_trend"], ""),
            ("風險提醒", sr["comment_risk"], ""),
            ("觀察重點", sr["comment_focus"], ""),
            ("操作建議", sr["comment_action"], ""),
        ],
        chips=["專業評語", "風險", "操作重點"]
    )


def render_radar_board(radar: dict):
    render_pro_section("雷達評分", "將趨勢、動能、量能、位置、結構轉成五維評分，快速看出強弱輪廓")

    categories = ["趨勢", "動能", "量能", "位置", "結構"]
    values = [
        radar["trend"],
        radar["momentum"],
        radar["volume"],
        radar["position"],
        radar["structure"],
    ]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values + [values[0]],
        theta=categories + [categories[0]],
        fill="toself",
        name="評分",
        line=dict(width=3),
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=False,
        height=430,
        margin=dict(l=20, r=20, t=20, b=20),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

    avg_score = round(sum(values) / len(values), 1)

    render_pro_info_card(
        "雷達評語",
        [
            ("趨勢", f"{radar['trend']} / 100", ""),
            ("動能", f"{radar['momentum']} / 100", ""),
            ("量能", f"{radar['volume']} / 100", ""),
            ("位置", f"{radar['position']} / 100", ""),
            ("結構", f"{radar['structure']} / 100", ""),
            ("平均分數", f"{avg_score} / 100", ""),
            ("整體評語", radar["summary"], ""),
        ],
        chips=["Radar", "Multi-Factor", "Strength Profile"]
    )


def render_event_board(df: pd.DataFrame):
    count_up = int(df["起漲點"].sum()) if "起漲點" in df.columns else 0
    count_down = int(df["起跌點"].sum()) if "起跌點" in df.columns else 0
    count_ma_g = int(df["MA黃金交叉"].sum()) if "MA黃金交叉" in df.columns else 0
    count_ma_d = int(df["MA死亡交叉"].sum()) if "MA死亡交叉" in df.columns else 0
    count_kd_g = int(df["KD黃金交叉"].sum()) if "KD黃金交叉" in df.columns else 0
    count_kd_d = int(df["KD死亡交叉"].sum()) if "KD死亡交叉" in df.columns else 0
    count_macd_g = int(df["MACD黃金交叉"].sum()) if "MACD黃金交叉" in df.columns else 0
    count_macd_d = int(df["MACD死亡交叉"].sum()) if "MACD死亡交叉" in df.columns else 0

    render_pro_section("事件標記面板", "還原起漲點、起跌點、交叉點，直接在盤面中做事件判讀")

    render_pro_info_card(
        "事件統計",
        [
            ("起漲點數", str(count_up), "pro-up" if count_up > 0 else ""),
            ("起跌點數", str(count_down), "pro-down" if count_down > 0 else ""),
            ("MA黃金交叉", str(count_ma_g), "pro-up" if count_ma_g > 0 else ""),
            ("MA死亡交叉", str(count_ma_d), "pro-down" if count_ma_d > 0 else ""),
            ("KD黃金交叉", str(count_kd_g), "pro-up" if count_kd_g > 0 else ""),
            ("KD死亡交叉", str(count_kd_d), "pro-down" if count_kd_d > 0 else ""),
            ("MACD黃金交叉", str(count_macd_g), "pro-up" if count_macd_g > 0 else ""),
            ("MACD死亡交叉", str(count_macd_d), "pro-down" if count_macd_d > 0 else ""),
        ],
        chips=["起漲點", "起跌點", "Cross Signals"]
    )


def render_recent_event_summary(df: pd.DataFrame):
    recent_events = []
    recent_events += get_recent_events(df, "起漲點", "起漲點", "最低價", 3)
    recent_events += get_recent_events(df, "起跌點", "起跌點", "最高價", 3)
    recent_events += get_recent_events(df, "MA黃金交叉", "MA黃金交叉", "收盤價", 3)
    recent_events += get_recent_events(df, "MA死亡交叉", "MA死亡交叉", "收盤價", 3)
    recent_events += get_recent_events(df, "KD黃金交叉", "KD黃金交叉", "K", 3)
    recent_events += get_recent_events(df, "KD死亡交叉", "KD死亡交叉", "K", 3)
    recent_events += get_recent_events(df, "MACD黃金交叉", "MACD黃金交叉", "DIF", 3)
    recent_events += get_recent_events(df, "MACD死亡交叉", "MACD死亡交叉", "DIF", 3)

    if not recent_events:
        render_pro_section("最近事件說明", "近三次事件與文字摘要")
        render_pro_info_card(
            "最近事件",
            [("結果", "目前查詢區間內沒有可列示的事件", "")],
            chips=["Recent Events"]
        )
        return

    event_df = pd.DataFrame(recent_events)
    event_df["日期_dt"] = pd.to_datetime(event_df["日期"], errors="coerce")
    event_df = event_df.sort_values("日期_dt", ascending=False).drop(columns=["日期_dt"]).reset_index(drop=True)

    top3 = event_df.head(3)
    summary_lines = []
    for _, row in top3.iterrows():
        summary_lines.append(f"{row['日期']}：{row['事件']}（價格/指標 {row['價格']}）")

    render_pro_section("最近事件說明", "把最近 3 次重要事件轉成可讀文字，不只看點位，還能快速回顧節奏")

    render_pro_info_card(
        "事件文字摘要",
        [
            ("最近事件 1", summary_lines[0] if len(summary_lines) >= 1 else "—", ""),
            ("最近事件 2", summary_lines[1] if len(summary_lines) >= 2 else "—", ""),
            ("最近事件 3", summary_lines[2] if len(summary_lines) >= 3 else "—", ""),
        ],
        chips=["Recent Summary", "Turning Rhythm"]
    )

    st.dataframe(event_df.head(12), use_container_width=True, hide_index=True, height=320)


def render_summary_card(stock_info, start_date, end_date, selected_indicators, df: pd.DataFrame, realtime_info: dict):
    latest_close = df["收盤價"].iloc[-1] if "收盤價" in df.columns and not df.empty else None
    first_close = df["收盤價"].iloc[0] if "收盤價" in df.columns and not df.empty else None

    change_text = "—"
    change_class = ""
    if latest_close is not None and first_close not in [None, 0]:
        chg = latest_close - first_close
        pct = chg / first_close * 100
        change_text = f"{chg:+.2f} / {pct:+.2f}%"
        change_class = "pro-up" if chg > 0 else "pro-down" if chg < 0 else "pro-flat"

    render_pro_info_card(
        "查詢總覽",
        [
            ("股票", f"{stock_info['name']}（{stock_info['code']}）", ""),
            ("市場別", stock_info["market"], ""),
            ("日期區間", f"{start_date} ~ {end_date}", ""),
            ("技術指標", "、".join(selected_indicators) if selected_indicators else "無", ""),
            ("最新收盤", format_number(latest_close, 2), ""),
            ("區間漲跌", change_text, change_class),
            ("即時現價", format_number(realtime_info.get("price"), 2), ""),
            ("即時更新", realtime_info.get("update_time", "—"), ""),
        ],
        chips=["K線分析", "即時同步", "規則判讀"]
    )


def render_table(df: pd.DataFrame):
    if df.empty:
        st.info("目前沒有可顯示的資料。")
        return

    base_cols = ["日期", "開盤價", "最高價", "最低價", "收盤價", "成交股數", "成交金額", "成交筆數"]
    indicator_cols = [
        "MA5", "MA10", "MA20", "MA60", "MA120", "MA240",
        "K", "D", "DIF", "DEA", "MACD_HIST",
        "起漲點", "起跌點", "MA黃金交叉", "MA死亡交叉", "KD黃金交叉", "KD死亡交叉", "MACD黃金交叉", "MACD死亡交叉"
    ]
    show_cols = [c for c in base_cols + indicator_cols if c in df.columns]
    display_df = df[show_cols].copy()

    if "日期" in display_df.columns:
        display_df["日期"] = pd.to_datetime(display_df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")

    if "收盤價" in display_df.columns:
        display_df["漲跌"] = display_df["收盤價"].diff()
        cols = list(display_df.columns)
        insert_at = cols.index("收盤價") + 1
        ordered_cols = cols[:insert_at] + ["漲跌"] + [c for c in cols[insert_at:] if c != "漲跌"]
        display_df = display_df[ordered_cols]

    bool_cols = ["起漲點", "起跌點", "MA黃金交叉", "MA死亡交叉", "KD黃金交叉", "KD死亡交叉", "MACD黃金交叉", "MACD死亡交叉"]
    for col in bool_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: "●" if bool(x) else "")

    format_dict = {}
    for col in ["開盤價", "最高價", "最低價", "收盤價", "漲跌", "MA5", "MA10", "MA20", "MA60", "MA120", "MA240", "K", "D", "DIF", "DEA", "MACD_HIST"]:
        if col in display_df.columns:
            format_dict[col] = "{:,.2f}"
    for col in ["成交股數", "成交金額", "成交筆數"]:
        if col in display_df.columns:
            format_dict[col] = "{:,.0f}"

    def color_change(val):
        if pd.isna(val):
            return ""
        try:
            v = float(val)
        except Exception:
            return ""
        if v > 0:
            return "color: #dc2626; font-weight: 800;"
        elif v < 0:
            return "color: #059669; font-weight: 800;"
        return "color: #64748b; font-weight: 700;"

    styler = display_df.style.format(format_dict, na_rep="—")
    if "漲跌" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌"])

    st.dataframe(styler, use_container_width=True, hide_index=True, height=760)


def render_chart(df: pd.DataFrame, selected_indicators: list, selected_events: list):
    if df.empty:
        return

    has_volume = "成交股數" in df.columns

    render_pro_section("K線主圖", "可自行控制顯示哪些事件，群組與股票選擇已完全同步")

    if has_volume:
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            row_heights=[0.72, 0.28]
        )
    else:
        fig = make_subplots(rows=1, cols=1)

    fig.add_trace(
        go.Candlestick(
            x=df["日期"],
            open=df["開盤價"],
            high=df["最高價"],
            low=df["最低價"],
            close=df["收盤價"],
            name="K線",
            increasing_line_color="#e53935",
            decreasing_line_color="#26a69a",
        ),
        row=1,
        col=1
    )

    ma_colors = {
        "MA5": "#3b82f6",
        "MA10": "#ef4444",
        "MA20": "#f59e0b",
        "MA60": "#8b5cf6",
        "MA120": "#64748b",
        "MA240": "#0f172a",
    }

    for ma_name, color in ma_colors.items():
        if ma_name in selected_indicators and ma_name in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df[ma_name],
                    mode="lines",
                    name=ma_name,
                    line=dict(width=2.2, color=color),
                ),
                row=1,
                col=1
            )

    def add_marker(flag_col, y_col, name, symbol, color, event_key):
        if event_key not in selected_events:
            return
        if flag_col in df.columns and y_col in df.columns:
            sub = df[df[flag_col] == True].copy()
            if not sub.empty:
                fig.add_trace(
                    go.Scatter(
                        x=sub["日期"],
                        y=sub[y_col],
                        mode="markers",
                        name=name,
                        marker=dict(symbol=symbol, size=12, color=color, line=dict(width=1, color="#111827")),
                    ),
                    row=1,
                    col=1
                )

    add_marker("起漲點", "最低價", "起漲點", "triangle-up", "#16a34a", "起漲點")
    add_marker("起跌點", "最高價", "起跌點", "triangle-down", "#dc2626", "起跌點")
    add_marker("MA黃金交叉", "收盤價", "MA黃金交叉", "star", "#2563eb", "MA交叉")
    add_marker("MA死亡交叉", "收盤價", "MA死亡交叉", "x", "#7c2d12", "MA交叉")

    if has_volume:
        volume_colors = []
        for _, row_data in df.iterrows():
            if row_data["收盤價"] >= row_data["開盤價"]:
                volume_colors.append("rgba(229,57,53,0.6)")
            else:
                volume_colors.append("rgba(38,166,154,0.6)")

        fig.add_trace(
            go.Bar(
                x=df["日期"],
                y=df["成交股數"],
                name="成交量",
                marker_color=volume_colors,
            ),
            row=2,
            col=1
        )

    fig.update_layout(
        height=840 if has_volume else 580,
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(orientation="h", y=1.02, x=0),
    )

    st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

    if "KD" in selected_indicators and all(col in df.columns for col in ["日期", "K", "D"]):
        with st.expander("查看 KD 指標", expanded=False):
            kd_fig = go.Figure()
            kd_fig.add_trace(go.Scatter(x=df["日期"], y=df["K"], mode="lines", name="K", line=dict(width=2.5)))
            kd_fig.add_trace(go.Scatter(x=df["日期"], y=df["D"], mode="lines", name="D", line=dict(width=2.5)))

            if "KD交叉" in selected_events:
                add_kd_g = df[df["KD黃金交叉"] == True] if "KD黃金交叉" in df.columns else pd.DataFrame()
                add_kd_d = df[df["KD死亡交叉"] == True] if "KD死亡交叉" in df.columns else pd.DataFrame()

                if not add_kd_g.empty:
                    kd_fig.add_trace(go.Scatter(
                        x=add_kd_g["日期"], y=add_kd_g["K"], mode="markers", name="KD黃金交叉",
                        marker=dict(symbol="triangle-up", size=11, color="#16a34a")
                    ))
                if not add_kd_d.empty:
                    kd_fig.add_trace(go.Scatter(
                        x=add_kd_d["日期"], y=add_kd_d["K"], mode="markers", name="KD死亡交叉",
                        marker=dict(symbol="triangle-down", size=11, color="#dc2626")
                    ))

            kd_fig.update_layout(height=340, hovermode="x unified")
            st.plotly_chart(kd_fig, use_container_width=True, config={"displaylogo": False})

    if "MACD" in selected_indicators and all(col in df.columns for col in ["日期", "DIF", "DEA", "MACD_HIST"]):
        with st.expander("查看 MACD 指標", expanded=False):
            macd_fig = go.Figure()
            macd_fig.add_trace(go.Bar(x=df["日期"], y=df["MACD_HIST"], name="MACD柱"))
            macd_fig.add_trace(go.Scatter(x=df["日期"], y=df["DIF"], mode="lines", name="DIF", line=dict(width=2.5)))
            macd_fig.add_trace(go.Scatter(x=df["日期"], y=df["DEA"], mode="lines", name="DEA", line=dict(width=2.5)))

            if "MACD交叉" in selected_events:
                add_macd_g = df[df["MACD黃金交叉"] == True] if "MACD黃金交叉" in df.columns else pd.DataFrame()
                add_macd_d = df[df["MACD死亡交叉"] == True] if "MACD死亡交叉" in df.columns else pd.DataFrame()

                if not add_macd_g.empty:
                    macd_fig.add_trace(go.Scatter(
                        x=add_macd_g["日期"], y=add_macd_g["DIF"], mode="markers", name="MACD黃金交叉",
                        marker=dict(symbol="triangle-up", size=11, color="#16a34a")
                    ))
                if not add_macd_d.empty:
                    macd_fig.add_trace(go.Scatter(
                        x=add_macd_d["日期"], y=add_macd_d["DIF"], mode="markers", name="MACD死亡交叉",
                        marker=dict(symbol="triangle-down", size=11, color="#dc2626")
                    ))

            macd_fig.update_layout(height=360, hovermode="x unified")
            st.plotly_chart(macd_fig, use_container_width=True, config={"displaylogo": False})


st.set_page_config(page_title="歷史K線分析", page_icon="📊", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)
inject_pro_theme()

watchlist_dict = get_normalized_watchlist()
group_names = list(watchlist_dict.keys())

render_pro_hero(
    "歷史K線分析｜群組股票同步最終版",
    "搜尋股票後按『帶入股票』，群組與群組股票會真正同步，不再發生對不上。"
)

if not group_names:
    st.warning("目前沒有自選股群組，請先到「自選股中心」建立群組與股票。")
    st.stop()

last_state = load_last_query_state()
today_dt = date.today()

default_group = last_state.get("quick_group", "")
default_code = last_state.get("quick_stock_code", "")
default_start = parse_date_safe(last_state.get("home_start", ""), today_dt - timedelta(days=90))
default_end = parse_date_safe(last_state.get("home_end", ""), today_dt)

lookup_date = today_dt.strftime("%Y%m%d")
all_stock_options = build_all_stock_options(watchlist_dict, lookup_date)

# session state 初始化
if "history_selected_group" not in st.session_state:
    st.session_state.history_selected_group = default_group if default_group in group_names else group_names[0]

if "history_selected_stock_code" not in st.session_state:
    st.session_state.history_selected_stock_code = default_code if default_code else ""

# 搜尋區
st.markdown("### 快速搜尋股票")
search_keyword = st.text_input(
    "輸入股票名稱或代號",
    placeholder="例如：台積電 / 2330 / 鴻海 / 聯發科",
    key="history_search_keyword"
)

matched_options = filter_stock_options(all_stock_options, search_keyword)

if matched_options:
    quick_pick = st.selectbox(
        "搜尋結果",
        [x["label"] for x in matched_options],
        index=0,
        key="history_search_result"
    )

    c_search1, c_search2 = st.columns([4, 1])
    with c_search2:
        if st.button("帶入股票", use_container_width=True):
            picked = next(x for x in matched_options if x["label"] == quick_pick)
            st.session_state.history_selected_group = picked["group"]
            st.session_state.history_selected_stock_code = picked["code"]
            st.rerun()
else:
    st.info("找不到符合的股票，請改用下方群組選擇。")

# 非 form 控制，才會即時同步
render_pro_section("查詢條件", "群組改變時，股票清單立即同步")

selected_group = st.selectbox(
    "選擇群組",
    group_names,
    index=group_names.index(st.session_state.history_selected_group) if st.session_state.history_selected_group in group_names else 0,
    key="history_group_widget"
)
st.session_state.history_selected_group = selected_group

items = watchlist_dict.get(selected_group, [])
group_stock_options = build_group_stock_options(items, lookup_date)

if not group_stock_options:
    st.warning("此群組目前沒有股票。")
    st.stop()

stock_code_list = [x["code"] for x in group_stock_options]
stock_label_map = {x["code"]: x["label"] for x in group_stock_options}

if st.session_state.history_selected_stock_code not in stock_code_list:
    st.session_state.history_selected_stock_code = stock_code_list[0]

selected_stock_code = st.selectbox(
    "群組股票",
    stock_code_list,
    index=stock_code_list.index(st.session_state.history_selected_stock_code),
    format_func=lambda code: stock_label_map.get(code, code),
    key="history_stock_widget"
)
st.session_state.history_selected_stock_code = selected_stock_code

selected_stock = next(x for x in group_stock_options if x["code"] == selected_stock_code)

d1, d2 = st.columns(2)
with d1:
    start_date = st.date_input("開始日期", value=default_start)
with d2:
    end_date = st.date_input("結束日期", value=default_end)

selected_indicators = st.multiselect(
    "技術指標",
    ["MA5", "MA10", "MA20", "MA60", "MA120", "MA240", "KD", "MACD"],
    default=["MA5", "MA10", "MA20", "KD", "MACD"]
)

selected_events = st.multiselect(
    "事件篩選",
    ["起漲點", "起跌點", "MA交叉", "KD交叉", "MACD交叉"],
    default=["起漲點", "起跌點", "MA交叉", "KD交叉", "MACD交叉"]
)

query_btn = st.button("開始查詢", type="primary", use_container_width=True)

if start_date > end_date:
    st.error("開始日期不能大於結束日期")
    st.stop()

if query_btn:
    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code=selected_stock["code"],
        home_start=start_date,
        home_end=end_date
    )

    realtime_info = get_realtime_stock_info(
        selected_stock["code"],
        selected_stock["name"],
        selected_stock["market"]
    )

    with st.spinner("正在查詢歷史資料..."):
        df = get_history_data(
            selected_stock["code"],
            selected_stock["name"],
            selected_stock["market"],
            start_date,
            end_date
        )

    if df.empty:
        st.warning("查無歷史資料。")
        st.stop()

    df = add_indicators(df, selected_indicators)
    df = add_event_points(df)

    signal = compute_signal_snapshot(df)
    sr = compute_support_resistance_snapshot(df)
    radar = compute_radar_scores(df)

    render_summary_card(selected_stock, start_date, end_date, selected_indicators, df, realtime_info)
    render_signal_board(signal)
    render_sr_board(sr)
    render_radar_board(radar)
    render_event_board(df)
    render_recent_event_summary(df)

    download_df = df.copy()
    if "日期" in download_df.columns:
        download_df["日期"] = pd.to_datetime(download_df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")

    excel_bytes = to_excel_bytes({"歷史K線資料": download_df})

    c1, c2 = st.columns([1, 4])
    with c1:
        st.download_button(
            label="下載 Excel",
            data=excel_bytes,
            file_name=f"{selected_stock['code']}_{selected_stock['name']}_歷史K線分析.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    render_chart(df, selected_indicators, selected_events)
    render_pro_section("歷史資料表", "事件點、燈號、支撐壓力與原始數值整合檢視")
    render_table(df)

    st.success(f"查詢完成，共 {len(df)} 筆資料。")
