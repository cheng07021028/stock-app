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


def add_indicators(df: pd.DataFrame, selected_indicators: list):
    if df.empty or "收盤價" not in df.columns:
        return df

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
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100])
        ),
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
    indicator_cols = ["MA5", "MA10", "MA20", "MA60", "MA120", "MA240", "K", "D", "DIF", "DEA", "MACD_HIST"]
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

    st.dataframe(styler, use_container_width=True, hide_index=True, height=700)


def render_chart(df: pd.DataFrame, selected_indicators: list):
    if df.empty:
        return

    has_volume = "成交股數" in df.columns

    render_pro_section("K線主圖", "以 K 棒、均線與成交量為主，搭配訊號燈號與支撐壓力做趨勢判讀")

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
        height=820 if has_volume else 560,
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
            kd_fig.update_layout(height=340, hovermode="x unified")
            st.plotly_chart(kd_fig, use_container_width=True, config={"displaylogo": False})

    if "MACD" in selected_indicators and all(col in df.columns for col in ["日期", "DIF", "DEA", "MACD_HIST"]):
        with st.expander("查看 MACD 指標", expanded=False):
            macd_fig = go.Figure()
            macd_fig.add_trace(go.Bar(x=df["日期"], y=df["MACD_HIST"], name="MACD柱"))
            macd_fig.add_trace(go.Scatter(x=df["日期"], y=df["DIF"], mode="lines", name="DIF", line=dict(width=2.5)))
            macd_fig.add_trace(go.Scatter(x=df["日期"], y=df["DEA"], mode="lines", name="DEA", line=dict(width=2.5)))
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
    "歷史K線分析｜雷達評分版",
    "多空燈號、支撐壓力、雷達評分整合，從看圖進一步升級到多因子結構判讀。"
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

group_index = group_names.index(default_group) if default_group in group_names else 0
lookup_date = today_dt.strftime("%Y%m%d")

with st.form("history_query_form", clear_on_submit=False):
    c1, c2 = st.columns(2)

    with c1:
        selected_group = st.selectbox("選擇群組", group_names, index=group_index)

    items = watchlist_dict.get(selected_group, [])
    stock_options = build_stock_options(items, lookup_date)

    with c2:
        if stock_options:
            code_list = [x["code"] for x in stock_options]
            stock_index = code_list.index(default_code) if default_code in code_list else 0
            selected_stock_label = st.selectbox(
                "選擇股票",
                [x["label"] for x in stock_options],
                index=stock_index
            )
            selected_stock = next(x for x in stock_options if x["label"] == selected_stock_label)
        else:
            selected_stock = None
            st.selectbox("選擇股票", ["此群組目前沒有股票"], index=0)

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

    query_btn = st.form_submit_button("開始查詢", type="primary", use_container_width=True)

if not selected_stock:
    st.warning("此群組目前沒有可查詢股票。")
    st.stop()

if start_date > end_date:
    st.error("開始日期不能大於結束日期")
    st.stop()

if query_btn or selected_stock:
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
    signal = compute_signal_snapshot(df)
    sr = compute_support_resistance_snapshot(df)
    radar = compute_radar_scores(df)

    render_summary_card(selected_stock, start_date, end_date, selected_indicators, df, realtime_info)
    render_signal_board(signal)
    render_sr_board(sr)
    render_radar_board(radar)

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

    render_chart(df, selected_indicators)
    render_pro_section("歷史資料表", "雷達評分、燈號與支撐壓力判讀後，可回到原始數值做二次驗證")
    render_table(df)

    st.success(f"查詢完成，共 {len(df)} 筆資料。")
