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
    to_excel_bytes,
)

from query_state import load_last_query_state, save_last_query_state, parse_date_safe


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


def inject_overview_card_css():
    st.markdown(
        """
        <style>
        .overview-card-wrap {
            background: linear-gradient(180deg, #ffffff 0%, #fbfcfe 100%);
            border: 1px solid #e6ebf2;
            border-left: 6px solid #4a90e2;
            border-radius: 16px;
            padding: 18px 20px 18px 20px;
            margin: 10px 0 18px 0;
            box-shadow: 0 4px 14px rgba(31, 41, 55, 0.05);
        }

        .overview-title {
            font-size: 20px;
            font-weight: 700;
            color: #1f2937;
            margin-bottom: 14px;
        }

        .overview-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(160px, 1fr));
            gap: 12px 14px;
        }

        .overview-item {
            background: #f8fbff;
            border: 1px solid #edf2f7;
            border-radius: 12px;
            padding: 12px 14px;
            min-height: 72px;
        }

        .overview-item-wide {
            grid-column: span 2;
        }

        .overview-label {
            font-size: 12px;
            color: #6b7280;
            margin-bottom: 6px;
            font-weight: 600;
            letter-spacing: 0.3px;
        }

        .overview-value {
            font-size: 17px;
            color: #111827;
            font-weight: 700;
            line-height: 1.4;
            word-break: break-word;
        }

        .overview-value-small {
            font-size: 15px;
            color: #111827;
            font-weight: 600;
            line-height: 1.5;
            word-break: break-word;
        }

        .value-up {
            color: #d32f2f;
            font-weight: 700;
        }

        .value-down {
            color: #00897b;
            font-weight: 700;
        }

        .value-flat {
            color: #666;
            font-weight: 700;
        }

        .badge {
            display: inline-block;
            background: #eef6ff;
            color: #1d4ed8;
            border: 1px solid #dbeafe;
            border-radius: 999px;
            padding: 4px 10px;
            margin: 3px 6px 3px 0;
            font-size: 12px;
            font-weight: 700;
        }

        .signal-positive {
            display: inline-block;
            background: #eefaf0;
            color: #2e7d32;
            border: 1px solid #c8e6c9;
            border-radius: 999px;
            padding: 4px 10px;
            margin: 3px 6px 3px 0;
            font-size: 12px;
            font-weight: 700;
        }

        .signal-negative {
            display: inline-block;
            background: #fff1f0;
            color: #c62828;
            border: 1px solid #ffcdd2;
            border-radius: 999px;
            padding: 4px 10px;
            margin: 3px 6px 3px 0;
            font-size: 12px;
            font-weight: 700;
        }

        .signal-neutral {
            display: inline-block;
            background: #f3f4f6;
            color: #6b7280;
            border: 1px solid #e5e7eb;
            border-radius: 999px;
            padding: 4px 10px;
            margin: 3px 6px 3px 0;
            font-size: 12px;
            font-weight: 700;
        }

        @media (max-width: 1200px) {
            .overview-grid {
                grid-template-columns: repeat(2, minmax(160px, 1fr));
            }
            .overview-item-wide {
                grid-column: span 2;
            }
        }

        @media (max-width: 768px) {
            .overview-grid {
                grid-template-columns: 1fr;
            }
            .overview-item-wide {
                grid-column: span 1;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def add_moving_averages(df: pd.DataFrame, selected_indicators: list) -> pd.DataFrame:
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

    return df


def add_kd_macd(df: pd.DataFrame, selected_indicators: list) -> pd.DataFrame:
    if df.empty:
        return df

    need_kd = "KD" in selected_indicators
    need_macd = "MACD" in selected_indicators

    if need_kd and all(col in df.columns for col in ["最高價", "最低價", "收盤價"]):
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

    if need_macd and "收盤價" in df.columns:
        ema12 = df["收盤價"].ewm(span=12, adjust=False).mean()
        ema26 = df["收盤價"].ewm(span=26, adjust=False).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9, adjust=False).mean()
        macd_hist = dif - dea

        df["DIF"] = dif
        df["DEA"] = dea
        df["MACD_HIST"] = macd_hist

    return df


def add_signal_flags(df: pd.DataFrame, selected_indicators: list) -> pd.DataFrame:
    if df.empty:
        return df

    if "KD" in selected_indicators and all(col in df.columns for col in ["K", "D"]):
        df["KD_GOLDEN"] = (df["K"] > df["D"]) & (df["K"].shift(1) <= df["D"].shift(1))
        df["KD_DEATH"] = (df["K"] < df["D"]) & (df["K"].shift(1) >= df["D"].shift(1))

    if "MACD" in selected_indicators and all(col in df.columns for col in ["DIF", "DEA"]):
        df["MACD_GOLDEN"] = (df["DIF"] > df["DEA"]) & (df["DIF"].shift(1) <= df["DEA"].shift(1))
        df["MACD_DEATH"] = (df["DIF"] < df["DEA"]) & (df["DIF"].shift(1) >= df["DEA"].shift(1))

    return df


def prepare_display_df(df: pd.DataFrame, selected_indicators: list) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(df).copy()

    if "日期" in df.columns:
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        df = df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    df = add_moving_averages(df, selected_indicators)
    df = add_kd_macd(df, selected_indicators)
    df = add_signal_flags(df, selected_indicators)

    return df


def get_recent_signal_text(df: pd.DataFrame, signal_name: str, signal_col: str):
    if df.empty or signal_col not in df.columns or "日期" not in df.columns:
        return '<span class="signal-neutral">無</span>'

    signal_df = df[df[signal_col] == True]
    if signal_df.empty:
        return '<span class="signal-neutral">無</span>'

    last_row = signal_df.iloc[-1]
    signal_date = pd.to_datetime(last_row["日期"], errors="coerce")
    date_text = "" if pd.isna(signal_date) else signal_date.strftime("%Y-%m-%d")

    if "黃金交叉" in signal_name or "買點" in signal_name:
        return f'<span class="signal-positive">{signal_name}：{date_text}</span>'
    elif "死亡交叉" in signal_name or "賣點" in signal_name:
        return f'<span class="signal-negative">{signal_name}：{date_text}</span>'
    return f'<span class="signal-neutral">{signal_name}：{date_text}</span>'


def render_overview_card(group_name, stock_info, start_date, end_date, selected_indicators, df: pd.DataFrame):
    stock_text = "尚未匹配"
    market_text = "—"

    if stock_info is not None:
        stock_text = f"{stock_info['name']}（{stock_info['code']}）"
        market_text = stock_info.get("market", "—")

    if selected_indicators:
        indicator_html = "".join(
            [f'<span class="badge">{x}</span>' for x in selected_indicators]
        )
    else:
        indicator_html = '<span class="badge">無</span>'

    latest_close_text = "—"
    price_change_text = "—"
    price_change_pct_text = "—"
    price_change_class = "value-flat"

    kd_signal_html = '<span class="signal-neutral">未啟用</span>'
    macd_signal_html = '<span class="signal-neutral">未啟用</span>'

    if df is not None and not df.empty and "收盤價" in df.columns:
        latest_close = df["收盤價"].iloc[-1]
        first_close = df["收盤價"].iloc[0]
        latest_close_text = f"{latest_close:,.2f}"

        if pd.notna(first_close) and first_close != 0:
            price_change = latest_close - first_close
            price_change_pct = price_change / first_close * 100

            if price_change > 0:
                price_change_text = f"+{price_change:,.2f}"
                price_change_pct_text = f"+{price_change_pct:,.2f}%"
                price_change_class = "value-up"
            elif price_change < 0:
                price_change_text = f"{price_change:,.2f}"
                price_change_pct_text = f"{price_change_pct:,.2f}%"
                price_change_class = "value-down"
            else:
                price_change_text = f"{price_change:,.2f}"
                price_change_pct_text = f"{price_change_pct:,.2f}%"
                price_change_class = "value-flat"

        if "KD" in selected_indicators:
            kd_parts = []
            if "KD_GOLDEN" in df.columns:
                kd_parts.append(get_recent_signal_text(df, "KD黃金交叉", "KD_GOLDEN"))
            if "KD_DEATH" in df.columns:
                kd_parts.append(get_recent_signal_text(df, "KD死亡交叉", "KD_DEATH"))
            kd_signal_html = "".join(kd_parts) if kd_parts else '<span class="signal-neutral">無</span>'

        if "MACD" in selected_indicators:
            macd_parts = []
            if "MACD_GOLDEN" in df.columns:
                macd_parts.append(get_recent_signal_text(df, "MACD黃金交叉", "MACD_GOLDEN"))
            if "MACD_DEATH" in df.columns:
                macd_parts.append(get_recent_signal_text(df, "MACD死亡交叉", "MACD_DEATH"))
            macd_signal_html = "".join(macd_parts) if macd_parts else '<span class="signal-neutral">無</span>'

    html = f"""
    <div class="overview-card-wrap">
        <div class="overview-title">查詢總覽</div>

        <div class="overview-grid">
            <div class="overview-item">
                <div class="overview-label">群組</div>
                <div class="overview-value">{group_name if group_name else "—"}</div>
            </div>

            <div class="overview-item overview-item-wide">
                <div class="overview-label">股票</div>
                <div class="overview-value">{stock_text}</div>
            </div>

            <div class="overview-item">
                <div class="overview-label">市場別</div>
                <div class="overview-value">{market_text}</div>
            </div>

            <div class="overview-item overview-item-wide">
                <div class="overview-label">日期區間</div>
                <div class="overview-value-small">{start_date} ~ {end_date}</div>
            </div>

            <div class="overview-item">
                <div class="overview-label">最新收盤價</div>
                <div class="overview-value">{latest_close_text}</div>
            </div>

            <div class="overview-item">
                <div class="overview-label">區間漲跌</div>
                <div class="overview-value {price_change_class}">{price_change_text}</div>
            </div>

            <div class="overview-item">
                <div class="overview-label">區間漲跌幅</div>
                <div class="overview-value {price_change_class}">{price_change_pct_text}</div>
            </div>

            <div class="overview-item overview-item-wide">
                <div class="overview-label">技術指標</div>
                <div class="overview-value-small">{indicator_html}</div>
            </div>

            <div class="overview-item overview-item-wide">
                <div class="overview-label">最近 KD 訊號</div>
                <div class="overview-value-small">{kd_signal_html}</div>
            </div>

            <div class="overview-item overview-item-wide">
                <div class="overview-label">最近 MACD 訊號</div>
                <div class="overview-value-small">{macd_signal_html}</div>
            </div>
        </div>
    </div>
    """

    st.markdown(html, unsafe_allow_html=True)


def render_top_toolbar(stock_info, selected_indicators, df: pd.DataFrame):
    stock_text = "—"
    latest_close_text = "—"
    indicator_text = "、".join(selected_indicators) if selected_indicators else "無"

    if stock_info is not None:
        stock_text = f"{stock_info['name']}（{stock_info['code']}）"

    if df is not None and not df.empty and "收盤價" in df.columns:
        latest_close = df["收盤價"].iloc[-1]
        latest_close_text = f"{latest_close:,.2f}"

    c1, c2, c3 = st.columns([2, 1, 3])

    with c1:
        st.markdown(f"**目前股票：** {stock_text}")
    with c2:
        st.markdown(f"**最新收盤：** {latest_close_text}")
    with c3:
        st.markdown(f"**技術指標：** {indicator_text}")


def render_table(df: pd.DataFrame):
    if df.empty:
        st.info("目前沒有可顯示的資料。")
        return

    base_cols = ["日期", "開盤價", "最高價", "最低價", "收盤價", "成交股數", "成交金額", "成交筆數"]
    indicator_cols = [
        "MA5", "MA10", "MA20", "MA60", "MA120", "MA240",
        "K", "D", "DIF", "DEA", "MACD_HIST"
    ]

    show_cols = [c for c in base_cols + indicator_cols if c in df.columns]
    display_df = df[show_cols].copy() if show_cols else df.copy()

    if "日期" in display_df.columns:
        display_df["日期"] = pd.to_datetime(
            display_df["日期"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    if "收盤價" in display_df.columns:
        display_df["漲跌"] = display_df["收盤價"].diff()
        cols = list(display_df.columns)
        insert_at = cols.index("收盤價") + 1
        ordered_cols = cols[:insert_at] + ["漲跌"] + [c for c in cols[insert_at:] if c != "漲跌"]
        display_df = display_df[ordered_cols]

    price_cols = [
        "開盤價", "最高價", "最低價", "收盤價", "漲跌",
        "MA5", "MA10", "MA20", "MA60", "MA120", "MA240",
        "K", "D", "DIF", "DEA", "MACD_HIST"
    ]
    int_cols = ["成交股數", "成交金額", "成交筆數"]

    format_dict = {}
    for col in price_cols:
        if col in display_df.columns:
            format_dict[col] = "{:,.2f}"
    for col in int_cols:
        if col in display_df.columns:
            format_dict[col] = "{:,.0f}"

    def color_price_change(val):
        if pd.isna(val):
            return ""
        try:
            v = float(val)
        except Exception:
            return ""
        if v > 0:
            return "color: #d32f2f; font-weight: 600;"
        elif v < 0:
            return "color: #00897b; font-weight: 600;"
        return "color: #666;"

    styler = display_df.style.format(format_dict, na_rep="—")

    if "漲跌" in display_df.columns:
        styler = styler.map(color_price_change, subset=["漲跌"])

    styler = styler.set_properties(**{
        "text-align": "center",
        "white-space": "nowrap"
    }).set_table_styles([
        {
            "selector": "thead th",
            "props": [
                ("background-color", "#f7f9fc"),
                ("color", "#1f2937"),
                ("font-weight", "700"),
                ("font-size", "13px"),
                ("border-bottom", "1px solid #dbe2ea"),
                ("text-align", "center"),
                ("padding", "10px 8px"),
            ],
        },
        {
            "selector": "tbody td",
            "props": [
                ("font-size", "13px"),
                ("padding", "8px 10px"),
                ("border-bottom", "1px solid #eef2f7"),
            ],
        },
        {
            "selector": "tbody tr:hover",
            "props": [
                ("background-color", "#f8fbff"),
            ],
        },
        {
            "selector": "table",
            "props": [
                ("border-collapse", "collapse"),
                ("width", "100%"),
            ],
        },
    ])

    st.dataframe(
        styler,
        use_container_width=True,
        hide_index=True,
        height=min(720, 80 + len(display_df) * 35)
    )


def render_chart(df: pd.DataFrame, selected_indicators: list):
    if df.empty:
        return

    need_price_cols = all(col in df.columns for col in ["日期", "開盤價", "最高價", "最低價", "收盤價"])
    has_volume = "成交股數" in df.columns

    bg_color = "#ffffff"
    grid_color = "rgba(180, 180, 180, 0.25)"
    axis_color = "#444"
    up_color = "#e53935"
    down_color = "#26a69a"

    ma_colors = {
        "MA5": "#4A90E2",
        "MA10": "#F44336",
        "MA20": "#F5A623",
        "MA60": "#8E44AD",
        "MA120": "#7F8C8D",
        "MA240": "#2C3E50",
    }

    if need_price_cols:
        st.subheader("K線趨勢圖")

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
                increasing_line_color=up_color,
                increasing_fillcolor="rgba(229, 57, 53, 0.85)",
                decreasing_line_color=down_color,
                decreasing_fillcolor="rgba(38, 166, 154, 0.85)",
                whiskerwidth=0.5,
                hovertemplate=(
                    "日期：%{x|%Y-%m-%d}<br>"
                    "開盤：%{open:.2f}<br>"
                    "最高：%{high:.2f}<br>"
                    "最低：%{low:.2f}<br>"
                    "收盤：%{close:.2f}<extra></extra>"
                )
            ),
            row=1,
            col=1
        )

        ma_lines = ["MA5", "MA10", "MA20", "MA60", "MA120", "MA240"]
        for ma_name in ma_lines:
            if ma_name in selected_indicators and ma_name in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df["日期"],
                        y=df[ma_name],
                        mode="lines",
                        name=ma_name,
                        line=dict(
                            width=2.3 if ma_name in ["MA5", "MA10", "MA20"] else 2,
                            color=ma_colors.get(ma_name, "#666")
                        ),
                        hovertemplate=f"日期：%{{x|%Y-%m-%d}}<br>{ma_name}：%{{y:.2f}}<extra></extra>"
                    ),
                    row=1,
                    col=1
                )

        if "KD" in selected_indicators and "收盤價" in df.columns:
            if "KD_GOLDEN" in df.columns:
                kd_buy_df = df[df["KD_GOLDEN"]]
                if not kd_buy_df.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=kd_buy_df["日期"],
                            y=kd_buy_df["收盤價"] * 0.985,
                            mode="markers",
                            name="KD買點",
                            marker=dict(size=12, symbol="triangle-up", color="#2E7D32", line=dict(color="white", width=1)),
                            hovertemplate="日期：%{x|%Y-%m-%d}<br>KD買點：%{y:.2f}<extra></extra>"
                        ),
                        row=1,
                        col=1
                    )

            if "KD_DEATH" in df.columns:
                kd_sell_df = df[df["KD_DEATH"]]
                if not kd_sell_df.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=kd_sell_df["日期"],
                            y=kd_sell_df["收盤價"] * 1.015,
                            mode="markers",
                            name="KD賣點",
                            marker=dict(size=12, symbol="triangle-down", color="#C62828", line=dict(color="white", width=1)),
                            hovertemplate="日期：%{x|%Y-%m-%d}<br>KD賣點：%{y:.2f}<extra></extra>"
                        ),
                        row=1,
                        col=1
                    )

        if "MACD" in selected_indicators and "收盤價" in df.columns:
            if "MACD_GOLDEN" in df.columns:
                macd_buy_df = df[df["MACD_GOLDEN"]]
                if not macd_buy_df.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=macd_buy_df["日期"],
                            y=macd_buy_df["收盤價"] * 0.97,
                            mode="markers",
                            name="MACD買點",
                            marker=dict(size=13, symbol="star", color="#1565C0", line=dict(color="white", width=1)),
                            hovertemplate="日期：%{x|%Y-%m-%d}<br>MACD買點：%{y:.2f}<extra></extra>"
                        ),
                        row=1,
                        col=1
                    )

            if "MACD_DEATH" in df.columns:
                macd_sell_df = df[df["MACD_DEATH"]]
                if not macd_sell_df.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=macd_sell_df["日期"],
                            y=macd_sell_df["收盤價"] * 1.03,
                            mode="markers",
                            name="MACD賣點",
                            marker=dict(size=13, symbol="x", color="#111111", line=dict(color="white", width=1)),
                            hovertemplate="日期：%{x|%Y-%m-%d}<br>MACD賣點：%{y:.2f}<extra></extra>"
                        ),
                        row=1,
                        col=1
                    )

        latest_close = df["收盤價"].iloc[-1]
        fig.add_hline(
            y=latest_close,
            line_width=1,
            line_dash="dot",
            line_color="rgba(80, 80, 80, 0.5)",
            annotation_text=f"最新收盤 {latest_close:.2f}",
            annotation_position="top right",
            row=1,
            col=1
        )

        if has_volume:
            volume_colors = []
            for _, row_data in df.iterrows():
                open_p = row_data.get("開盤價")
                close_p = row_data.get("收盤價")
                if pd.notna(close_p) and pd.notna(open_p) and close_p >= open_p:
                    volume_colors.append("rgba(229, 57, 53, 0.65)")
                else:
                    volume_colors.append("rgba(38, 166, 154, 0.65)")

            fig.add_trace(
                go.Bar(
                    x=df["日期"],
                    y=df["成交股數"],
                    name="成交量",
                    marker_color=volume_colors,
                    hovertemplate="日期：%{x|%Y-%m-%d}<br>成交量：%{y:,.0f}<extra></extra>"
                ),
                row=2,
                col=1
            )

        fig.update_layout(
            height=780 if has_volume else 560,
            xaxis_rangeslider_visible=False,
            hovermode="x unified",
            plot_bgcolor=bg_color,
            paper_bgcolor=bg_color,
            margin=dict(l=20, r=20, t=20, b=20),
            font=dict(size=13, color=axis_color),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="left",
                x=0,
                bgcolor="rgba(255,255,255,0.85)",
                bordercolor="rgba(0,0,0,0.08)",
                borderwidth=1,
                font=dict(size=12)
            )
        )

        fig.update_xaxes(
            showgrid=True,
            gridcolor=grid_color,
            linecolor="rgba(0,0,0,0.15)",
            tickfont=dict(size=12, color=axis_color),
            showspikes=True,
            spikemode="across",
            spikecolor="rgba(100,100,100,0.4)",
            spikethickness=1
        )

        fig.update_yaxes(
            title_text="股價",
            showgrid=True,
            gridcolor=grid_color,
            linecolor="rgba(0,0,0,0.15)",
            tickfont=dict(size=12, color=axis_color),
            title_font=dict(size=14, color=axis_color),
            row=1,
            col=1
        )

        if has_volume:
            fig.update_yaxes(
                title_text="成交量",
                showgrid=True,
                gridcolor=grid_color,
                linecolor="rgba(0,0,0,0.15)",
                tickfont=dict(size=12, color=axis_color),
                title_font=dict(size=14, color=axis_color),
                row=2,
                col=1
            )

        st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

    if "KD" in selected_indicators and all(col in df.columns for col in ["日期", "K", "D"]):
        with st.expander("查看 KD 指標", expanded=False):
            st.subheader("KD 指標")

            kd_fig = go.Figure()
            kd_fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df["K"],
                    mode="lines",
                    name="K",
                    line=dict(color="#1E88E5", width=2.5),
                    hovertemplate="日期：%{x|%Y-%m-%d}<br>K：%{y:.2f}<extra></extra>"
                )
            )
            kd_fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df["D"],
                    mode="lines",
                    name="D",
                    line=dict(color="#FB8C00", width=2.5),
                    hovertemplate="日期：%{x|%Y-%m-%d}<br>D：%{y:.2f}<extra></extra>"
                )
            )
            kd_fig.add_hline(y=80, line_dash="dot", line_color="rgba(200,0,0,0.4)", annotation_text="超買 80")
            kd_fig.add_hline(y=20, line_dash="dot", line_color="rgba(0,150,0,0.4)", annotation_text="超賣 20")

            if "KD_GOLDEN" in df.columns:
                kd_buy_df = df[df["KD_GOLDEN"]]
                if not kd_buy_df.empty:
                    kd_fig.add_trace(
                        go.Scatter(
                            x=kd_buy_df["日期"],
                            y=kd_buy_df["K"],
                            mode="markers",
                            name="KD黃金交叉",
                            marker=dict(size=11, symbol="triangle-up", color="#2E7D32", line=dict(color="white", width=1)),
                            hovertemplate="日期：%{x|%Y-%m-%d}<br>KD黃金交叉：%{y:.2f}<extra></extra>"
                        )
                    )

            if "KD_DEATH" in df.columns:
                kd_sell_df = df[df["KD_DEATH"]]
                if not kd_sell_df.empty:
                    kd_fig.add_trace(
                        go.Scatter(
                            x=kd_sell_df["日期"],
                            y=kd_sell_df["K"],
                            mode="markers",
                            name="KD死亡交叉",
                            marker=dict(size=11, symbol="triangle-down", color="#C62828", line=dict(color="white", width=1)),
                            hovertemplate="日期：%{x|%Y-%m-%d}<br>KD死亡交叉：%{y:.2f}<extra></extra>"
                        )
                    )

            kd_fig.update_layout(
                height=340,
                hovermode="x unified",
                plot_bgcolor="#ffffff",
                paper_bgcolor="#ffffff",
                margin=dict(l=20, r=20, t=20, b=20),
                font=dict(size=13, color="#444"),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="left",
                    x=0,
                    bgcolor="rgba(255,255,255,0.85)"
                )
            )
            kd_fig.update_xaxes(showgrid=True, gridcolor="rgba(180, 180, 180, 0.25)")
            kd_fig.update_yaxes(showgrid=True, gridcolor="rgba(180, 180, 180, 0.25)", range=[0, 100])

            st.plotly_chart(kd_fig, use_container_width=True, config={"displaylogo": False})

    if "MACD" in selected_indicators and all(col in df.columns for col in ["日期", "DIF", "DEA", "MACD_HIST"]):
        with st.expander("查看 MACD 指標", expanded=False):
            st.subheader("MACD 指標")

            macd_colors = ["rgba(229, 57, 53, 0.65)" if v >= 0 else "rgba(38, 166, 154, 0.65)" for v in df["MACD_HIST"]]

            macd_fig = go.Figure()
            macd_fig.add_trace(
                go.Bar(
                    x=df["日期"],
                    y=df["MACD_HIST"],
                    name="MACD柱",
                    marker_color=macd_colors,
                    hovertemplate="日期：%{x|%Y-%m-%d}<br>MACD柱：%{y:.2f}<extra></extra>"
                )
            )
            macd_fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df["DIF"],
                    mode="lines",
                    name="DIF",
                    line=dict(color="#1565C0", width=2.5),
                    hovertemplate="日期：%{x|%Y-%m-%d}<br>DIF：%{y:.2f}<extra></extra>"
                )
            )
            macd_fig.add_trace(
                go.Scatter(
                    x=df["日期"],
                    y=df["DEA"],
                    mode="lines",
                    name="DEA",
                    line=dict(color="#E53935", width=2.5),
                    hovertemplate="日期：%{x|%Y-%m-%d}<br>DEA：%{y:.2f}<extra></extra>"
                )
            )

            if "MACD_GOLDEN" in df.columns:
                macd_buy_df = df[df["MACD_GOLDEN"]]
                if not macd_buy_df.empty:
                    macd_fig.add_trace(
                        go.Scatter(
                            x=macd_buy_df["日期"],
                            y=macd_buy_df["DIF"],
                            mode="markers",
                            name="MACD黃金交叉",
                            marker=dict(size=11, symbol="triangle-up", color="#2E7D32", line=dict(color="white", width=1)),
                            hovertemplate="日期：%{x|%Y-%m-%d}<br>MACD黃金交叉：%{y:.2f}<extra></extra>"
                        )
                    )

            if "MACD_DEATH" in df.columns:
                macd_sell_df = df[df["MACD_DEATH"]]
                if not macd_sell_df.empty:
                    macd_fig.add_trace(
                        go.Scatter(
                            x=macd_sell_df["日期"],
                            y=macd_sell_df["DIF"],
                            mode="markers",
                            name="MACD死亡交叉",
                            marker=dict(size=11, symbol="triangle-down", color="#C62828", line=dict(color="white", width=1)),
                            hovertemplate="日期：%{x|%Y-%m-%d}<br>MACD死亡交叉：%{y:.2f}<extra></extra>"
                        )
                    )

            macd_fig.add_hline(y=0, line_dash="dot", line_color="rgba(80,80,80,0.4)")
            macd_fig.update_layout(
                height=360,
                hovermode="x unified",
                plot_bgcolor="#ffffff",
                paper_bgcolor="#ffffff",
                margin=dict(l=20, r=20, t=20, b=20),
                font=dict(size=13, color="#444"),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="left",
                    x=0,
                    bgcolor="rgba(255,255,255,0.85)"
                )
            )
            macd_fig.update_xaxes(showgrid=True, gridcolor="rgba(180, 180, 180, 0.25)")
            macd_fig.update_yaxes(showgrid=True, gridcolor="rgba(180, 180, 180, 0.25)")

            st.plotly_chart(macd_fig, use_container_width=True, config={"displaylogo": False})


def run_query(selected_group, selected_stock, start_date, end_date, selected_indicators):
    st.session_state.kline_group = selected_group
    st.session_state.kline_stock_code = selected_stock["code"]
    st.session_state.kline_start = start_date
    st.session_state.kline_end = end_date
    st.session_state.kline_indicators = selected_indicators

    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code=selected_stock["code"],
        home_start=start_date,
        home_end=end_date
    )

    df = get_history_data(
        selected_stock["code"],
        selected_stock["name"],
        selected_stock["market"],
        start_date,
        end_date
    )

    st.session_state.kline_result_df = df
    st.session_state.kline_selected_stock = selected_stock
    st.session_state.kline_selected_group = selected_group


st.set_page_config(page_title="歷史K線分析", page_icon="📊", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)
inject_overview_card_css()

last_state = load_last_query_state()
today_dt = date.today()

home_group = last_state.get("quick_group", "")
home_stock_code = last_state.get("quick_stock_code", "")
home_start = parse_date_safe(last_state.get("home_start", ""), today_dt - timedelta(days=90))
home_end = parse_date_safe(last_state.get("home_end", ""), today_dt)

home_signature = f"{home_group}|{home_stock_code}|{home_start}|{home_end}"

if "kline_group" not in st.session_state:
    st.session_state.kline_group = home_group
if "kline_stock_code" not in st.session_state:
    st.session_state.kline_stock_code = home_stock_code
if "kline_start" not in st.session_state:
    st.session_state.kline_start = home_start
if "kline_end" not in st.session_state:
    st.session_state.kline_end = home_end
if "kline_result_df" not in st.session_state:
    st.session_state.kline_result_df = None
if "kline_selected_stock" not in st.session_state:
    st.session_state.kline_selected_stock = None
if "kline_selected_group" not in st.session_state:
    st.session_state.kline_selected_group = None
if "kline_indicators" not in st.session_state:
    st.session_state.kline_indicators = ["MA5", "MA10", "MA20"]
if "last_home_signature_applied" not in st.session_state:
    st.session_state.last_home_signature_applied = ""

st.session_state.kline_group = home_group
st.session_state.kline_stock_code = home_stock_code
st.session_state.kline_start = home_start
st.session_state.kline_end = home_end

st.title("📊 歷史K線分析")
st.caption("依群組、股票、日期區間查詢歷史K線資料，並可選擇技術指標")

lookup_date = today_dt.strftime("%Y%m%d")
watchlist_dict = get_normalized_watchlist()
group_names = list(watchlist_dict.keys())

if not group_names:
    st.warning("目前沒有自選股群組，請先到「自選股中心」建立群組與股票。")
    st.stop()

saved_group = st.session_state.get("kline_group", "")
group_index = group_names.index(saved_group) if saved_group in group_names else 0

with st.form("kline_query_form", clear_on_submit=False):
    c1, c2 = st.columns(2)

    with c1:
        selected_group = st.selectbox(
            "選擇群組",
            group_names,
            index=group_index
        )

    items = watchlist_dict.get(selected_group, [])
    stock_options = build_stock_options(items, lookup_date)

    with c2:
        if stock_options:
            saved_stock_code = st.session_state.get("kline_stock_code", "")
            stock_codes = [x["code"] for x in stock_options]
            stock_index = stock_codes.index(saved_stock_code) if saved_stock_code in stock_codes else 0

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
        start_date = st.date_input(
            "開始日期",
            value=st.session_state.get("kline_start", today_dt - timedelta(days=90))
        )

    with d2:
        end_date = st.date_input(
            "結束日期",
            value=st.session_state.get("kline_end", today_dt)
        )

    selected_indicators = st.multiselect(
        "技術指標",
        ["MA5", "MA10", "MA20", "MA60", "MA120", "MA240", "KD", "MACD"],
        default=st.session_state.get("kline_indicators", ["MA5", "MA10", "MA20"])
    )

    query_btn = st.form_submit_button("開始查詢", type="primary", use_container_width=True)

auto_query_needed = False
auto_selected_stock = None

if home_signature != st.session_state.get("last_home_signature_applied", ""):
    auto_items = watchlist_dict.get(home_group, [])
    auto_stock_options = build_stock_options(auto_items, lookup_date)
    auto_selected_stock = next((x for x in auto_stock_options if x["code"] == home_stock_code), None)

    if home_group and auto_selected_stock is not None and home_start <= home_end:
        auto_query_needed = True

if auto_query_needed:
    with st.spinner("已套用首頁條件，正在自動查詢歷史資料..."):
        run_query(
            selected_group=home_group,
            selected_stock=auto_selected_stock,
            start_date=home_start,
            end_date=home_end,
            selected_indicators=st.session_state.get("kline_indicators", ["MA5", "MA10", "MA20"])
        )
    st.session_state.last_home_signature_applied = home_signature

if query_btn:
    if start_date > end_date:
        st.error("開始日期不能大於結束日期")
        st.stop()

    if selected_stock is None:
        st.warning("此群組目前沒有可查詢股票。")
        st.stop()

    with st.spinner("正在查詢歷史資料..."):
        run_query(
            selected_group=selected_group,
            selected_stock=selected_stock,
            start_date=start_date,
            end_date=end_date,
            selected_indicators=selected_indicators
        )

    st.session_state.last_home_signature_applied = f"{selected_group}|{selected_stock['code']}|{start_date}|{end_date}"

result_df = st.session_state.get("kline_result_df", None)
result_stock = st.session_state.get("kline_selected_stock", None)
result_group = st.session_state.get("kline_selected_group", None)
selected_indicators = st.session_state.get("kline_indicators", ["MA5", "MA10", "MA20"])

if result_df is not None and result_stock is not None:
    df = prepare_display_df(result_df, selected_indicators)

    if df.empty:
        st.warning("查無歷史資料。")
        st.stop()

    render_overview_card(
        group_name=result_group,
        stock_info=result_stock,
        start_date=st.session_state.kline_start,
        end_date=st.session_state.kline_end,
        selected_indicators=selected_indicators,
        df=df
    )

    render_top_toolbar(result_stock, selected_indicators, df)

    if "MA10" in selected_indicators and len(df) < 10:
        st.info("目前資料筆數不足 10 筆，MA10 以現有資料平均顯示。")
    if "MA20" in selected_indicators and len(df) < 20:
        st.info("目前資料筆數不足 20 筆，MA20 以現有資料平均顯示。")
    if "MA60" in selected_indicators and len(df) < 60:
        st.info("目前資料筆數不足 60 筆，MA60 以現有資料平均顯示。")
    if "MA120" in selected_indicators and len(df) < 120:
        st.info("目前資料筆數不足 120 筆，MA120 以現有資料平均顯示。")
    if "MA240" in selected_indicators and len(df) < 240:
        st.info("目前資料筆數不足 240 筆，MA240 以現有資料平均顯示。")

    st.markdown("---")
    st.subheader("歷史資料")

    download_df = df.copy()
    if "日期" in download_df.columns:
        download_df["日期"] = pd.to_datetime(download_df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")

    excel_bytes = to_excel_bytes({
        "歷史K線資料": download_df
    })

    d1, d2 = st.columns([1, 5])
    with d1:
        st.download_button(
            label="下載 Excel",
            data=excel_bytes,
            file_name=f"{result_stock['code']}_{result_stock['name']}_歷史K線分析.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    render_table(df)
    render_chart(df, selected_indicators)

    st.success(f"查詢完成，共 {len(df)} 筆資料。")
else:
    preview_group = st.session_state.get("kline_group", "")
    preview_stock_code = st.session_state.get("kline_stock_code", "")
    preview_items = build_stock_options(watchlist_dict.get(preview_group, []), lookup_date)
    preview_stock = next((x for x in preview_items if x["code"] == preview_stock_code), None)

    if preview_group:
        render_overview_card(
            group_name=preview_group,
            stock_info=preview_stock,
            start_date=st.session_state.kline_start,
            end_date=st.session_state.kline_end,
            selected_indicators=selected_indicators,
            df=pd.DataFrame()
        )
        st.info("首頁條件已帶入；正在等待自動查詢或可直接手動查詢。")
    else:
        st.info("請先在首頁套用條件，或直接在本頁查詢。")
