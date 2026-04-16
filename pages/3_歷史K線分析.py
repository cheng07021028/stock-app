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


def render_summary_metrics(df: pd.DataFrame):
    if df.empty or "收盤價" not in df.columns:
        return

    latest_close = df["收盤價"].iloc[-1]
    first_close = df["收盤價"].iloc[0]
    price_change = latest_close - first_close
    price_change_pct = (price_change / first_close * 100) if first_close not in [0, None] else 0

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("最新收盤價", f"{latest_close:,.2f}")
    with m2:
        st.metric("區間漲跌", f"{price_change:,.2f}")
    with m3:
        st.metric("區間漲跌幅", f"{price_change_pct:,.2f}%")


def render_signal_summary(df: pd.DataFrame, selected_indicators: list):
    if df.empty or "日期" not in df.columns:
        return

    signals = []

    if "KD" in selected_indicators:
        if "KD_GOLDEN" in df.columns:
            kd_golden = df[df["KD_GOLDEN"]]
            if not kd_golden.empty:
                last_row = kd_golden.iloc[-1]
                signals.append(f"KD 黃金交叉：{last_row['日期'].strftime('%Y-%m-%d')}")

        if "KD_DEATH" in df.columns:
            kd_death = df[df["KD_DEATH"]]
            if not kd_death.empty:
                last_row = kd_death.iloc[-1]
                signals.append(f"KD 死亡交叉：{last_row['日期'].strftime('%Y-%m-%d')}")

    if "MACD" in selected_indicators:
        if "MACD_GOLDEN" in df.columns:
            macd_golden = df[df["MACD_GOLDEN"]]
            if not macd_golden.empty:
                last_row = macd_golden.iloc[-1]
                signals.append(f"MACD 黃金交叉：{last_row['日期'].strftime('%Y-%m-%d')}")

        if "MACD_DEATH" in df.columns:
            macd_death = df[df["MACD_DEATH"]]
            if not macd_death.empty:
                last_row = macd_death.iloc[-1]
                signals.append(f"MACD 死亡交叉：{last_row['日期'].strftime('%Y-%m-%d')}")

    if signals:
        st.markdown("### 訊號摘要")
        for text in signals:
            st.write(f"- {text}")


def render_table(df: pd.DataFrame):
    base_cols = ["日期", "開盤價", "最高價", "最低價", "收盤價", "成交股數", "成交金額", "成交筆數"]
    ordered_indicator_cols = [
        "MA5", "MA10", "MA20", "MA60", "MA120", "MA240",
        "K", "D", "DIF", "DEA", "MACD_HIST"
    ]

    show_cols = [c for c in base_cols + ordered_indicator_cols if c in df.columns]

    if show_cols:
        st.dataframe(df[show_cols], use_container_width=True, hide_index=True)
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def render_chart(df: pd.DataFrame, selected_indicators: list):
    if df.empty:
        return

    need_price_cols = all(col in df.columns for col in ["日期", "開盤價", "最高價", "最低價", "收盤價"])
    has_volume = "成交股數" in df.columns

    if need_price_cols:
        st.subheader("K線趨勢圖")

        if has_volume:
            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.06,
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
                hovertemplate=(
                    "日期: %{x|%Y-%m-%d}<br>"
                    "開盤: %{open:.2f}<br>"
                    "最高: %{high:.2f}<br>"
                    "最低: %{low:.2f}<br>"
                    "收盤: %{close:.2f}<extra></extra>"
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
                        line=dict(width=2),
                        hovertemplate=f"日期: %{{x|%Y-%m-%d}}<br>{ma_name}: %{{y:.2f}}<extra></extra>"
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
                            y=kd_buy_df["收盤價"],
                            mode="markers",
                            name="KD買點",
                            marker=dict(size=10, symbol="triangle-up", color="green"),
                            hovertemplate="日期: %{x|%Y-%m-%d}<br>KD買點: %{y:.2f}<extra></extra>"
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
                            y=kd_sell_df["收盤價"],
                            mode="markers",
                            name="KD賣點",
                            marker=dict(size=10, symbol="triangle-down", color="red"),
                            hovertemplate="日期: %{x|%Y-%m-%d}<br>KD賣點: %{y:.2f}<extra></extra>"
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
                            y=macd_buy_df["收盤價"],
                            mode="markers",
                            name="MACD買點",
                            marker=dict(size=11, symbol="star", color="blue"),
                            hovertemplate="日期: %{x|%Y-%m-%d}<br>MACD買點: %{y:.2f}<extra></extra>"
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
                            y=macd_sell_df["收盤價"],
                            mode="markers",
                            name="MACD賣點",
                            marker=dict(size=11, symbol="x", color="black"),
                            hovertemplate="日期: %{x|%Y-%m-%d}<br>MACD賣點: %{y:.2f}<extra></extra>"
                        ),
                        row=1,
                        col=1
                    )

        if has_volume:
            volume_colors = []
            for _, row_data in df.iterrows():
                open_p = row_data.get("開盤價")
                close_p = row_data.get("收盤價")
                if pd.notna(close_p) and pd.notna(open_p) and close_p >= open_p:
                    volume_colors.append("#ef5350")
                else:
                    volume_colors.append("#26a69a")

            fig.add_trace(
                go.Bar(
                    x=df["日期"],
                    y=df["成交股數"],
                    name="成交量",
                    marker_color=volume_colors,
                    hovertemplate="日期: %{x|%Y-%m-%d}<br>成交量: %{y:,.0f}<extra></extra>"
                ),
                row=2,
                col=1
            )

        fig.update_layout(
            height=760 if has_volume else 540,
            xaxis_rangeslider_visible=False,
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="left",
                x=0
            ),
            margin=dict(l=20, r=20, t=20, b=20)
        )

        fig.update_yaxes(title_text="股價", row=1, col=1)
        if has_volume:
            fig.update_yaxes(title_text="成交量", row=2, col=1)

        st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

    if "KD" in selected_indicators and all(col in df.columns for col in ["日期", "K", "D"]):
        st.subheader("KD 指標")

        kd_fig = go.Figure()
        kd_fig.add_trace(
            go.Scatter(
                x=df["日期"],
                y=df["K"],
                mode="lines",
                name="K",
                hovertemplate="日期: %{x|%Y-%m-%d}<br>K: %{y:.2f}<extra></extra>"
            )
        )
        kd_fig.add_trace(
            go.Scatter(
                x=df["日期"],
                y=df["D"],
                mode="lines",
                name="D",
                hovertemplate="日期: %{x|%Y-%m-%d}<br>D: %{y:.2f}<extra></extra>"
            )
        )

        if "KD_GOLDEN" in df.columns:
            kd_buy_df = df[df["KD_GOLDEN"]]
            if not kd_buy_df.empty:
                kd_fig.add_trace(
                    go.Scatter(
                        x=kd_buy_df["日期"],
                        y=kd_buy_df["K"],
                        mode="markers",
                        name="KD黃金交叉",
                        marker=dict(size=10, symbol="triangle-up", color="green"),
                        hovertemplate="日期: %{x|%Y-%m-%d}<br>KD黃金交叉: %{y:.2f}<extra></extra>"
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
                        marker=dict(size=10, symbol="triangle-down", color="red"),
                        hovertemplate="日期: %{x|%Y-%m-%d}<br>KD死亡交叉: %{y:.2f}<extra></extra>"
                    )
                )

        kd_fig.update_layout(
            height=320,
            hovermode="x unified",
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(kd_fig, use_container_width=True, config={"displaylogo": False})

    if "MACD" in selected_indicators and all(col in df.columns for col in ["日期", "DIF", "DEA", "MACD_HIST"]):
        st.subheader("MACD 指標")

        macd_fig = go.Figure()
        macd_fig.add_trace(
            go.Bar(
                x=df["日期"],
                y=df["MACD_HIST"],
                name="MACD柱",
                hovertemplate="日期: %{x|%Y-%m-%d}<br>MACD柱: %{y:.2f}<extra></extra>"
            )
        )
        macd_fig.add_trace(
            go.Scatter(
                x=df["日期"],
                y=df["DIF"],
                mode="lines",
                name="DIF",
                hovertemplate="日期: %{x|%Y-%m-%d}<br>DIF: %{y:.2f}<extra></extra>"
            )
        )
        macd_fig.add_trace(
            go.Scatter(
                x=df["日期"],
                y=df["DEA"],
                mode="lines",
                name="DEA",
                hovertemplate="日期: %{x|%Y-%m-%d}<br>DEA: %{y:.2f}<extra></extra>"
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
                        marker=dict(size=10, symbol="triangle-up", color="blue"),
                        hovertemplate="日期: %{x|%Y-%m-%d}<br>MACD黃金交叉: %{y:.2f}<extra></extra>"
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
                        marker=dict(size=10, symbol="triangle-down", color="black"),
                        hovertemplate="日期: %{x|%Y-%m-%d}<br>MACD死亡交叉: %{y:.2f}<extra></extra>"
                    )
                )

        macd_fig.update_layout(
            height=360,
            hovermode="x unified",
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(macd_fig, use_container_width=True, config={"displaylogo": False})


st.set_page_config(page_title="歷史K線分析", page_icon="📊", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)

# 每次進頁都先讀首頁最新條件
last_state = load_last_query_state()
today_dt = date.today()

home_group = last_state.get("quick_group", "")
home_stock_code = last_state.get("quick_stock_code", "")
home_start = parse_date_safe(
    last_state.get("home_start", ""),
    today_dt - timedelta(days=90)
)
home_end = parse_date_safe(
    last_state.get("home_end", ""),
    today_dt
)

# 初始化
if "kline_group" not in st.session_state:
    st.session_state.kline_group = home_group
if "kline_stock_code" not in st.session_state:
    st.session_state.kline_stock_code = home_stock_code
if "kline_start" not in st.session_state:
    st.session_state.kline_start = home_start
if "kline_end" not in st.session_state:
    st.session_state.kline_end = home_end

# 同步首頁最新條件到歷史頁
st.session_state.kline_group = home_group
st.session_state.kline_stock_code = home_stock_code
st.session_state.kline_start = home_start
st.session_state.kline_end = home_end

if "kline_result_df" not in st.session_state:
    st.session_state.kline_result_df = None
if "kline_selected_stock" not in st.session_state:
    st.session_state.kline_selected_stock = None
if "kline_selected_group" not in st.session_state:
    st.session_state.kline_selected_group = None
if "kline_indicators" not in st.session_state:
    st.session_state.kline_indicators = ["MA5", "MA10", "MA20"]

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

if query_btn:
    if start_date > end_date:
        st.error("開始日期不能大於結束日期")
        st.stop()

    if selected_stock is None:
        st.warning("此群組目前沒有可查詢股票。")
        st.stop()

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

    with st.spinner("正在查詢歷史資料..."):
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

result_df = st.session_state.get("kline_result_df", None)
result_stock = st.session_state.get("kline_selected_stock", None)
result_group = st.session_state.get("kline_selected_group", None)
selected_indicators = st.session_state.get("kline_indicators", ["MA5", "MA10", "MA20"])

# 如果還沒查詢過，但首頁已經有套用條件，就先顯示目前條件
if result_stock is None:
    preview_group = st.session_state.get("kline_group", "")
    preview_stock_code = st.session_state.get("kline_stock_code", "")
    preview_items = build_stock_options(watchlist_dict.get(preview_group, []), lookup_date)
    preview_stock = next((x for x in preview_items if x["code"] == preview_stock_code), None)

    if preview_group:
        st.markdown(
            f"""
**目前查詢條件：**  
群組：{preview_group}  
股票：{preview_stock['name']}（{preview_stock['code']}）  \n市場別：{preview_stock['market']}  
日期區間：{st.session_state.kline_start} ~ {st.session_state.kline_end}  
技術指標：{", ".join(selected_indicators) if selected_indicators else "無"}
"""
            if preview_stock else
            f"""
**目前查詢條件：**  
群組：{preview_group}  
股票：尚未匹配  
日期區間：{st.session_state.kline_start} ~ {st.session_state.kline_end}  
技術指標：{", ".join(selected_indicators) if selected_indicators else "無"}
"""
        )
        st.info("首頁條件已帶入；按「開始查詢」即可載入歷史資料。")

if result_df is not None and result_stock is not None:
    st.markdown(
        f"""
**目前查詢條件：**  
群組：{result_group}  
股票：{result_stock['name']}（{result_stock['code']}）  
市場別：{result_stock['market']}  
日期區間：{st.session_state.kline_start} ~ {st.session_state.kline_end}  
技術指標：{", ".join(selected_indicators) if selected_indicators else "無"}
"""
    )

    df = prepare_display_df(result_df, selected_indicators)

    if df.empty:
        st.warning("查無歷史資料。")
        st.stop()

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

    render_summary_metrics(df)
    render_signal_summary(df, selected_indicators)

    st.markdown("---")
    st.subheader("歷史資料")
    render_table(df)
    render_chart(df, selected_indicators)

    st.success(f"查詢完成，共 {len(df)} 筆資料。")
else:
    if not st.session_state.get("kline_group", ""):
        st.info("請先選擇條件後按「開始查詢」。")
