from datetime import date, datetime, timedelta
import io
import json
import os
import time

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import streamlit as st
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="歷史K線分析", page_icon="📊", layout="wide")

WATCHLIST_CANDIDATES = [
    "watchlist.json",
    "watchlists.json",
    "data/watchlist.json",
    "data/watchlists.json",
]

STATE_FILE = "last_query_state.json"


def load_watchlist():
    for path in WATCHLIST_CANDIDATES:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict):
                    normalized = {}
                    for group_name, items in data.items():
                        group_name = str(group_name).strip()
                        if not group_name:
                            continue

                        normalized[group_name] = []
                        if isinstance(items, list):
                            for item in items:
                                if isinstance(item, dict):
                                    code = str(item.get("code", "")).strip()
                                    name = str(item.get("name", "")).strip()
                                    market = str(item.get("market", "")).strip() or "上市"
                                    if code:
                                        normalized[group_name].append({
                                            "code": code,
                                            "name": name if name else code,
                                            "market": market,
                                        })
                    return normalized
            except Exception:
                pass
    return {}


def load_query_state():
    default_state = {
        "quick_group": "",
        "quick_stock_code": "",
        "home_start": "",
        "home_end": "",
    }

    if not os.path.exists(STATE_FILE):
        return default_state

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for k, v in default_state.items():
            if k not in data:
                data[k] = v
        return data
    except Exception:
        return default_state


def save_query_state(quick_group="", quick_stock_code="", home_start=None, home_end=None):
    data = {
        "quick_group": quick_group or "",
        "quick_stock_code": quick_stock_code or "",
        "home_start": str(home_start) if home_start else "",
        "home_end": str(home_end) if home_end else "",
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def parse_date_safe(value, fallback):
    if not value:
        return fallback
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except Exception:
        return fallback


def safe_text(v):
    if v is None:
        return ""
    t = str(v).strip()
    if t in ["", "-", "--", "—", "null", "None"]:
        return ""
    return t


def safe_num(v):
    t = safe_text(v).replace(",", "")
    if not t:
        return None
    try:
        return float(t)
    except Exception:
        return None


def fmt_num(v, digits=2):
    if v is None or pd.isna(v):
        return "—"
    try:
        if digits == 0:
            return f"{float(v):,.0f}"
        return f"{float(v):,.{digits}f}"
    except Exception:
        return "—"


def market_prefix(market_type: str):
    return "otc" if str(market_type).strip() == "上櫃" else "tse"


@st.cache_data(ttl=15, show_spinner=False)
def get_realtime_stock_info(stock_no: str, stock_name: str = "", market_type: str = "上市") -> dict:
    stock_no = str(stock_no).strip()
    stock_name = str(stock_name).strip()
    market_type = str(market_type).strip() or "上市"

    if not stock_no:
        return {"ok": False, "message": "股票代號為空白"}

    ex_ch = f"{market_prefix(market_type)}_{stock_no}.tw"
    url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://mis.twse.com.tw/stock/",
        "Accept": "application/json,text/plain,*/*",
    }
    params = {
        "ex_ch": ex_ch,
        "json": "1",
        "delay": "0",
        "_": str(int(time.time() * 1000)),
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=20, verify=False)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"ok": False, "message": f"即時資料取得失敗：{e}"}

    msg_array = data.get("msgArray", [])
    if not msg_array:
        return {"ok": False, "message": "查無即時資料"}

    raw = msg_array[0]

    code = safe_text(raw.get("c")) or stock_no
    name = safe_text(raw.get("n")) or stock_name or stock_no
    prev_close = safe_num(raw.get("y"))
    price = safe_num(raw.get("z"))
    if price is None:
        price = prev_close

    open_price = safe_num(raw.get("o"))
    high_price = safe_num(raw.get("h"))
    low_price = safe_num(raw.get("l"))
    total_volume = safe_num(raw.get("v"))

    change = None
    change_pct = None
    if price is not None and prev_close not in [None, 0]:
        change = price - prev_close
        change_pct = change / prev_close * 100

    update_date = safe_text(raw.get("d"))
    update_time = safe_text(raw.get("t"))
    update_text = f"{update_date} {update_time}".strip() if update_date or update_time else "—"

    return {
        "ok": True,
        "code": code,
        "name": name,
        "market": market_type,
        "price": price,
        "prev_close": prev_close,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "change": change,
        "change_pct": change_pct,
        "total_volume": total_volume,
        "update_time": update_text,
    }


def render_realtime_info_card(info: dict, title="今日即時資訊"):
    st.markdown(f"### {title}")

    if not info or not info.get("ok"):
        st.info(info.get("message", "目前沒有即時資訊。") if isinstance(info, dict) else "目前沒有即時資訊。")
        return

    st.caption(
        f"{info.get('name', '—')}（{info.get('code', '—')}）｜"
        f"{info.get('market', '—')}｜更新時間：{info.get('update_time', '—')}"
    )

    delta_text = None
    if info.get("change") is not None and info.get("change_pct") is not None:
        delta_text = f"{info['change']:+.2f} ({info['change_pct']:+.2f}%)"
    elif info.get("change") is not None:
        delta_text = f"{info['change']:+.2f}"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("現價", fmt_num(info.get("price"), 2), delta=delta_text)
    with c2:
        st.metric("開盤", fmt_num(info.get("open"), 2))
    with c3:
        st.metric("最高", fmt_num(info.get("high"), 2))
    with c4:
        st.metric("最低", fmt_num(info.get("low"), 2))

    c5, c6 = st.columns(2)
    with c5:
        st.metric("總量", fmt_num(info.get("total_volume"), 0))
    with c6:
        st.metric("昨收", fmt_num(info.get("prev_close"), 2))


def download_excel_bytes(df_dict):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in df_dict.items():
            safe_sheet_name = str(sheet_name)[:31]
            df.to_excel(writer, index=False, sheet_name=safe_sheet_name)
    output.seek(0)
    return output.getvalue()


@st.cache_data(ttl=1800, show_spinner=False)
def get_history_data(stock_no: str, market_type: str, start_date, end_date) -> pd.DataFrame:
    stock_no = str(stock_no).strip()
    market_type = str(market_type).strip() or "上市"

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    if end_date < start_date:
        return pd.DataFrame()

    month_starts = pd.date_range(start=start_date.replace(day=1), end=end_date, freq="MS")

    frames = []

    for dt in month_starts:
        if market_type == "上櫃":
            url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
            try:
                r = requests.get(url, timeout=30, verify=False)
                r.raise_for_status()
                data = r.json()
                df_month = pd.DataFrame(data)

                if df_month.empty:
                    continue

                if "SecuritiesCompanyCode" in df_month.columns:
                    df_month = df_month[df_month["SecuritiesCompanyCode"].astype(str) == stock_no]

                rename_map = {
                    "Date": "日期",
                    "Open": "開盤價",
                    "High": "最高價",
                    "Low": "最低價",
                    "Close": "收盤價",
                    "TradeVolume": "成交股數",
                    "TransactionAmount": "成交金額",
                    "TransactionNumber": "成交筆數",
                }
                for old_col, new_col in rename_map.items():
                    if old_col in df_month.columns:
                        df_month = df_month.rename(columns={old_col: new_col})

                if "日期" not in df_month.columns:
                    continue

            except Exception:
                continue
        else:
            month_str = dt.strftime("%Y%m01")
            url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
            params = {
                "response": "json",
                "date": month_str,
                "stockNo": stock_no,
            }

            try:
                r = requests.get(url, params=params, timeout=30, verify=False)
                r.raise_for_status()
                data = r.json()

                if data.get("stat") != "OK":
                    continue

                rows = data.get("data", [])
                cols = data.get("fields", [])

                if not rows or not cols:
                    continue

                df_month = pd.DataFrame(rows, columns=cols)

                rename_map = {
                    "日期": "日期",
                    "成交股數": "成交股數",
                    "成交金額": "成交金額",
                    "開盤價": "開盤價",
                    "最高價": "最高價",
                    "最低價": "最低價",
                    "收盤價": "收盤價",
                    "成交筆數": "成交筆數",
                }
                df_month = df_month.rename(columns=rename_map)

            except Exception:
                continue

        if "日期" not in df_month.columns:
            continue

        frames.append(df_month)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    def convert_tw_date(x):
        x = safe_text(x)
        if not x:
            return pd.NaT

        if "/" in x:
            parts = x.split("/")
            if len(parts) == 3:
                try:
                    year = int(parts[0]) + 1911
                    month = int(parts[1])
                    day = int(parts[2])
                    return pd.Timestamp(year=year, month=month, day=day)
                except Exception:
                    return pd.NaT

        try:
            return pd.to_datetime(x)
        except Exception:
            return pd.NaT

    df["日期"] = df["日期"].apply(convert_tw_date)
    df = df.dropna(subset=["日期"])

    for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", "", regex=False)
                .replace(["--", "---", ""], pd.NA)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[(df["日期"] >= start_date) & (df["日期"] <= end_date)]
    df = df.sort_values("日期").reset_index(drop=True)

    return df


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

    if "MACD" in selected_indicators:
        ema12 = df["收盤價"].ewm(span=12, adjust=False).mean()
        ema26 = df["收盤價"].ewm(span=26, adjust=False).mean()
        df["DIF"] = ema12 - ema26
        df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
        df["MACD_HIST"] = df["DIF"] - df["DEA"]

    return df


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
            return "color: #d32f2f; font-weight: 700;"
        elif v < 0:
            return "color: #00897b; font-weight: 700;"
        return "color: #666;"

    styler = display_df.style.format(format_dict, na_rep="—")
    if "漲跌" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌"])

    st.dataframe(styler, use_container_width=True, hide_index=True, height=680)


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
            ),
            row=1,
            col=1
        )

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
            )
        )

        st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

    if "KD" in selected_indicators and all(col in df.columns for col in ["日期", "K", "D"]):
        with st.expander("查看 KD 指標", expanded=False):
            kd_fig = go.Figure()
            kd_fig.add_trace(go.Scatter(x=df["日期"], y=df["K"], mode="lines", name="K"))
            kd_fig.add_trace(go.Scatter(x=df["日期"], y=df["D"], mode="lines", name="D"))
            kd_fig.update_layout(height=320, hovermode="x unified")
            st.plotly_chart(kd_fig, use_container_width=True, config={"displaylogo": False})

    if "MACD" in selected_indicators and all(col in df.columns for col in ["日期", "DIF", "DEA", "MACD_HIST"]):
        with st.expander("查看 MACD 指標", expanded=False):
            macd_fig = go.Figure()
            macd_fig.add_trace(go.Bar(x=df["日期"], y=df["MACD_HIST"], name="MACD柱"))
            macd_fig.add_trace(go.Scatter(x=df["日期"], y=df["DIF"], mode="lines", name="DIF"))
            macd_fig.add_trace(go.Scatter(x=df["日期"], y=df["DEA"], mode="lines", name="DEA"))
            macd_fig.update_layout(height=360, hovermode="x unified")
            st.plotly_chart(macd_fig, use_container_width=True, config={"displaylogo": False})


st.title("📊 歷史K線分析")
st.caption("救援版｜先恢復歷史資料查詢")

watchlist_dict = load_watchlist()
group_names = list(watchlist_dict.keys())

if not group_names:
    st.warning("目前沒有讀到自選股清單。請確認 watchlist.json 是否存在。")
    st.stop()

last_state = load_query_state()
today_dt = date.today()

default_group = last_state.get("quick_group", "")
default_code = last_state.get("quick_stock_code", "")
default_start = parse_date_safe(last_state.get("home_start", ""), today_dt - timedelta(days=90))
default_end = parse_date_safe(last_state.get("home_end", ""), today_dt)

group_index = group_names.index(default_group) if default_group in group_names else 0

with st.form("history_query_form", clear_on_submit=False):
    c1, c2 = st.columns(2)

    with c1:
        selected_group = st.selectbox("選擇群組", group_names, index=group_index)

    stock_items = watchlist_dict.get(selected_group, [])
    stock_options = []
    for item in stock_items:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip() or code
        market = str(item.get("market", "")).strip() or "上市"
        if code:
            stock_options.append({
                "label": f"{name} ({code}) [{market}]",
                "code": code,
                "name": name,
                "market": market,
            })

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
        default=["MA5", "MA10", "MA20"]
    )

    query_btn = st.form_submit_button("開始查詢", type="primary", use_container_width=True)

if not selected_stock:
    st.warning("此群組目前沒有可查詢股票。")
    st.stop()

if start_date > end_date:
    st.error("開始日期不能大於結束日期")
    st.stop()

if query_btn or selected_stock:
    save_query_state(
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
    render_realtime_info_card(realtime_info, title="今日即時資訊")

    with st.spinner("正在查詢歷史資料..."):
        df = get_history_data(
            selected_stock["code"],
            selected_stock["market"],
            start_date,
            end_date
        )

    if df.empty:
        st.warning("查無歷史資料。")
        st.stop()

    df = add_indicators(df, selected_indicators)

    st.markdown(
        f"""
**目前查詢條件：**  
群組：{selected_group}  
股票：{selected_stock['name']}（{selected_stock['code']}）  
市場別：{selected_stock['market']}  
日期區間：{start_date} ~ {end_date}  
技術指標：{", ".join(selected_indicators) if selected_indicators else "無"}
"""
    )

    download_df = df.copy()
    if "日期" in download_df.columns:
        download_df["日期"] = pd.to_datetime(download_df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")

    excel_bytes = download_excel_bytes({"歷史K線資料": download_df})

    d1, d2 = st.columns([1, 5])
    with d1:
        st.download_button(
            label="下載 Excel",
            data=excel_bytes,
            file_name=f"{selected_stock['code']}_{selected_stock['name']}_歷史K線分析.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    st.markdown("---")
    st.subheader("歷史資料")
    render_table(df)
    render_chart(df, selected_indicators)

    st.success(f"查詢完成，共 {len(df)} 筆資料。")
