from datetime import date
from io import BytesIO
from pathlib import Path
import json

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import pandas as pd
import requests
import streamlit as st
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(
    page_title="台股自選股 + K線圖升級版",
    page_icon="📈",
    layout="wide"
)

WATCHLIST_FILE = Path("watchlist.json")
DEFAULT_WATCHLIST = ["2330", "2454", "2317"]


# =========================
# 基本工具
# =========================
def to_number(value):
    if value is None:
        return None

    text = str(value).replace(",", "").strip()

    if text in ["", "--", "X", "null", "None"]:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def format_number(value, digits=2):
    if value is None or pd.isna(value):
        return ""
    value = float(value)
    if value.is_integer():
        return f"{int(value):,}"
    return f"{value:,.{digits}f}"


def roc_to_ad(roc_text):
    parts = str(roc_text).strip().split("/")
    if len(parts) != 3:
        return pd.NaT

    try:
        y = int(parts[0]) + 1911
        m = int(parts[1])
        d = int(parts[2])
        return pd.Timestamp(year=y, month=m, day=d)
    except Exception:
        return pd.NaT


def month_range(start_dt: date, end_dt: date):
    months = []
    y = start_dt.year
    m = start_dt.month

    while (y < end_dt.year) or (y == end_dt.year and m <= end_dt.month):
        months.append(f"{y:04d}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    return months


def to_excel_bytes(df_dict):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in df_dict.items():
            safe_name = str(sheet_name)[:31]
            df.to_excel(writer, index=False, sheet_name=safe_name)
    output.seek(0)
    return output.getvalue()


# =========================
# 自選股永久儲存
# =========================
def load_watchlist():
    if not WATCHLIST_FILE.exists():
        save_watchlist(DEFAULT_WATCHLIST)
        return DEFAULT_WATCHLIST.copy()

    try:
        with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
        return DEFAULT_WATCHLIST.copy()
    except Exception:
        return DEFAULT_WATCHLIST.copy()


def save_watchlist(watchlist):
    clean_list = list(dict.fromkeys([str(x).strip() for x in watchlist if str(x).strip()]))

    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(clean_list, f, ensure_ascii=False, indent=2)


if "watchlist" not in st.session_state:
    st.session_state.watchlist = load_watchlist()


# =========================
# 抓股票代碼 / 名稱清單
# =========================
@st.cache_data(show_spinner=False, ttl=3600)
def get_twse_code_name_map(query_date: str) -> pd.DataFrame:
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={query_date}&type=ALL"
    r = requests.get(url, timeout=30, verify=False)
    r.raise_for_status()
    data = r.json()

    tables = data.get("tables", [])
    all_rows = []

    for table in tables:
        fields = table.get("fields", [])
        rows = table.get("data", [])

        if "證券代號" in fields and "證券名稱" in fields:
            for row in rows:
                if len(row) != len(fields):
                    continue
                record = dict(zip(fields, row))
                stock_no = str(record.get("證券代號", "")).strip()
                stock_name = str(record.get("證券名稱", "")).strip()
                if stock_no and stock_name:
                    all_rows.append({
                        "證券代號": stock_no,
                        "證券名稱": stock_name
                    })

    if not all_rows:
        return pd.DataFrame(columns=["證券代號", "證券名稱"])

    df = pd.DataFrame(all_rows).drop_duplicates().reset_index(drop=True)
    return df


# =========================
# 抓個股歷史資料
# =========================
@st.cache_data(show_spinner=False, ttl=3600)
def get_month_stock_data(stock_no: str, yyyy_mm: str) -> pd.DataFrame:
    url = (
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        f"?response=json&date={yyyy_mm}01&stockNo={stock_no}"
    )

    r = requests.get(url, timeout=30, verify=False)
    r.raise_for_status()
    data = r.json()

    if data.get("stat") != "OK":
        return pd.DataFrame()

    fields = data.get("fields", [])
    rows = data.get("data", [])

    if not fields or not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=fields)

    if "日期" in df.columns:
        df["日期"] = df["日期"].apply(roc_to_ad)

    numeric_cols = [
        "成交股數", "成交金額", "開盤價", "最高價",
        "最低價", "收盤價", "漲跌價差", "成交筆數"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(to_number)

    return df


def get_history_data(stock_no: str, stock_name: str, start_dt: date, end_dt: date) -> pd.DataFrame:
    months = month_range(start_dt, end_dt)
    all_df = []

    for ym in months:
        df = get_month_stock_data(stock_no, ym)
        if not df.empty:
            df["證券代號"] = stock_no
            df["證券名稱"] = stock_name
            all_df.append(df)

    if not all_df:
        return pd.DataFrame()

    result = pd.concat(all_df, ignore_index=True)
    result = result.dropna(subset=["日期"])
    result = result[(result["日期"].dt.date >= start_dt) & (result["日期"].dt.date <= end_dt)]
    result = result.sort_values("日期").reset_index(drop=True)

    return result


def build_summary_df(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame()

    group_cols = ["證券代號", "證券名稱"]

    def get_last_valid(series):
        s = series.dropna()
        return s.iloc[-1] if not s.empty else None

    summary = (
        history_df.sort_values("日期")
        .groupby(group_cols, as_index=False)
        .agg(
            開始日期=("日期", "min"),
            結束日期=("日期", "max"),
            最新收盤價=("收盤價", get_last_valid),
            區間最高價=("最高價", "max"),
            區間最低價=("最低價", "min"),
            區間成交金額合計=("成交金額", "sum"),
            交易天數=("日期", "count"),
        )
    )

    return summary


def format_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    show_df = df.copy()

    for col in ["最新收盤價", "區間最高價", "區間最低價", "區間成交金額合計"]:
        if col in show_df.columns:
            show_df[col] = show_df[col].apply(format_number)

    for col in ["開始日期", "結束日期"]:
        if col in show_df.columns:
            show_df[col] = pd.to_datetime(show_df[col]).dt.strftime("%Y-%m-%d")

    return show_df


def format_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    show_df = df.copy()

    if "日期" in show_df.columns:
        show_df["日期"] = pd.to_datetime(show_df["日期"]).dt.strftime("%Y-%m-%d")

    numeric_cols = ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌價差", "成交筆數"]
    for col in numeric_cols:
        if col in show_df.columns:
            show_df[col] = show_df[col].apply(format_number)

    return show_df


# =========================
# 畫圖
# =========================
def draw_price_chart(history_df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(12, 5))

    for stock_no, sub_df in history_df.groupby("證券代號"):
        sub_df = sub_df.sort_values("日期")
        stock_name = sub_df["證券名稱"].iloc[0]
        ax.plot(sub_df["日期"], sub_df["收盤價"], label=f"{stock_no} {stock_name}")

    ax.set_xlabel("日期")
    ax.set_ylabel("收盤價")
    ax.set_title("自選股收盤價走勢")
    ax.grid(True)
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    st.pyplot(fig)


def draw_candlestick_chart(df: pd.DataFrame, title: str):
    chart_df = df.dropna(subset=["日期", "開盤價", "最高價", "最低價", "收盤價"]).copy()

    if chart_df.empty:
        st.warning("此股票沒有足夠的 K 線資料。")
        return

    chart_df = chart_df.sort_values("日期").reset_index(drop=True)
    chart_df["MA5"] = chart_df["收盤價"].rolling(5).mean()
    chart_df["MA20"] = chart_df["收盤價"].rolling(20).mean()
    chart_df["x"] = mdates.date2num(chart_df["日期"])

    fig, (ax1, ax2) = plt.subplots(
        2, 1,
        figsize=(12, 7),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1]}
    )

    candle_width = 0.6

    for _, row in chart_df.iterrows():
        x = row["x"]
        open_price = row["開盤價"]
        high_price = row["最高價"]
        low_price = row["最低價"]
        close_price = row["收盤價"]
        volume = row["成交股數"] if pd.notna(row["成交股數"]) else 0

        is_up = close_price >= open_price
        line_color = "red" if is_up else "green"
        face_color = "none" if is_up else "green"

        ax1.plot([x, x], [low_price, high_price], color=line_color, linewidth=1)

        lower = min(open_price, close_price)
        height = abs(close_price - open_price)
        if height == 0:
            height = 0.01

        rect = Rectangle(
            (x - candle_width / 2, lower),
            candle_width,
            height,
            edgecolor=line_color,
            facecolor=face_color,
            linewidth=1.2
        )
        ax1.add_patch(rect)

        ax2.bar(x, volume, width=candle_width, color=line_color, alpha=0.6)

    ax1.plot(chart_df["日期"], chart_df["MA5"], label="MA5", linestyle="--")
    ax1.plot(chart_df["日期"], chart_df["MA20"], label="MA20", linestyle="-.")

    ax1.set_title(title)
    ax1.set_ylabel("價格")
    ax1.grid(True)
    ax1.legend()

    ax2.set_ylabel("成交股數")
    ax2.grid(True)

    ax2.xaxis_date()
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.xticks(rotation=45)
    plt.tight_layout()

    st.pyplot(fig)


# =========================
# 主畫面
# =========================
st.title("📈 台股自選股 + K線圖升級版")
st.caption("支援自選股永久儲存、模糊搜尋、K線圖、均線、成交量。")

c1, c2, c3 = st.columns([1.6, 1, 1])

with c2:
    start_dt = st.date_input("開始日期", value=date(2026, 4, 1))

with c3:
    end_dt = st.date_input("結束日期", value=date(2026, 4, 30))

if start_dt > end_dt:
    st.error("開始日期不能大於結束日期。")
    st.stop()

if (end_dt - start_dt).days > 370:
    st.error("建議先查 1 年內資料，避免一次抓太多。")
    st.stop()

lookup_date = end_dt.strftime("%Y%m%d")
code_name_df = get_twse_code_name_map(lookup_date)

if code_name_df.empty:
    st.error("無法取得股票清單。")
    st.stop()

code_name_df["顯示"] = code_name_df["證券名稱"] + " (" + code_name_df["證券代號"] + ")"

with c1:
    st.markdown("### 自選股設定")

    search_text = st.text_input("🔍 搜尋股票（輸入名稱或代碼）", "")

    filtered_df = code_name_df.copy()
    if search_text.strip():
        filtered_df = filtered_df[
            filtered_df["證券名稱"].str.contains(search_text, na=False) |
            filtered_df["證券代號"].str.contains(search_text, na=False)
        ]

    filtered_df = filtered_df.head(50)

    selected_to_add = st.multiselect(
        "搜尋結果",
        options=filtered_df["顯示"].tolist()
    )

    if st.button("加入自選股"):
        add_codes = [item.split("(")[-1].replace(")", "").strip() for item in selected_to_add]
        merged = list(dict.fromkeys(st.session_state.watchlist + add_codes))
        st.session_state.watchlist = merged
        save_watchlist(st.session_state.watchlist)
        st.success("已加入自選股")

st.markdown("### 目前自選股")

watchlist_df = code_name_df[code_name_df["證券代號"].isin(st.session_state.watchlist)].copy()

if watchlist_df.empty:
    st.warning("目前沒有自選股，請先從上方搜尋加入。")
    st.stop()

watchlist_df["顯示"] = watchlist_df["證券名稱"] + " (" + watchlist_df["證券代號"] + ")"

selected_watchlist = st.multiselect(
    "要查詢的自選股",
    options=watchlist_df["顯示"].tolist(),
    default=watchlist_df["顯示"].tolist()
)

remove_targets = st.multiselect(
    "要從自選股移除的股票",
    options=watchlist_df["顯示"].tolist()
)

r1, r2, r3 = st.columns([1, 1, 4])

with r1:
    if st.button("移除選取股票"):
        remove_codes = [item.split("(")[-1].replace(")", "").strip() for item in remove_targets]
        st.session_state.watchlist = [x for x in st.session_state.watchlist if x not in remove_codes]
        save_watchlist(st.session_state.watchlist)
        st.success("已移除選取股票")
        st.rerun()

with r2:
    if st.button("重設自選股"):
        st.session_state.watchlist = DEFAULT_WATCHLIST.copy()
        save_watchlist(st.session_state.watchlist)
        st.success("已重設為預設自選股")
        st.rerun()

with r3:
    query_btn = st.button("開始查詢", use_container_width=True)

if not query_btn:
    st.info("請選擇要查詢的自選股後，按「開始查詢」。")
    st.stop()

selected_codes = [item.split("(")[-1].replace(")", "").strip() for item in selected_watchlist]

if not selected_codes:
    st.error("請至少選擇一支自選股。")
    st.stop()

selected_info_df = code_name_df[code_name_df["證券代號"].isin(selected_codes)][["證券代號", "證券名稱"]].drop_duplicates()

all_history = []

with st.spinner("正在抓取歷史資料..."):
    for _, row in selected_info_df.iterrows():
        stock_no = row["證券代號"]
        stock_name = row["證券名稱"]
        hist_df = get_history_data(stock_no, stock_name, start_dt, end_dt)
        if not hist_df.empty:
            all_history.append(hist_df)

if not all_history:
    st.warning("查無歷史資料。")
    st.stop()

history_df = pd.concat(all_history, ignore_index=True)
history_df = history_df.sort_values(["證券代號", "日期"]).reset_index(drop=True)

summary_df = build_summary_df(history_df)

st.subheader("摘要結果")
st.dataframe(format_summary_df(summary_df), use_container_width=True, hide_index=True)

st.subheader("收盤價走勢圖")
draw_price_chart(history_df)

st.subheader("K 線圖")

kline_options_df = selected_info_df.copy()
kline_options_df["顯示"] = kline_options_df["證券名稱"] + " (" + kline_options_df["證券代號"] + ")"

selected_kline = st.selectbox(
    "選擇要顯示 K 線圖的股票",
    options=kline_options_df["顯示"].tolist()
)

kline_code = selected_kline.split("(")[-1].replace(")", "").strip()
kline_name = kline_options_df[kline_options_df["證券代號"] == kline_code]["證券名稱"].iloc[0]

kline_df = history_df[history_df["證券代號"] == kline_code].copy()
draw_candlestick_chart(kline_df, f"{kline_code} {kline_name} K線圖（含 MA5 / MA20 / 成交量）")

st.subheader("歷史明細資料")
st.dataframe(format_history_df(history_df), use_container_width=True, hide_index=True)

excel_bytes = to_excel_bytes({
    "摘要結果": summary_df,
    "歷史明細": history_df,
    "自選股": selected_info_df
})

st.download_button(
    label="下載 Excel",
    data=excel_bytes,
    file_name=f"watchlist_stock_{start_dt}_{end_dt}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)