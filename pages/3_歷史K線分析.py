from datetime import date
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import matplotlib as mpl
import streamlit as st
import pandas as pd

from utils import (
    get_all_code_name_map,
    get_history_data,
    format_number,
    load_watchlist,
    build_watchlist_df,
    apply_font_scale,
    get_font_scale,
)

mpl.rcParams["font.sans-serif"] = ["Microsoft JhengHei", "SimHei", "Arial Unicode MS", "DejaVu Sans"]
mpl.rcParams["axes.unicode_minus"] = False

st.set_page_config(page_title="歷史K線分析", page_icon="📊", layout="wide")

with st.sidebar:
    st.markdown("## 顯示設定")
    st.session_state.font_scale = st.slider("字體大小 (%)", 80, 160, get_font_scale(), 5)

apply_font_scale(st.session_state.font_scale)

st.title("📊 歷史 K 線分析")

today_dt = date.today()

c1, c2, c3 = st.columns([2, 1, 1])
with c2:
    start_dt = st.date_input("開始日期", value=date(today_dt.year, today_dt.month, 1))
with c3:
    end_dt = st.date_input("結束日期", value=today_dt, max_value=today_dt)

if start_dt > end_dt:
    st.error("開始日期不能大於結束日期")
    st.stop()

lookup_date = end_dt.strftime("%Y%m%d")
all_code_name_df = get_all_code_name_map(lookup_date)

if all_code_name_df.empty:
    st.error("無法取得股票清單")
    st.stop()

watchlist_dict = load_watchlist()
watchlist_df = build_watchlist_df(all_code_name_df, watchlist_dict)

if watchlist_df.empty:
    st.warning("目前沒有自選股，請先到自選股中心建立")
    st.stop()

with c1:
    selected_group = st.selectbox("選擇群組", options=watchlist_df["群組"].drop_duplicates().tolist())

group_df = watchlist_df[watchlist_df["群組"] == selected_group].copy()

selected = st.selectbox("選擇股票（僅顯示自選股）", options=group_df["顯示"].tolist())

stock_code = selected.split("(")[-1].split(")")[0].strip()
stock_row = group_df[group_df["證券代號"] == stock_code].iloc[0]
stock_name = stock_row["證券名稱"]
market_type = stock_row["市場別"]

hist_df = get_history_data(stock_code, stock_name, market_type, start_dt, end_dt)

if hist_df.empty:
    st.warning("查無歷史資料")
    st.stop()

chart_df = hist_df.dropna(subset=["日期", "開盤價", "最高價", "最低價", "收盤價"]).copy()
chart_df = chart_df.sort_values("日期").reset_index(drop=True)
chart_df["MA5"] = chart_df["收盤價"].rolling(5).mean()
chart_df["MA20"] = chart_df["收盤價"].rolling(20).mean()
chart_df["x"] = mdates.date2num(chart_df["日期"])

latest = chart_df.iloc[-1]
m1, m2, m3, m4 = st.columns(4)
with m1:
    st.metric("最新收盤價", format_number(latest["收盤價"]))
with m2:
    st.metric("最新開盤價", format_number(latest["開盤價"]))
with m3:
    st.metric("區間最高價", format_number(chart_df["最高價"].max()))
with m4:
    st.metric("區間最低價", format_number(chart_df["最低價"].min()))

fig, (ax1, ax2) = plt.subplots(
    2, 1,
    figsize=(14, 8),
    dpi=120,
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
    edge_color = "red" if is_up else "green"
    fill_color = "#ffdddd" if is_up else "#c6f6c6"

    ax1.plot([x, x], [low_price, high_price], color=edge_color, linewidth=1.3)

    lower = min(open_price, close_price)
    height = abs(close_price - open_price)
    if height == 0:
        height = 0.3

    rect = Rectangle(
        (x - candle_width / 2, lower),
        candle_width,
        height,
        edgecolor=edge_color,
        facecolor=fill_color,
        linewidth=1.4
    )
    ax1.add_patch(rect)
    ax2.bar(x, volume, width=candle_width, color=edge_color, alpha=0.65)

ax1.plot(chart_df["日期"], chart_df["MA5"], label="MA5", linestyle="--", linewidth=2)
ax1.plot(chart_df["日期"], chart_df["MA20"], label="MA20", linestyle="-.", linewidth=2)

ax1.set_title(f"{stock_code} {stock_name} [{market_type}] K線圖", fontsize=18, fontweight="bold")
ax1.set_ylabel("股價", fontsize=12)
ax1.grid(True, linestyle="--", alpha=0.4)
ax1.legend()

ax2.set_title("成交量", fontsize=13)
ax2.set_xlabel("日期", fontsize=12)
ax2.set_ylabel("成交股數", fontsize=12)
ax2.grid(True, linestyle="--", alpha=0.4)
ax2.xaxis_date()
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
plt.xticks(rotation=45)
plt.tight_layout()

st.pyplot(fig)

show_df = chart_df[["日期", "開盤價", "最高價", "最低價", "收盤價", "成交股數", "MA5", "MA20"]].copy()
show_df["日期"] = show_df["日期"].dt.strftime("%Y-%m-%d")
for col in ["開盤價", "最高價", "最低價", "收盤價", "成交股數", "MA5", "MA20"]:
    show_df[col] = show_df[col].apply(format_number)

st.markdown("### K線明細")
st.dataframe(show_df, use_container_width=True, hide_index=True)