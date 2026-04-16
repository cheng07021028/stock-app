from datetime import date
import pandas as pd
import streamlit as st

from utils import (
    get_normalized_watchlist,
    get_realtime_watchlist_df,
    apply_font_scale,
    get_font_scale,
    inject_pro_theme,
    render_pro_hero,
    render_pro_section,
    render_pro_kpi_row,
)


def render_rank_table(df: pd.DataFrame, height: int = 760):
    if df is None or df.empty:
        st.info("目前沒有資料。")
        return

    show_cols = [
        "群組", "股票代號", "股票名稱", "市場別",
        "現價", "漲跌", "漲跌幅(%)",
        "開盤", "最高", "最低", "總量", "更新時間"
    ]
    show_cols = [c for c in show_cols if c in df.columns]
    display_df = df[show_cols].copy()

    format_dict = {}
    for col in ["現價", "漲跌", "漲跌幅(%)", "開盤", "最高", "最低"]:
        if col in display_df.columns:
            format_dict[col] = "{:,.2f}"
    if "總量" in display_df.columns:
        format_dict["總量"] = "{:,.0f}"

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
    if "漲跌幅(%)" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌幅(%)"])

    st.dataframe(styler, use_container_width=True, hide_index=True, height=height)


st.set_page_config(page_title="排行榜", page_icon="🏆", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)
inject_pro_theme()

render_pro_hero(
    "排行榜",
    "強弱排序終端｜以漲跌幅、漲跌值、成交量與現價快速篩出市場焦點"
)

watchlist_dict = get_normalized_watchlist()
if not watchlist_dict:
    st.warning("目前沒有自選股群組。")
    st.stop()

query_date = date.today().strftime("%Y%m%d")

sort_col = st.selectbox(
    "排序方式",
    ["漲跌幅(%)", "漲跌", "總量", "現價"],
    index=0
)

ascending = st.toggle("升冪排序", value=False)

if st.button("更新排行榜", type="primary", use_container_width=True):
    get_realtime_watchlist_df.clear()

with st.spinner("正在讀取排行榜資料..."):
    realtime_df = get_realtime_watchlist_df(watchlist_dict, query_date)

if realtime_df.empty:
    st.info("目前沒有資料。")
    st.stop()

rank_df = realtime_df.copy()
if sort_col in rank_df.columns:
    rank_df = rank_df.sort_values(by=sort_col, ascending=ascending, na_position="last").reset_index(drop=True)

top_name = "—"
top_metric = "—"
if not rank_df.empty and sort_col in rank_df.columns:
    top_row = rank_df.iloc[0]
    top_name = f"{top_row.get('股票名稱', '—')}（{top_row.get('股票代號', '—')}）"
    value = top_row.get(sort_col)
    if pd.notna(value):
        if sort_col == "總量":
            top_metric = f"{value:,.0f}"
        else:
            top_metric = f"{value:,.2f}"

render_pro_kpi_row([
    {"label": "排序欄位", "value": sort_col, "delta": "Ranking Factor", "delta_class": "pro-kpi-delta-flat"},
    {"label": "排序方向", "value": "升冪" if ascending else "降冪", "delta": "Sort Direction", "delta_class": "pro-kpi-delta-flat"},
    {"label": "榜首標的", "value": top_name, "delta": f"{sort_col}：{top_metric}", "delta_class": "pro-kpi-delta-flat"},
])

render_pro_section("排行結果", "適合快速找出最強、最弱、最有量的標的，做進一步聚焦")
render_rank_table(rank_df, height=780)
