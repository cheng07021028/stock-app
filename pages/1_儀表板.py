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
    format_number,
)


def render_dashboard_summary(df: pd.DataFrame):
    if df is None or df.empty:
        return

    total_count = len(df)
    up_count = int((df["漲跌"] > 0).sum()) if "漲跌" in df.columns else 0
    down_count = int((df["漲跌"] < 0).sum()) if "漲跌" in df.columns else 0
    flat_count = total_count - up_count - down_count

    avg_change_pct = df["漲跌幅(%)"].mean() if "漲跌幅(%)" in df.columns else None
    total_volume = df["總量"].sum() if "總量" in df.columns else None

    avg_text = f"{avg_change_pct:+.2f}%" if avg_change_pct is not None and pd.notna(avg_change_pct) else "—"
    avg_class = "pro-kpi-delta-up" if avg_change_pct and avg_change_pct > 0 else "pro-kpi-delta-down" if avg_change_pct and avg_change_pct < 0 else "pro-kpi-delta-flat"

    render_pro_kpi_row([
        {"label": "監控股票數", "value": f"{total_count:,}", "delta": f"上漲 {up_count}｜下跌 {down_count}｜平盤 {flat_count}", "delta_class": "pro-kpi-delta-flat"},
        {"label": "平均漲跌幅", "value": avg_text, "delta": "Portfolio Breadth", "delta_class": avg_class},
        {"label": "合計成交量", "value": format_number(total_volume, 0), "delta": "Total Session Volume", "delta_class": "pro-kpi-delta-flat"},
    ])


def render_dashboard_table(df: pd.DataFrame, height: int = 720):
    if df is None or df.empty:
        st.info("目前沒有可顯示的即時資料。")
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


st.set_page_config(page_title="儀表板", page_icon="📊", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)
inject_pro_theme()

render_pro_hero(
    "盤面儀表板",
    "專業監控視圖｜集中觀察自選股強弱、即時波動與成交量分布"
)

watchlist_dict = get_normalized_watchlist()
if not watchlist_dict:
    st.warning("目前沒有自選股群組。")
    st.stop()

query_date = date.today().strftime("%Y%m%d")

if st.button("更新即時資料", type="primary", use_container_width=True):
    get_realtime_watchlist_df.clear()

with st.spinner("正在讀取即時資料..."):
    realtime_df = get_realtime_watchlist_df(watchlist_dict, query_date)

if realtime_df.empty:
    st.info("目前沒有可顯示的即時資料。")
else:
    render_dashboard_summary(realtime_df)
    render_pro_section("即時盤面清單", "可快速掃描各群組成員的價格、漲跌幅與成交量變化")
    render_dashboard_table(realtime_df, height=760)
