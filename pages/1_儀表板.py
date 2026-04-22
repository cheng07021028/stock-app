from datetime import date
import time
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

PAGE_TITLE = "儀表板"
PFX = "dash_"


def _k(key: str) -> str:
    return f"{PFX}{key}"


def _safe_float(v, default=None):
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default


@st.cache_data(ttl=5, show_spinner=False)
def _prepare_dashboard_metrics(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "total_count": 0,
            "up_count": 0,
            "down_count": 0,
            "flat_count": 0,
            "avg_change_pct": None,
            "total_volume": None,
        }

    work = df.copy()

    if "漲跌" in work.columns:
        work["漲跌"] = pd.to_numeric(work["漲跌"], errors="coerce")
    if "漲跌幅(%)" in work.columns:
        work["漲跌幅(%)"] = pd.to_numeric(work["漲跌幅(%)"], errors="coerce")
    if "總量" in work.columns:
        work["總量"] = pd.to_numeric(work["總量"], errors="coerce")

    total_count = len(work)
    up_count = int((work["漲跌"] > 0).sum()) if "漲跌" in work.columns else 0
    down_count = int((work["漲跌"] < 0).sum()) if "漲跌" in work.columns else 0
    flat_count = max(0, total_count - up_count - down_count)

    avg_change_pct = work["漲跌幅(%)"].mean() if "漲跌幅(%)" in work.columns else None
    total_volume = work["總量"].sum() if "總量" in work.columns else None

    return {
        "total_count": total_count,
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "avg_change_pct": avg_change_pct,
        "total_volume": total_volume,
    }


@st.cache_data(ttl=5, show_spinner=False)
def _prepare_dashboard_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    show_cols = [
        "群組", "股票代號", "股票名稱", "市場別",
        "現價", "昨收", "漲跌", "漲跌幅(%)",
        "開盤", "最高", "最低", "總量",
        "價格來源", "漲跌來源", "更新時間"
    ]
    show_cols = [c for c in show_cols if c in df.columns]

    display_df = df[show_cols].copy()

    numeric_cols = ["現價", "昨收", "漲跌", "漲跌幅(%)", "開盤", "最高", "最低", "總量"]
    exist_numeric_cols = [c for c in numeric_cols if c in display_df.columns]
    if exist_numeric_cols:
        display_df[exist_numeric_cols] = display_df[exist_numeric_cols].apply(pd.to_numeric, errors="coerce")

    if "群組" in display_df.columns and "漲跌幅(%)" in display_df.columns:
        display_df = display_df.sort_values(
            by=["群組", "漲跌幅(%)"],
            ascending=[True, False],
            na_position="last"
        ).reset_index(drop=True)

    return display_df


def render_dashboard_summary(df: pd.DataFrame):
    metrics = _prepare_dashboard_metrics(df)

    avg_change_pct = metrics["avg_change_pct"]
    avg_text = f"{avg_change_pct:+.2f}%" if avg_change_pct is not None and pd.notna(avg_change_pct) else "—"

    if avg_change_pct is not None and pd.notna(avg_change_pct):
        if avg_change_pct > 0:
            avg_class = "pro-kpi-delta-up"
        elif avg_change_pct < 0:
            avg_class = "pro-kpi-delta-down"
        else:
            avg_class = "pro-kpi-delta-flat"
    else:
        avg_class = "pro-kpi-delta-flat"

    render_pro_kpi_row([
        {
            "label": "監控股票數",
            "value": f"{metrics['total_count']:,}",
            "delta": f"上漲 {metrics['up_count']}｜下跌 {metrics['down_count']}｜平盤 {metrics['flat_count']}",
            "delta_class": "pro-kpi-delta-flat"
        },
        {
            "label": "平均漲跌幅",
            "value": avg_text,
            "delta": "Portfolio Breadth",
            "delta_class": avg_class
        },
        {
            "label": "合計成交量",
            "value": format_number(metrics["total_volume"], 0),
            "delta": "Total Session Volume",
            "delta_class": "pro-kpi-delta-flat"
        },
    ])


def render_dashboard_table(df: pd.DataFrame, height: int = 720):
    display_df = _prepare_dashboard_table(df)

    if display_df.empty:
        st.info("目前沒有可顯示的即時資料。")
        return

    format_dict = {}
    for col in ["現價", "昨收", "漲跌", "漲跌幅(%)", "開盤", "最高", "最低"]:
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


def _init_page_state():
    if "font_scale" not in st.session_state:
        st.session_state.font_scale = get_font_scale()

    if _k("last_refresh_date") not in st.session_state:
        st.session_state[_k("last_refresh_date")] = ""

    if _k("force_refresh") not in st.session_state:
        st.session_state[_k("force_refresh")] = False

    if _k("refresh_token") not in st.session_state:
        st.session_state[_k("refresh_token")] = "init"


def main():
    st.set_page_config(page_title=PAGE_TITLE, page_icon="📊", layout="wide")
    _init_page_state()

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

    top1, top2, top3 = st.columns([1.2, 3, 2])
    with top1:
        refresh_clicked = st.button("更新即時資料", type="primary", use_container_width=True)
    with top2:
        st.caption(f"查詢日期：{query_date}")
    with top3:
        st.caption(f"刷新識別：{st.session_state[_k('refresh_token')]}")

    if refresh_clicked:
        new_token = str(int(time.time() * 1000))
        st.session_state[_k("force_refresh")] = True
        st.session_state[_k("refresh_token")] = new_token
        st.session_state[_k("last_refresh_date")] = query_date

        get_realtime_watchlist_df.clear()
        _prepare_dashboard_metrics.clear()
        _prepare_dashboard_table.clear()

    refresh_token = st.session_state[_k("refresh_token")]

    with st.spinner("正在讀取即時資料..."):
        realtime_df = get_realtime_watchlist_df(
            watchlist_dict=watchlist_dict,
            query_date=query_date,
            refresh_token=refresh_token,
        )

    st.session_state[_k("force_refresh")] = False

    if realtime_df is None or realtime_df.empty:
        st.info("目前沒有可顯示的即時資料。")
        return

    render_dashboard_summary(realtime_df)
    render_pro_section("即時盤面清單", "可快速掃描各群組成員的價格、漲跌幅與成交量變化")
    render_dashboard_table(realtime_df, height=760)


if __name__ == "__main__":
    main()
