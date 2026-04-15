from datetime import date, timedelta
import pandas as pd
import streamlit as st

from utils import (
    get_all_code_name_map,
    get_history_data,
    format_number,
    load_watchlist,
    apply_font_scale,
    get_font_scale,
)

st.set_page_config(page_title="儀表板", page_icon="📊", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

with st.sidebar:
    st.markdown("## 顯示設定")
    st.session_state.font_scale = st.slider("字體大小 (%)", 80, 160, st.session_state.font_scale, 5)

apply_font_scale(st.session_state.font_scale)

st.title("📊 儀表板")
st.caption("依自選股群組顯示最新行情摘要")

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")

watchlist_dict = load_watchlist()
if not watchlist_dict:
    st.warning("目前沒有自選股群組")
    st.stop()

all_code_name_df = get_all_code_name_map(lookup_date)

if all_code_name_df.empty:
    st.warning("目前無法從交易所取得完整股票清單，改用自選股代號模式顯示。")


def build_fallback_info_df(watchlist_dict: dict) -> pd.DataFrame:
    rows = []
    all_codes = []
    for _, codes in watchlist_dict.items():
        all_codes.extend(codes)

    unique_codes = list(dict.fromkeys([str(x).strip() for x in all_codes if str(x).strip()]))

    for code in unique_codes:
        if code.startswith("00"):
            market_type = "上市"
        elif code.startswith("3") or code.startswith("4") or code.startswith("5") or code.startswith("6") or code.startswith("7") or code.startswith("8") or code.startswith("9"):
            market_type = "上櫃"
        else:
            market_type = "上市"

        rows.append({
            "證券代號": code,
            "證券名稱": f"股票{code}",
            "市場別": market_type
        })

    return pd.DataFrame(rows)


if all_code_name_df.empty:
    all_code_name_df = build_fallback_info_df(watchlist_dict)


@st.cache_data(ttl=30, show_spinner=False)
def get_group_dashboard_data(group_name: str, codes: list[str], info_source_df: pd.DataFrame) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()

    info_df = info_source_df[info_source_df["證券代號"].isin(codes)].copy()

    if info_df.empty:
        fallback_rows = []
        for code in codes:
            fallback_rows.append({
                "證券代號": code,
                "證券名稱": f"股票{code}",
                "市場別": "上市"
            })
        info_df = pd.DataFrame(fallback_rows)

    rows = []
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=40)

    for _, row in info_df.iterrows():
        stock_no = row["證券代號"]
        stock_name = row["證券名稱"]
        market_type = row["市場別"]

        hist_df = get_history_data(stock_no, stock_name, market_type, start_dt, end_dt)
        if hist_df.empty:
            rows.append({
                "群組": group_name,
                "證券代號": stock_no,
                "證券名稱": stock_name,
                "市場別": market_type,
                "日期": None,
                "最新價": None,
                "漲跌": None,
                "漲跌幅%": None,
                "開盤價": None,
                "最高價": None,
                "最低價": None,
                "成交股數": None,
                "成交金額": None,
                "成交筆數": None,
            })
            continue

        hist_df = hist_df.sort_values("日期").reset_index(drop=True)
        latest = hist_df.iloc[-1]

        prev_close = None
        if len(hist_df) >= 2:
            prev_close = hist_df.iloc[-2].get("收盤價")

        latest_close = latest.get("收盤價")
        price_change = None
        pct_change = None

        if prev_close is not None and latest_close is not None:
            price_change = latest_close - prev_close
            if prev_close != 0:
                pct_change = (price_change / prev_close) * 100

        rows.append({
            "群組": group_name,
            "證券代號": stock_no,
            "證券名稱": stock_name,
            "市場別": market_type,
            "日期": latest.get("日期"),
            "最新價": latest_close,
            "漲跌": price_change,
            "漲跌幅%": pct_change,
            "開盤價": latest.get("開盤價"),
            "最高價": latest.get("最高價"),
            "最低價": latest.get("最低價"),
            "成交股數": latest.get("成交股數"),
            "成交金額": latest.get("成交金額"),
            "成交筆數": latest.get("成交筆數"),
        })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def render_stock_card(row: pd.Series):
    with st.container(border=True):
        st.markdown(f"### {row['證券名稱']} ({row['證券代號']})")
        st.caption(f"市場別：{row['市場別']}")

        delta = ""
        if pd.notna(row.get("漲跌")) and pd.notna(row.get("漲跌幅%")):
            sign = "+" if row["漲跌"] >= 0 else ""
            delta = f"{sign}{row['漲跌']:.2f} ({sign}{row['漲跌幅%']:.2f}%)"

        c1, c2 = st.columns(2)
        with c1:
            st.metric("最新價", format_number(row.get("最新價")), delta=delta)
            st.metric("開盤價", format_number(row.get("開盤價")))
            st.metric("最低價", format_number(row.get("最低價")))
        with c2:
            st.metric("最高價", format_number(row.get("最高價")))
            st.metric("成交股數", format_number(row.get("成交股數"), 0))
            st.metric("成交金額", format_number(row.get("成交金額"), 0))


group_count = len(watchlist_dict)
stock_count = sum(len(v) for v in watchlist_dict.values())

m1, m2, m3 = st.columns(3)
with m1:
    st.metric("群組數量", group_count)
with m2:
    st.metric("自選股總數", stock_count)
with m3:
    st.metric("資料日期", today_dt.strftime("%Y-%m-%d"))

st.markdown("---")

for group_name, codes in watchlist_dict.items():
    st.markdown(f"## {group_name}")

    if not codes:
        st.info(f"群組「{group_name}」目前沒有股票")
        continue

    with st.spinner(f"正在整理群組：{group_name}"):
        group_df = get_group_dashboard_data(group_name, codes, all_code_name_df)

    if group_df.empty:
        st.warning(f"群組「{group_name}」查無資料")
        continue

    up_count = (group_df["漲跌"] > 0).sum() if "漲跌" in group_df.columns else 0
    down_count = (group_df["漲跌"] < 0).sum() if "漲跌" in group_df.columns else 0
    flat_count = len(group_df) - up_count - down_count

    s1, s2, s3, s4 = st.columns(4)
    with s1:
        st.metric("股票數", len(group_df))
    with s2:
        st.metric("上漲", int(up_count))
    with s3:
        st.metric("下跌", int(down_count))
    with s4:
        st.metric("平盤/無資料", int(flat_count))

    if "漲跌幅%" in group_df.columns:
        group_df = group_df.sort_values("漲跌幅%", ascending=False, na_position="last")

    cols = st.columns(3)
    for idx, (_, row) in enumerate(group_df.iterrows()):
        with cols[idx % 3]:
            render_stock_card(row)

    with st.expander(f"查看 {group_name} 明細表"):
        show_df = group_df.copy()
        if "日期" in show_df.columns:
            show_df["日期"] = pd.to_datetime(show_df["日期"]).dt.strftime("%Y-%m-%d")

        for col in ["最新價", "漲跌", "漲跌幅%", "開盤價", "最高價", "最低價", "成交股數", "成交金額", "成交筆數"]:
            if col in show_df.columns:
                if col == "漲跌幅%":
                    show_df[col] = show_df[col].apply(lambda x: f"{x:,.2f}%" if pd.notna(x) else "")
                else:
                    digits = 0 if col in ["成交股數", "成交金額", "成交筆數"] else 2
                    show_df[col] = show_df[col].apply(lambda x: format_number(x, digits) if pd.notna(x) else "")

        st.dataframe(show_df, use_container_width=True, hide_index=True)

    st.markdown("---")
