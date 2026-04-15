from datetime import date, timedelta
import ast
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
    st.session_state.font_scale = st.slider("字體大小 (%)", 100, 220, st.session_state.font_scale, 10)

apply_font_scale(st.session_state.font_scale)

st.title("📊 儀表板")
st.caption("依自選股群組顯示最新行情摘要")

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")

raw_watchlist = load_watchlist()
if not raw_watchlist:
    st.warning("目前沒有自選股群組")
    st.stop()

all_code_name_df = get_all_code_name_map(lookup_date)
if all_code_name_df.empty:
    st.info("目前使用備援模式顯示，部分股票名稱與行情可能不完整。")

FALLBACK_NAME_MAP = {
    "2330": "台積電",
    "2454": "聯發科",
    "3711": "日月光投控",
    "2317": "鴻海",
    "2382": "廣達",
    "0050": "元大台灣50",
    "0056": "元大高股息",
    "2881": "富邦金",
    "2882": "國泰金",
    "6270": "倍微"
}


def normalize_watchlist(data):
    result = {}

    def parse_item(item):
        if isinstance(item, dict):
            code = item.get("code", "")
            name = item.get("name", "")

            if isinstance(code, str):
                code_text = code.strip()
                try:
                    parsed_code = ast.literal_eval(code_text)
                    if isinstance(parsed_code, dict):
                        code = parsed_code.get("code", "")
                        if not name:
                            name = parsed_code.get("name", "")
                except Exception:
                    pass

            return {
                "code": str(code).strip(),
                "name": str(name).strip()
            }

        if isinstance(item, str):
            text = item.strip()
            if not text:
                return None

            if text.isdigit():
                return {"code": text, "name": ""}

            try:
                parsed = ast.literal_eval(text)
                if isinstance(parsed, dict):
                    code = parsed.get("code", "")
                    name = parsed.get("name", "")
                    return {
                        "code": str(code).strip(),
                        "name": str(name).strip()
                    }
            except Exception:
                pass

            return {"code": text, "name": ""}

        return None

    for group_name, items in data.items():
        clean_items = []

        if isinstance(items, list):
            for item in items:
                parsed_item = parse_item(item)
                if parsed_item and parsed_item["code"]:
                    clean_items.append(parsed_item)

        dedup = []
        seen = set()
        for item in clean_items:
            if item["code"] not in seen:
                dedup.append(item)
                seen.add(item["code"])

        result[str(group_name).strip()] = dedup

    return result


watchlist_dict = normalize_watchlist(raw_watchlist)


def guess_market_type(code: str) -> str:
    code = str(code).strip()
    if code.startswith("00"):
        return "上市"
    if code in ["3711"]:
        return "上市"
    return "上市"


def get_stock_name_and_market(code: str, manual_name: str = ""):
    code = str(code).strip()
    manual_name = str(manual_name).strip()

    if manual_name:
        return manual_name, guess_market_type(code)

    if not all_code_name_df.empty:
        match = all_code_name_df[all_code_name_df["證券代號"] == code]
        if not match.empty:
            row = match.iloc[0]
            return str(row["證券名稱"]).strip(), str(row["市場別"]).strip()

    return FALLBACK_NAME_MAP.get(code, f"股票{code}"), guess_market_type(code)


@st.cache_data(ttl=30, show_spinner=False)
def get_group_dashboard_data(group_name: str, items: list[dict]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame()

    rows = []
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=40)

    for item in items:
        stock_no = str(item.get("code", "")).strip()
        manual_name = str(item.get("name", "")).strip()

        if not stock_no:
            continue

        stock_name, market_type = get_stock_name_and_market(stock_no, manual_name)

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

for group_name, items in watchlist_dict.items():
    st.markdown(f"## {group_name}")

    if not items:
        st.info(f"群組「{group_name}」目前沒有股票")
        continue

    with st.spinner(f"正在整理群組：{group_name}"):
        group_df = get_group_dashboard_data(group_name, items)

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
