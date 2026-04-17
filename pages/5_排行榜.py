from datetime import date, timedelta
import ast
import pandas as pd
import streamlit as st

from utils import (
    load_watchlist,
    get_all_code_name_map,
    get_history_data,
    apply_font_scale,
    get_font_scale,
    format_number,
)

st.set_page_config(page_title="排行榜", page_icon="🏆", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

with st.sidebar:
    st.markdown("## 顯示設定")
    st.session_state.font_scale = st.slider("字體大小 (%)", 100, 220, st.session_state.font_scale, 10)

apply_font_scale(st.session_state.font_scale)

st.title("🏆 排行榜")
st.caption("依自選股群組顯示漲跌幅、成交金額、成交股數排行")

today_dt = date.today()
lookup_date = today_dt.strftime("%Y%m%d")

raw_watchlist = load_watchlist()
if not raw_watchlist:
    st.warning("目前沒有自選股群組，請先到自選股中心建立清單。")
    st.stop()

all_code_name_df = get_all_code_name_map(lookup_date)
if all_code_name_df.empty:
    st.info("目前使用備援模式，部分股票名稱可能以內建對照或代號顯示。")

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
    "6271": "同欣電"
}


def normalize_watchlist(data):
    result = {}

    def parse_item(item):
        if isinstance(item, dict):
            code = item.get("code", "")
            name = item.get("name", "")

            if isinstance(code, str):
                try:
                    parsed_code = ast.literal_eval(code.strip())
                    if isinstance(parsed_code, dict):
                        code = parsed_code.get("code", "")
                        if not name:
                            name = parsed_code.get("name", "")
                except Exception:
                    pass

            return {"code": str(code).strip(), "name": str(name).strip()}

        if isinstance(item, str):
            text = item.strip()
            if not text:
                return None
            if text.isdigit():
                return {"code": text, "name": ""}
            try:
                parsed = ast.literal_eval(text)
                if isinstance(parsed, dict):
                    return {
                        "code": str(parsed.get("code", "")).strip(),
                        "name": str(parsed.get("name", "")).strip()
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


group_names = list(watchlist_dict.keys())
selected_group = st.selectbox("選擇群組", group_names, index=0)

items = watchlist_dict.get(selected_group, [])
if not items:
    st.warning("此群組目前沒有股票。")
    st.stop()


@st.cache_data(ttl=60, show_spinner=False)
def build_rank_df(items: list[dict], start_dt: date, end_dt: date) -> pd.DataFrame:
    rows = []

    for item in items:
        code = str(item.get("code", "")).strip()
        manual_name = str(item.get("name", "")).strip()
        stock_name, market_type = get_stock_name_and_market(code, manual_name)

        hist_df = get_history_data(
            stock_no=code,
            stock_name=stock_name,
            market_type=market_type,
            start_dt=start_dt,
            end_dt=end_dt
        )

        if hist_df.empty:
            rows.append({
                "證券代號": code,
                "證券名稱": stock_name,
                "市場別": market_type,
                "日期": None,
                "最新價": None,
                "漲跌": None,
                "漲跌幅%": None,
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
            "證券代號": code,
            "證券名稱": stock_name,
            "市場別": market_type,
            "日期": latest.get("日期"),
            "最新價": latest_close,
            "漲跌": price_change,
            "漲跌幅%": pct_change,
            "成交股數": latest.get("成交股數"),
            "成交金額": latest.get("成交金額"),
            "成交筆數": latest.get("成交筆數"),
        })

    return pd.DataFrame(rows)


start_dt = today_dt - timedelta(days=40)
end_dt = today_dt

with st.spinner("正在整理排行榜資料..."):
    rank_df = build_rank_df(items, start_dt, end_dt)

if rank_df.empty:
    st.warning("查無排行資料。")
    st.stop()

summary1, summary2, summary3, summary4 = st.columns(4)
with summary1:
    st.metric("群組股票數", len(rank_df))
with summary2:
    valid_up = rank_df["漲跌幅%"].dropna()
    st.metric("可比較漲跌股票數", len(valid_up))
with summary3:
    st.metric("有成交金額資料", int(rank_df["成交金額"].notna().sum()))
with summary4:
    st.metric("有成交股數資料", int(rank_df["成交股數"].notna().sum()))

st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["漲幅排行", "跌幅排行", "成交金額排行", "成交股數排行"])


def format_rank_df(df: pd.DataFrame) -> pd.DataFrame:
    show_df = df.copy()

    if "日期" in show_df.columns:
        show_df["日期"] = pd.to_datetime(show_df["日期"]).dt.strftime("%Y-%m-%d")

    for col in ["最新價", "漲跌", "成交股數", "成交金額", "成交筆數"]:
        if col in show_df.columns:
            digits = 0 if col in ["成交股數", "成交金額", "成交筆數"] else 2
            show_df[col] = show_df[col].apply(lambda x: format_number(x, digits) if pd.notna(x) else "")

    if "漲跌幅%" in show_df.columns:
        show_df["漲跌幅%"] = show_df["漲跌幅%"].apply(lambda x: f"{x:,.2f}%" if pd.notna(x) else "")

    return show_df


with tab1:
    up_df = rank_df.dropna(subset=["漲跌幅%"]).sort_values("漲跌幅%", ascending=False).reset_index(drop=True)
    st.subheader("漲幅排行")
    st.dataframe(format_rank_df(up_df), use_container_width=True, hide_index=True)

with tab2:
    down_df = rank_df.dropna(subset=["漲跌幅%"]).sort_values("漲跌幅%", ascending=True).reset_index(drop=True)
    st.subheader("跌幅排行")
    st.dataframe(format_rank_df(down_df), use_container_width=True, hide_index=True)

with tab3:
    amount_df = rank_df.dropna(subset=["成交金額"]).sort_values("成交金額", ascending=False).reset_index(drop=True)
    st.subheader("成交金額排行")
    st.dataframe(format_rank_df(amount_df), use_container_width=True, hide_index=True)

with tab4:
    volume_df = rank_df.dropna(subset=["成交股數"]).sort_values("成交股數", ascending=False).reset_index(drop=True)
    st.subheader("成交股數排行")
    st.dataframe(format_rank_df(volume_df), use_container_width=True, hide_index=True)
