from datetime import date
import json
import os
import time

import pandas as pd
import requests
import streamlit as st
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="排行榜", page_icon="🏆", layout="wide")

WATCHLIST_CANDIDATES = [
    "watchlist.json",
    "watchlists.json",
    "data/watchlist.json",
    "data/watchlists.json",
]


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

    prev_close = safe_num(raw.get("y"))
    price = safe_num(raw.get("z"))
    if price is None:
        price = prev_close

    change = None
    change_pct = None
    if price is not None and prev_close not in [None, 0]:
        change = price - prev_close
        change_pct = change / prev_close * 100

    return {
        "ok": True,
        "price": price,
        "prev_close": prev_close,
        "open": safe_num(raw.get("o")),
        "high": safe_num(raw.get("h")),
        "low": safe_num(raw.get("l")),
        "change": change,
        "change_pct": change_pct,
        "total_volume": safe_num(raw.get("v")),
        "update_time": f"{safe_text(raw.get('d'))} {safe_text(raw.get('t'))}".strip(),
    }


@st.cache_data(ttl=15, show_spinner=False)
def build_rank_df(watchlist_dict: dict) -> pd.DataFrame:
    rows = []

    for group_name, items in watchlist_dict.items():
        for item in items:
            code = str(item.get("code", "")).strip()
            name = str(item.get("name", "")).strip() or code
            market = str(item.get("market", "")).strip() or "上市"

            if not code:
                continue

            info = get_realtime_stock_info(code, name, market)
            rows.append({
                "群組": group_name,
                "股票代號": code,
                "股票名稱": name,
                "市場別": market,
                "現價": info.get("price"),
                "漲跌": info.get("change"),
                "漲跌幅(%)": info.get("change_pct"),
                "開盤": info.get("open"),
                "最高": info.get("high"),
                "最低": info.get("low"),
                "總量": info.get("total_volume"),
                "更新時間": info.get("update_time"),
            })

    df = pd.DataFrame(rows)

    if not df.empty:
        for col in ["現價", "漲跌", "漲跌幅(%)", "開盤", "最高", "最低", "總量"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def render_rank_table(df: pd.DataFrame, height: int = 680):
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
            return "color: #d32f2f; font-weight: 700;"
        elif v < 0:
            return "color: #00897b; font-weight: 700;"
        return "color: #666;"

    styler = display_df.style.format(format_dict, na_rep="—")
    if "漲跌" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌"])
    if "漲跌幅(%)" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌幅(%)"])

    st.dataframe(styler, use_container_width=True, hide_index=True, height=height)


st.title("🏆 排行榜")
st.caption("救援版｜依即時數據排序")

watchlist_dict = load_watchlist()

if not watchlist_dict:
    st.warning("目前沒有讀到自選股清單。請確認 watchlist.json 是否存在。")
    st.stop()

sort_col = st.selectbox(
    "排序方式",
    ["漲跌幅(%)", "漲跌", "總量", "現價"],
    index=0
)

ascending = st.toggle("升冪排序", value=False)

if st.button("更新排行榜", type="primary", use_container_width=True):
    build_rank_df.clear()
    get_realtime_stock_info.clear()

with st.spinner("正在讀取排行榜資料..."):
    rank_df = build_rank_df(watchlist_dict)

if rank_df.empty:
    st.info("目前沒有資料。")
else:
    if sort_col in rank_df.columns:
        rank_df = rank_df.sort_values(by=sort_col, ascending=ascending, na_position="last").reset_index(drop=True)
    render_rank_table(rank_df, height=700)
