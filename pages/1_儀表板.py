from datetime import date
import json
import os
import time

import pandas as pd
import requests
import streamlit as st
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="儀表板", page_icon="📊", layout="wide")

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


def fmt_num(v, digits=2):
    if v is None:
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
        "群組": "",
        "股票代號": code,
        "股票名稱": name,
        "市場別": market_type,
        "現價": price,
        "昨收": prev_close,
        "開盤": open_price,
        "最高": high_price,
        "最低": low_price,
        "漲跌": change,
        "漲跌幅(%)": change_pct,
        "總量": total_volume,
        "更新時間": update_text,
        "message": "",
    }


@st.cache_data(ttl=15, show_spinner=False)
def build_dashboard_df(watchlist_dict: dict) -> pd.DataFrame:
    rows = []

    for group_name, items in watchlist_dict.items():
        for item in items:
            code = str(item.get("code", "")).strip()
            name = str(item.get("name", "")).strip() or code
            market = str(item.get("market", "")).strip() or "上市"

            if not code:
                continue

            info = get_realtime_stock_info(code, name, market)
            row = {
                "群組": group_name,
                "股票代號": code,
                "股票名稱": name,
                "市場別": market,
                "現價": info.get("現價"),
                "昨收": info.get("昨收"),
                "開盤": info.get("開盤"),
                "最高": info.get("最高"),
                "最低": info.get("最低"),
                "漲跌": info.get("漲跌"),
                "漲跌幅(%)": info.get("漲跌幅(%)"),
                "總量": info.get("總量"),
                "更新時間": info.get("更新時間"),
                "是否成功": info.get("ok", False),
                "訊息": info.get("message", ""),
            }
            rows.append(row)

    df = pd.DataFrame(rows)

    if not df.empty:
        for col in ["現價", "昨收", "開盤", "最高", "最低", "漲跌", "漲跌幅(%)", "總量"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def render_realtime_table(df: pd.DataFrame, height: int = 650):
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


st.title("📊 儀表板")
st.caption("救援版｜先恢復群組即時摘要顯示")

watchlist_dict = load_watchlist()

if not watchlist_dict:
    st.warning("目前沒有讀到自選股清單。請確認 watchlist.json 是否存在。")
    st.stop()

c1, c2, c3 = st.columns(3)
with c1:
    st.metric("群組數量", len(watchlist_dict))
with c2:
    st.metric("自選股總數", sum(len(v) for v in watchlist_dict.values()))
with c3:
    st.metric("今日日期", date.today().strftime("%Y-%m-%d"))

if st.button("更新即時資料", type="primary", use_container_width=True):
    build_dashboard_df.clear()
    get_realtime_stock_info.clear()

with st.spinner("正在讀取即時資料..."):
    dashboard_df = build_dashboard_df(watchlist_dict)

if dashboard_df.empty:
    st.info("目前沒有可顯示的即時資料。")
else:
    render_realtime_table(dashboard_df, height=680)
