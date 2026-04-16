from datetime import date
import json
import os
import time

import requests
import streamlit as st
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="行情查詢", page_icon="📈", layout="wide")

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
        "code": code,
        "name": name,
        "market": market_type,
        "price": price,
        "prev_close": prev_close,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "change": change,
        "change_pct": change_pct,
        "total_volume": total_volume,
        "update_time": update_text,
    }


def render_realtime_info_card(info: dict, title="即時行情"):
    st.markdown(f"### {title}")

    if not info or not info.get("ok"):
        st.info(info.get("message", "目前沒有即時資訊。") if isinstance(info, dict) else "目前沒有即時資訊。")
        return

    st.caption(
        f"{info.get('name', '—')}（{info.get('code', '—')}）｜"
        f"{info.get('market', '—')}｜更新時間：{info.get('update_time', '—')}"
    )

    delta_text = None
    if info.get("change") is not None and info.get("change_pct") is not None:
        delta_text = f"{info['change']:+.2f} ({info['change_pct']:+.2f}%)"
    elif info.get("change") is not None:
        delta_text = f"{info['change']:+.2f}"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("現價", fmt_num(info.get("price"), 2), delta=delta_text)
    with c2:
        st.metric("開盤", fmt_num(info.get("open"), 2))
    with c3:
        st.metric("最高", fmt_num(info.get("high"), 2))
    with c4:
        st.metric("最低", fmt_num(info.get("low"), 2))

    c5, c6 = st.columns(2)
    with c5:
        st.metric("總量", fmt_num(info.get("total_volume"), 0))
    with c6:
        st.metric("昨收", fmt_num(info.get("prev_close"), 2))


st.title("📈 行情查詢")
st.caption("救援版｜查詢單一股票即時行情")

watchlist_dict = load_watchlist()
group_names = list(watchlist_dict.keys())

if not group_names:
    st.warning("目前沒有讀到自選股清單。請確認 watchlist.json 是否存在。")
    st.stop()

c1, c2 = st.columns(2)

with c1:
    selected_group = st.selectbox("選擇群組", group_names, index=0)

items = watchlist_dict.get(selected_group, [])
stock_options = []
for item in items:
    code = str(item.get("code", "")).strip()
    name = str(item.get("name", "")).strip() or code
    market = str(item.get("market", "")).strip() or "上市"
    if code:
        stock_options.append({
            "label": f"{name} ({code}) [{market}]",
            "code": code,
            "name": name,
            "market": market,
        })

with c2:
    if stock_options:
        selected_stock_label = st.selectbox(
            "選擇股票",
            [x["label"] for x in stock_options],
            index=0
        )
        selected_stock = next(x for x in stock_options if x["label"] == selected_stock_label)
    else:
        selected_stock = None
        st.selectbox("選擇股票", ["此群組目前沒有股票"], index=0)

if selected_stock is None:
    st.warning("此群組目前沒有可查詢股票。")
    st.stop()

if st.button("查詢即時資訊", type="primary", use_container_width=True):
    with st.spinner("正在查詢即時資訊..."):
        info = get_realtime_stock_info(
            selected_stock["code"],
            selected_stock["name"],
            selected_stock["market"]
        )
    render_realtime_info_card(info, title="即時行情")
else:
    info = get_realtime_stock_info(
        selected_stock["code"],
        selected_stock["name"],
        selected_stock["market"]
    )
    render_realtime_info_card(info, title="即時行情")
