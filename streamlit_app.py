from datetime import date, datetime, timedelta
import json
import os
import time

import requests
import streamlit as st
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="股市專家系統", page_icon="📈", layout="wide")

WATCHLIST_CANDIDATES = [
    "watchlist.json",
    "watchlists.json",
    "data/watchlist.json",
    "data/watchlists.json",
]

STATE_FILE = "last_query_state.json"


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
                                    market = str(item.get("market", "")).strip()
                                    if code:
                                        normalized[group_name].append({
                                            "code": code,
                                            "name": name,
                                            "market": market,
                                        })
                        if not normalized[group_name]:
                            normalized[group_name] = []
                    return normalized
            except Exception:
                pass
    return {}


def load_query_state():
    default_state = {
        "quick_group": "",
        "quick_stock_code": "",
        "home_start": "",
        "home_end": "",
    }

    if not os.path.exists(STATE_FILE):
        return default_state

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        for k, v in default_state.items():
            if k not in data:
                data[k] = v
        return data
    except Exception:
        return default_state


def save_query_state(quick_group="", quick_stock_code="", home_start=None, home_end=None):
    data = {
        "quick_group": quick_group or "",
        "quick_stock_code": quick_stock_code or "",
        "home_start": str(home_start) if home_start else "",
        "home_end": str(home_end) if home_end else "",
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def parse_date_safe(value, fallback):
    if not value:
        return fallback
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except Exception:
        return fallback


def apply_font_scale(scale: int):
    base_pct = max(80, min(220, int(scale)))
    st.markdown(
        f"""
        <style>
        html, body, [class*="css"] {{
            font-size: {base_pct}% !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


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

    d = safe_text(raw.get("d"))
    t = safe_text(raw.get("t"))
    update_time = f"{d} {t}".strip() if d or t else "—"

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
        "update_time": update_time,
    }


def fmt_num(v, digits=2):
    if v is None:
        return "—"
    try:
        if digits == 0:
            return f"{float(v):,.0f}"
        return f"{float(v):,.{digits}f}"
    except Exception:
        return "—"


def render_realtime_info_card(info: dict, title="今日即時資訊"):
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


if "font_scale" not in st.session_state:
    st.session_state.font_scale = 110

last_state = load_query_state()
today_dt = date.today()

if "home_state_loaded" not in st.session_state:
    st.session_state.last_quick_group = last_state.get("quick_group", "")
    st.session_state.last_quick_stock_code = last_state.get("quick_stock_code", "")
    st.session_state.home_start = parse_date_safe(last_state.get("home_start", ""), today_dt - timedelta(days=90))
    st.session_state.home_end = parse_date_safe(last_state.get("home_end", ""), today_dt)
    st.session_state.home_state_loaded = True

with st.sidebar:
    st.markdown("## 顯示設定")
    st.session_state.font_scale = st.slider("字體大小 (%)", 100, 220, st.session_state.font_scale, 10)

apply_font_scale(st.session_state.font_scale)

st.title("📈 股市專家系統")
st.caption("首頁救援版｜先恢復正常使用")

watchlist_dict = load_watchlist()
group_names = list(watchlist_dict.keys())

group_count = len(watchlist_dict)
stock_count = sum(len(v) for v in watchlist_dict.values())

c1, c2, c3 = st.columns(3)
with c1:
    st.metric("群組數量", group_count)
with c2:
    st.metric("自選股總數", stock_count)
with c3:
    st.metric("今日日期", today_dt.strftime("%Y-%m-%d"))

st.markdown("---")
st.subheader("快速查詢入口")
st.caption("可在首頁快速選擇群組、股票與日期區間，再切換到『歷史K線分析』頁面使用")

if not group_names:
    st.warning("目前沒有讀到自選股清單。請確認 watchlist.json 是否存在。")
else:
    saved_group = st.session_state.get("last_quick_group", "")
    group_index = group_names.index(saved_group) if saved_group in group_names else 0

    with st.form("home_quick_query_form", clear_on_submit=False):
        c1, c2 = st.columns(2)

        with c1:
            quick_group = st.selectbox("選擇群組", group_names, index=group_index)

        stock_items = watchlist_dict.get(quick_group, [])
        stock_options = []
        for item in stock_items:
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
                saved_code = st.session_state.get("last_quick_stock_code", "")
                code_list = [x["code"] for x in stock_options]
                stock_index = code_list.index(saved_code) if saved_code in code_list else 0

                quick_stock_label = st.selectbox(
                    "選擇股票",
                    [x["label"] for x in stock_options],
                    index=stock_index
                )
                quick_stock = next(x for x in stock_options if x["label"] == quick_stock_label)
            else:
                quick_stock = None
                st.selectbox("選擇股票", ["此群組目前沒有股票"], index=0)

        d1, d2 = st.columns(2)
        with d1:
            quick_start = st.date_input("開始日期", value=st.session_state.get("home_start", today_dt - timedelta(days=90)))
        with d2:
            quick_end = st.date_input("結束日期", value=st.session_state.get("home_end", today_dt))

        save_btn = st.form_submit_button("套用查詢條件", type="primary", use_container_width=True)

    if save_btn:
        if quick_start > quick_end:
            st.error("開始日期不能大於結束日期")
            st.stop()

        st.session_state.last_quick_group = quick_group
        st.session_state.last_quick_stock_code = quick_stock["code"] if quick_stock is not None else ""
        st.session_state.home_start = quick_start
        st.session_state.home_end = quick_end

        save_query_state(
            quick_group=quick_group,
            quick_stock_code=quick_stock["code"] if quick_stock is not None else "",
            home_start=quick_start,
            home_end=quick_end
        )

        st.success("查詢條件已更新。")

    current_group = st.session_state.get("last_quick_group", group_names[0])
    current_start = st.session_state.get("home_start", today_dt - timedelta(days=90))
    current_end = st.session_state.get("home_end", today_dt)
    current_stock_code = st.session_state.get("last_quick_stock_code", "")

    current_stock = None
    for item in watchlist_dict.get(current_group, []):
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip() or code
        market = str(item.get("market", "")).strip() or "上市"
        if code == current_stock_code:
            current_stock = {"code": code, "name": name, "market": market}
            break

    if current_start > current_end:
        st.error("開始日期不能大於結束日期")
    else:
        if current_stock is not None:
            info = get_realtime_stock_info(
                current_stock["code"],
                current_stock["name"],
                current_stock["market"]
            )
            render_realtime_info_card(info, title="今日即時資訊")

            st.markdown(
                f"""
**目前快速查詢條件：**  
群組：{current_group}  
股票：{current_stock['name']}（{current_stock['code']}）  
市場別：{current_stock['market']}  
日期區間：{current_start} ~ {current_end}
"""
            )
        else:
            st.markdown(
                f"""
**目前快速查詢條件：**  
群組：{current_group}  
股票：尚未選定  
日期區間：{current_start} ~ {current_end}
"""
            )

        st.info("首頁已恢復可用。其他頁面若仍報錯，再逐頁修復。")

st.markdown("---")
st.subheader("系統功能")
st.markdown("""
- 儀表板：查看群組摘要
- 行情查詢：查單一股票
- 歷史K線分析：查歷史資料
- 自選股中心：管理自選股
- 排行榜：查看排行
""")
