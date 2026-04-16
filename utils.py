from datetime import date
from io import BytesIO
from pathlib import Path
from difflib import SequenceMatcher
import ast
import json

import pandas as pd
import requests
import urllib3
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).resolve().parent
WATCHLIST_FILE = BASE_DIR / "watchlist.json"

DEFAULT_WATCHLIST = {
    "半導體": [
        {"code": "2330", "name": "台積電"},
        {"code": "2454", "name": "聯發科"},
        {"code": "3711", "name": "日月光投控"}
    ],
    "AI": [
        {"code": "2317", "name": "鴻海"},
        {"code": "2382", "name": "廣達"}
    ],
    "ETF": [
        {"code": "0050", "name": "元大台灣50"},
        {"code": "0056", "name": "元大高股息"}
    ],
    "金融": [
        {"code": "2881", "name": "富邦金"},
        {"code": "2882", "name": "國泰金"}
    ],
    "我的觀察名單": []
}

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
    "2303": "聯電",
    "2308": "台達電",
    "2603": "長榮",
    "3037": "欣興",
    "3008": "大立光",
    "6505": "台塑化",
    "1301": "台塑",
    "1303": "南亞",
    "2002": "中鋼",
    "2891": "中信金",
    "2892": "第一金",
    "6271": "同欣電"
}

TWSE_LISTED_CSV_URL = "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
TPEX_OTC_CSV_URL = "https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv"


def to_number(value):
    if value is None:
        return None

    text = str(value).replace(",", "").strip()
    if text in ["", "--", "X", "null", "None"]:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def format_number(value, digits=2):
    if value is None or pd.isna(value):
        return ""
    value = float(value)
    if value.is_integer():
        return f"{int(value):,}"
    return f"{value:,.{digits}f}"


def roc_to_ad(roc_text):
    parts = str(roc_text).strip().split("/")
    if len(parts) != 3:
        return pd.NaT

    try:
        y = int(parts[0]) + 1911
        m = int(parts[1])
        d = int(parts[2])
        return pd.Timestamp(year=y, month=m, day=d)
    except Exception:
        return pd.NaT


def month_range(start_dt: date, end_dt: date):
    months = []
    y = start_dt.year
    m = start_dt.month

    while (y < end_dt.year) or (y == end_dt.year and m <= end_dt.month):
        months.append(f"{y:04d}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    return months


def to_excel_bytes(df_dict):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in df_dict.items():
            safe_name = str(sheet_name)[:31]
            df.to_excel(writer, index=False, sheet_name=safe_name)
    output.seek(0)
    return output.getvalue()


def get_font_scale():
    if "font_scale" not in st.session_state:
        st.session_state.font_scale = 110
    return st.session_state.font_scale


def apply_font_scale(scale_percent: int = 100):
    base = scale_percent / 100

    st.markdown(
        f"""
        <style>
        .stApp {{
            font-size: {16 * base}px;
        }}

        .stApp p,
        .stApp div,
        .stApp label,
        .stApp span,
        .stApp li {{
            font-size: {16 * base}px !important;
        }}

        .stApp h1 {{
            font-size: {42 * base}px !important;
            font-weight: 700 !important;
        }}

        .stApp h2 {{
            font-size: {32 * base}px !important;
            font-weight: 700 !important;
        }}

        .stApp h3 {{
            font-size: {24 * base}px !important;
            font-weight: 600 !important;
        }}

        div[data-testid="stMetricValue"] {{
            font-size: {32 * base}px !important;
            font-weight: 700 !important;
        }}

        div[data-testid="stMetricLabel"] {{
            font-size: {16 * base}px !important;
        }}

        div[data-testid="stMetricDelta"] {{
            font-size: {16 * base}px !important;
        }}

        button, input, textarea, select {{
            font-size: {16 * base}px !important;
        }}

        .stDataFrame, .stTable {{
            font-size: {15 * base}px !important;
        }}

        div[data-testid="stExpander"] summary {{
            font-size: {17 * base}px !important;
            font-weight: 600 !important;
        }}

        div[data-testid="stVerticalBlock"] {{
            gap: 0.6rem;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_watchlist():
    if not WATCHLIST_FILE.exists():
        with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_WATCHLIST, f, ensure_ascii=False, indent=2)
        return DEFAULT_WATCHLIST.copy()

    try:
        with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return {"我的觀察名單": [{"code": str(x).strip(), "name": ""} for x in data if str(x).strip()]}

        if isinstance(data, dict):
            return data

        return DEFAULT_WATCHLIST.copy()
    except Exception:
        return DEFAULT_WATCHLIST.copy()


def save_watchlist(watchlist_dict):
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(watchlist_dict, f, ensure_ascii=False, indent=2)

    load_watchlist.clear()
    get_normalized_watchlist.clear()


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


@st.cache_data(ttl=300, show_spinner=False)
def get_normalized_watchlist():
    return normalize_watchlist(load_watchlist())


def guess_market_type(code: str) -> str:
    code = str(code).strip()
    if code.startswith("00"):
        return "上市"
    if code in ["3711"]:
        return "上市"
    return "上市"


def get_stock_name_and_market(code: str, all_code_name_df: pd.DataFrame, manual_name: str = ""):
    code = str(code).strip()
    manual_name = str(manual_name).strip()

    if manual_name:
        return manual_name, guess_market_type(code)

    if all_code_name_df is not None and not all_code_name_df.empty:
        match = all_code_name_df[all_code_name_df["證券代號"] == code]
        if not match.empty:
            row = match.iloc[0]
            return str(row["證券名稱"]).strip(), str(row["市場別"]).strip()

    return FALLBACK_NAME_MAP.get(code, f"股票{code}"), guess_market_type(code)


def get_stock_name(code: str, all_code_name_df: pd.DataFrame, manual_name: str = "") -> str:
    name, _ = get_stock_name_and_market(code, all_code_name_df, manual_name)
    return name


@st.cache_data(ttl=21600, show_spinner=False)
def _read_official_company_csv(url: str, market_name: str) -> pd.DataFrame:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv,application/csv,text/plain,*/*"
    }

    try:
        r = requests.get(url, headers=headers, timeout=30, verify=False)
        r.raise_for_status()

        content = r.content
        if not content:
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        df = None
        for enc in ["utf-8-sig", "utf-8", "cp950", "big5"]:
            try:
                text = content.decode(enc, errors="strict")
                df = pd.read_csv(BytesIO(text.encode("utf-8")))
                break
            except Exception:
                df = None

        if df is None or df.empty:
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        df.columns = [str(c).strip() for c in df.columns]

        code_col = None
        name_col = None

        for c in df.columns:
            if c in ["公司代號", "公司代碼", "股票代號", "證券代號"]:
                code_col = c
            elif c in ["公司簡稱", "公司名稱", "股票名稱", "證券名稱"]:
                if name_col is None:
                    name_col = c

        if code_col is None:
            for c in df.columns:
                if "代號" in c:
                    code_col = c
                    break

        if name_col is None:
            if "公司簡稱" in df.columns:
                name_col = "公司簡稱"
            elif "公司名稱" in df.columns:
                name_col = "公司名稱"
            else:
                for c in df.columns:
                    if "名稱" in c or "簡稱" in c:
                        name_col = c
                        break

        if code_col is None or name_col is None:
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        result = pd.DataFrame()
        result["證券代號"] = df[code_col].astype(str).str.strip()
        result["證券名稱"] = df[name_col].astype(str).str.strip()
        result["市場別"] = market_name

        result = result[
            (result["證券代號"] != "") &
            (result["證券名稱"] != "")
        ].drop_duplicates(subset=["證券代號"]).reset_index(drop=True)

        return result

    except Exception:
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])


@st.cache_data(ttl=21600, show_spinner=False)
def get_twse_code_name_map(query_date: str = "") -> pd.DataFrame:
    return _read_official_company_csv(TWSE_LISTED_CSV_URL, "上市")


@st.cache_data(ttl=21600, show_spinner=False)
def get_tpex_code_name_map() -> pd.DataFrame:
    return _read_official_company_csv(TPEX_OTC_CSV_URL, "上櫃")


@st.cache_data(ttl=21600, show_spinner=False)
def get_all_code_name_map(query_date: str = "") -> pd.DataFrame:
    twse_df = get_twse_code_name_map(query_date)
    tpex_df = get_tpex_code_name_map()
    all_df = pd.concat([twse_df, tpex_df], ignore_index=True)

    if all_df.empty:
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

    return all_df.drop_duplicates(subset=["證券代號"]).reset_index(drop=True)


def fuzzy_score(keyword: str, target: str) -> float:
    keyword = str(keyword).strip().lower()
    target = str(target).strip().lower()

    if not keyword or not target:
        return 0.0
    if keyword == target:
        return 1.0
    if keyword in target:
        return 0.9
    return SequenceMatcher(None, keyword, target).ratio()


def search_stocks(df: pd.DataFrame, keyword: str, top_n: int = 50) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    keyword = str(keyword).strip()
    if not keyword:
        return df.head(top_n).copy()

    result = df.copy()
    result["代號完全符合"] = result["證券代號"].astype(str).eq(keyword)
    result["名稱完全符合"] = result["證券名稱"].astype(str).eq(keyword)
    result["代號包含"] = result["證券代號"].astype(str).str.contains(keyword, case=False, na=False)
    result["名稱包含"] = result["證券名稱"].astype(str).str.contains(keyword, case=False, na=False)
    result["代號分數"] = result["證券代號"].apply(lambda x: fuzzy_score(keyword, x))
    result["名稱分數"] = result["證券名稱"].apply(lambda x: fuzzy_score(keyword, x))
    result["總分"] = result[["代號分數", "名稱分數"]].max(axis=1)

    filtered = result[
        result["代號完全符合"] |
        result["名稱完全符合"] |
        result["代號包含"] |
        result["名稱包含"] |
        (result["總分"] >= 0.35)
    ].copy()

    filtered = filtered.sort_values(
        by=["代號完全符合", "名稱完全符合", "代號包含", "名稱包含", "總分", "證券代號"],
        ascending=[False, False, False, False, False, True]
    )

    return filtered.head(top_n).copy()


@st.cache_data(ttl=1800, show_spinner=False)
def get_month_stock_data_twse(stock_no: str, yyyy_mm: str) -> pd.DataFrame:
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={yyyy_mm}01&stockNo={stock_no}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*"
    }

    try:
        r = requests.get(url, headers=headers, timeout=30, verify=False)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return pd.DataFrame()

    if data.get("stat") != "OK":
        return pd.DataFrame()

    fields = data.get("fields", [])
    rows = data.get("data", [])

    if not fields or not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=fields)

    if "日期" in df.columns:
        df["日期"] = df["日期"].apply(roc_to_ad)

    numeric_cols = [
        "成交股數", "成交金額", "開盤價", "最高價",
        "最低價", "收盤價", "漲跌價差", "成交筆數"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(to_number)

    return df


@st.cache_data(ttl=1800, show_spinner=False)
def get_history_data(stock_no: str, stock_name: str, market_type: str, start_dt: date, end_dt: date) -> pd.DataFrame:
    months = month_range(start_dt, end_dt)
    all_df = []

    for ym in months:
        if market_type == "上市":
            df = get_month_stock_data_twse(stock_no, ym)
        else:
            df = pd.DataFrame()

        if not df.empty:
            df = df.copy()
            df["證券代號"] = stock_no
            df["證券名稱"] = stock_name
            df["市場別"] = market_type
            all_df.append(df)

    if not all_df:
        return pd.DataFrame()

    result = pd.concat(all_df, ignore_index=True)
    result = result.dropna(subset=["日期"])
    result = result[(result["日期"].dt.date >= start_dt) & (result["日期"].dt.date <= end_dt)]
    result = result.sort_values("日期").reset_index(drop=True)
    return result
    import time


def _safe_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if text in ["", "-", "--", "—", "null", "None"]:
        return ""
    return text


def _safe_num(value):
    text = _safe_text(value)
    if not text:
        return None
    return to_number(text)


def _market_prefix(market_type: str):
    return "tse" if str(market_type).strip() == "上市" else "otc"


@st.cache_data(ttl=15, show_spinner=False)
def get_realtime_stock_info(stock_no: str, stock_name: str = "", market_type: str = "上市") -> dict:
    stock_no = str(stock_no).strip()
    stock_name = str(stock_name).strip()
    market_type = str(market_type).strip() or "上市"

    if not stock_no:
        return {
            "ok": False,
            "code": "",
            "name": stock_name,
            "market": market_type,
            "message": "股票代號為空白",
        }

    ex_ch = f"{_market_prefix(market_type)}_{stock_no}.tw"
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
        return {
            "ok": False,
            "code": stock_no,
            "name": stock_name,
            "market": market_type,
            "message": f"即時資料取得失敗：{e}",
        }

    msg_array = data.get("msgArray", [])
    if not msg_array:
        return {
            "ok": False,
            "code": stock_no,
            "name": stock_name,
            "market": market_type,
            "message": "查無即時資料",
        }

    raw = msg_array[0]

    code = _safe_text(raw.get("c")) or stock_no
    name = _safe_text(raw.get("n")) or stock_name or f"股票{stock_no}"

    prev_close = _safe_num(raw.get("y"))
    current_price = _safe_num(raw.get("z"))

    if current_price is None:
        current_price = prev_close

    open_price = _safe_num(raw.get("o"))
    high_price = _safe_num(raw.get("h"))
    low_price = _safe_num(raw.get("l"))
    total_volume = _safe_num(raw.get("v"))
    trade_volume = _safe_num(raw.get("tv"))

    change_value = None
    change_pct = None
    if current_price is not None and prev_close not in [None, 0]:
        change_value = current_price - prev_close
        change_pct = (change_value / prev_close) * 100

    update_date = _safe_text(raw.get("d"))
    update_time = _safe_text(raw.get("t"))
    update_text = ""
    if update_date and update_time:
        update_text = f"{update_date} {update_time}"
    elif update_time:
        update_text = update_time

    return {
        "ok": True,
        "code": code,
        "name": name,
        "market": market_type,
        "price": current_price,
        "prev_close": prev_close,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "change": change_value,
        "change_pct": change_pct,
        "total_volume": total_volume,
        "trade_volume": trade_volume,
        "update_time": update_text,
        "raw": raw,
        "message": "",
    }


@st.cache_data(ttl=15, show_spinner=False)
def get_realtime_watchlist_df(watchlist_dict: dict, query_date: str = "") -> pd.DataFrame:
    all_code_name_df = get_all_code_name_map(query_date)
    rows = []

    for group_name, items in watchlist_dict.items():
        for item in items:
            code = str(item.get("code", "")).strip()
            manual_name = str(item.get("name", "")).strip()
            if not code:
                continue

            stock_name, market_type = get_stock_name_and_market(code, all_code_name_df, manual_name)
            info = get_realtime_stock_info(code, stock_name, market_type)

            rows.append({
                "群組": group_name,
                "股票代號": code,
                "股票名稱": stock_name,
                "市場別": market_type,
                "現價": info.get("price"),
                "昨收": info.get("prev_close"),
                "開盤": info.get("open"),
                "最高": info.get("high"),
                "最低": info.get("low"),
                "漲跌": info.get("change"),
                "漲跌幅(%)": info.get("change_pct"),
                "總量": info.get("total_volume"),
                "單量": info.get("trade_volume"),
                "更新時間": info.get("update_time"),
                "是否成功": info.get("ok", False),
                "訊息": info.get("message", ""),
            })

    df = pd.DataFrame(rows)

    if not df.empty:
        for col in ["現價", "昨收", "開盤", "最高", "最低", "漲跌", "漲跌幅(%)", "總量", "單量"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def render_realtime_info_card(info: dict, title: str = "即時資訊"):
    if not info:
        st.info("目前沒有即時資訊。")
        return

    if not info.get("ok"):
        st.warning(info.get("message", "查無即時資訊"))
        return

    name = info.get("name", "")
    code = info.get("code", "")
    market = info.get("market", "")
    price = info.get("price")
    open_price = info.get("open")
    high_price = info.get("high")
    low_price = info.get("low")
    change = info.get("change")
    change_pct = info.get("change_pct")
    total_volume = info.get("total_volume")
    update_time = info.get("update_time", "")

    st.markdown(f"### {title}")
    st.caption(f"{name}（{code}）｜{market}｜更新時間：{update_time or '—'}")

    c1, c2, c3, c4 = st.columns(4)

    delta_text = None
    if change is not None and change_pct is not None:
        delta_text = f"{change:+.2f} ({change_pct:+.2f}%)"
    elif change is not None:
        delta_text = f"{change:+.2f}"

    with c1:
        st.metric("現價", format_number(price, 2) if price is not None else "—", delta=delta_text)
    with c2:
        st.metric("開盤", format_number(open_price, 2) if open_price is not None else "—")
    with c3:
        st.metric("最高", format_number(high_price, 2) if high_price is not None else "—")
    with c4:
        st.metric("最低", format_number(low_price, 2) if low_price is not None else "—")

    c5, c6 = st.columns(2)
    with c5:
        st.metric("總量", format_number(total_volume, 0) if total_volume is not None else "—")
    with c6:
        st.metric("昨收", format_number(info.get("prev_close"), 2) if info.get("prev_close") is not None else "—")


def render_realtime_table(df: pd.DataFrame, height: int = 520):
    if df is None or df.empty:
        st.info("目前沒有即時資料。")
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
@st.cache_data(ttl=600, show_spinner=False)
def search_stock_candidates(keyword: str, query_date: str = "", top_n: int = 20) -> pd.DataFrame:
    keyword = str(keyword).strip()
    all_df = get_all_code_name_map(query_date)

    if all_df.empty:
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

    result = search_stocks(all_df, keyword, top_n=top_n).copy()

    keep_cols = [c for c in ["證券代號", "證券名稱", "市場別"] if c in result.columns]
    if keep_cols:
        result = result[keep_cols].copy()

    return result.reset_index(drop=True)


def build_stock_candidate_labels(candidate_df: pd.DataFrame) -> list:
    if candidate_df is None or candidate_df.empty:
        return []

    labels = []
    for _, row in candidate_df.iterrows():
        code = str(row.get("證券代號", "")).strip()
        name = str(row.get("證券名稱", "")).strip()
        market = str(row.get("市場別", "")).strip()
        labels.append(f"{name} ({code}) [{market}]")

    return labels
@st.cache_data(ttl=600, show_spinner=False)
def search_stock_candidates(keyword: str, query_date: str = "", top_n: int = 20) -> pd.DataFrame:
    keyword = str(keyword).strip()
    all_df = get_all_code_name_map(query_date)

    if all_df.empty:
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

    result = search_stocks(all_df, keyword, top_n=top_n).copy()

    keep_cols = [c for c in ["證券代號", "證券名稱", "市場別"] if c in result.columns]
    if keep_cols:
        result = result[keep_cols].copy()

    return result.reset_index(drop=True)


def build_stock_candidate_labels(candidate_df: pd.DataFrame) -> list:
    if candidate_df is None or candidate_df.empty:
        return []

    labels = []
    for _, row in candidate_df.iterrows():
        code = str(row.get("證券代號", "")).strip()
        name = str(row.get("證券名稱", "")).strip()
        market = str(row.get("市場別", "")).strip()
        labels.append(f"{name} ({code}) [{market}]")

    return labels


@st.cache_data(ttl=600, show_spinner=False)
def validate_stock_input(stock_code: str = "", stock_name: str = "", query_date: str = ""):
    stock_code = str(stock_code).strip()
    stock_name = str(stock_name).strip()

    all_df = get_all_code_name_map(query_date)
    if all_df.empty:
        return {
            "is_valid": False,
            "message": "目前無法取得正式股票清單，暫時不能驗證股票資料。",
            "code": stock_code,
            "name": stock_name,
            "market": "",
        }

    if stock_code:
        match = all_df[all_df["證券代號"].astype(str).str.strip() == stock_code]
        if not match.empty:
            row = match.iloc[0]
            return {
                "is_valid": True,
                "message": "",
                "code": str(row["證券代號"]).strip(),
                "name": str(row["證券名稱"]).strip(),
                "market": str(row["市場別"]).strip(),
            }

    if stock_name:
        exact_name = all_df[all_df["證券名稱"].astype(str).str.strip() == stock_name]
        if not exact_name.empty:
            row = exact_name.iloc[0]
            return {
                "is_valid": True,
                "message": "",
                "code": str(row["證券代號"]).strip(),
                "name": str(row["證券名稱"]).strip(),
                "market": str(row["市場別"]).strip(),
            }

        fuzzy = search_stocks(all_df, stock_name, top_n=5)
        if not fuzzy.empty:
            row = fuzzy.iloc[0]
            return {
                "is_valid": True,
                "message": f"未找到完全相同名稱，已自動使用最接近結果：{row['證券名稱']}（{row['證券代號']}）",
                "code": str(row["證券代號"]).strip(),
                "name": str(row["證券名稱"]).strip(),
                "market": str(row["市場別"]).strip(),
            }

    return {
        "is_valid": False,
        "message": "查無此股票代號或名稱，請重新輸入。",
        "code": stock_code,
        "name": stock_name,
        "market": "",
    }
