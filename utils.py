from datetime import date
from io import BytesIO
from pathlib import Path
from difflib import SequenceMatcher
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


def load_watchlist():
    if not WATCHLIST_FILE.exists():
        save_watchlist(DEFAULT_WATCHLIST)
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


TWSE_LISTED_CSV_URL = "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
TPEX_OTC_CSV_URL = "https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv"


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


def get_twse_code_name_map(query_date: str = "") -> pd.DataFrame:
    return _read_official_company_csv(TWSE_LISTED_CSV_URL, "上市")


def get_tpex_code_name_map() -> pd.DataFrame:
    return _read_official_company_csv(TPEX_OTC_CSV_URL, "上櫃")


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


def get_history_data(stock_no: str, stock_name: str, market_type: str, start_dt: date, end_dt: date) -> pd.DataFrame:
    months = month_range(start_dt, end_dt)
    all_df = []

    for ym in months:
        if market_type == "上市":
            df = get_month_stock_data_twse(stock_no, ym)
        else:
            df = pd.DataFrame()

        if not df.empty:
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
@st.cache_data(ttl=300, show_spinner=False)
def load_watchlist():
    if not WATCHLIST_FILE.exists():
        save_watchlist(DEFAULT_WATCHLIST)
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
