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
    "半導體": ["2330", "2454", "3711"],
    "AI": ["2317", "2382"],
    "ETF": ["0050", "0056"],
    "金融": ["2881", "2882"],
    "我的觀察名單": []
}


# =========================
# 基本工具
# =========================
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


# =========================
# 字體設定
# =========================
def get_font_scale():
    if "font_scale" not in st.session_state:
        st.session_state.font_scale = 110
    return st.session_state.font_scale


def apply_font_scale(scale_percent: int = 100):
    base = scale_percent / 100
    st.markdown(
        f"""
        <style>
        html, body, [class*="css"] {{
            font-size: {16 * base}px;
        }}
        .stMetricValue {{
            font-size: {28 * base}px !important;
        }}
        .stMetricLabel {{
            font-size: {14 * base}px !important;
        }}
        h1 {{ font-size: {2.2 * base}rem !important; }}
        h2 {{ font-size: {1.8 * base}rem !important; }}
        h3 {{ font-size: {1.4 * base}rem !important; }}
        p, label, div {{
            font-size: {1.0 * base}rem;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# =========================
# 自選股
# =========================
def load_watchlist():
    if not WATCHLIST_FILE.exists():
        save_watchlist(DEFAULT_WATCHLIST)
        return DEFAULT_WATCHLIST.copy()

    try:
        with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return {"我的觀察名單": [str(x).strip() for x in data if str(x).strip()]}

        if isinstance(data, dict):
            clean_data = {}
            for group_name, codes in data.items():
                if not isinstance(codes, list):
                    continue
                clean_group = [str(x).strip() for x in codes if str(x).strip()]
                clean_data[str(group_name).strip()] = list(dict.fromkeys(clean_group))
            return clean_data if clean_data else DEFAULT_WATCHLIST.copy()

        return DEFAULT_WATCHLIST.copy()
    except Exception as e:
        st.warning(f"讀取 watchlist.json 失敗：{e}")
        return DEFAULT_WATCHLIST.copy()


def save_watchlist(watchlist_dict):
    clean_dict = {}
    for group_name, codes in watchlist_dict.items():
        group_name = str(group_name).strip()
        if not group_name:
            continue

        clean_codes = [str(x).strip() for x in codes if str(x).strip()]
        clean_dict[group_name] = list(dict.fromkeys(clean_codes))

    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(clean_dict, f, ensure_ascii=False, indent=2)


def flatten_watchlist_groups(watchlist_dict):
    all_codes = []
    for _, codes in watchlist_dict.items():
        all_codes.extend(codes)
    return list(dict.fromkeys(all_codes))


def build_watchlist_df(all_code_name_df, watchlist_dict):
    rows = []
    for group_name, codes in watchlist_dict.items():
        for code in codes:
            match = all_code_name_df[all_code_name_df["證券代號"] == code]
            if not match.empty:
                row = match.iloc[0]
                rows.append({
                    "群組": group_name,
                    "證券代號": row["證券代號"],
                    "證券名稱": row["證券名稱"],
                    "市場別": row["市場別"],
                    "顯示": f"{row['證券名稱']} ({row['證券代號']}) [{row['市場別']}]"
                })
    if not rows:
        return pd.DataFrame(columns=["群組", "證券代號", "證券名稱", "市場別", "顯示"])
    return pd.DataFrame(rows).drop_duplicates(subset=["群組", "證券代號"]).reset_index(drop=True)


# =========================
# 股票清單：上市 / 上櫃
# =========================
def get_twse_code_name_map(query_date: str) -> pd.DataFrame:
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={query_date}&type=ALL"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*"
    }

    try:
        r = requests.get(url, headers=headers, timeout=30, verify=False)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        st.warning(f"上市清單抓取失敗：{e}")
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

    tables = data.get("tables", [])
    all_rows = []

    for table in tables:
        fields = table.get("fields", [])
        rows = table.get("data", [])

        if "證券代號" in fields and "證券名稱" in fields:
            for row in rows:
                if len(row) != len(fields):
                    continue
                record = dict(zip(fields, row))
                stock_no = str(record.get("證券代號", "")).strip()
                stock_name = str(record.get("證券名稱", "")).strip()
                if stock_no and stock_name:
                    all_rows.append({
                        "證券代號": stock_no,
                        "證券名稱": stock_name,
                        "市場別": "上市"
                    })

    if not all_rows:
        st.warning("上市清單有回應，但未解析到資料")
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

    return pd.DataFrame(all_rows).drop_duplicates(subset=["證券代號"]).reset_index(drop=True)


def get_tpex_code_name_map() -> pd.DataFrame:
    url = "https://www.tpex.org.tw/openapi/v1/mkt/sii_and_otc_company_info"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*"
    }

    try:
        r = requests.get(url, headers=headers, timeout=30, verify=False)
        r.raise_for_status()

        if not r.text or not r.text.strip():
            st.warning("上櫃清單回傳空白內容")
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        data = r.json()
        if not data:
            st.warning("上櫃清單 JSON 為空")
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        df = pd.DataFrame(data)
        if df.empty:
            st.warning("上櫃清單 DataFrame 為空")
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        code_col = None
        name_col = None

        for c in df.columns:
            cs = str(c).strip()
            if cs in ["SecuritiesCompanyCode", "公司代號", "股票代號", "代號"]:
                code_col = c
            elif cs in ["CompanyName", "公司名稱", "股票名稱", "名稱"]:
                name_col = c

        if code_col is None:
            for c in df.columns:
                if "代號" in str(c):
                    code_col = c
                    break

        if name_col is None:
            for c in df.columns:
                if "名稱" in str(c):
                    name_col = c
                    break

        if code_col is None or name_col is None:
            st.warning(f"上櫃清單欄位辨識失敗，目前欄位：{list(df.columns)}")
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        result = pd.DataFrame()
        result["證券代號"] = df[code_col].astype(str).str.strip()
        result["證券名稱"] = df[name_col].astype(str).str.strip()
        result["市場別"] = "上櫃"

        result = result[
            (result["證券代號"] != "") &
            (result["證券名稱"] != "")
        ].drop_duplicates(subset=["證券代號"]).reset_index(drop=True)

        return result

    except Exception as e:
        st.warning(f"上櫃清單抓取失敗：{e}")
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])


def get_all_code_name_map(query_date: str) -> pd.DataFrame:
    twse_df = get_twse_code_name_map(query_date)
    tpex_df = get_tpex_code_name_map()
    all_df = pd.concat([twse_df, tpex_df], ignore_index=True)

    if all_df.empty:
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

    return all_df.drop_duplicates(subset=["證券代號"]).reset_index(drop=True)


# =========================
# 搜尋
# =========================
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


# =========================
# 歷史資料（目前上市較完整）
# =========================
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
    except Exception as e:
        st.warning(f"{stock_no} 歷史資料抓取失敗：{e}")
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
