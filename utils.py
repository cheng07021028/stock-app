import io
import json
import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
import streamlit as st
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

WATCHLIST_CANDIDATES = [
    "watchlist.json",
    "watchlists.json",
    "data/watchlist.json",
    "data/watchlists.json",
]


def to_number(value):
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if text in ["", "-", "--", "—", "None", "null"]:
        return None
    try:
        return float(text)
    except Exception:
        return None


def format_number(value, digits=2):
    if value is None or pd.isna(value):
        return "—"
    try:
        if digits == 0:
            return f"{float(value):,.0f}"
        return f"{float(value):,.{digits}f}"
    except Exception:
        return "—"


def get_font_scale():
    return 110


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


def _load_json_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def load_watchlist_raw():
    for path in WATCHLIST_CANDIDATES:
        if os.path.exists(path):
            data = _load_json_file(path)
            if isinstance(data, dict):
                return data
    return {}


def get_normalized_watchlist():
    data = load_watchlist_raw()
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


def save_watchlist(data: dict, filepath: str = "watchlist.json"):
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


@st.cache_data(ttl=3600, show_spinner=False)
def get_all_code_name_map(lookup_date: str = "") -> pd.DataFrame:
    rows = []

    # TWSE 上市
    try:
        url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
        r = requests.get(url, timeout=30, verify=False)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data)

        code_col = None
        name_col = None
        for c in df.columns:
            if "公司代號" in c or "證券代號" in c:
                code_col = c
            if "公司簡稱" in c or "證券名稱" in c:
                name_col = c

        if code_col and name_col:
            for _, row in df.iterrows():
                code = str(row.get(code_col, "")).strip()
                name = str(row.get(name_col, "")).strip()
                if code:
                    rows.append({
                        "code": code,
                        "name": name if name else code,
                        "market": "上市",
                    })
    except Exception:
        pass

    # TPEx 上櫃
    try:
        url = "https://www.tpex.org.tw/openapi/v1/mkt/sm_mainboard"
        r = requests.get(url, timeout=30, verify=False)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data)

        code_col = None
        name_col = None
        for c in df.columns:
            if "SecuritiesCompanyCode" in c or "股票代號" in c or "代號" == c:
                code_col = c
            if "CompanyName" in c or "股票名稱" in c or "名稱" == c:
                name_col = c

        if code_col and name_col:
            for _, row in df.iterrows():
                code = str(row.get(code_col, "")).strip()
                name = str(row.get(name_col, "")).strip()
                if code:
                    rows.append({
                        "code": code,
                        "name": name if name else code,
                        "market": "上櫃",
                    })
    except Exception:
        pass

    if not rows:
        return pd.DataFrame(columns=["code", "name", "market"])

    df_all = pd.DataFrame(rows).drop_duplicates(subset=["code", "market"]).reset_index(drop=True)
    return df_all


def get_stock_name_and_market(code: str, all_code_name_df: pd.DataFrame, manual_name: str = ""):
    code = str(code).strip()
    manual_name = str(manual_name).strip()

    if all_code_name_df is not None and not all_code_name_df.empty and "code" in all_code_name_df.columns:
        matched = all_code_name_df[all_code_name_df["code"].astype(str) == code]
        if not matched.empty:
            row = matched.iloc[0]
            stock_name = str(row.get("name", "")).strip() or manual_name or code
            market_type = str(row.get("market", "")).strip() or "上市"
            return stock_name, market_type

    if manual_name:
        return manual_name, "上市"

    return code, "上市"


def _market_prefix(market_type: str):
    return "otc" if str(market_type).strip() == "上櫃" else "tse"


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

    st.markdown(f"### {title}")

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
    prev_close = info.get("prev_close")
    update_time = info.get("update_time", "")

    st.caption(f"{name}（{code}）｜{market}｜更新時間：{update_time or '—'}")

    delta_text = None
    if change is not None and change_pct is not None:
        delta_text = f"{change:+.2f} ({change_pct:+.2f}%)"
    elif change is not None:
        delta_text = f"{change:+.2f}"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("現價", format_number(price, 2), delta=delta_text)
    with c2:
        st.metric("開盤", format_number(open_price, 2))
    with c3:
        st.metric("最高", format_number(high_price, 2))
    with c4:
        st.metric("最低", format_number(low_price, 2))

    c5, c6 = st.columns(2)
    with c5:
        st.metric("總量", format_number(total_volume, 0))
    with c6:
        st.metric("昨收", format_number(prev_close, 2))


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


@st.cache_data(ttl=1800, show_spinner=False)
def get_history_data(stock_no: str, stock_name: str = "", market_type: str = "上市", start_date=None, end_date=None) -> pd.DataFrame:
    stock_no = str(stock_no).strip()
    market_type = str(market_type).strip() or "上市"

    if not stock_no:
        return pd.DataFrame()

    if start_date is None:
        start_date = date.today() - timedelta(days=90)
    if end_date is None:
        end_date = date.today()

    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)

    if end_ts < start_ts:
        return pd.DataFrame()

    month_starts = pd.date_range(start=start_ts.replace(day=1), end=end_ts, freq="MS")
    frames = []

    for dt in month_starts:
        month_str = dt.strftime("%Y%m01")

        if market_type == "上櫃":
            try:
                url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"
                params = {
                    "date": dt.strftime("%Y/%m"),
                    "id": stock_no,
                    "response": "json",
                }
                r = requests.get(url, params=params, timeout=30, verify=False)
                r.raise_for_status()
                data = r.json()

                aa_data = data.get("tables", [])
                if not aa_data:
                    continue
            except Exception:
                continue
        else:
            try:
                url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                params = {
                    "response": "json",
                    "date": month_str,
                    "stockNo": stock_no,
                }
                r = requests.get(url, params=params, timeout=30, verify=False)
                r.raise_for_status()
                data = r.json()

                if data.get("stat") != "OK":
                    continue

                rows = data.get("data", [])
                cols = data.get("fields", [])
                if not rows or not cols:
                    continue

                df_month = pd.DataFrame(rows, columns=cols)
                frames.append(df_month)
            except Exception:
                continue

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    rename_map = {
        "日期": "日期",
        "成交股數": "成交股數",
        "成交金額": "成交金額",
        "開盤價": "開盤價",
        "最高價": "最高價",
        "最低價": "最低價",
        "收盤價": "收盤價",
        "成交筆數": "成交筆數",
    }
    df = df.rename(columns=rename_map)

    if "日期" not in df.columns:
        return pd.DataFrame()

    def convert_tw_date(x):
        x = _safe_text(x)
        if not x:
            return pd.NaT

        if "/" in x:
            parts = x.split("/")
            if len(parts) == 3:
                try:
                    year = int(parts[0]) + 1911
                    month = int(parts[1])
                    day = int(parts[2])
                    return pd.Timestamp(year=year, month=month, day=day)
                except Exception:
                    return pd.NaT

        try:
            return pd.to_datetime(x)
        except Exception:
            return pd.NaT

    df["日期"] = df["日期"].apply(convert_tw_date)
    df = df.dropna(subset=["日期"])

    for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", "", regex=False)
                .replace(["--", "---", ""], pd.NA)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[(df["日期"] >= start_ts) & (df["日期"] <= end_ts)]
    df = df.sort_values("日期").reset_index(drop=True)
    return df


def to_excel_bytes(df_dict):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in df_dict.items():
            safe_sheet_name = str(sheet_name)[:31]
            pd.DataFrame(df).to_excel(writer, index=False, sheet_name=safe_sheet_name)
    output.seek(0)
    return output.getvalue()
