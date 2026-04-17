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

STATE_FILE = "last_query_state.json"


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
    if value is None:
        return "—"
    try:
        if pd.isna(value):
            return "—"
    except Exception:
        pass

    try:
        if digits == 0:
            return f"{float(value):,.0f}"
        return f"{float(value):,.{digits}f}"
    except Exception:
        return "—"


def get_font_scale():
    return 110


def apply_font_scale(scale):
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


def inject_pro_theme():
    st.markdown(
        """
        <style>
        .main > div {
            padding-top: 1.2rem;
        }

        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 2rem;
        }

        .pro-hero {
            background: linear-gradient(135deg, #0f172a 0%, #162033 45%, #1e293b 100%);
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 22px;
            padding: 24px 26px;
            margin: 0 0 18px 0;
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.18);
        }

        .pro-hero-title {
            color: #f8fafc;
            font-size: 28px;
            font-weight: 800;
            line-height: 1.25;
            margin-bottom: 6px;
            letter-spacing: 0.2px;
        }

        .pro-hero-subtitle {
            color: #cbd5e1;
            font-size: 14px;
            line-height: 1.6;
        }

        .pro-section-title {
            font-size: 20px;
            font-weight: 800;
            color: #0f172a;
            margin: 10px 0 2px 0;
            letter-spacing: 0.2px;
        }

        .pro-section-subtitle {
            font-size: 13px;
            color: #64748b;
            margin-bottom: 10px;
        }

        .pro-card {
            background: linear-gradient(180deg, #ffffff 0%, #fbfdff 100%);
            border: 1px solid #e2e8f0;
            border-radius: 18px;
            padding: 18px 18px 16px 18px;
            box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
            margin-bottom: 14px;
        }

        .pro-card-title {
            font-size: 15px;
            font-weight: 800;
            color: #0f172a;
            margin-bottom: 10px;
        }

        .pro-chip {
            display: inline-block;
            background: #eff6ff;
            color: #1d4ed8;
            border: 1px solid #dbeafe;
            border-radius: 999px;
            padding: 4px 10px;
            margin: 2px 6px 2px 0;
            font-size: 12px;
            font-weight: 700;
        }

        .pro-kpi-card {
            background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
            border: 1px solid #e2e8f0;
            border-radius: 18px;
            padding: 16px 18px;
            box-shadow: 0 5px 14px rgba(15, 23, 42, 0.05);
            min-height: 112px;
        }

        .pro-kpi-label {
            font-size: 12px;
            color: #64748b;
            font-weight: 700;
            margin-bottom: 8px;
            letter-spacing: 0.3px;
        }

        .pro-kpi-value {
            font-size: 28px;
            color: #0f172a;
            font-weight: 800;
            line-height: 1.2;
        }

        .pro-kpi-delta-up {
            font-size: 13px;
            font-weight: 700;
            color: #dc2626;
            margin-top: 6px;
        }

        .pro-kpi-delta-down {
            font-size: 13px;
            font-weight: 700;
            color: #059669;
            margin-top: 6px;
        }

        .pro-kpi-delta-flat {
            font-size: 13px;
            font-weight: 700;
            color: #64748b;
            margin-top: 6px;
        }

        .pro-info-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(160px, 1fr));
            gap: 12px;
        }

        .pro-info-item {
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 14px;
            padding: 12px 14px;
        }

        .pro-info-label {
            font-size: 12px;
            color: #64748b;
            font-weight: 700;
            margin-bottom: 6px;
        }

        .pro-info-value {
            font-size: 16px;
            color: #0f172a;
            font-weight: 800;
            line-height: 1.35;
        }

        .pro-up {
            color: #dc2626 !important;
            font-weight: 800 !important;
        }

        .pro-down {
            color: #059669 !important;
            font-weight: 800 !important;
        }

        .pro-flat {
            color: #64748b !important;
            font-weight: 800 !important;
        }

        @media (max-width: 1100px) {
            .pro-info-grid {
                grid-template-columns: repeat(2, minmax(160px, 1fr));
            }
        }

        @media (max-width: 768px) {
            .pro-info-grid {
                grid-template-columns: 1fr;
            }

            .pro-hero-title {
                font-size: 22px;
            }

            .pro-kpi-value {
                font-size: 24px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_pro_hero(title, subtitle=""):
    st.markdown(
        f"""
        <div class="pro-hero">
            <div class="pro-hero-title">{title}</div>
            <div class="pro-hero-subtitle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_pro_section(title, subtitle=""):
    st.markdown(
        f"""
        <div class="pro-section-title">{title}</div>
        <div class="pro-section-subtitle">{subtitle}</div>
        """,
        unsafe_allow_html=True,
    )


def render_pro_kpi_row(items):
    cols = st.columns(len(items))
    for idx, item in enumerate(items):
        label = item.get("label", "—")
        value = item.get("value", "—")
        delta = item.get("delta", "")
        delta_class = item.get("delta_class", "pro-kpi-delta-flat")

        with cols[idx]:
            st.markdown(
                f"""
                <div class="pro-kpi-card">
                    <div class="pro-kpi-label">{label}</div>
                    <div class="pro-kpi-value">{value}</div>
                    <div class="{delta_class}">{delta}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_pro_info_card(title, info_pairs, chips=None):
    chips_html = ""
    if chips:
        chips_html = "".join([f'<span class="pro-chip">{x}</span>' for x in chips])

    items_html = ""
    for label, value, css_class in info_pairs:
        css = css_class if css_class else ""
        items_html += f'''
        <div class="pro-info-item">
            <div class="pro-info-label">{label}</div>
            <div class="pro-info-value {css}">{value}</div>
        </div>
        '''

    st.markdown(
        f"""
        <div class="pro-card">
            <div class="pro-card-title">{title}</div>
            <div style="margin-bottom:10px;">{chips_html}</div>
            <div class="pro-info-grid">
                {items_html}
            </div>
        </div>
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


def parse_date_safe(value, fallback):
    if not value:
        return fallback
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except Exception:
        return fallback


def load_last_query_state():
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


def save_last_query_state(quick_group="", quick_stock_code="", home_start=None, home_end=None):
    data = {
        "quick_group": quick_group or "",
        "quick_stock_code": quick_stock_code or "",
        "home_start": str(home_start) if home_start else "",
        "home_end": str(home_end) if home_end else "",
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


def load_watchlist_raw():
    for path in WATCHLIST_CANDIDATES:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
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


def save_watchlist(data, filepath="watchlist.json"):
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


@st.cache_data(ttl=86400, show_spinner=False)
def get_all_code_name_map(lookup_date=""):
    rows = []

    def _pick_col(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    def _append_rows(df, code_candidates, name_candidates, fallback_market=""):
        nonlocal rows

        if df is None or df.empty:
            return

        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        code_col = _pick_col(df, code_candidates)
        name_col = _pick_col(df, name_candidates)
        market_col = _pick_col(df, ["市場別", "market", "Market", "交易市場", "上市櫃"])

        if not code_col or not name_col:
            return

        for _, row in df.iterrows():
            code = str(row.get(code_col, "")).strip()
            name = str(row.get(name_col, "")).strip()
            market = str(row.get(market_col, "")).strip() if market_col else ""

            digits = "".join(ch for ch in code if ch.isdigit())
            if 4 <= len(digits) <= 6:
                code = digits

            if not code:
                continue

            if not name:
                name = code

            if not market:
                market = fallback_market or "上市"

            rows.append({
                "code": code,
                "name": name,
                "market": market,
            })

    twse_urls = [
        "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
        "https://openapi.twse.com.tw/v1/opendata/t187ap03_O",
        "https://openapi.twse.com.tw/v1/opendata/t187ap03_P",
    ]

    for url in twse_urls:
        try:
            r = requests.get(url, timeout=30, verify=False)
            r.raise_for_status()
            data = r.json()
            df = pd.DataFrame(data)
            _append_rows(
                df,
                ["公司代號", "證券代號", "Code", "code", "股票代號", "代號"],
                ["公司簡稱", "證券名稱", "Name", "name", "股票名稱", "名稱"],
                fallback_market="上市",
            )
        except Exception:
            pass

    tpex_urls = [
        ("https://www.tpex.org.tw/openapi/v1/mkt/sm_mainboard", "上櫃"),
        ("https://www.tpex.org.tw/openapi/v1/mkt/sm_esb", "興櫃"),
        ("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O", "上櫃"),
        ("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_R", "上櫃"),
    ]

    for url, fallback_market in tpex_urls:
        try:
            r = requests.get(url, timeout=30, verify=False)
            r.raise_for_status()
            data = r.json()
            df = pd.DataFrame(data)
            _append_rows(
                df,
                [
                    "SecuritiesCompanyCode", "股票代號", "Code", "code",
                    "代號", "公司代號", "證券代號"
                ],
                [
                    "CompanyName", "股票名稱", "Name", "name",
                    "名稱", "公司簡稱", "證券名稱"
                ],
                fallback_market=fallback_market,
            )
        except Exception:
            pass

    if not rows:
        return pd.DataFrame(columns=["code", "name", "market"])

    df_all = pd.DataFrame(rows)

    df_all["code"] = df_all["code"].astype(str).str.strip()
    df_all["name"] = df_all["name"].astype(str).str.strip()
    df_all["market"] = df_all["market"].astype(str).str.strip().replace("", "上市")

    df_all = df_all[(df_all["code"] != "") & (df_all["name"] != "")]
    df_all = df_all.drop_duplicates(subset=["code", "market"]).reset_index(drop=True)

    if lookup_date in ["上市", "上櫃", "興櫃"]:
        df_all = df_all[df_all["market"] == lookup_date].reset_index(drop=True)

    return df_all


def get_stock_name_and_market(code, all_code_name_df, manual_name=""):
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


def _market_prefix(market_type):
    return "otc" if str(market_type).strip() == "上櫃" else "tse"


@st.cache_data(ttl=15, show_spinner=False)
def get_realtime_stock_info(stock_no, stock_name="", market_type="上市"):
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


def render_realtime_info_card(info, title="即時資訊"):
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


@st.cache_data(ttl=15, show_spinner=False)
def get_realtime_watchlist_df(watchlist_dict, query_date=""):
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


def render_realtime_table(df, height=520):
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
def get_history_data(stock_no, stock_name="", market_type="上市", start_date=None, end_date=None):
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


def classify_signal(value, positive_text="偏多", negative_text="偏空", neutral_text="中性"):
    if value is True:
        return positive_text, "pro-up"
    if value is False:
        return negative_text, "pro-down"
    return neutral_text, "pro-flat"


def compute_signal_snapshot(df):
    result = {
        "ma_trend": ("中性", "pro-flat"),
        "kd_cross": ("中性", "pro-flat"),
        "macd_trend": ("中性", "pro-flat"),
        "price_vs_ma20": ("中性", "pro-flat"),
        "breakout_20d": ("中性", "pro-flat"),
        "volume_state": ("中性", "pro-flat"),
        "score": 0,
        "comment": "資料不足",
    }

    if df is None or df.empty or "收盤價" not in df.columns:
        return result

    last = df.iloc[-1]
    score = 0

    ma_cols = ["MA5", "MA10", "MA20"]
    if all(col in df.columns for col in ma_cols):
        ma5 = last["MA5"]
        ma10 = last["MA10"]
        ma20 = last["MA20"]

        if pd.notna(ma5) and pd.notna(ma10) and pd.notna(ma20):
            if ma5 > ma10 > ma20:
                result["ma_trend"] = ("多頭排列", "pro-up")
                score += 2
            elif ma5 < ma10 < ma20:
                result["ma_trend"] = ("空頭排列", "pro-down")
                score -= 2
            else:
                result["ma_trend"] = ("均線糾結", "pro-flat")

    if all(col in df.columns for col in ["K", "D"]) and len(df) >= 2:
        prev = df.iloc[-2]
        if pd.notna(prev["K"]) and pd.notna(prev["D"]) and pd.notna(last["K"]) and pd.notna(last["D"]):
            if prev["K"] <= prev["D"] and last["K"] > last["D"]:
                result["kd_cross"] = ("黃金交叉", "pro-up")
                score += 1
            elif prev["K"] >= prev["D"] and last["K"] < last["D"]:
                result["kd_cross"] = ("死亡交叉", "pro-down")
                score -= 1
            else:
                result["kd_cross"] = ("無新交叉", "pro-flat")

    if all(col in df.columns for col in ["DIF", "DEA", "MACD_HIST"]):
        dif = last["DIF"]
        dea = last["DEA"]
        hist = last["MACD_HIST"]

        if pd.notna(dif) and pd.notna(dea) and pd.notna(hist):
            if dif > dea and hist > 0:
                result["macd_trend"] = ("MACD翻多", "pro-up")
                score += 1
            elif dif < dea and hist < 0:
                result["macd_trend"] = ("MACD翻空", "pro-down")
                score -= 1
            else:
                result["macd_trend"] = ("MACD整理", "pro-flat")

    if "MA20" in df.columns and pd.notna(last.get("MA20", None)):
        close_price = last["收盤價"]
        ma20 = last["MA20"]
        if pd.notna(close_price) and pd.notna(ma20):
            if close_price > ma20:
                result["price_vs_ma20"] = ("站上MA20", "pro-up")
                score += 1
            elif close_price < ma20:
                result["price_vs_ma20"] = ("跌破MA20", "pro-down")
                score -= 1
            else:
                result["price_vs_ma20"] = ("貼近MA20", "pro-flat")

    if len(df) >= 20 and all(col in df.columns for col in ["最高價", "最低價", "收盤價"]):
        recent_20 = df.tail(20)
        max_20 = recent_20["最高價"].max()
        min_20 = recent_20["最低價"].min()
        close_price = last["收盤價"]

        if pd.notna(close_price) and pd.notna(max_20) and pd.notna(min_20):
            if close_price >= max_20:
                result["breakout_20d"] = ("突破20日高", "pro-up")
                score += 1
            elif close_price <= min_20:
                result["breakout_20d"] = ("跌破20日低", "pro-down")
                score -= 1
            else:
                result["breakout_20d"] = ("區間內震盪", "pro-flat")

    if "成交股數" in df.columns and len(df) >= 5:
        vol5 = df["成交股數"].tail(5).mean()
        last_vol = last["成交股數"]
        if pd.notna(vol5) and pd.notna(last_vol):
            if last_vol > vol5 * 1.3:
                result["volume_state"] = ("量能放大", "pro-up")
                score += 1
            elif last_vol < vol5 * 0.7:
                result["volume_state"] = ("量能偏弱", "pro-down")
                score -= 1
            else:
                result["volume_state"] = ("量能平穩", "pro-flat")

    result["score"] = score

    if score >= 4:
        result["comment"] = "多方優勢明確，可偏多看待，但仍需留意追價風險。"
    elif score >= 2:
        result["comment"] = "短線偏多，適合觀察是否延續攻擊。"
    elif score <= -4:
        result["comment"] = "空方壓力明確，宜保守應對，避免逆勢承接。"
    elif score <= -2:
        result["comment"] = "短線偏弱，建議先觀察止跌與量價是否改善。"
    else:
        result["comment"] = "多空訊號混合，暫屬整理盤，適合等待方向更明確。"

    return result


def score_to_badge(score):
    if score >= 4:
        return "強多", "pro-up"
    if score >= 2:
        return "偏多", "pro-up"
    if score <= -4:
        return "強空", "pro-down"
    if score <= -2:
        return "偏空", "pro-down"
    return "整理", "pro-flat"


def compute_support_resistance_snapshot(df):
    result = {
        "res_20": None,
        "sup_20": None,
        "res_60": None,
        "sup_60": None,
        "dist_res_20_pct": None,
        "dist_sup_20_pct": None,
        "dist_res_60_pct": None,
        "dist_sup_60_pct": None,
        "pressure_signal": ("中性", "pro-flat"),
        "support_signal": ("中性", "pro-flat"),
        "break_signal": ("區間內", "pro-flat"),
        "comment_trend": "資料不足",
        "comment_risk": "資料不足",
        "comment_focus": "資料不足",
        "comment_action": "資料不足",
    }

    if df is None or df.empty or "收盤價" not in df.columns:
        return result

    last = df.iloc[-1]
    close_price = last["收盤價"]

    if pd.isna(close_price):
        return result

    if len(df) >= 20 and all(col in df.columns for col in ["最高價", "最低價"]):
        df20 = df.tail(20)
        res_20 = df20["最高價"].max()
        sup_20 = df20["最低價"].min()
        result["res_20"] = res_20
        result["sup_20"] = sup_20

        if pd.notna(res_20) and res_20 != 0:
            result["dist_res_20_pct"] = (res_20 - close_price) / res_20 * 100
        if pd.notna(sup_20) and sup_20 != 0:
            result["dist_sup_20_pct"] = (close_price - sup_20) / sup_20 * 100

    if len(df) >= 60 and all(col in df.columns for col in ["最高價", "最低價"]):
        df60 = df.tail(60)
        res_60 = df60["最高價"].max()
        sup_60 = df60["最低價"].min()
        result["res_60"] = res_60
        result["sup_60"] = sup_60

        if pd.notna(res_60) and res_60 != 0:
            result["dist_res_60_pct"] = (res_60 - close_price) / res_60 * 100
        if pd.notna(sup_60) and sup_60 != 0:
            result["dist_sup_60_pct"] = (close_price - sup_60) / sup_60 * 100

    dist_res_20_pct = result["dist_res_20_pct"]
    dist_sup_20_pct = result["dist_sup_20_pct"]

    if dist_res_20_pct is not None:
        if dist_res_20_pct < 1.5 and dist_res_20_pct >= 0:
            result["pressure_signal"] = ("接近20日壓力", "pro-down")
        elif dist_res_20_pct < 0:
            result["pressure_signal"] = ("突破20日壓力", "pro-up")
        else:
            result["pressure_signal"] = ("壓力尚遠", "pro-flat")

    if dist_sup_20_pct is not None:
        if dist_sup_20_pct < 1.5 and dist_sup_20_pct >= 0:
            result["support_signal"] = ("接近20日支撐", "pro-up")
        elif dist_sup_20_pct < 0:
            result["support_signal"] = ("跌破20日支撐", "pro-down")
        else:
            result["support_signal"] = ("支撐尚遠", "pro-flat")

    if result["res_20"] is not None and close_price > result["res_20"]:
        result["break_signal"] = ("有效突破20日壓力", "pro-up")
    elif result["sup_20"] is not None and close_price < result["sup_20"]:
        result["break_signal"] = ("跌破20日支撐", "pro-down")
    else:
        result["break_signal"] = ("區間內整理", "pro-flat")

    pressure_text = result["pressure_signal"][0]
    support_text = result["support_signal"][0]
    break_text = result["break_signal"][0]

    if "突破" in break_text:
        result["comment_trend"] = "股價已進入突破型態，短線趨勢偏強。"
    elif "跌破" in break_text:
        result["comment_trend"] = "股價已跌破重要區間，短線結構轉弱。"
    else:
        result["comment_trend"] = "目前仍在區間結構內，趨勢等待進一步表態。"

    if "接近20日壓力" in pressure_text:
        result["comment_risk"] = "現價接近短壓區，追價風險升高。"
    elif "接近20日支撐" in support_text:
        result["comment_risk"] = "現價接近短撐區，需觀察是否有防守買盤。"
    elif "跌破20日支撐" in support_text:
        result["comment_risk"] = "支撐失守，若無量價回穩，弱勢可能延續。"
    else:
        result["comment_risk"] = "目前壓力與支撐距離適中，風險屬中性。"

    if result["dist_res_20_pct"] is not None and result["dist_sup_20_pct"] is not None:
        result["comment_focus"] = (
            f"短壓約在 {format_number(result['res_20'], 2)}，"
            f"短撐約在 {format_number(result['sup_20'], 2)}，"
            "建議觀察價格是否帶量突破壓力，或回測支撐是否止穩。"
        )
    else:
        result["comment_focus"] = "觀察近期高低點位置與量能是否同步放大。"

    if "有效突破" in break_text:
        result["comment_action"] = "可偏多思考，但應防假突破，宜搭配量能確認。"
    elif "跌破20日支撐" in break_text:
        result["comment_action"] = "宜保守應對，等待止跌訊號或重新站回支撐區。"
    elif "接近20日支撐" in support_text:
        result["comment_action"] = "可觀察支撐區反應，不宜在未止穩前過度預設反彈。"
    elif "接近20日壓力" in pressure_text:
        result["comment_action"] = "若無法放量突破壓力，短線宜防拉回。"
    else:
        result["comment_action"] = "可先觀察，等待價格脫離區間後再提高部位判斷。"

    return result


def compute_radar_scores(df: pd.DataFrame) -> dict:
    result = {
        "trend": 50,
        "momentum": 50,
        "volume": 50,
        "position": 50,
        "structure": 50,
        "summary": "資料不足，暫以中性評估。"
    }

    if df is None or df.empty or "收盤價" not in df.columns:
        return result

    last = df.iloc[-1]

    trend_score = 50
    if all(col in df.columns for col in ["MA5", "MA10", "MA20"]):
        ma5 = last.get("MA5")
        ma10 = last.get("MA10")
        ma20 = last.get("MA20")
        close_price = last.get("收盤價")

        if pd.notna(ma5) and pd.notna(ma10) and pd.notna(ma20) and pd.notna(close_price):
            if ma5 > ma10 > ma20 and close_price > ma20:
                trend_score = 90
            elif ma5 > ma10 and close_price > ma20:
                trend_score = 75
            elif ma5 < ma10 < ma20 and close_price < ma20:
                trend_score = 20
            elif close_price < ma20:
                trend_score = 35
            else:
                trend_score = 55

    momentum_score = 50
    if all(col in df.columns for col in ["K", "D", "DIF", "DEA"]):
        k = last.get("K")
        d = last.get("D")
        dif = last.get("DIF")
        dea = last.get("DEA")

        if pd.notna(k) and pd.notna(d) and pd.notna(dif) and pd.notna(dea):
            if k > d and dif > dea:
                momentum_score = 85
            elif k > d or dif > dea:
                momentum_score = 68
            elif k < d and dif < dea:
                momentum_score = 25
            else:
                momentum_score = 45

    volume_score = 50
    if "成交股數" in df.columns and len(df) >= 5:
        last_vol = last.get("成交股數")
        avg5 = df["成交股數"].tail(5).mean()

        if pd.notna(last_vol) and pd.notna(avg5) and avg5 > 0:
            ratio = last_vol / avg5
            if ratio >= 1.8:
                volume_score = 90
            elif ratio >= 1.3:
                volume_score = 75
            elif ratio >= 0.9:
                volume_score = 55
            elif ratio >= 0.7:
                volume_score = 40
            else:
                volume_score = 25

    position_score = 50
    if len(df) >= 20 and all(col in df.columns for col in ["最高價", "最低價", "收盤價"]):
        recent = df.tail(20)
        recent_high = recent["最高價"].max()
        recent_low = recent["最低價"].min()
        close_price = last.get("收盤價")

        if pd.notna(recent_high) and pd.notna(recent_low) and pd.notna(close_price) and recent_high > recent_low:
            pos = (close_price - recent_low) / (recent_high - recent_low)
            if pos >= 0.85:
                position_score = 85
            elif pos >= 0.65:
                position_score = 70
            elif pos >= 0.35:
                position_score = 50
            elif pos >= 0.15:
                position_score = 35
            else:
                position_score = 20

    structure_score = 50
    if len(df) >= 20:
        close_price = last.get("收盤價")
        recent_high = df["最高價"].tail(20).max() if "最高價" in df.columns else None
        recent_low = df["最低價"].tail(20).min() if "最低價" in df.columns else None

        if pd.notna(close_price):
            if recent_high is not None and pd.notna(recent_high) and close_price >= recent_high:
                structure_score = 88
            elif recent_low is not None and pd.notna(recent_low) and close_price <= recent_low:
                structure_score = 18
            else:
                structure_score = 55

    result["trend"] = int(round(trend_score))
    result["momentum"] = int(round(momentum_score))
    result["volume"] = int(round(volume_score))
    result["position"] = int(round(position_score))
    result["structure"] = int(round(structure_score))

    avg_score = (result["trend"] + result["momentum"] + result["volume"] + result["position"] + result["structure"]) / 5

    if avg_score >= 80:
        result["summary"] = "整體評分強勢，趨勢、動能與位置多數站在多方。"
    elif avg_score >= 65:
        result["summary"] = "整體評分偏強，可優先列入觀察名單。"
    elif avg_score >= 45:
        result["summary"] = "整體評分中性，偏向整理或等待方向確認。"
    elif avg_score >= 30:
        result["summary"] = "整體評分偏弱，短線保守為宜。"
    else:
        result["summary"] = "整體評分明顯偏弱，宜先以風險控管為主。"

    return result
