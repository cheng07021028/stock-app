# pages/2_行情查詢.py
from __future__ import annotations

from datetime import date, datetime, timedelta
import time
from typing import Any

import pandas as pd
import streamlit as st

from utils import (
    compute_signal_snapshot,
    compute_support_resistance_snapshot,
    format_number,
    get_all_code_name_map,
    get_history_data,
    get_normalized_watchlist,
    get_realtime_stock_info,
    get_stock_name_and_market,
    inject_pro_theme,
    load_last_query_state,
    parse_date_safe,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
    save_last_query_state,
    score_to_badge,
)

PAGE_TITLE = "行情查詢"
PFX = "rt_"


# =========================================================
# 基礎工具
# =========================================================
def _k(key: str) -> str:
    return f"{PFX}{key}"


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default=None):
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default


def _to_date(v: Any, fallback: date) -> date:
    if v is None:
        return fallback
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        x = pd.to_datetime(v, errors="coerce")
        if pd.notna(x):
            return x.date()
    except Exception:
        pass
    return fallback


def _html(s: str):
    st.markdown(s, unsafe_allow_html=True)


def _is_html_noise(v: Any) -> bool:
    import html
    import re

    s = _safe_str(v)
    if not s:
        return False
    for _ in range(3):
        ns = html.unescape(s)
        if ns == s:
            break
        s = ns
    low = s.lower().strip()
    if low in {"<div>", "</div>", "<span>", "</span>", "<p>", "</p>", "div", "/div", "none", "nan"}:
        return True
    return bool(re.search(r"</?\s*(div|span|p|style|script)\b|class\s*=|pro-info-|pro-card|unsafe_allow_html", low))


def _clean_card_text(v: Any, default: str = "—") -> str:
    """強制清除 HTML 殘片，避免 </div> 顯示在卡片裡。"""
    import html
    import re

    if _is_html_noise(v):
        return default

    text = _safe_str(v)
    if not text:
        return default

    for _ in range(3):
        new_text = html.unescape(text)
        if new_text == text:
            break
        text = new_text

    text = re.sub(r"<[^>]*>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text or _is_html_noise(text):
        return default

    return html.escape(text)


def _render_info_card_safe(title: str, info_pairs, chips=None):
    """本頁專用安全卡片：不依賴 utils.render_pro_info_card，避免 HTML 殘片外露。"""
    safe_title = _clean_card_text(title, "—")

    chip_html = ""
    if chips:
        if isinstance(chips, str):
            chips = [chips]
        safe_chips = []
        for c in chips:
            if _is_html_noise(c):
                continue
            ct = _clean_card_text(c, "")
            if ct:
                safe_chips.append(f'<span class="pro-chip">{ct}</span>')
        chip_html = "".join(safe_chips)

    items_html = ""
    if isinstance(info_pairs, dict):
        info_pairs = list(info_pairs.items())
    if not isinstance(info_pairs, (list, tuple)):
        info_pairs = []

    for item in info_pairs:
        if not isinstance(item, (list, tuple)):
            continue

        label = item[0] if len(item) >= 1 else "—"
        value = item[1] if len(item) >= 2 else "—"
        css_class = item[2] if len(item) >= 3 else ""

        if _is_html_noise(label) or _is_html_noise(value):
            continue

        safe_label = _clean_card_text(label, "")
        safe_value = _clean_card_text(value, "")
        if not safe_label and not safe_value:
            continue
        if not safe_label:
            safe_label = "資訊"
        if not safe_value:
            safe_value = "—"

        safe_css = _safe_str(css_class)
        if safe_css not in ["pro-up", "pro-down", "pro-flat"]:
            safe_css = ""

        items_html += f"""
        <div class="pro-info-item">
            <div class="pro-info-label">{safe_label}</div>
            <div class="pro-info-value {safe_css}">{safe_value}</div>
        </div>
        """

    if not items_html:
        items_html = """
        <div class="pro-info-item">
            <div class="pro-info-label">狀態</div>
            <div class="pro-info-value">資料不足</div>
        </div>
        """

    st.markdown(
        f"""
        <div class="pro-card">
            <div class="pro-card-title">{safe_title}</div>
            <div style="margin-bottom:10px;">{chip_html}</div>
            <div class="quote-info-grid quote-info-grid-3">
                {items_html}
            </div>
        </div>
        <style>
        .quote-info-grid {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 12px;
        }}
        .quote-info-grid .pro-info-item {{
            min-width: 0;
            overflow-wrap: anywhere;
            word-break: break-word;
        }}
        @media (max-width: 1100px) {{
            .quote-info-grid {{
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }}
        }}
        @media (max-width: 768px) {{
            .quote-info-grid {{
                grid-template-columns: 1fr;
            }}
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )



# =========================================================
# 快取資料
# =========================================================
@st.cache_data(ttl=120, show_spinner=False)
def _get_group_stock_map_cached(watchlist_items: tuple) -> dict[str, list[dict[str, str]]]:
    group_map: dict[str, list[dict[str, str]]] = {}

    for group_name, items in watchlist_items:
        g = _safe_str(group_name) or "未分組"
        rows = []

        if isinstance(items, tuple):
            for item in items:
                if not isinstance(item, tuple) or len(item) < 3:
                    continue

                code = _safe_str(item[0])
                name = _safe_str(item[1]) or code
                market = _safe_str(item[2]) or "上市"

                if code:
                    rows.append(
                        {
                            "code": code,
                            "name": name,
                            "market": market,
                            "label": f"{code} {name}",
                        }
                    )

        group_map[g] = rows

    return group_map


@st.cache_data(ttl=120, show_spinner=False)
def _flatten_group_map_cached(group_items: tuple) -> list[dict[str, str]]:
    rows = []
    for group_name, items in group_items:
        for item in items:
            rows.append(
                {
                    "group": _safe_str(group_name),
                    "code": _safe_str(item.get("code")),
                    "name": _safe_str(item.get("name")),
                    "market": _safe_str(item.get("market")),
                    "label": _safe_str(item.get("label")),
                }
            )
    return rows


@st.cache_data(ttl=1800, show_spinner=False)
def _get_tpex_history_data(stock_no: str, start_date: date, end_date: date) -> pd.DataFrame:
    stock_no = _safe_str(stock_no)
    if not stock_no:
        return pd.DataFrame()

    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    if end_ts < start_ts:
        return pd.DataFrame()

    month_starts = pd.date_range(start=start_ts.replace(day=1), end=end_ts, freq="MS")
    frames = []

    try:
        import requests
    except Exception:
        return pd.DataFrame()

    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.tpex.org.tw/"}

    for dt in month_starts:
        roc_year = dt.year - 1911
        roc_date = f"{roc_year}/{dt.month:02d}"
        try:
            r = requests.get(
                "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php",
                params={"l": "zh-tw", "d": roc_date, "stkno": stock_no},
                headers=headers,
                timeout=15,
                verify=False,
            )
            r.raise_for_status()
            data = r.json()
            aa_data = data.get("aaData", [])
            fields = data.get("fields", [])
            if not aa_data:
                continue
            temp = pd.DataFrame(aa_data, columns=fields if fields and len(fields) == len(aa_data[0]) else None)
            frames.append(temp)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    rename_map = {}
    for col in df.columns:
        c = _safe_str(col)
        if c in ["日期", "日 期"]:
            rename_map[col] = "日期"
        elif "成交仟股" in c or "成交股數" in c:
            rename_map[col] = "成交股數"
        elif "成交仟元" in c or "成交金額" in c:
            rename_map[col] = "成交金額"
        elif "開盤" in c:
            rename_map[col] = "開盤價"
        elif "最高" in c:
            rename_map[col] = "最高價"
        elif "最低" in c:
            rename_map[col] = "最低價"
        elif "收盤" in c:
            rename_map[col] = "收盤價"
        elif "成交筆數" in c:
            rename_map[col] = "成交筆數"
    df = df.rename(columns=rename_map)

    if "日期" not in df.columns:
        return pd.DataFrame()

    def convert_roc_date(x):
        x = _safe_str(x)
        if not x:
            return pd.NaT
        if "/" in x:
            parts = x.split("/")
            if len(parts) == 3:
                try:
                    return pd.Timestamp(year=int(parts[0]) + 1911, month=int(parts[1]), day=int(parts[2]))
                except Exception:
                    return pd.NaT
        try:
            return pd.to_datetime(x)
        except Exception:
            return pd.NaT

    df["日期"] = df["日期"].apply(convert_roc_date)
    df = df.dropna(subset=["日期"])

    for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
                .replace(["--", "---", "", "----"], pd.NA)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "成交股數" in df.columns:
        try:
            med = df["成交股數"].dropna().median()
            if pd.notna(med) and med < 100000:
                df["成交股數"] = df["成交股數"] * 1000
        except Exception:
            pass

    df = df[(df["日期"] >= start_ts) & (df["日期"] <= end_ts)]
    df = df.sort_values("日期").drop_duplicates(subset=["日期"], keep="last").reset_index(drop=True)
    return df



@st.cache_data(ttl=900, show_spinner=False)
def _get_yahoo_history_data(stock_no: str, market_type: str, start_date: date, end_date: date, refresh_token: str = "init") -> tuple[pd.DataFrame, str]:
    """Yahoo Finance 備援日線：支援上市 .TW 與上櫃 .TWO，避免 TWSE/TPEX 官方端點異常時整頁無資料。"""
    stock_no = _safe_str(stock_no)
    market_type = _safe_str(market_type)
    if not stock_no:
        return pd.DataFrame(), ""

    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    if end_ts < start_ts:
        return pd.DataFrame(), ""

    period1 = int(start_ts.timestamp())
    # Yahoo period2 是 exclusive，補一天避免結束日漏抓
    period2 = int((end_ts + pd.Timedelta(days=1)).timestamp())

    if market_type == "上櫃":
        symbols = [f"{stock_no}.TWO", f"{stock_no}.TW"]
    elif market_type == "上市":
        symbols = [f"{stock_no}.TW", f"{stock_no}.TWO"]
    else:
        symbols = [f"{stock_no}.TW", f"{stock_no}.TWO"]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
    }

    for symbol in symbols:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            params = {
                "period1": period1,
                "period2": period2,
                "interval": "1d",
                "events": "history",
                "includeAdjustedClose": "true",
            }
            r = requests.get(url, params=params, headers=headers, timeout=18, verify=False)
            r.raise_for_status()
            data = r.json()
            result = (((data or {}).get("chart") or {}).get("result") or [])
            if not result:
                continue

            item = result[0]
            timestamps = item.get("timestamp") or []
            quote = (((item.get("indicators") or {}).get("quote") or [{}])[0]) or {}
            if not timestamps or not quote:
                continue

            adjclose = (((item.get("indicators") or {}).get("adjclose") or [{}])[0] or {}).get("adjclose") or []
            rows = []
            for idx, ts in enumerate(timestamps):
                try:
                    d = pd.to_datetime(int(ts), unit="s").tz_localize("UTC").tz_convert("Asia/Taipei").tz_localize(None).normalize()
                except Exception:
                    d = pd.to_datetime(int(ts), unit="s")

                open_v = quote.get("open", [None] * len(timestamps))[idx]
                high_v = quote.get("high", [None] * len(timestamps))[idx]
                low_v = quote.get("low", [None] * len(timestamps))[idx]
                close_v = quote.get("close", [None] * len(timestamps))[idx]
                volume_v = quote.get("volume", [None] * len(timestamps))[idx]

                if close_v is None:
                    continue

                rows.append({
                    "日期": d,
                    "開盤價": open_v,
                    "最高價": high_v,
                    "最低價": low_v,
                    "收盤價": close_v,
                    "成交股數": volume_v,
                    "成交金額": pd.NA,
                    "成交筆數": pd.NA,
                })

            if not rows:
                continue

            df = pd.DataFrame(rows)
            for col in ["開盤價", "最高價", "最低價", "收盤價", "成交股數", "成交金額", "成交筆數"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df[(df["日期"] >= start_ts) & (df["日期"] <= end_ts)]
            df = df.sort_values("日期").drop_duplicates(subset=["日期"], keep="last").reset_index(drop=True)
            if not df.empty:
                return df, symbol
        except Exception:
            continue

    return pd.DataFrame(), ""



@st.cache_data(ttl=900, show_spinner=False)
def _get_history_from_kline_page(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date, refresh_token: str = "init") -> tuple[pd.DataFrame, str]:
    """
    最後保險：直接呼叫同資料夾的 3_歷史K線分析.py 歷史資料函式。
    因為 3 頁已確認可抓到 3548 yahoo:3548.TWO，所以 2 頁若自身來源都失敗，就沿用 3 頁已驗證邏輯。
    """
    try:
        import importlib.util
        from pathlib import Path as _Path

        page3_path = _Path(__file__).with_name("3_歷史K線分析.py")
        if not page3_path.exists():
            return pd.DataFrame(), "3頁檔案不存在"

        spec = importlib.util.spec_from_file_location("kline_page_bridge_for_quote", str(page3_path))
        if spec is None or spec.loader is None:
            return pd.DataFrame(), "3頁載入失敗"

        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        fn = getattr(mod, "_get_history_data_smart", None)
        prep = getattr(mod, "_prepare_history_df", None)
        if fn is None:
            return pd.DataFrame(), "3頁沒有 _get_history_data_smart"

        result = fn(stock_no, stock_name, market_type, start_date, end_date)

        if isinstance(result, tuple):
            df = result[0] if len(result) >= 1 else pd.DataFrame()
            source = result[2] if len(result) >= 3 else (result[1] if len(result) >= 2 else "kline_page")
        else:
            df = result
            source = "kline_page"

        if prep is not None:
            try:
                df = prep(df)
            except Exception:
                pass

        df = _prepare_history_df(df)
        if not df.empty:
            df["資料源"] = f"3頁橋接:{source}"
            return df, f"3頁橋接:{source}"

        return pd.DataFrame(), f"3頁橋接空資料:{source}"
    except Exception as e:
        return pd.DataFrame(), f"3頁橋接錯誤:{str(e)[:100]}"


@st.cache_data(ttl=900, show_spinner=False)
def _get_history_data_smart(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date, refresh_token: str = "init") -> pd.DataFrame:
    stock_no = _safe_str(stock_no)
    stock_name = _safe_str(stock_name)
    market_type = _safe_str(market_type) or "上市"
    debug_try = []

    # 1) utils + 市場交叉
    for try_name, try_market in _market_candidates_quote(stock_no, stock_name, market_type):
        try:
            df = get_history_data(
                stock_no=stock_no,
                stock_name=try_name,
                market_type=try_market,
                start_date=start_date,
                end_date=end_date,
            )
            df = _prepare_history_df(df)
            debug_try.append(f"utils:{try_market or '空'}={len(df)}")
            if not df.empty:
                df["資料源"] = f"utils:{try_market or '空'}"
                st.session_state[_k("history_debug_try")] = "｜".join(debug_try[-16:])
                return df
        except Exception as e:
            debug_try.append(f"utils:{try_market or '空'}=ERR {str(e)[:60]}")

    # 2) TWSE 直連
    try:
        df_twse = _get_twse_history_data_direct_quote(stock_no, start_date, end_date, refresh_token)
        df_twse = _prepare_history_df(df_twse)
        debug_try.append(f"twse_direct={len(df_twse)}")
        if not df_twse.empty:
            df_twse["資料源"] = "twse_direct"
            st.session_state[_k("history_debug_try")] = "｜".join(debug_try[-16:])
            return df_twse
    except Exception as e:
        debug_try.append(f"twse_direct=ERR {str(e)[:60]}")

    # 3) TPEX 直連
    try:
        df_tpex = _get_tpex_history_data(stock_no, start_date, end_date)
        df_tpex = _prepare_history_df(df_tpex)
        debug_try.append(f"tpex_direct={len(df_tpex)}")
        if not df_tpex.empty:
            df_tpex["資料源"] = "tpex_direct"
            st.session_state[_k("history_debug_try")] = "｜".join(debug_try[-16:])
            return df_tpex
    except Exception as e:
        debug_try.append(f"tpex_direct=ERR {str(e)[:60]}")

    # 4) Yahoo，沿用 3_歷史K線分析.py 的成功格式
    try:
        df_yahoo, yahoo_symbol = _get_yahoo_history_data(stock_no, market_type, start_date, end_date, refresh_token)
        df_yahoo = _prepare_history_df(df_yahoo)
        debug_try.append(f"yahoo:{yahoo_symbol or '-'}={len(df_yahoo)}")
        if not df_yahoo.empty:
            df_yahoo["資料源"] = f"yahoo:{yahoo_symbol}"
            st.session_state[_k("history_debug_try")] = "｜".join(debug_try[-16:])
            return df_yahoo
    except Exception as e:
        debug_try.append(f"yahoo=ERR {str(e)[:80]}")

    # 5) 最後保險：直接橋接 3_歷史K線分析.py，使用該頁已驗證可成功的歷史資料邏輯。
    try:
        df_kline, kline_source = _get_history_from_kline_page(stock_no, stock_name, market_type, start_date, end_date, refresh_token)
        df_kline = _prepare_history_df(df_kline)
        debug_try.append(f"kline_page:{kline_source}={len(df_kline)}")
        if not df_kline.empty:
            df_kline["資料源"] = kline_source or "3頁橋接"
            st.session_state[_k("history_debug_try")] = "｜".join(debug_try[-16:])
            return df_kline
    except Exception as e:
        debug_try.append(f"kline_page=ERR {str(e)[:80]}")

    st.session_state[_k("history_debug_try")] = "｜".join(debug_try[-16:])
    return pd.DataFrame()


# =========================================================
# 群組 / 搜尋
# =========================================================
def _build_group_stock_map() -> dict[str, list[dict[str, str]]]:
    watchlist = get_normalized_watchlist()

    watchlist_items = []
    if isinstance(watchlist, dict):
        for group_name, items in watchlist.items():
            temp_items = []
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    temp_items.append(
                        (
                            _safe_str(item.get("code")),
                            _safe_str(item.get("name")),
                            _safe_str(item.get("market")),
                        )
                    )
            watchlist_items.append((group_name, tuple(temp_items)))

    group_map = _get_group_stock_map_cached(tuple(watchlist_items))

    if not group_map:
        try:
            all_df = get_all_code_name_map("")
            if isinstance(all_df, pd.DataFrame) and not all_df.empty:
                rows = []
                sample_df = all_df.head(150)
                for _, row in sample_df.iterrows():
                    code = _safe_str(row.get("code"))
                    name = _safe_str(row.get("name")) or code
                    market = _safe_str(row.get("market")) or "上市"
                    if code:
                        rows.append(
                            {
                                "code": code,
                                "name": name,
                                "market": market,
                                "label": f"{code} {name}",
                            }
                        )
                if rows:
                    group_map["全部股票"] = rows
        except Exception:
            pass

    return group_map


def _flatten_group_map(group_map: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    group_items = tuple((group_name, tuple(items)) for group_name, items in group_map.items())
    return _flatten_group_map_cached(group_items)


def _find_search_target(keyword: str, flat_rows: list[dict[str, str]]) -> dict[str, str] | None:
    q = _safe_str(keyword).lower()
    if not q:
        return None

    exact_code = next((row for row in flat_rows if q == row["code"].lower()), None)
    if exact_code:
        return exact_code

    exact_name = next((row for row in flat_rows if q == row["name"].lower()), None)
    if exact_name:
        return exact_name

    exact_label = next((row for row in flat_rows if q == row["label"].lower()), None)
    if exact_label:
        return exact_label

    prefix_hit = next(
        (r for r in flat_rows if r["code"].lower().startswith(q) or r["name"].lower().startswith(q)),
        None,
    )
    if prefix_hit:
        return prefix_hit

    contain_hit = next(
        (r for r in flat_rows if q in f"{r['group']} {r['code']} {r['name']} {r['label']}".lower()),
        None,
    )
    if contain_hit:
        return contain_hit

    return None


# =========================================================
# State
# =========================================================
def _init_state(group_map: dict[str, list[dict[str, str]]]):
    saved = load_last_query_state()
    groups = list(group_map.keys())
    today = date.today()
    default_start = today - timedelta(days=180)
    default_end = today

    if _k("refresh_token") not in st.session_state:
        st.session_state[_k("refresh_token")] = "init"

    if _k("group") not in st.session_state:
        saved_group = _safe_str(saved.get("quick_group", ""))
        st.session_state[_k("group")] = saved_group if saved_group in groups else (groups[0] if groups else "")

    if _k("stock_code") not in st.session_state:
        st.session_state[_k("stock_code")] = _safe_str(saved.get("quick_stock_code", ""))

    if _k("search_input") not in st.session_state:
        st.session_state[_k("search_input")] = ""

    if _k("start_date") not in st.session_state:
        st.session_state[_k("start_date")] = parse_date_safe(saved.get("home_start"), default_start)

    if _k("end_date") not in st.session_state:
        st.session_state[_k("end_date")] = parse_date_safe(saved.get("home_end"), default_end)

    _repair_state(group_map)


def _repair_state(group_map: dict[str, list[dict[str, str]]]):
    groups = list(group_map.keys())
    current_group = _safe_str(st.session_state.get(_k("group"), ""))

    if current_group not in group_map:
        st.session_state[_k("group")] = groups[0] if groups else ""
        current_group = st.session_state[_k("group")]

    items = group_map.get(current_group, [])
    valid_codes = [x["code"] for x in items]
    current_code = _safe_str(st.session_state.get(_k("stock_code"), ""))

    if valid_codes:
        if current_code not in valid_codes:
            st.session_state[_k("stock_code")] = valid_codes[0]
    else:
        st.session_state[_k("stock_code")] = ""


def _on_group_change(group_map: dict[str, list[dict[str, str]]]):
    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    st.session_state[_k("stock_code")] = items[0]["code"] if items else ""



def _resolve_market_from_master_quote(stock_no: str, stock_name: str, fallback_market: str) -> tuple[str, str]:
    """用股票主檔再校正市場別；避免 3548 這類上櫃股被自選股資料誤標成上市。"""
    code = _safe_str(stock_no)
    name = _safe_str(stock_name)
    market = _safe_str(fallback_market) or "上市"
    try:
        nm, mk = get_stock_name_and_market(code)
        if _safe_str(nm):
            name = _safe_str(nm)
        if _safe_str(mk):
            market = _safe_str(mk)
    except Exception:
        pass
    return name, market


def _market_candidates_quote(stock_no: str, stock_name: str, market_type: str) -> list[tuple[str, str]]:
    real_name, real_market = _resolve_market_from_master_quote(stock_no, stock_name, market_type)
    raw = [
        (real_name, real_market),
        (stock_name or real_name, market_type),
        (real_name, "上櫃"),
        (real_name, "上市"),
        (real_name, "興櫃"),
        (real_name, ""),
        (stock_name or real_name, ""),
    ]
    out = []
    seen = set()
    for nm, mk in raw:
        key = (_safe_str(nm), _safe_str(mk))
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


@st.cache_data(ttl=900, show_spinner=False)
def _get_twse_history_data_direct_quote(stock_no: str, start_date: date, end_date: date, refresh_token: str = "init") -> pd.DataFrame:
    stock_no = _safe_str(stock_no)
    if not stock_no:
        return pd.DataFrame()

    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    if end_ts < start_ts:
        return pd.DataFrame()

    month_starts = pd.date_range(start=start_ts.replace(day=1), end=end_ts, freq="MS")
    frames = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Referer": "https://www.twse.com.tw/",
        "Accept": "application/json,text/plain,*/*",
    }

    urls = [
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY",
        "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY",
    ]

    for dt in month_starts:
        month_str = dt.strftime("%Y%m01")
        for url in urls:
            try:
                r = requests.get(
                    url,
                    params={"response": "json", "date": month_str, "stockNo": stock_no},
                    headers=headers,
                    timeout=15,
                    verify=False,
                )
                if r.status_code != 200:
                    continue
                data = r.json()
                raw_rows = data.get("data") or []
                fields = data.get("fields") or []
                if not raw_rows:
                    continue
                temp = pd.DataFrame(raw_rows, columns=fields if fields and len(fields) == len(raw_rows[0]) else None)
                frames.append(temp)
                break
            except Exception:
                continue

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    rename_map = {}
    for col in df.columns:
        c = _safe_str(col)
        if "日期" in c:
            rename_map[col] = "日期"
        elif "成交股數" in c:
            rename_map[col] = "成交股數"
        elif "成交金額" in c:
            rename_map[col] = "成交金額"
        elif "開盤" in c:
            rename_map[col] = "開盤價"
        elif "最高" in c:
            rename_map[col] = "最高價"
        elif "最低" in c:
            rename_map[col] = "最低價"
        elif "收盤" in c:
            rename_map[col] = "收盤價"
        elif "成交筆數" in c:
            rename_map[col] = "成交筆數"

    df = df.rename(columns=rename_map)
    if "日期" not in df.columns:
        return pd.DataFrame()

    def convert_roc_date(x):
        x = _safe_str(x)
        if not x:
            return pd.NaT
        parts = x.split("/")
        if len(parts) == 3:
            try:
                y = int(parts[0])
                if y < 1911:
                    y += 1911
                return pd.Timestamp(year=y, month=int(parts[1]), day=int(parts[2]))
            except Exception:
                return pd.NaT
        return pd.to_datetime(x, errors="coerce")

    df["日期"] = df["日期"].apply(convert_roc_date)
    df = df.dropna(subset=["日期"])
    for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("X", "", regex=False)
                .str.replace("+", "", regex=False)
                .str.replace(" ", "", regex=False)
                .replace(["--", "---", "", "----"], pd.NA)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[(df["日期"] >= start_ts) & (df["日期"] <= end_ts)]
    df = df.sort_values("日期").drop_duplicates(subset=["日期"], keep="last").reset_index(drop=True)
    return df


# =========================================================
# 歷史資料整理
# =========================================================
def _prepare_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "日期" not in df.columns:
        return pd.DataFrame()

    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    numeric_cols = ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "收盤價" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["收盤價"]).copy()
    if df.empty:
        return pd.DataFrame()

    close = df["收盤價"]
    high = df["最高價"] if "最高價" in df.columns else close
    low = df["最低價"] if "最低價" in df.columns else close

    for n in [5, 10, 20, 60, 120, 240]:
        df[f"MA{n}"] = close.rolling(n).mean()

    low_9 = low.rolling(9).min()
    high_9 = high.rolling(9).max()
    rsv = (close - low_9) / (high_9 - low_9).replace(0, pd.NA) * 100
    df["K"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    df["D"] = df["K"].ewm(alpha=1 / 3, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["DIF"] - df["DEA"]

    return df


# =========================================================
# 事件摘要
# =========================================================
def _build_recent_event_summary(df: pd.DataFrame) -> list[tuple[str, str, str]]:
    if df is None or df.empty or len(df) < 3:
        return [("最近事件", "資料不足", "")]

    rows: list[tuple[str, str, str]] = []

    try:
        last = df.iloc[-1]
        prev = df.iloc[-2]

        date_text = pd.to_datetime(last["日期"]).strftime("%Y-%m-%d")

        ma5 = _safe_float(last.get("MA5"))
        ma10 = _safe_float(last.get("MA10"))
        prev_ma5 = _safe_float(prev.get("MA5"))
        prev_ma10 = _safe_float(prev.get("MA10"))

        if ma5 is not None and ma10 is not None and prev_ma5 is not None and prev_ma10 is not None:
            if prev_ma5 <= prev_ma10 and ma5 > ma10:
                rows.append((date_text, "MA黃金交叉", ""))
            elif prev_ma5 >= prev_ma10 and ma5 < ma10:
                rows.append((date_text, "MA死亡交叉", ""))

        k = _safe_float(last.get("K"))
        d = _safe_float(last.get("D"))
        prev_k = _safe_float(prev.get("K"))
        prev_d = _safe_float(prev.get("D"))
        if k is not None and d is not None and prev_k is not None and prev_d is not None:
            if prev_k <= prev_d and k > d:
                rows.append((date_text, "KD黃金交叉", ""))
            elif prev_k >= prev_d and k < d:
                rows.append((date_text, "KD死亡交叉", ""))

        dif = _safe_float(last.get("DIF"))
        dea = _safe_float(last.get("DEA"))
        prev_dif = _safe_float(prev.get("DIF"))
        prev_dea = _safe_float(prev.get("DEA"))
        if dif is not None and dea is not None and prev_dif is not None and prev_dea is not None:
            if prev_dif <= prev_dea and dif > dea:
                rows.append((date_text, "MACD黃金交叉", ""))
            elif prev_dif >= prev_dea and dif < dea:
                rows.append((date_text, "MACD死亡交叉", ""))

        if len(df) >= 20 and all(c in df.columns for c in ["最高價", "最低價", "收盤價"]):
            df20 = df.tail(20)
            high20 = _safe_float(df20["最高價"].max())
            low20 = _safe_float(df20["最低價"].min())
            close_price = _safe_float(last.get("收盤價"))
            if close_price is not None and high20 is not None and close_price >= high20:
                rows.append((date_text, "突破20日高", ""))
            if close_price is not None and low20 is not None and close_price <= low20:
                rows.append((date_text, "跌破20日低", ""))

        ma20 = _safe_float(last.get("MA20"))
        if ma20 is not None:
            close_price = _safe_float(last.get("收盤價"))
            if close_price is not None:
                if close_price > ma20:
                    rows.append((date_text, "站上MA20", ""))
                else:
                    rows.append((date_text, "跌落MA20下", ""))
    except Exception:
        pass

    if not rows:
        return [("最近事件", "目前無明確新事件", "")]
    return rows[:6]


# =========================================================
# 即時卡片
# =========================================================
def _render_realtime_hero(info: dict[str, Any], stock_label: str, market_type: str):
    price = _safe_float(info.get("price"))
    prev_close = _safe_float(info.get("prev_close"))
    open_price = _safe_float(info.get("open"))
    high_price = _safe_float(info.get("high"))
    low_price = _safe_float(info.get("low"))
    change = _safe_float(info.get("change"))
    change_pct = _safe_float(info.get("change_pct"))
    total_volume = _safe_float(info.get("total_volume"))
    update_time = _safe_str(info.get("update_time"))
    price_source = _safe_str(info.get("price_source"))
    change_source = _safe_str(info.get("change_source"))

    source_map = {
        "trade": "成交價",
        "match": "撮合價",
        "mid": "買賣中間價",
        "bid": "買進價",
        "ask": "賣出價",
        "prev_close": "昨收回退",
        "none": "無",
    }
    change_map = {
        "realtime_vs_prev": "即時價對昨收",
        "prev_close_missing": "缺昨收",
    }

    delta_text = "—"
    if change is not None and change_pct is not None:
        delta_text = f"{change:+.2f} ({change_pct:+.2f}%)"
    elif change is not None:
        delta_text = f"{change:+.2f}"

    render_pro_kpi_row(
        [
            {
                "label": "現價",
                "value": format_number(price, 2),
                "delta": delta_text,
                "delta_class": "pro-kpi-delta-up" if (change or 0) > 0 else ("pro-kpi-delta-down" if (change or 0) < 0 else "pro-kpi-delta-flat"),
            },
            {
                "label": "開盤",
                "value": format_number(open_price, 2),
                "delta": market_type,
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "最高 / 最低",
                "value": f"{format_number(high_price, 2)} / {format_number(low_price, 2)}",
                "delta": f"昨收 {format_number(prev_close, 2)}",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "總量",
                "value": format_number(total_volume, 0),
                "delta": update_time or "—",
                "delta_class": "pro-kpi-delta-flat",
            },
        ]
    )

    _html(
        f"""
        <div style="background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);border-radius:18px;padding:14px 16px;margin-bottom:14px;">
            <div style="font-size:22px;font-weight:900;color:#f8fafc;">{stock_label}</div>
            <div style="font-size:12px;color:#cbd5e1;margin-top:4px;">
            市場：{market_type}｜更新時間：{update_time or '—'}｜價格來源：{source_map.get(price_source, price_source or '—')}｜漲跌來源：{change_map.get(change_source, change_source or '—')}
        </div>
        </div>
        """
    )


# =========================================================
# 主頁
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    group_map = _build_group_stock_map()
    flat_rows = _flatten_group_map(group_map)
    _init_state(group_map)

    render_pro_hero(
        title="行情查詢｜股神版",
        subtitle="單股即時資訊、訊號燈號、支撐壓力、最近事件摘要，一頁快速看懂。",
    )

    render_pro_section("快速搜尋股票")
    s1, s2 = st.columns([5, 1])

    with s1:
        st.text_input(
            "輸入股票代碼或名稱",
            key=_k("search_input"),
            placeholder="例如：2330、台積電、3548 兆利",
            label_visibility="collapsed",
        )

    with s2:
        if st.button("帶入", use_container_width=True, type="primary"):
            target = _find_search_target(st.session_state.get(_k("search_input"), ""), flat_rows)
            if target:
                st.session_state[_k("group")] = target["group"]
                st.session_state[_k("stock_code")] = target["code"]
                save_last_query_state(
                    quick_group=target["group"],
                    quick_stock_code=target["code"],
                    home_start=st.session_state.get(_k("start_date")),
                    home_end=st.session_state.get(_k("end_date")),
                )
                st.rerun()
            else:
                st.warning("找不到對應股票。")

    render_pro_section("查詢條件")
    _repair_state(group_map)

    refresh_cols = st.columns([1.2, 5])
    with refresh_cols[0]:
        if st.button("更新即時資料", use_container_width=True, type="primary", key=_k("refresh_btn")):
            st.session_state[_k("refresh_token")] = str(int(time.time() * 1000))
            # 清掉之前抓不到歷史資料時留下的空快取，避免修正後仍讀到舊空資料。
            for _func in [_get_history_data_smart, _get_yahoo_history_data, _get_tpex_history_data, _get_twse_history_data_direct_quote, _get_history_from_kline_page]:
                try:
                    _func.clear()
                except Exception:
                    pass
    with refresh_cols[1]:
        st.caption(f"刷新識別：{st.session_state.get(_k('refresh_token'), 'init')}")

    groups = list(group_map.keys())
    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    code_to_item = {x["code"]: x for x in items}
    code_options = [x["code"] for x in items]

    c1, c2 = st.columns([2, 3])
    with c1:
        st.selectbox("選擇群組", options=groups, key=_k("group"), on_change=_on_group_change, args=(group_map,))
    with c2:
        st.selectbox(
            "群組股票",
            options=code_options if code_options else [""],
            key=_k("stock_code"),
            format_func=lambda code: code_to_item.get(code, {}).get("label", code),
        )

    selected_group = _safe_str(st.session_state.get(_k("group"), ""))
    selected_code = _safe_str(st.session_state.get(_k("stock_code"), ""))

    if not selected_code or selected_code not in code_to_item:
        st.warning("請先選擇股票。")
        st.stop()

    selected_item = code_to_item[selected_code]
    stock_name = _safe_str(selected_item.get("name"))
    market_type = _safe_str(selected_item.get("market")) or "上市"
    stock_label = f"{selected_code} {stock_name}"

    start_date = _to_date(st.session_state.get(_k("start_date")), date.today() - timedelta(days=180))
    end_date = _to_date(st.session_state.get(_k("end_date")), date.today())

    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code=selected_code,
        home_start=start_date,
        home_end=end_date,
    )

    all_code_name_df = get_all_code_name_map("")
    stock_name2, market_type2 = get_stock_name_and_market(selected_code, all_code_name_df, stock_name)
    final_name = stock_name2 or stock_name
    final_market = market_type2 or market_type

    with st.spinner("載入即時資料中..."):
        info = get_realtime_stock_info(
            selected_code,
            final_name,
            final_market,
            refresh_token=st.session_state.get(_k("refresh_token"), "init"),
        )

    history_df = _get_history_data_smart(
        stock_no=selected_code,
        stock_name=final_name,
        market_type=final_market,
        start_date=start_date,
        end_date=end_date,
        refresh_token=st.session_state.get(_k("refresh_token"), "init"),
    )

    signal_snapshot = compute_signal_snapshot(history_df) if not history_df.empty else {}
    sr_snapshot = compute_support_resistance_snapshot(history_df) if not history_df.empty else {}
    badge_text, _ = score_to_badge(signal_snapshot.get("score", 0)) if signal_snapshot else ("整理", "pro-flat")
    recent_events = _build_recent_event_summary(history_df)

    history_source = "none"
    try:
        if isinstance(history_df, pd.DataFrame) and not history_df.empty and "資料源" in history_df.columns:
            history_source = _safe_str(history_df["資料源"].dropna().iloc[-1]) or "unknown"
    except Exception:
        history_source = "unknown"

    _render_realtime_hero(info, f"{selected_code} {final_name}", final_market)

    if history_df.empty:
        st.warning(
            f"技術訊號歷史資料暫時抓不到：{selected_code} {final_name} / 市場 {final_market}。"
            "已嘗試 utils / TWSE / TPEX / Yahoo fallback / 3頁橋接。即時報價仍可使用。"
            "請先按一次「更新即時資料」清除舊空快取；若 3_歷史K線分析可抓到，2頁會同步使用相同 Yahoo 邏輯。"
        )
        with st.expander("歷史資料抓取除錯", expanded=False):
            st.write(st.session_state.get(_k("history_debug_try"), "尚無除錯資料"))
    else:
        st.caption(f"技術訊號資料源：{history_source}｜歷史筆數：{len(history_df)}")
        with st.expander("歷史資料抓取除錯", expanded=False):
            st.write(st.session_state.get(_k("history_debug_try"), ""))

    # 版面防重疊：
    # 原本這裡用左右欄並排，當螢幕寬度、字體比例或卡片內容較長時，
    # 左側卡片會壓到右側卡片。這版改為上下分區，保留全部資訊但不重疊。
    render_pro_section("股神即時判讀", "訊號、支撐壓力、事件與操作提醒分區顯示，避免資訊互相覆蓋。")

    _render_info_card_safe(
        "訊號燈號",
        [
            ("燈號", badge_text, ""),
            ("均線趨勢", _safe_str(signal_snapshot.get("ma_trend", ("—", ""))[0]), ""),
            ("KD交叉", _safe_str(signal_snapshot.get("kd_cross", ("—", ""))[0]), ""),
            ("MACD趨勢", _safe_str(signal_snapshot.get("macd_trend", ("—", ""))[0]), ""),
            ("價位狀態", _safe_str(signal_snapshot.get("price_vs_ma20", ("—", ""))[0]), ""),
            ("量能狀態", _safe_str(signal_snapshot.get("volume_state", ("—", ""))[0]), ""),
        ],
        chips=[badge_text],
    )

    _render_info_card_safe(
        "支撐壓力",
        [
            ("20日壓力", format_number(sr_snapshot.get("res_20"), 2), ""),
            ("20日支撐", format_number(sr_snapshot.get("sup_20"), 2), ""),
            ("60日壓力", format_number(sr_snapshot.get("res_60"), 2), ""),
            ("60日支撐", format_number(sr_snapshot.get("sup_60"), 2), ""),
            ("壓力訊號", _safe_str(sr_snapshot.get("pressure_signal", ("—", ""))[0]), ""),
            ("支撐訊號", _safe_str(sr_snapshot.get("support_signal", ("—", ""))[0]), ""),
            ("區間判斷", _safe_str(sr_snapshot.get("break_signal", ("—", ""))[0]), ""),
        ],
        chips=["結構位階"],
    )

    _render_info_card_safe(
        "最近事件摘要",
        recent_events,
        chips=[final_market],
    )

    _render_info_card_safe(
        "股神快速判讀",
        [
            ("目前結論", _safe_str(signal_snapshot.get("comment", "資料不足")), ""),
            ("趨勢觀察", _safe_str(sr_snapshot.get("comment_trend", "資料不足")), ""),
            ("風險提醒", _safe_str(sr_snapshot.get("comment_risk", "資料不足")), ""),
            ("焦點重點", _safe_str(sr_snapshot.get("comment_focus", "資料不足")), ""),
            ("操作提醒", _safe_str(sr_snapshot.get("comment_action", "資料不足")), ""),
        ],
        chips=[badge_text, final_market],
    )

    with st.expander("原始即時資料"):
        raw_df = pd.DataFrame(
            [
                {
                    "股票代號": selected_code,
                    "股票名稱": final_name,
                    "市場別": final_market,
                    "現價": info.get("price"),
                    "昨收": info.get("prev_close"),
                    "開盤": info.get("open"),
                    "最高": info.get("high"),
                    "最低": info.get("low"),
                    "漲跌": info.get("change"),
                    "漲跌幅(%)": info.get("change_pct"),
                    "總量": info.get("total_volume"),
                    "單量": info.get("trade_volume"),
                    "價格來源": info.get("price_source"),
                    "漲跌來源": info.get("change_source"),
                    "更新時間": info.get("update_time"),
                    "是否成功": info.get("ok"),
                    "訊息": info.get("message"),
                }
            ]
        )
        st.dataframe(raw_df, use_container_width=True, hide_index=True)

    with st.expander("效能說明"):
        st.write("1. 即時資料與歷史資料分開處理。")
        st.write("2. 歷史資料用 cache，避免每次重抓。")
        st.write("3. 上櫃股票會自動嘗試 fallback。")
        st.write("4. 群組與股票選擇以 session_state 真同步。")
        st.write("5. 股票主檔只抓一次，不重複取得。")
        st.write("6. 群組展平與搜尋映射已加快取。")


if __name__ == "__main__":
    main()
