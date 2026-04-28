from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import streamlit as st

from utils import (
    compute_radar_scores,
    compute_signal_snapshot,
    compute_support_resistance_snapshot,
    format_number,
    get_all_code_name_map,
    get_history_data,
    get_normalized_watchlist,
    inject_pro_theme,
    load_last_query_state,
    parse_date_safe,
    render_pro_hero,
    render_pro_info_card as _utils_render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
    save_last_query_state,
    score_to_badge,
)

PAGE_TITLE = "歷史K線分析"
PFX = "hk_"


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



def _ensure_radar_dict(radar_obj: Any) -> dict[str, Any]:
    """
    相容 utils.compute_radar_scores 不同版本：
    - 正常新版：dict
    - 舊版 / 異常：None、tuple、list、字串
    避免頁面在 radar.get(...) 時 AttributeError。
    """
    default = {
        "trend": 50,
        "momentum": 50,
        "volume": 50,
        "position": 50,
        "structure": 50,
        "summary": "雷達資料異常，已用中性分數保護顯示。",
    }

    if isinstance(radar_obj, dict):
        out = default.copy()
        out.update(radar_obj)
        return out

    if isinstance(radar_obj, (list, tuple)):
        out = default.copy()
        keys = ["trend", "momentum", "volume", "position", "structure"]
        for i, key in enumerate(keys):
            if i < len(radar_obj):
                try:
                    out[key] = float(radar_obj[i])
                except Exception:
                    pass
        if len(radar_obj) > 5:
            out["summary"] = str(radar_obj[5])
        else:
            out["summary"] = "雷達回傳格式為 list/tuple，已自動轉換。"
        return out

    if radar_obj is None:
        return default

    out = default.copy()
    out["summary"] = str(radar_obj)
    return out

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


# =========================================================
# 本頁專用卡片渲染防呆
# 目的：完全避開舊版 utils.render_pro_info_card 造成 HTML 殘片 </div> 顯示在畫面上的問題。
# 保留同名函式，下面所有原本呼叫不用改。
# =========================================================
def _strip_html_artifact(v: Any, default: str = "—") -> str:
    import html as _html_lib
    import re

    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass

    s = str(v).strip()
    if not s or s.lower() in {"none", "nan", "null"}:
        return default

    low = s.lower()
    if "<div" in low or "</div" in low or "class=\"pro-" in low or "class='pro-" in low:
        return default

    s = re.sub(r"<[^>]+>", "", s).strip()
    if not s:
        return default
    return _html_lib.escape(s)


def render_pro_info_card(title, info_pairs, chips=None):
    """本頁安全版資訊卡：重建乾淨 HTML，並過濾所有 HTML 殘片。"""
    safe_title = _strip_html_artifact(title, "")

    chips_html = ""
    if chips:
        if isinstance(chips, str):
            chips_iter = [chips]
        else:
            try:
                chips_iter = list(chips)
            except Exception:
                chips_iter = [chips]
        chip_parts = []
        for c in chips_iter:
            cs = _strip_html_artifact(c, "")
            if cs:
                chip_parts.append(f'<span class="pro-chip">{cs}</span>')
        chips_html = "".join(chip_parts)

    items_html = ""
    if info_pairs is None:
        info_pairs = []

    for item in info_pairs:
        label, value, css_class = "", "—", ""
        try:
            if isinstance(item, dict):
                label = item.get("label", "")
                value = item.get("value", "—")
                css_class = item.get("css_class", "") or item.get("class", "")
            elif isinstance(item, (list, tuple)):
                if len(item) >= 1:
                    label = item[0]
                if len(item) >= 2:
                    value = item[1]
                if len(item) >= 3:
                    css_class = item[2]
            else:
                label = "項目"
                value = item
        except Exception:
            label, value, css_class = "項目", item, ""

        safe_label = _strip_html_artifact(label, "—")
        safe_value = _strip_html_artifact(value, "—")
        css = str(css_class or "").strip()
        if css not in {"pro-up", "pro-down", "pro-flat", ""}:
            css = ""

        items_html += (
            '<div class="pro-info-item">'
            f'<div class="pro-info-label">{safe_label}</div>'
            f'<div class="pro-info-value {css}">{safe_value}</div>'
            '</div>'
        )

    if not items_html.strip():
        items_html = '<div class="pro-info-item"><div class="pro-info-label">狀態</div><div class="pro-info-value">—</div></div>'

    card_html = (
        '<div class="pro-card">'
        f'<div class="pro-card-title">{safe_title}</div>'
        f'<div style="margin-bottom:10px;">{chips_html}</div>'
        f'<div class="pro-info-grid">{items_html}</div>'
        '</div>'
    )
    st.markdown(card_html, unsafe_allow_html=True)


# =========================================================
# 快取輔助
# =========================================================
@st.cache_data(ttl=120, show_spinner=False)
def _flatten_group_map_cached(group_items: tuple) -> list[dict[str, str]]:
    rows = []
    for group_name, items in group_items:
        g = _safe_str(group_name)
        for item in items:
            if not isinstance(item, tuple) or len(item) < 4:
                continue
            rows.append(
                {
                    "group": g,
                    "code": _safe_str(item[0]),
                    "name": _safe_str(item[1]),
                    "market": _safe_str(item[2]),
                    "label": _safe_str(item[3]),
                }
            )
    return rows


@st.cache_data(ttl=120, show_spinner=False)
def _build_group_stock_map_cached(watchlist_items: tuple) -> dict[str, list[dict[str, str]]]:
    group_map: dict[str, list[dict[str, str]]] = {}

    for group_name, items in watchlist_items:
        g = _safe_str(group_name) or "未分組"
        rows = []
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


def _pack_group_map(group_map: dict[str, list[dict[str, str]]]) -> tuple:
    packed = []
    for group_name, items in group_map.items():
        temp = []
        for item in items:
            temp.append(
                (
                    _safe_str(item.get("code")),
                    _safe_str(item.get("name")),
                    _safe_str(item.get("market")),
                    _safe_str(item.get("label")),
                )
            )
        packed.append((group_name, tuple(temp)))
    return tuple(packed)


# =========================================================
# watchlist 真同步
# =========================================================
def _get_watchlist_source() -> dict:
    shared = st.session_state.get("watchlist_data")
    if isinstance(shared, dict) and shared:
        return shared

    raw = get_normalized_watchlist()
    if isinstance(raw, dict):
        return raw

    return {}


def _sync_watchlist_meta():
    if _k("watchlist_version_seen") not in st.session_state:
        st.session_state[_k("watchlist_version_seen")] = st.session_state.get("watchlist_version", 0)

    if _k("watchlist_saved_at_seen") not in st.session_state:
        st.session_state[_k("watchlist_saved_at_seen")] = st.session_state.get("watchlist_last_saved_at", "")

    if _k("watchlist_hash_seen") not in st.session_state:
        st.session_state[_k("watchlist_hash_seen")] = st.session_state.get("watchlist_last_saved_hash", "")


def _build_group_stock_map() -> dict[str, list[dict[str, str]]]:
    watchlist = _get_watchlist_source()
    packed = []

    if isinstance(watchlist, dict):
        for group_name, items in watchlist.items():
            temp = []
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    temp.append(
                        (
                            _safe_str(item.get("code")),
                            _safe_str(item.get("name")),
                            _safe_str(item.get("market")),
                        )
                    )
            packed.append((group_name, tuple(temp)))

    group_map = _build_group_stock_map_cached(tuple(packed))

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
    return _flatten_group_map_cached(_pack_group_map(group_map))


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


def _init_state(group_map: dict[str, list[dict[str, str]]]):
    _sync_watchlist_meta()

    saved = load_last_query_state()
    today = date.today()
    default_start = today - timedelta(days=365)
    default_end = today
    groups = list(group_map.keys())

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

    if _k("event_filter") not in st.session_state:
        st.session_state[_k("event_filter")] = "全部"

    if _k("focus_event_idx") not in st.session_state:
        st.session_state[_k("focus_event_idx")] = -1

    if _k("focus_window") not in st.session_state:
        st.session_state[_k("focus_window")] = "全部"

    if _k("show_ma") not in st.session_state:
        st.session_state[_k("show_ma")] = True

    if _k("show_pivots") not in st.session_state:
        st.session_state[_k("show_pivots")] = True

    if _k("left_panel_limit") not in st.session_state:
        st.session_state[_k("left_panel_limit")] = 12

    st.session_state[_k("start_date")] = _to_date(st.session_state.get(_k("start_date")), default_start)
    st.session_state[_k("end_date")] = _to_date(st.session_state.get(_k("end_date")), default_end)

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


def _apply_watchlist_sync_if_needed(group_map: dict[str, list[dict[str, str]]]) -> bool:
    current_version = st.session_state.get("watchlist_version", 0)
    current_saved_at = st.session_state.get("watchlist_last_saved_at", "")
    current_hash = st.session_state.get("watchlist_last_saved_hash", "")

    old_version = st.session_state.get(_k("watchlist_version_seen"), 0)
    old_saved_at = st.session_state.get(_k("watchlist_saved_at_seen"), "")
    old_hash = st.session_state.get(_k("watchlist_hash_seen"), "")

    changed = (
        current_version != old_version
        or current_saved_at != old_saved_at
        or current_hash != old_hash
    )

    if changed:
        st.session_state[_k("watchlist_version_seen")] = current_version
        st.session_state[_k("watchlist_saved_at_seen")] = current_saved_at
        st.session_state[_k("watchlist_hash_seen")] = current_hash
        _repair_state(group_map)
        return True

    return False


def _on_group_change(group_map: dict[str, list[dict[str, str]]]):
    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    st.session_state[_k("stock_code")] = items[0]["code"] if items else ""
    st.session_state[_k("focus_event_idx")] = -1


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
    volume = pd.to_numeric(df["成交股數"], errors="coerce") if "成交股數" in df.columns else pd.Series(index=df.index, dtype=float)

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

    df["漲跌幅(%)"] = close.pct_change() * 100
    df["VOL5"] = volume.rolling(5).mean()
    df["VOL20"] = volume.rolling(20).mean()

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["ATR14"] = tr.rolling(14).mean()

    return df


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
            temp = pd.DataFrame(
                aa_data,
                columns=fields if fields and len(fields) == len(aa_data[0]) else None,
            )
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


@st.cache_data(ttl=1800, show_spinner=False)
def _load_master_stock_df() -> pd.DataFrame:
    dfs = []
    for market_arg in ["", "上市", "上櫃", "興櫃"]:
        try:
            df = get_all_code_name_map(market_arg)
            if isinstance(df, pd.DataFrame) and not df.empty:
                temp = df.copy()
                for col in ["code", "name", "market"]:
                    if col not in temp.columns:
                        temp[col] = ""
                temp["code"] = temp["code"].astype(str).str.strip()
                temp["name"] = temp["name"].astype(str).str.strip()
                temp["market"] = temp["market"].astype(str).str.strip()
                if market_arg in ["上市", "上櫃", "興櫃"]:
                    temp["market"] = temp["market"].replace("", market_arg)
                dfs.append(temp[["code", "name", "market"]])
        except Exception:
            pass

    if not dfs:
        return pd.DataFrame(columns=["code", "name", "market"])

    out = pd.concat(dfs, ignore_index=True)
    out["code"] = out["code"].astype(str).str.strip()
    out["name"] = out["name"].astype(str).str.strip()
    out["market"] = out["market"].astype(str).str.strip().replace("", "上市")
    out = out[out["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out


def _resolve_market_from_master(stock_no: str, stock_name: str, market_type: str) -> tuple[str, str]:
    stock_no = _safe_str(stock_no)
    stock_name = _safe_str(stock_name)
    market_type = _safe_str(market_type)

    master = _load_master_stock_df()
    if not master.empty:
        matched = master[master["code"].astype(str) == stock_no]
        if not matched.empty:
            row = matched.iloc[0]
            real_name = _safe_str(row.get("name")) or stock_name or stock_no
            real_market = _safe_str(row.get("market")) or market_type or "上市"
            return real_name, real_market

        matched2 = master[master["name"].astype(str) == stock_name]
        if not matched2.empty:
            row = matched2.iloc[0]
            real_name = _safe_str(row.get("name")) or stock_name or stock_no
            real_market = _safe_str(row.get("market")) or market_type or "上市"
            return real_name, real_market

    return stock_name or stock_no, market_type or "上市"


def _market_candidates(stock_no: str, stock_name: str, market_type: str) -> list[tuple[str, str]]:
    real_name, real_market = _resolve_market_from_master(stock_no, stock_name, market_type)

    candidates = []
    raw = [
        (real_name, real_market),
        (stock_name or real_name, market_type),
        (real_name, "上市"),
        (real_name, "上櫃"),
        (real_name, "興櫃"),
        (real_name, ""),
        (stock_name or real_name, ""),
    ]

    seen = set()
    for nm, mk in raw:
        key = (_safe_str(nm), _safe_str(mk))
        if key in seen:
            continue
        seen.add(key)
        candidates.append(key)

    return candidates


@st.cache_data(ttl=1800, show_spinner=False)
def _get_twse_history_data_direct(stock_no: str, start_date: date, end_date: date) -> pd.DataFrame:
    """上市股票歷史日線直連備援：不依賴 utils，避免共用層失敗時整頁查無資料。"""
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
                r.raise_for_status()
                data = r.json()
                stat = _safe_str(data.get("stat"))
                raw_rows = data.get("data", []) or []
                fields = data.get("fields", []) or []
                if "OK" not in stat.upper() or not raw_rows or not fields:
                    continue
                temp = pd.DataFrame(raw_rows, columns=fields if len(fields) == len(raw_rows[0]) else None)
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
        if c in ["日期", "日 期"]:
            rename_map[col] = "日期"
        elif "成交股數" in c or "成交量" in c:
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

    def convert_tw_date(x):
        x = _safe_str(x)
        if not x:
            return pd.NaT
        if "/" in x:
            parts = x.split("/")
            if len(parts) == 3:
                try:
                    y = int(parts[0])
                    if y < 1911:
                        y += 1911
                    return pd.Timestamp(year=y, month=int(parts[1]), day=int(parts[2]))
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
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
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



@st.cache_data(ttl=1800, show_spinner=False)
def _get_yahoo_history_data(stock_no: str, market_type: str, start_date: date, end_date: date) -> tuple[pd.DataFrame, str]:
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


@st.cache_data(ttl=1800, show_spinner=False)
def _get_history_data_smart(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date) -> tuple[pd.DataFrame, str, str]:
    stock_no = _safe_str(stock_no)
    stock_name = _safe_str(stock_name)
    market_type = _safe_str(market_type) or "上市"

    debug_try = []

    # 1) 先走 utils：保留原架構與共用快取。
    for try_name, try_market in _market_candidates(stock_no, stock_name, market_type):
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
                return df, (_safe_str(try_market) or market_type or "未標示"), "utils"
        except Exception as e:
            debug_try.append(f"utils:{try_market or '空'}=ERR {e}")

    # 2) 上市直連 TWSE：修正 6271 這類上市股票被 utils fallback 失敗的情況。
    try:
        df_twse = _get_twse_history_data_direct(stock_no, start_date, end_date)
        df_twse = _prepare_history_df(df_twse)
        debug_try.append(f"twse_direct={len(df_twse)}")
        if not df_twse.empty:
            return df_twse, "上市", "twse_direct"
    except Exception as e:
        debug_try.append(f"twse_direct=ERR {e}")

    # 3) 上櫃直連 TPEX：保留原本 fallback。
    try:
        df_tpex = _get_tpex_history_data(stock_no, start_date, end_date)
        df_tpex = _prepare_history_df(df_tpex)
        debug_try.append(f"tpex_direct={len(df_tpex)}")
        if not df_tpex.empty:
            return df_tpex, "上櫃", "tpex_direct"
    except Exception as e:
        debug_try.append(f"tpex_direct=ERR {e}")

    # 4) Yahoo Finance 最後備援：官方 TWSE/TPEX 端點異常時仍可取得日線。
    try:
        df_yahoo, yahoo_symbol = _get_yahoo_history_data(stock_no, market_type, start_date, end_date)
        df_yahoo = _prepare_history_df(df_yahoo)
        debug_try.append(f"yahoo:{yahoo_symbol or '-'}={len(df_yahoo)}")
        if not df_yahoo.empty:
            yahoo_market = "上櫃" if yahoo_symbol.endswith(".TWO") else "上市"
            return df_yahoo, yahoo_market, f"yahoo:{yahoo_symbol}"
    except Exception as e:
        debug_try.append(f"yahoo=ERR {e}")

    st.session_state[_k("history_debug_try")] = "｜".join(debug_try[-16:])
    return pd.DataFrame(), (_safe_str(market_type) or "未知"), "none"
@st.cache_data(ttl=1800, show_spinner=False)
def _detect_pivots_smart(df: pd.DataFrame, window: int = 4, min_gap: int = 6):
    if df is None or df.empty or len(df) < window * 2 + 3:
        return [], []

    highs = df["最高價"].tolist()
    lows = df["最低價"].tolist()
    peak_idx = []
    trough_idx = []

    for i in range(window, len(df) - window):
        cur_high = highs[i]
        cur_low = lows[i]
        if pd.isna(cur_high) or pd.isna(cur_low):
            continue

        left_high = highs[i - window:i]
        right_high = highs[i + 1:i + 1 + window]
        left_low = lows[i - window:i]
        right_low = lows[i + 1:i + 1 + window]

        is_peak = all(cur_high >= x for x in left_high + right_high if pd.notna(x))
        is_trough = all(cur_low <= x for x in left_low + right_low if pd.notna(x))

        if is_peak:
            if not peak_idx or (i - peak_idx[-1] >= min_gap):
                peak_idx.append(i)
            elif cur_high > highs[peak_idx[-1]]:
                peak_idx[-1] = i

        if is_trough:
            if not trough_idx or (i - trough_idx[-1] >= min_gap):
                trough_idx.append(i)
            elif cur_low < lows[trough_idx[-1]]:
                trough_idx[-1] = i

    return peak_idx, trough_idx


@st.cache_data(ttl=1800, show_spinner=False)
def _build_event_df(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if df is None or df.empty or len(df) < 3:
        return pd.DataFrame(columns=["日期", "事件分類", "事件", "說明"])

    peak_idx, trough_idx = _detect_pivots_smart(df, window=4, min_gap=6)

    for i in trough_idx:
        r = df.iloc[i]
        rows.append({"日期": r["日期"], "事件分類": "起漲點", "事件": "起漲點", "說明": f"局部低點形成，低點約 {format_number(r.get('最低價'), 2)}。"})
    for i in peak_idx:
        r = df.iloc[i]
        rows.append({"日期": r["日期"], "事件分類": "起跌點", "事件": "起跌點", "說明": f"局部高點形成，高點約 {format_number(r.get('最高價'), 2)}。"})

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        d = cur["日期"]

        if all(c in df.columns for c in ["MA5", "MA10"]):
            if pd.notna(prev["MA5"]) and pd.notna(prev["MA10"]) and pd.notna(cur["MA5"]) and pd.notna(cur["MA10"]):
                if prev["MA5"] <= prev["MA10"] and cur["MA5"] > cur["MA10"]:
                    rows.append({"日期": d, "事件分類": "MA", "事件": "MA黃金交叉", "說明": "MA5 上穿 MA10，短線偏強。"})
                elif prev["MA5"] >= prev["MA10"] and cur["MA5"] < cur["MA10"]:
                    rows.append({"日期": d, "事件分類": "MA", "事件": "MA死亡交叉", "說明": "MA5 下破 MA10，短線偏弱。"})

        if all(c in df.columns for c in ["K", "D"]):
            if pd.notna(prev["K"]) and pd.notna(prev["D"]) and pd.notna(cur["K"]) and pd.notna(cur["D"]):
                if prev["K"] <= prev["D"] and cur["K"] > cur["D"]:
                    rows.append({"日期": d, "事件分類": "KD", "事件": "KD黃金交叉", "說明": "KD 轉強。"})
                elif prev["K"] >= prev["D"] and cur["K"] < cur["D"]:
                    rows.append({"日期": d, "事件分類": "KD", "事件": "KD死亡交叉", "說明": "KD 轉弱。"})

        if all(c in df.columns for c in ["DIF", "DEA"]):
            if pd.notna(prev["DIF"]) and pd.notna(prev["DEA"]) and pd.notna(cur["DIF"]) and pd.notna(cur["DEA"]):
                if prev["DIF"] <= prev["DEA"] and cur["DIF"] > cur["DEA"]:
                    rows.append({"日期": d, "事件分類": "MACD", "事件": "MACD黃金交叉", "說明": "DIF 上穿 DEA，動能轉強。"})
                elif prev["DIF"] >= prev["DEA"] and cur["DIF"] < cur["DEA"]:
                    rows.append({"日期": d, "事件分類": "MACD", "事件": "MACD死亡交叉", "說明": "DIF 下破 DEA，動能轉弱。"})

        if i >= 19 and all(c in df.columns for c in ["最高價", "最低價", "收盤價"]):
            recent = df.iloc[max(0, i - 19): i + 1]
            high20 = recent["最高價"].max()
            low20 = recent["最低價"].min()
            close_price = cur["收盤價"]

            if pd.notna(close_price) and pd.notna(high20) and close_price >= high20:
                rows.append({"日期": d, "事件分類": "突破", "事件": "突破20日高", "說明": "股價創 20 日新高。"})
            if pd.notna(close_price) and pd.notna(low20) and close_price <= low20:
                rows.append({"日期": d, "事件分類": "跌破", "事件": "跌破20日低", "說明": "股價創 20 日新低。"})

    if not rows:
        return pd.DataFrame(columns=["日期", "事件分類", "事件", "說明"])

    return pd.DataFrame(rows).drop_duplicates(subset=["日期", "事件", "說明"]).sort_values("日期", ascending=False).reset_index(drop=True)


@st.cache_data(ttl=1800, show_spinner=False)
def _compute_analysis_bundle(df: pd.DataFrame) -> dict[str, Any]:
    signal_snapshot = compute_signal_snapshot(df)
    sr_snapshot = compute_support_resistance_snapshot(df)
    radar = _ensure_radar_dict(compute_radar_scores(df))
    badge_text, _ = score_to_badge(signal_snapshot.get("score", 0))
    event_df = _build_event_df(df)
    peak_idx, trough_idx = _detect_pivots_smart(df, window=4, min_gap=6)

    return {
        "signal_snapshot": signal_snapshot,
        "sr_snapshot": sr_snapshot,
        "radar": radar,
        "badge_text": badge_text,
        "event_df": event_df,
        "peak_idx": peak_idx,
        "trough_idx": trough_idx,
    }


def _slice_by_focus(df: pd.DataFrame, event_df: pd.DataFrame, focus_event_idx: int, focus_window: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    if focus_event_idx is not None and focus_event_idx >= 0 and event_df is not None and not event_df.empty and focus_event_idx < len(event_df):
        event_date = pd.to_datetime(event_df.iloc[focus_event_idx]["日期"], errors="coerce")
        if pd.notna(event_date):
            around = 30
            mask = (df["日期"] >= event_date - pd.Timedelta(days=around)) & (df["日期"] <= event_date + pd.Timedelta(days=around))
            focus_df = df.loc[mask].copy()
            if not focus_df.empty:
                return focus_df.reset_index(drop=True)

    if focus_window == "30":
        return df.tail(30).reset_index(drop=True)
    if focus_window == "60":
        return df.tail(60).reset_index(drop=True)
    if focus_window == "120":
        return df.tail(120).reset_index(drop=True)
    if focus_window == "240":
        return df.tail(240).reset_index(drop=True)

    return df.reset_index(drop=True)


def _event_style(event_type: str) -> dict[str, str]:
    mapping = {
        "起漲點": {"bg": "#ecfdf5", "border": "#10b981", "tag": "#047857", "text": "#065f46"},
        "起跌點": {"bg": "#fef2f2", "border": "#ef4444", "tag": "#b91c1c", "text": "#7f1d1d"},
        "MA": {"bg": "#eff6ff", "border": "#3b82f6", "tag": "#1d4ed8", "text": "#1e3a8a"},
        "KD": {"bg": "#faf5ff", "border": "#a855f7", "tag": "#7e22ce", "text": "#581c87"},
        "MACD": {"bg": "#fff7ed", "border": "#f97316", "tag": "#c2410c", "text": "#9a3412"},
        "突破": {"bg": "#f0fdfa", "border": "#14b8a6", "tag": "#0f766e", "text": "#134e4a"},
        "跌破": {"bg": "#f8fafc", "border": "#334155", "tag": "#0f172a", "text": "#334155"},
    }
    return mapping.get(event_type, {"bg": "#f8fafc", "border": "#94a3b8", "tag": "#475569", "text": "#334155"})


def _event_direction_meta(event_name: str, event_type: str) -> dict[str, str]:
    name = _safe_str(event_name)
    typ = _safe_str(event_type)

    if typ in ["起漲點", "突破"]:
        return {"arrow": "↑", "label": "偏多", "bg": "#dcfce7", "color": "#166534"}
    if typ in ["起跌點", "跌破"]:
        return {"arrow": "↓", "label": "偏空", "bg": "#fee2e2", "color": "#991b1b"}
    if "黃金交叉" in name:
        return {"arrow": "↑", "label": "轉強", "bg": "#dbeafe", "color": "#1d4ed8"}
    if "死亡交叉" in name:
        return {"arrow": "↓", "label": "轉弱", "bg": "#fee2e2", "color": "#b91c1c"}
    return {"arrow": "→", "label": "觀察", "bg": "#e2e8f0", "color": "#334155"}


@st.cache_data(ttl=600, show_spinner=False)
def _build_candlestick_chart(df: pd.DataFrame, stock_label: str, show_ma: bool, show_pivots: bool, peak_idx: tuple[int, ...], trough_idx: tuple[int, ...]) -> go.Figure:
    """專業版 K 線圖。

    設計重點：
    - 台股慣例：紅 K 上漲、綠 K 下跌。
    - K棒實體與上下影線加粗，避免紅綠棒與引線不清楚。
    - 起漲 / 起跌標記加大、加邊框、加文字，並用淡色垂直引導線提示位置。
    - 下方加入成交量柱，主圖只保留價格訊號，判讀更清楚。
    - 限制 pivot 輔助線數量，避免大量 shapes 拖慢 Streamlit / Plotly。
    """
    if df is None or df.empty:
        return go.Figure()

    work = df.copy()
    work = work.sort_values("日期").reset_index(drop=True)

    for c in ["開盤價", "最高價", "最低價", "收盤價"]:
        if c in work.columns:
            work[c] = pd.to_numeric(work[c], errors="coerce")
    work = work.dropna(subset=["日期", "開盤價", "最高價", "最低價", "收盤價"]).reset_index(drop=True)

    if work.empty:
        return go.Figure()

    has_volume = "成交股數" in work.columns
    if has_volume:
        work["成交股數"] = pd.to_numeric(work["成交股數"], errors="coerce").fillna(0)

    up_color = "#ef4444"      # 台股上漲紅
    down_color = "#16a34a"    # 台股下跌綠
    wick_color = "#334155"
    grid_color = "rgba(148,163,184,0.22)"

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.035,
        row_heights=[0.76, 0.24],
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]],
    )

    customdata = work[["開盤價", "最高價", "最低價", "收盤價"]].round(2).to_numpy()

    fig.add_trace(
        go.Candlestick(
            x=work["日期"],
            open=work["開盤價"],
            high=work["最高價"],
            low=work["最低價"],
            close=work["收盤價"],
            name="K線",
            increasing=dict(line=dict(color=up_color, width=2.2), fillcolor="rgba(239,68,68,0.86)"),
            decreasing=dict(line=dict(color=down_color, width=2.2), fillcolor="rgba(22,163,74,0.86)"),
            whiskerwidth=0.55,
            customdata=customdata,
            hovertemplate=(
                "<b>%{x|%Y-%m-%d}</b><br>"
                "開盤：%{customdata[0]:,.2f}<br>"
                "最高：%{customdata[1]:,.2f}<br>"
                "最低：%{customdata[2]:,.2f}<br>"
                "收盤：%{customdata[3]:,.2f}<extra>K線</extra>"
            ),
        ),
        row=1,
        col=1,
    )

    if has_volume:
        volume_colors = [up_color if c >= o else down_color for o, c in zip(work["開盤價"], work["收盤價"])]
        fig.add_trace(
            go.Bar(
                x=work["日期"],
                y=work["成交股數"],
                name="成交量",
                marker=dict(color=volume_colors, opacity=0.34, line=dict(width=0)),
                hovertemplate="<b>%{x|%Y-%m-%d}</b><br>成交量：%{y:,.0f}<extra>成交量</extra>",
            ),
            row=2,
            col=1,
        )

    if show_ma:
        ma_styles = {
            5: dict(color="#60a5fa", width=1.8),
            10: dict(color="#f97316", width=1.9),
            20: dict(color="#ef4444", width=2.2),
            60: dict(color="#14b8a6", width=2.1),
            120: dict(color="#22c55e", width=1.9),
            240: dict(color="#a855f7", width=1.8),
        }
        for n in [5, 10, 20, 60, 120, 240]:
            col = f"MA{n}"
            if col in work.columns:
                y = pd.to_numeric(work[col], errors="coerce")
                if y.notna().sum() == 0:
                    continue
                style = ma_styles.get(n, dict(color="#64748b", width=1.6))
                fig.add_trace(
                    go.Scatter(
                        x=work["日期"],
                        y=y,
                        mode="lines",
                        name=col,
                        line=dict(color=style["color"], width=style["width"]),
                        connectgaps=False,
                        hovertemplate=f"<b>%{{x|%Y-%m-%d}}</b><br>{col}：%{{y:,.2f}}<extra>{col}</extra>",
                    ),
                    row=1,
                    col=1,
                )

    price_min = float(work["最低價"].min())
    price_max = float(work["最高價"].max())
    span = max(price_max - price_min, abs(price_max) * 0.06, 1.0)
    marker_offset = span * 0.035

    if show_pivots:
        # 避免過多標記拖慢，只顯示圖上最後 35 個起漲 / 起跌訊號。
        max_marker_count = 35

        if trough_idx:
            idxs = [int(i) for i in trough_idx if 0 <= int(i) < len(work)][-max_marker_count:]
            if idxs:
                sub = work.iloc[idxs].copy()
                y_mark = sub["最低價"] - marker_offset
                fig.add_trace(
                    go.Scatter(
                        x=sub["日期"],
                        y=y_mark,
                        mode="markers+text",
                        name="起漲點",
                        text=["起漲"] * len(sub),
                        textposition="bottom center",
                        textfont=dict(size=12, color="#92400e", family="Arial Black"),
                        marker=dict(
                            size=15,
                            symbol="triangle-up",
                            color="#facc15",
                            line=dict(color="#92400e", width=1.8),
                        ),
                        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>起漲訊號<br>低點：%{customdata:,.2f}<extra>起漲點</extra>",
                        customdata=sub["最低價"].round(2),
                    ),
                    row=1,
                    col=1,
                )
                for _, rr in sub.tail(18).iterrows():
                    fig.add_shape(
                        type="line",
                        x0=rr["日期"], x1=rr["日期"],
                        y0=rr["最低價"], y1=float(rr["最低價"]) - marker_offset * 0.72,
                        line=dict(color="rgba(250,204,21,0.55)", width=1.4, dash="dot"),
                        row=1, col=1,
                    )

        if peak_idx:
            idxs = [int(i) for i in peak_idx if 0 <= int(i) < len(work)][-max_marker_count:]
            if idxs:
                sub = work.iloc[idxs].copy()
                y_mark = sub["最高價"] + marker_offset
                fig.add_trace(
                    go.Scatter(
                        x=sub["日期"],
                        y=y_mark,
                        mode="markers+text",
                        name="起跌點",
                        text=["起跌"] * len(sub),
                        textposition="top center",
                        textfont=dict(size=12, color="#6d28d9", family="Arial Black"),
                        marker=dict(
                            size=15,
                            symbol="triangle-down",
                            color="#7c3aed",
                            line=dict(color="#ffffff", width=1.8),
                        ),
                        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>起跌訊號<br>高點：%{customdata:,.2f}<extra>起跌點</extra>",
                        customdata=sub["最高價"].round(2),
                    ),
                    row=1,
                    col=1,
                )
                for _, rr in sub.tail(18).iterrows():
                    fig.add_shape(
                        type="line",
                        x0=rr["日期"], x1=rr["日期"],
                        y0=rr["最高價"], y1=float(rr["最高價"]) + marker_offset * 0.72,
                        line=dict(color="rgba(124,58,237,0.48)", width=1.4, dash="dot"),
                        row=1, col=1,
                    )

    latest_close = float(work["收盤價"].iloc[-1])
    latest_date = work["日期"].iloc[-1]
    fig.add_hline(
        y=latest_close,
        line=dict(color="rgba(15,23,42,0.55)", width=1.2, dash="dot"),
        annotation_text=f"最新 {latest_close:,.2f}",
        annotation_position="right",
        row=1,
        col=1,
    )

    fig.update_layout(
        title=dict(
            text=f"{stock_label}｜歷史K線分析",
            x=0.01,
            xanchor="left",
            font=dict(size=20, color="#0f172a"),
        ),
        height=820,
        margin=dict(l=18, r=24, t=58, b=26),
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        hovermode="x unified",
        hoverlabel=dict(bgcolor="rgba(15,23,42,0.92)", font_size=13, font_color="#ffffff"),
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.98,
            xanchor="left",
            x=1.02,
            bgcolor="rgba(255,255,255,0.88)",
            bordercolor="rgba(148,163,184,0.35)",
            borderwidth=1,
        ),
        xaxis_rangeslider_visible=False,
        uirevision=f"kline_{stock_label}",
        dragmode="pan",
    )

    fig.update_xaxes(
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
        spikecolor="rgba(15,23,42,0.35)",
        spikethickness=1,
        showgrid=False,
        tickformat="%Y-%m-%d",
        row=1,
        col=1,
    )
    fig.update_xaxes(
        showgrid=False,
        tickformat="%Y-%m-%d",
        title_text="日期",
        row=2,
        col=1,
    )

    fig.update_yaxes(
        title_text="價格",
        showgrid=True,
        gridcolor=grid_color,
        zeroline=False,
        range=[price_min - marker_offset * 2.2, price_max + marker_offset * 2.2],
        row=1,
        col=1,
    )
    fig.update_yaxes(
        title_text="成交量",
        showgrid=True,
        gridcolor="rgba(148,163,184,0.14)",
        zeroline=False,
        row=2,
        col=1,
    )

    return fig

@st.cache_data(ttl=600, show_spinner=False)
def _build_kd_chart(df: pd.DataFrame, stock_label: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["日期"], y=df["K"], mode="lines", name="K"))
    fig.add_trace(go.Scatter(x=df["日期"], y=df["D"], mode="lines", name="D"))
    fig.add_trace(go.Scatter(x=df["日期"], y=[80] * len(df), mode="lines", name="80", line=dict(dash="dot")))
    fig.add_trace(go.Scatter(x=df["日期"], y=[20] * len(df), mode="lines", name="20", line=dict(dash="dot")))
    fig.update_layout(title=f"{stock_label}｜KD", height=320, margin=dict(l=20, r=20, t=50, b=20))
    return fig


@st.cache_data(ttl=600, show_spinner=False)
def _build_macd_chart(df: pd.DataFrame, stock_label: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["日期"], y=df["DIF"], mode="lines", name="DIF"))
    fig.add_trace(go.Scatter(x=df["日期"], y=df["DEA"], mode="lines", name="DEA"))
    fig.add_trace(go.Bar(x=df["日期"], y=df["MACD_HIST"], name="MACD柱"))
    fig.update_layout(title=f"{stock_label}｜MACD", height=340, margin=dict(l=20, r=20, t=50, b=20))
    return fig


def _render_focus_summary_bar(filtered_event_df: pd.DataFrame, signal_snapshot: dict, sr_snapshot: dict, badge_text: str):
    focus_idx = int(st.session_state.get(_k("focus_event_idx"), -1))

    if focus_idx >= 0 and filtered_event_df is not None and not filtered_event_df.empty and focus_idx < len(filtered_event_df):
        row = filtered_event_df.iloc[focus_idx]
        event_type = _safe_str(row["事件分類"])
        event_name = _safe_str(row["事件"])
        event_desc = _safe_str(row["說明"])
        try:
            d = pd.to_datetime(row["日期"]).strftime("%Y-%m-%d")
        except Exception:
            d = _safe_str(row["日期"])

        style = _event_style(event_type)
        direction = _event_direction_meta(event_name, event_type)

        html = (
            f'<div style="background:linear-gradient(135deg,{style["bg"]} 0%,#ffffff 100%);border:2px solid {style["border"]};border-radius:18px;padding:14px 16px;margin-bottom:12px;box-shadow:0 8px 20px rgba(15,23,42,0.06);">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">'
            f'<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">'
            f'<span style="font-size:12px;font-weight:800;color:white;background:{style["tag"]};padding:4px 10px;border-radius:999px;">{event_type}</span>'
            f'<span style="font-size:12px;font-weight:800;color:{direction["color"]};background:{direction["bg"]};padding:4px 10px;border-radius:999px;">{direction["arrow"]} {direction["label"]}</span>'
            f'<span style="font-size:12px;font-weight:700;color:#475569;">{d}</span>'
            f"</div>"
            f'<div style="font-size:12px;font-weight:800;color:#1e293b;">目前焦點事件</div>'
            f"</div>"
            f'<div style="font-size:20px;font-weight:900;color:{style["text"]};margin-top:10px;margin-bottom:6px;">{event_name}</div>'
            f'<div style="font-size:13px;color:#475569;line-height:1.7;">{event_desc}</div>'
            f"</div>"
        )
        _html(html)
    else:
        trend_text = _safe_str(signal_snapshot.get("ma_trend", ("整理", ""))[0])
        kd_text = _safe_str(signal_snapshot.get("kd_cross", ("無新交叉", ""))[0])
        macd_text = _safe_str(signal_snapshot.get("macd_trend", ("整理", ""))[0])
        break_text = _safe_str(sr_snapshot.get("break_signal", ("區間內", ""))[0])

        html = (
            '<div style="background:linear-gradient(135deg,#eff6ff 0%,#ffffff 100%);border:2px solid #bfdbfe;border-radius:18px;padding:14px 16px;margin-bottom:12px;box-shadow:0 8px 20px rgba(15,23,42,0.06);">'
            '<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">'
            '<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">'
            '<span style="font-size:12px;font-weight:800;color:white;background:#1d4ed8;padding:4px 10px;border-radius:999px;">全區間摘要</span>'
            f'<span style="font-size:12px;font-weight:800;color:#1e3a8a;background:#dbeafe;padding:4px 10px;border-radius:999px;">燈號 {badge_text}</span>'
            "</div>"
            '<div style="font-size:12px;font-weight:800;color:#1e293b;">目前全區間狀態</div>'
            "</div>"
            '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px;">'
            f'<span style="font-size:12px;font-weight:800;color:#334155;background:#f8fafc;border:1px solid #e2e8f0;padding:5px 10px;border-radius:999px;">均線：{trend_text}</span>'
            f'<span style="font-size:12px;font-weight:800;color:#334155;background:#f8fafc;border:1px solid #e2e8f0;padding:5px 10px;border-radius:999px;">KD：{kd_text}</span>'
            f'<span style="font-size:12px;font-weight:800;color:#334155;background:#f8fafc;border:1px solid #e2e8f0;padding:5px 10px;border-radius:999px;">MACD：{macd_text}</span>'
            f'<span style="font-size:12px;font-weight:800;color:#334155;background:#f8fafc;border:1px solid #e2e8f0;padding:5px 10px;border-radius:999px;">結構：{break_text}</span>'
            "</div>"
            "</div>"
        )
        _html(html)


def _render_key_price_bar(df: pd.DataFrame, sr_snapshot: dict):
    if df is None or df.empty:
        return

    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"))
    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))
    res60 = _safe_float(sr_snapshot.get("res_60"))
    sup60 = _safe_float(sr_snapshot.get("sup_60"))

    def dist_to_pressure(target, price):
        if target in [None, 0] or price is None:
            return "—"
        pct = ((target - price) / target) * 100
        return f"{pct:+.2f}%"

    def dist_to_support(target, price):
        if target in [None, 0] or price is None:
            return "—"
        pct = ((price - target) / target) * 100
        return f"{pct:+.2f}%"

    pressure_dist = dist_to_pressure(res20, close_now)
    support_dist = dist_to_support(sup20, close_now)
    structure_text = _safe_str(sr_snapshot.get("break_signal", ("區間內", ""))[0])

    html = (
        '<div style="background:linear-gradient(135deg,#0f172a 0%,#162033 45%,#1e293b 100%);border:1px solid rgba(148,163,184,0.2);border-radius:18px;padding:14px 16px;margin-bottom:12px;box-shadow:0 10px 26px rgba(15,23,42,0.18);">'
        '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px;">'
        '<div style="font-size:13px;font-weight:800;color:#e2e8f0;">關鍵價位摘要條</div>'
        f'<div style="font-size:12px;font-weight:800;color:#cbd5e1;">結構：{structure_text}</div>'
        "</div>"
        '<div style="display:flex;gap:8px;flex-wrap:wrap;">'
        f'<span style="font-size:12px;font-weight:900;color:#f8fafc;background:rgba(255,255,255,0.08);padding:6px 10px;border-radius:999px;">現價：{format_number(close_now, 2)}</span>'
        f'<span style="font-size:12px;font-weight:800;color:#fecaca;background:rgba(239,68,68,0.15);padding:6px 10px;border-radius:999px;">20日壓力：{format_number(res20, 2)}</span>'
        f'<span style="font-size:12px;font-weight:800;color:#bbf7d0;background:rgba(16,185,129,0.15);padding:6px 10px;border-radius:999px;">20日支撐：{format_number(sup20, 2)}</span>'
        f'<span style="font-size:12px;font-weight:800;color:#fecaca;background:rgba(244,63,94,0.12);padding:6px 10px;border-radius:999px;">60日壓力：{format_number(res60, 2)}</span>'
        f'<span style="font-size:12px;font-weight:800;color:#bbf7d0;background:rgba(34,197,94,0.12);padding:6px 10px;border-radius:999px;">60日支撐：{format_number(sup60, 2)}</span>'
        f'<span style="font-size:12px;font-weight:800;color:#e0f2fe;background:rgba(14,165,233,0.14);padding:6px 10px;border-radius:999px;">距20壓力：{pressure_dist}</span>'
        f'<span style="font-size:12px;font-weight:800;color:#e0f2fe;background:rgba(14,165,233,0.14);padding:6px 10px;border-radius:999px;">距20支撐：{support_dist}</span>'
        "</div>"
        "</div>"
    )
    _html(html)


def _render_left_event_panel(filtered_event_df: pd.DataFrame):
    st.markdown("### 事件面板")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("上一事件", key=_k("prev_event"), use_container_width=True):
            if filtered_event_df is not None and not filtered_event_df.empty:
                cur_idx = int(st.session_state.get(_k("focus_event_idx"), -1))
                valid = filtered_event_df.index.tolist()
                if cur_idx in valid:
                    pos = valid.index(cur_idx)
                    st.session_state[_k("focus_event_idx")] = valid[max(0, pos - 1)]
                else:
                    st.session_state[_k("focus_event_idx")] = valid[0]
                st.rerun()

    with c2:
        if st.button("下一事件", key=_k("next_event"), use_container_width=True):
            if filtered_event_df is not None and not filtered_event_df.empty:
                cur_idx = int(st.session_state.get(_k("focus_event_idx"), -1))
                valid = filtered_event_df.index.tolist()
                if cur_idx in valid:
                    pos = valid.index(cur_idx)
                    st.session_state[_k("focus_event_idx")] = valid[min(len(valid) - 1, pos + 1)]
                else:
                    st.session_state[_k("focus_event_idx")] = valid[0]
                st.rerun()

    with c3:
        if st.button("全區間", key=_k("back_all"), use_container_width=True):
            st.session_state[_k("focus_event_idx")] = -1
            st.rerun()

    limit = int(st.session_state.get(_k("left_panel_limit"), 12))
    panel_df = filtered_event_df.head(limit) if filtered_event_df is not None else pd.DataFrame()

    if panel_df is None or panel_df.empty:
        st.info("目前沒有可切換事件。")
        return

    for idx, row in panel_df.iterrows():
        try:
            d = pd.to_datetime(row["日期"]).strftime("%Y-%m-%d")
        except Exception:
            d = _safe_str(row["日期"])

        current_focus = int(st.session_state.get(_k("focus_event_idx"), -1))
        event_type = _safe_str(row["事件分類"])
        event_name = _safe_str(row["事件"])
        subtitle = _safe_str(row["說明"])

        style = _event_style(event_type)
        direction = _event_direction_meta(event_name, event_type)
        is_active = idx == current_focus
        active_shadow = "0 0 0 3px rgba(29,78,216,0.18)" if is_active else "none"
        active_border = "#1d4ed8" if is_active else style["border"]

        html = (
            f'<div style="border:2px solid {active_border};background:{style["bg"]};border-radius:16px;padding:12px 12px 10px 12px;margin-bottom:10px;box-shadow:{active_shadow};">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px;">'
            f'<div style="font-size:12px;color:#475569;font-weight:700;">{d}</div>'
            f'<div style="font-size:11px;font-weight:800;color:white;background:{style["tag"]};padding:4px 8px;border-radius:999px;">{event_type}</div>'
            "</div>"
            f'<div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px;">'
            f'<div style="font-size:15px;font-weight:900;color:{style["text"]};">{event_name}</div>'
            f'<div style="min-width:64px;text-align:center;font-size:12px;font-weight:900;color:{direction["color"]};background:{direction["bg"]};padding:4px 8px;border-radius:999px;border:1px solid rgba(15,23,42,0.08);">{direction["arrow"]} {direction["label"]}</div>'
            "</div>"
            f'<div style="font-size:12px;color:#475569;line-height:1.55;">{subtitle}</div>'
            "</div>"
        )
        _html(html)

        if st.button(f"切到這個事件 {idx + 1}", key=_k(f"focus_btn_{idx}"), use_container_width=True):
            st.session_state[_k("focus_event_idx")] = idx
            st.rerun()


def _build_strategy_cards(df: pd.DataFrame, signal_snapshot: dict, sr_snapshot: dict, radar: dict):
    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"))
    ma20 = _safe_float(last.get("MA20"))
    ma60 = _safe_float(last.get("MA60"))
    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))
    score = int(_safe_float(signal_snapshot.get("score"), 0) or 0)
    structure_text = _safe_str(sr_snapshot.get("break_signal", ("區間內", ""))[0])
    radar_summary = _safe_str(radar.get("summary", "—"))

    bullish_trigger = "站穩 20 日壓力並延續量能"
    if res20 is not None:
        bullish_trigger = f"有效站穩 {format_number(res20,2)} 上方"

    bearish_trigger = "跌破 20 日支撐且無法快速收復"
    if sup20 is not None:
        bearish_trigger = f"跌破 {format_number(sup20,2)} 且隔日無法站回"

    observe_text = "目前屬等待表態區，先看支撐壓力哪一側先被有效突破。"
    if score >= 3:
        observe_text = "雖偏多，但若接近壓力區，最好等拉回不破或突破確認再加碼。"
    elif score <= -3:
        observe_text = "雖偏弱，但若接近支撐區，先看是否有止跌反應，不宜追空過深。"

    fail_text = "若進場後關鍵位失守，代表原本劇本失效，應先做風險控管。"

    bullish = [
        ("偏多劇本", f"條件：{bullish_trigger}", ""),
        ("進場觀察", "突破後不回落、量能不失速、短均線維持上彎。", ""),
        ("優勢訊號", f"燈號分數 {score}；{radar_summary}", ""),
        ("失效條件", "突破後立刻跌回區間內，或量縮轉弱。", ""),
    ]
    bearish = [
        ("偏空劇本", f"條件：{bearish_trigger}", ""),
        ("進場觀察", "跌破後無法快速站回，MACD / KD 不同步轉強。", ""),
        ("風險訊號", f"結構判斷：{structure_text}", ""),
        ("失效條件", "跌破後隔日強勢收復，形成假跌破。", ""),
    ]
    observe = [
        ("觀察劇本", observe_text, ""),
        ("優先監看", f"現價 {format_number(close_now,2)} / MA20 {format_number(ma20,2)} / MA60 {format_number(ma60,2)}", ""),
        ("短線關鍵", f"20日壓力 {format_number(res20,2)}；20日支撐 {format_number(sup20,2)}", ""),
        ("策略重點", "不要預設方向，等市場先表態。", ""),
    ]
    fail = [
        ("失敗劇本", fail_text, ""),
        ("多單失敗", "跌破回測低點或跌回重要均線下方。", ""),
        ("空單失敗", "站回跌破點之上並伴隨明顯買盤。", ""),
        ("執行原則", "劇本失效先處理風險，再評估新劇本。", ""),
    ]

    return bullish, bearish, observe, fail


def _build_execution_plan(df: pd.DataFrame, signal_snapshot: dict, sr_snapshot: dict) -> dict[str, list[tuple[str, str, str]]]:
    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"))
    atr14 = _safe_float(last.get("ATR14"), 0) or 0
    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))
    res60 = _safe_float(sr_snapshot.get("res_60"))
    sup60 = _safe_float(sr_snapshot.get("sup_60"))
    score = _safe_float(signal_snapshot.get("score"), 0) or 0

    if close_now is None:
        close_now = 0.0

    if atr14 <= 0:
        atr14 = max(close_now * 0.03, 1.0)

    long_entry = res20 if res20 is not None else close_now
    long_stop = sup20 if sup20 is not None else max(close_now - atr14, 0)
    long_target = res60 if res60 is not None and (res60 > long_entry) else long_entry + atr14 * 2

    short_entry = sup20 if sup20 is not None else close_now
    short_stop = res20 if res20 is not None else close_now + atr14
    short_target = sup60 if sup60 is not None and (sup60 < short_entry) else max(short_entry - atr14 * 2, 0)

    def rr(entry: float | None, stop: float | None, target: float | None, side: str) -> str:
        if entry is None or stop is None or target is None:
            return "—"
        if side == "long":
            risk = entry - stop
            reward = target - entry
        else:
            risk = stop - entry
            reward = entry - target
        if risk <= 0:
            return "—"
        return f"約 1 : {reward / risk:.2f}"

    long_rr = rr(long_entry, long_stop, long_target, "long")
    short_rr = rr(short_entry, short_stop, short_target, "short")

    stance = "偏多優先" if score >= 2 else ("偏空優先" if score <= -2 else "等待表態")
    size_hint = "可分批" if abs(score) >= 3 else "宜輕倉"

    long_plan = [
        ("偏多進場位", format_number(long_entry, 2), ""),
        ("偏多失效位", format_number(long_stop, 2), ""),
        ("偏多目標位", format_number(long_target, 2), ""),
        ("風險報酬", long_rr, ""),
        ("執行提醒", f"{stance} / {size_hint}", ""),
    ]
    short_plan = [
        ("偏空進場位", format_number(short_entry, 2), ""),
        ("偏空失效位", format_number(short_stop, 2), ""),
        ("偏空目標位", format_number(short_target, 2), ""),
        ("風險報酬", short_rr, ""),
        ("執行提醒", f"{stance} / {size_hint}", ""),
    ]
    notes = [
        ("規劃基準", "以 20 / 60 日支撐壓力與 ATR14 估算。", ""),
        ("多方前提", "突破後不回落、量能不失速。", ""),
        ("空方前提", "跌破後無法站回、反彈量縮。", ""),
        ("風控原則", "先看失效位，再決定是否進場。", ""),
    ]

    return {"long": long_plan, "short": short_plan, "notes": notes}


def _build_master_commentary(df: pd.DataFrame, signal_snapshot: dict, sr_snapshot: dict, radar: dict, event_df: pd.DataFrame):
    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"))
    ma20 = _safe_float(last.get("MA20"))
    ma60 = _safe_float(last.get("MA60"))
    k_val = _safe_float(last.get("K"))
    d_val = _safe_float(last.get("D"))
    dif = _safe_float(last.get("DIF"))
    dea = _safe_float(last.get("DEA"))

    views = []

    if close_now is not None and ma20 is not None and ma60 is not None:
        if close_now > ma20 and close_now > ma60:
            views.append(("趨勢觀點", "股價位於 MA20 與 MA60 之上，中期結構偏多。", ""))
        elif close_now < ma20 and close_now < ma60:
            views.append(("趨勢觀點", "股價位於 MA20 與 MA60 之下，中期結構偏弱。", ""))
        else:
            views.append(("趨勢觀點", "股價位在中期均線交界區，屬整理與等待方向選擇。", ""))

    if k_val is not None and d_val is not None and dif is not None and dea is not None:
        if k_val > d_val and dif > dea:
            views.append(("動能觀點", "KD 與 MACD 同步偏多，短線攻擊動能較佳。", ""))
        elif k_val < d_val and dif < dea:
            views.append(("動能觀點", "KD 與 MACD 同步偏弱，反彈宜防再轉弱。", ""))
        else:
            views.append(("動能觀點", "擺盪動能與趨勢動能未完全共振，走勢容易反覆。", ""))

    pressure_text = _safe_str(sr_snapshot.get("pressure_signal", ("—", ""))[0])
    support_text = _safe_str(sr_snapshot.get("support_signal", ("—", ""))[0])
    break_text = _safe_str(sr_snapshot.get("break_signal", ("—", ""))[0])

    if "突破" in break_text:
        views.append(("結構觀點", "目前屬突破結構，關鍵在突破後是否守住，不是只看站上那一刻。", ""))
    elif "跌破" in break_text:
        views.append(("結構觀點", "目前屬跌破結構，若無法快速站回，弱勢延續機率較高。", ""))
    else:
        if "接近20日壓力" in pressure_text:
            views.append(("結構觀點", "股價逼近短壓，沒有量就容易變成假突破或震盪。", ""))
        elif "接近20日支撐" in support_text:
            views.append(("結構觀點", "股價接近短撐，重點看是否出現防守量與止跌K棒。", ""))
        else:
            views.append(("結構觀點", "目前位於區間內部，較適合等待明確突破或跌破再提高把握度。", ""))

    radar_avg = round(sum([
        _safe_float(radar.get("trend"), 50),
        _safe_float(radar.get("momentum"), 50),
        _safe_float(radar.get("volume"), 50),
        _safe_float(radar.get("position"), 50),
        _safe_float(radar.get("structure"), 50),
    ]) / 5, 1)
    views.append(("雷達總評", f"五維均分約 {radar_avg}，{_safe_str(radar.get('summary', '—'))}", ""))

    if event_df is not None and not event_df.empty:
        last_event = event_df.iloc[0]
        views.append(("最近關鍵事件", f"{_safe_str(last_event.get('事件'))}：{_safe_str(last_event.get('說明'))}", ""))

    score = _safe_float(signal_snapshot.get("score"), 0)
    if score >= 4:
        action_text = "偏多架構，但在壓力區不建議無量追價，較佳節奏是等拉回不破或突破後續強。"
    elif score >= 2:
        action_text = "偏多但未到全面強攻，宜觀察回測支撐是否守穩。"
    elif score <= -4:
        action_text = "偏空結構明確，風險控管應優先於抄底預設。"
    elif score <= -2:
        action_text = "弱勢整理機率高，除非出現止跌與量能改善，否則先保守。"
    else:
        action_text = "多空混合，最佳策略通常不是猜，而是等關鍵位表態後再跟。"
    views.append(("股神操作觀點", action_text, ""))

    return views


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    group_map = _build_group_stock_map()
    flat_rows = _flatten_group_map(group_map)
    _init_state(group_map)

    if _apply_watchlist_sync_if_needed(group_map):
        group_map = _build_group_stock_map()
        flat_rows = _flatten_group_map(group_map)
        _repair_state(group_map)

    render_pro_hero(
        title="歷史K線分析｜策略區可執行版 + 效能優化版",
        subtitle="保留完整功能，補上自選股真同步、偏多/偏空進場位、失效位、目標位與風險報酬概念。",
    )

    watchlist_version = st.session_state.get("watchlist_version", 0)
    watchlist_saved_at = _safe_str(st.session_state.get("watchlist_last_saved_at", ""))
    if watchlist_version or watchlist_saved_at:
        st.caption(
            f"自選股同步狀態：watchlist_version = {watchlist_version}"
            + (f" / 最後更新：{watchlist_saved_at}" if watchlist_saved_at else "")
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
                st.session_state[_k("focus_event_idx")] = -1
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

    groups = list(group_map.keys())
    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    code_to_item = {x["code"]: x for x in items}
    code_options = [x["code"] for x in items]

    c1, c2, c3, c4 = st.columns([2, 3, 2, 2])

    with c1:
        st.selectbox("選擇群組", options=groups, key=_k("group"), on_change=_on_group_change, args=(group_map,))

    with c2:
        st.selectbox(
            "群組股票",
            options=code_options if code_options else [""],
            key=_k("stock_code"),
            format_func=lambda code: code_to_item.get(code, {}).get("label", code),
        )

    with c3:
        st.date_input("開始日期", key=_k("start_date"))

    with c4:
        st.date_input("結束日期", key=_k("end_date"))

    selected_group = _safe_str(st.session_state.get(_k("group"), ""))
    selected_code = _safe_str(st.session_state.get(_k("stock_code"), ""))
    start_date = _to_date(st.session_state.get(_k("start_date")), date.today() - timedelta(days=365))
    end_date = _to_date(st.session_state.get(_k("end_date")), date.today())

    if start_date > end_date:
        st.error("開始日期不可大於結束日期。")
        st.stop()

    if not selected_code or selected_code not in code_to_item:
        st.warning("請先選擇股票。")
        st.stop()

    selected_item = code_to_item[selected_code]
    stock_name = _safe_str(selected_item.get("name"))
    market_type = _safe_str(selected_item.get("market")) or "上市"
    stock_label = f"{selected_code} {stock_name}"

    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code=selected_code,
        home_start=start_date,
        home_end=end_date,
    )

    with st.spinner("載入股神資料中..."):
        df, actual_market, data_source = _get_history_data_smart(
            stock_no=selected_code,
            stock_name=stock_name,
            market_type=market_type,
            start_date=start_date,
            end_date=end_date,
        )

    st.caption(
        f"目前實際查詢值：群組【{selected_group}】 / 股票【{stock_label}】 / 自選市場【{market_type}】 / 實際市場【{actual_market}】 / 資料源【{data_source}】"
    )

    if df.empty:
        st.error("查無歷史資料，已自動嘗試上市 / 上櫃 / 興櫃與 fallback 來源，仍無資料。請更換股票或日期區間。")
        st.stop()

    bundle = _compute_analysis_bundle(df)
    signal_snapshot = bundle["signal_snapshot"]
    sr_snapshot = bundle["sr_snapshot"]
    radar = _ensure_radar_dict(bundle["radar"])
    badge_text = bundle["badge_text"]
    event_df = bundle["event_df"]
    peak_idx = bundle["peak_idx"]
    trough_idx = bundle["trough_idx"]

    render_pro_section("互動控制")
    i1, i2, i3, i4 = st.columns([2, 2, 2, 2])

    with i1:
        st.selectbox("事件篩選", options=["全部", "起漲點", "起跌點", "MA", "KD", "MACD", "突破", "跌破"], key=_k("event_filter"))
    with i2:
        st.selectbox("顯示區間", options=["全部", "30", "60", "120", "240"], key=_k("focus_window"))
    with i3:
        st.checkbox("顯示均線", key=_k("show_ma"))
    with i4:
        st.checkbox("顯示起漲起跌點", key=_k("show_pivots"))

    filtered_event_df = event_df.copy()
    selected_filter = st.session_state.get(_k("event_filter"))
    if not filtered_event_df.empty and selected_filter != "全部":
        filtered_event_df = filtered_event_df[filtered_event_df["事件分類"] == selected_filter].reset_index(drop=True)

    focus_df = _slice_by_focus(
        df=df,
        event_df=filtered_event_df if not filtered_event_df.empty else event_df,
        focus_event_idx=int(st.session_state.get(_k("focus_event_idx"), -1)),
        focus_window=_safe_str(st.session_state.get(_k("focus_window"), "全部")),
    )
    if focus_df.empty:
        focus_df = df.copy()

    focus_peak_idx, focus_trough_idx = _detect_pivots_smart(focus_df, window=3, min_gap=4)

    last = df.iloc[-1]
    first = df.iloc[0]
    close_now = _safe_float(last.get("收盤價"))
    close_first = _safe_float(first.get("收盤價"))
    interval_pct = ((close_now / close_first) - 1) * 100 if close_first not in [None, 0] else None

    render_pro_kpi_row(
        [
            {
                "label": "最新收盤",
                "value": format_number(close_now, 2),
                "delta": format_number(interval_pct, 2) + "%",
                "delta_class": "pro-kpi-delta-up" if _safe_float(interval_pct, 0) > 0 else ("pro-kpi-delta-down" if _safe_float(interval_pct, 0) < 0 else "pro-kpi-delta-flat"),
            },
            {
                "label": "訊號燈號",
                "value": badge_text,
                "delta": f"分數 {signal_snapshot.get('score', 0)}",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "資料筆數",
                "value": len(df),
                "delta": f"{actual_market} / {data_source}",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "起漲 / 起跌",
                "value": f"{len(trough_idx)} / {len(peak_idx)}",
                "delta": "局部轉折點",
                "delta_class": "pro-kpi-delta-flat",
            },
        ]
    )

    left, right = st.columns([1.15, 2.85])

    with left:
        _render_left_event_panel(filtered_event_df)

    with right:
        _render_focus_summary_bar(filtered_event_df, signal_snapshot, sr_snapshot, badge_text)
        _render_key_price_bar(df, sr_snapshot)

        st.plotly_chart(
            _build_candlestick_chart(
                focus_df,
                stock_label,
                show_ma=bool(st.session_state.get(_k("show_ma"), True)),
                show_pivots=bool(st.session_state.get(_k("show_pivots"), True)),
                peak_idx=tuple(focus_peak_idx),
                trough_idx=tuple(focus_trough_idx),
            ),
            use_container_width=True,
            config={"displaylogo": False, "scrollZoom": True, "responsive": True, "modeBarButtonsToRemove": ["lasso2d", "select2d"]},
        )

    # 版面修正：
    # 最近事件摘要原本放在左側事件面板下方，會被左欄寬度限制，造成卡片互相擠壓或覆蓋。
    # 改成主圖下方全寬顯示，保留內容但不影響左右欄。
    recent_pairs = [
        (pd.to_datetime(r["日期"]).strftime("%Y-%m-%d"), _safe_str(r["事件"]), "")
        for _, r in filtered_event_df.head(6).iterrows()
    ] if not filtered_event_df.empty else [("最近事件", "無明確新事件", "")]

    render_pro_info_card("最近事件摘要", recent_pairs, chips=[badge_text, actual_market])

    tabs = st.tabs(["KD / MACD", "雷達 / 訊號", "策略區", "最近事件", "原始資料"])

    with tabs[0]:
        c_kd, c_macd = st.columns(2)
        with c_kd:
            st.plotly_chart(_build_kd_chart(focus_df, stock_label), use_container_width=True)
        with c_macd:
            st.plotly_chart(_build_macd_chart(focus_df, stock_label), use_container_width=True)

    with tabs[1]:
        l2, r2 = st.columns(2)
        with l2:
            render_pro_info_card(
                "股神雷達評分",
                [
                    ("趨勢", radar.get("trend", 50), ""),
                    ("動能", radar.get("momentum", 50), ""),
                    ("量能", radar.get("volume", 50), ""),
                    ("位置", radar.get("position", 50), ""),
                    ("結構", radar.get("structure", 50), ""),
                    ("摘要", _safe_str(radar.get("summary", "—")), ""),
                ],
                chips=[badge_text],
            )
            render_pro_info_card(
                "訊號燈號",
                [
                    ("均線趨勢", _safe_str(signal_snapshot.get("ma_trend", ("—", ""))[0]), ""),
                    ("KD交叉", _safe_str(signal_snapshot.get("kd_cross", ("—", ""))[0]), ""),
                    ("MACD趨勢", _safe_str(signal_snapshot.get("macd_trend", ("—", ""))[0]), ""),
                    ("價位狀態", _safe_str(signal_snapshot.get("price_vs_ma20", ("—", ""))[0]), ""),
                    ("突破狀態", _safe_str(signal_snapshot.get("breakout_20d", ("—", ""))[0]), ""),
                    ("量能狀態", _safe_str(signal_snapshot.get("volume_state", ("—", ""))[0]), ""),
                ],
            )
        with r2:
            render_pro_info_card(
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
            )
            render_pro_info_card(
                "股神分析觀點",
                _build_master_commentary(df, signal_snapshot, sr_snapshot, radar, filtered_event_df if not filtered_event_df.empty else event_df),
                chips=[actual_market, badge_text],
            )

    with tabs[2]:
        bullish, bearish, observe, fail = _build_strategy_cards(df, signal_snapshot, sr_snapshot, radar)
        exec_plan = _build_execution_plan(df, signal_snapshot, sr_snapshot)
        s1, s2 = st.columns(2)
        with s1:
            render_pro_info_card("偏多劇本", bullish, chips=["順勢攻擊"])
            render_pro_info_card("觀察劇本", observe, chips=["等待表態"])
            render_pro_info_card("偏多可執行區", exec_plan["long"], chips=["進場/失效/目標"])
        with s2:
            render_pro_info_card("偏空劇本", bearish, chips=["弱勢延續"])
            render_pro_info_card("失敗劇本", fail, chips=["風險控管"])
            render_pro_info_card("偏空可執行區", exec_plan["short"], chips=["進場/失效/目標"])
        render_pro_info_card("執行說明", exec_plan["notes"], chips=["風控優先"])

    with tabs[3]:
        if filtered_event_df.empty:
            st.info("目前沒有符合條件的事件。")
        else:
            st.dataframe(filtered_event_df, use_container_width=True, hide_index=True)

    with tabs[4]:
        raw_cols = [
            "日期", "開盤價", "最高價", "最低價", "收盤價", "成交股數",
            "MA5", "MA10", "MA20", "MA60", "MA120", "MA240",
            "K", "D", "J", "DIF", "DEA", "MACD_HIST", "ATR14"
        ]
        raw_cols = [c for c in raw_cols if c in df.columns]
        st.dataframe(df[raw_cols].sort_values("日期", ascending=False), use_container_width=True, hide_index=True)

    with st.expander("效能說明"):
        st.write("1. 歷史資料與上櫃 fallback 皆有 cache。")
        st.write("2. 訊號 / 雷達 / 支撐壓力 / 事件偵測集中到 analysis bundle，只算一次。")
        st.write("3. 焦點事件切換只切 focus_df，不重抓歷史資料。")
        st.write("4. 已補上 watchlist 真同步，群組與股票失效時會自動修正。")
        st.write("5. 已補上市場自動 fallback，減少因市場別不一致造成的查無資料。")
        st.write("6. Plotly 圖表已加快取，切頁與重繪更快。")
        st.write("7. 保留全部功能，不用刪功能換速度。")


if __name__ == "__main__":
    main()
