# pages/3_歷史K線分析.py
import math
import traceback
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from utils import (
    inject_pro_theme,
    render_pro_hero,
    render_pro_section,
    render_pro_info_card,
    render_pro_kpi_row,
    format_number,
    get_normalized_watchlist,
    get_all_code_name_map,
    get_history_data,
    compute_signal_snapshot,
    compute_support_resistance_snapshot,
    compute_radar_scores,
)

# =========================
# 頁面設定
# =========================
st.set_page_config(page_title="歷史K線分析", page_icon="📈", layout="wide")
inject_pro_theme()

# =========================
# 安全樣式補強
# =========================
st.markdown(
    """
    <style>
    .hist-top-bar{
        padding:10px 14px;
        border-radius:14px;
        background:linear-gradient(135deg, rgba(20,24,38,.96), rgba(12,17,29,.92));
        border:1px solid rgba(255,255,255,.08);
        margin-bottom:10px;
        box-shadow:0 10px 24px rgba(0,0,0,.22);
    }
    .hist-top-title{
        font-size:12px;
        color:#9fb0d1;
        margin-bottom:4px;
        letter-spacing:.5px;
    }
    .hist-top-value{
        font-size:15px;
        color:#eef4ff;
        font-weight:700;
        line-height:1.45;
    }
    .hist-chip{
        display:inline-block;
        padding:4px 10px;
        margin:2px 6px 2px 0;
        border-radius:999px;
        font-size:12px;
        background:rgba(255,255,255,.06);
        border:1px solid rgba(255,255,255,.08);
        color:#dce7ff;
    }
    .event-card{
        border:1px solid rgba(255,255,255,.08);
        background:linear-gradient(135deg, rgba(17,22,34,.96), rgba(14,18,28,.94));
        border-radius:14px;
        padding:10px 12px;
        margin-bottom:8px;
    }
    .event-date{
        font-size:12px;
        color:#9fb0d1;
        margin-bottom:3px;
    }
    .event-title{
        font-size:13px;
        color:#f1f5ff;
        font-weight:700;
        margin-bottom:4px;
    }
    .event-desc{
        font-size:12px;
        color:#c8d3ea;
        line-height:1.5;
    }
    .strategy-box{
        border:1px solid rgba(255,255,255,.08);
        background:linear-gradient(135deg, rgba(17,22,34,.96), rgba(14,18,28,.94));
        border-radius:16px;
        padding:14px;
        height:100%;
    }
    .strategy-title{
        font-size:15px;
        font-weight:800;
        color:#eff5ff;
        margin-bottom:8px;
    }
    .strategy-line{
        font-size:13px;
        color:#d5e0f8;
        margin:5px 0;
        line-height:1.55;
    }
    .mini-muted{
        font-size:12px;
        color:#94a6c9;
    }
    .state-good{color:#3ddc97;font-weight:700;}
    .state-warn{color:#ffd166;font-weight:700;}
    .state-bad{color:#ff6b6b;font-weight:700;}
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================
# 基本常數
# =========================
DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_END = date.today()
DEFAULT_START = DEFAULT_END - timedelta(days=DEFAULT_LOOKBACK_DAYS)

SS_GROUP_KEY = "hist_selected_group"
SS_STOCK_KEY = "hist_selected_stock"
SS_SEARCH_KEY = "hist_search_keyword"
SS_CODE_KEY = "hist_selected_code"
SS_START_KEY = "hist_date_start"
SS_END_KEY = "hist_date_end"
SS_LAST_SEARCH_HIT_KEY = "hist_last_search_hit"


# =========================
# 基礎工具
# =========================
def _safe_text(v, default=""):
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    s = str(v).strip()
    return s if s else default


def _to_float(v, default=np.nan):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _ensure_ss_defaults():
    if SS_GROUP_KEY not in st.session_state:
        st.session_state[SS_GROUP_KEY] = "全部股票"
    if SS_STOCK_KEY not in st.session_state:
        st.session_state[SS_STOCK_KEY] = ""
    if SS_SEARCH_KEY not in st.session_state:
        st.session_state[SS_SEARCH_KEY] = ""
    if SS_CODE_KEY not in st.session_state:
        st.session_state[SS_CODE_KEY] = ""
    if SS_START_KEY not in st.session_state:
        st.session_state[SS_START_KEY] = DEFAULT_START
    if SS_END_KEY not in st.session_state:
        st.session_state[SS_END_KEY] = DEFAULT_END
    if SS_LAST_SEARCH_HIT_KEY not in st.session_state:
        st.session_state[SS_LAST_SEARCH_HIT_KEY] = ""


def _normalize_code(code: str) -> str:
    code = _safe_text(code)
    if "." in code:
        code = code.split(".")[0]
    return code.strip()


def _normalize_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame()

    x = df.copy()

    # 常見欄位對應
    rename_map = {}
    for c in x.columns:
        lc = str(c).strip().lower()
        if lc in ["date", "日期", "time", "datetime"]:
            rename_map[c] = "Date"
        elif lc in ["open", "開盤", "開盤價"]:
            rename_map[c] = "Open"
        elif lc in ["high", "最高", "最高價"]:
            rename_map[c] = "High"
        elif lc in ["low", "最低", "最低價"]:
            rename_map[c] = "Low"
        elif lc in ["close", "收盤", "收盤價", "adj close", "adj_close"]:
            rename_map[c] = "Close"
        elif lc in ["volume", "成交量"]:
            rename_map[c] = "Volume"

    x = x.rename(columns=rename_map)

    required = ["Date", "Open", "High", "Low", "Close"]
    for col in required:
        if col not in x.columns:
            return pd.DataFrame()

    if "Volume" not in x.columns:
        x["Volume"] = np.nan

    x["Date"] = pd.to_datetime(x["Date"], errors="coerce")
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        x[col] = pd.to_numeric(x[col], errors="coerce")

    x = x.dropna(subset=["Date", "Open", "High", "Low", "Close"]).sort_values("Date").reset_index(drop=True)

    if len(x) == 0:
        return pd.DataFrame()

    return x


@st.cache_data(show_spinner=False, ttl=300)
def _load_code_name_map_cached():
    try:
        m = get_all_code_name_map()
        if isinstance(m, dict):
            return {str(k): str(v) for k, v in m.items()}
        return {}
    except Exception:
        return {}


def _extract_group_map_from_watchlist():
    """
    盡量兼容目前專案各種 watchlist 結構
    輸出:
    {
        "全部股票": [{"code":"2330","name":"台積電"}, ...],
        "自選股": [...],
        "半導體": [...]
    }
    """
    code_name_map = _load_code_name_map_cached()
    raw = None

    try:
        raw = get_normalized_watchlist()
    except Exception:
        raw = None

    group_map = {"全部股票": []}
    all_seen = {}

    def add_item(group, code, name=""):
        code = _normalize_code(code)
        if not code:
            return
        name = _safe_text(name) or code_name_map.get(code, "")
        item = {"code": code, "name": name}
        if group not in group_map:
            group_map[group] = []
        if code not in {i["code"] for i in group_map[group]}:
            group_map[group].append(item)
        if code not in all_seen:
            all_seen[code] = item

    # 情況1: dict[group] = list
    if isinstance(raw, dict):
        for group, arr in raw.items():
            group_name = _safe_text(group, "未分類")
            if isinstance(arr, list):
                for v in arr:
                    if isinstance(v, dict):
                        add_item(group_name, v.get("code") or v.get("symbol") or v.get("股票代號"), v.get("name") or v.get("股票名稱"))
                    else:
                        code = _normalize_code(v)
                        add_item(group_name, code, code_name_map.get(code, ""))
            elif isinstance(arr, dict):
                for code, name in arr.items():
                    add_item(group_name, code, name)

    # 情況2: list
    elif isinstance(raw, list):
        for v in raw:
            if isinstance(v, dict):
                group_name = _safe_text(v.get("group") or v.get("分類") or v.get("群組"), "自選股")
                add_item(group_name, v.get("code") or v.get("symbol") or v.get("股票代號"), v.get("name") or v.get("股票名稱"))
            else:
                code = _normalize_code(v)
                add_item("自選股", code, code_name_map.get(code, ""))

    # 情況3: session_state 補抓
    for ss_key in ["watchlist_groups", "watchlist", "normalized_watchlist", "favorite_groups"]:
        raw_ss = st.session_state.get(ss_key)
        if isinstance(raw_ss, dict):
            for group, arr in raw_ss.items():
                group_name = _safe_text(group, "未分類")
                if isinstance(arr, list):
                    for v in arr:
                        if isinstance(v, dict):
                            add_item(group_name, v.get("code") or v.get("symbol") or v.get("股票代號"), v.get("name") or v.get("股票名稱"))
                        else:
                            code = _normalize_code(v)
                            add_item(group_name, code, code_name_map.get(code, ""))
        elif isinstance(raw_ss, list):
            for v in raw_ss:
                if isinstance(v, dict):
                    group_name = _safe_text(v.get("group") or v.get("分類") or v.get("群組"), "自選股")
                    add_item(group_name, v.get("code") or v.get("symbol") or v.get("股票代號"), v.get("name") or v.get("股票名稱"))
                else:
                    code = _normalize_code(v)
                    add_item("自選股", code, code_name_map.get(code, ""))

    # 若自選股空，至少帶入 code_name_map 前幾筆，避免頁面空掉
    if len(all_seen) == 0 and code_name_map:
        fallback_codes = list(code_name_map.keys())[:300]
        for code in fallback_codes:
            add_item("全部股票", code, code_name_map.get(code, ""))

    # 組裝全部股票
    all_items = list(all_seen.values())
    all_items = sorted(all_items, key=lambda x: (x["code"]))
    group_map["全部股票"] = all_items

    # 其他群組排序
    for g in list(group_map.keys()):
        group_map[g] = sorted(group_map[g], key=lambda x: (x["code"]))

    return group_map


def _build_search_records(group_map):
    records = []
    code_name_map = _load_code_name_map_cached()

    for code, name in code_name_map.items():
        records.append(
            {
                "code": _normalize_code(code),
                "name": _safe_text(name),
                "group": None,
                "label": f"{_normalize_code(code)} {_safe_text(name)}".strip(),
            }
        )

    # 讓群組關聯優先
    code_to_group = {}
    for g, arr in group_map.items():
        if g == "全部股票":
            continue
        for item in arr:
            code_to_group.setdefault(item["code"], g)

    dedup = {}
    for r in records:
        code = r["code"]
        if code not in dedup:
            r["group"] = code_to_group.get(code)
            dedup[code] = r

    # 補上 watchlist 裡但 map 可能沒有的
    for g, arr in group_map.items():
        for item in arr:
            code = item["code"]
            if code not in dedup:
                dedup[code] = {
                    "code": code,
                    "name": item.get("name", ""),
                    "group": None if g == "全部股票" else g,
                    "label": f'{code} {_safe_text(item.get("name",""))}'.strip(),
                }

    results = list(dedup.values())
    results = sorted(results, key=lambda x: x["code"])
    return results


def _search_stock_records(search_records, keyword: str, limit=30):
    keyword = _safe_text(keyword).lower()
    if not keyword:
        return search_records[:limit]

    out = []
    for r in search_records:
        code = _safe_text(r["code"]).lower()
        name = _safe_text(r["name"]).lower()
        group = _safe_text(r.get("group")).lower()

        score = None
        if keyword == code:
            score = 100
        elif code.startswith(keyword):
            score = 90
        elif keyword in code:
            score = 80
        elif keyword in name:
            score = 70
        elif keyword in group:
            score = 60

        if score is not None:
            out.append((score, r))

    out = sorted(out, key=lambda x: (-x[0], x[1]["code"]))
    return [x[1] for x in out[:limit]]


def _sync_search_group_stock(group_map, search_records):
    """
    真同步規則：
    1. 搜尋選到股票 -> 自動同步群組 + 群組股票
    2. 改群組 -> 股票清單同步
    3. 改股票 -> 搜尋框同步
    """
    groups = list(group_map.keys())
    current_group = st.session_state.get(SS_GROUP_KEY, "全部股票")
    if current_group not in groups:
        current_group = "全部股票"
        st.session_state[SS_GROUP_KEY] = current_group

    search_kw = _safe_text(st.session_state.get(SS_SEARCH_KEY, ""))
    stock_options = group_map.get(current_group, [])

    # 搜尋命中
    matches = _search_stock_records(search_records, search_kw, limit=20)

    # 搜尋精準命中時，自動同步
    exact_hit = None
    if search_kw:
        for m in matches:
            if search_kw == m["code"] or search_kw == f'{m["code"]} {_safe_text(m["name"])}'.lower():
                exact_hit = m
                break
        if exact_hit is None and len(matches) == 1:
            exact_hit = matches[0]

    if exact_hit:
        hit_group = exact_hit.get("group")
        if hit_group and hit_group in group_map:
            st.session_state[SS_GROUP_KEY] = hit_group
            current_group = hit_group
            stock_options = group_map.get(current_group, [])
        st.session_state[SS_CODE_KEY] = exact_hit["code"]
        st.session_state[SS_STOCK_KEY] = f'{exact_hit["code"]} {_safe_text(exact_hit["name"])}'.strip()
        st.session_state[SS_LAST_SEARCH_HIT_KEY] = exact_hit["code"]

    # 若目前股票不在當前群組，優先用 code 對應
    current_stock_label = _safe_text(st.session_state.get(SS_STOCK_KEY, ""))
    current_code = _normalize_code(st.session_state.get(SS_CODE_KEY, ""))

    valid_labels = [f'{x["code"]} {_safe_text(x["name"])}'.strip() for x in stock_options]
    valid_codes = [x["code"] for x in stock_options]

    if current_code and current_code in valid_codes:
        idx = valid_codes.index(current_code)
        st.session_state[SS_STOCK_KEY] = valid_labels[idx]
    elif current_stock_label not in valid_labels:
        if stock_options:
            first_item = stock_options[0]
            st.session_state[SS_CODE_KEY] = first_item["code"]
            st.session_state[SS_STOCK_KEY] = f'{first_item["code"]} {_safe_text(first_item["name"])}'.strip()
        else:
            st.session_state[SS_CODE_KEY] = ""
            st.session_state[SS_STOCK_KEY] = ""

    # 股票 -> 搜尋同步
    if st.session_state.get(SS_STOCK_KEY):
        st.session_state[SS_SEARCH_KEY] = st.session_state[SS_STOCK_KEY]


def _on_group_change(group_map):
    group = st.session_state.get(SS_GROUP_KEY, "全部股票")
    options = group_map.get(group, [])
    if options:
        cur_code = _normalize_code(st.session_state.get(SS_CODE_KEY, ""))
        if cur_code in [o["code"] for o in options]:
            found = next((o for o in options if o["code"] == cur_code), options[0])
        else:
            found = options[0]
        st.session_state[SS_CODE_KEY] = found["code"]
        st.session_state[SS_STOCK_KEY] = f'{found["code"]} {_safe_text(found["name"])}'.strip()
        st.session_state[SS_SEARCH_KEY] = st.session_state[SS_STOCK_KEY]
    else:
        st.session_state[SS_CODE_KEY] = ""
        st.session_state[SS_STOCK_KEY] = ""
        st.session_state[SS_SEARCH_KEY] = ""


def _on_stock_change():
    stock_label = _safe_text(st.session_state.get(SS_STOCK_KEY, ""))
    if stock_label:
        code = _normalize_code(stock_label.split(" ")[0])
        st.session_state[SS_CODE_KEY] = code
        st.session_state[SS_SEARCH_KEY] = stock_label


def _pick_code_name():
    code = _normalize_code(st.session_state.get(SS_CODE_KEY, ""))
    label = _safe_text(st.session_state.get(SS_STOCK_KEY, ""))
    name = ""
    if label and " " in label:
        name = label.split(" ", 1)[1].strip()
    if not name:
        name = _load_code_name_map_cached().get(code, "")
    return code, name


# =========================
# 技術分析
# =========================
@st.cache_data(show_spinner=False, ttl=300)
def _prepare_tech_df(hist_df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_history_df(hist_df)
    if len(df) == 0:
        return df

    df = df.copy()
    close = df["Close"]

    # 均線
    for n in [5, 10, 20, 60, 120]:
        df[f"MA{n}"] = close.rolling(n).mean()

    # KD
    low9 = df["Low"].rolling(9).min()
    high9 = df["High"].rolling(9).max()
    rsv = (close - low9) / (high9 - low9).replace(0, np.nan) * 100
    df["K"] = rsv.ewm(com=2, adjust=False).mean()
    df["D"] = df["K"].ewm(com=2, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["MACD"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["OSC"] = df["DIF"] - df["MACD"]

    # 報酬 / 波動
    df["Ret1"] = close.pct_change()
    df["Ret5"] = close.pct_change(5)
    df["Ret20"] = close.pct_change(20)
    df["Volatility20"] = df["Ret1"].rolling(20).std() * np.sqrt(252)

    # 量能
    df["VolMA5"] = df["Volume"].rolling(5).mean()
    df["VolMA20"] = df["Volume"].rolling(20).mean()

    # 事件點
    df["GoldenCross_5_20"] = (df["MA5"] > df["MA20"]) & (df["MA5"].shift(1) <= df["MA20"].shift(1))
    df["DeathCross_5_20"] = (df["MA5"] < df["MA20"]) & (df["MA5"].shift(1) >= df["MA20"].shift(1))
    df["KD_Golden"] = (df["K"] > df["D"]) & (df["K"].shift(1) <= df["D"].shift(1))
    df["KD_Death"] = (df["K"] < df["D"]) & (df["K"].shift(1) >= df["D"].shift(1))
    df["MACD_Golden"] = (df["DIF"] > df["MACD"]) & (df["DIF"].shift(1) <= df["MACD"].shift(1))
    df["MACD_Death"] = (df["DIF"] < df["MACD"]) & (df["DIF"].shift(1) >= df["MACD"].shift(1))

    return df


def _fallback_signal_snapshot(df: pd.DataFrame) -> dict:
    if len(df) == 0:
        return {}

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last

    close = _to_float(last["Close"])
    ma5 = _to_float(last.get("MA5"))
    ma20 = _to_float(last.get("MA20"))
    ma60 = _to_float(last.get("MA60"))
    k = _to_float(last.get("K"))
    d = _to_float(last.get("D"))
    dif = _to_float(last.get("DIF"))
    macd = _to_float(last.get("MACD"))
    vol = _to_float(last.get("Volume"))
    vol20 = _to_float(last.get("VolMA20"))

    trend_score = 0
    if not np.isnan(close) and not np.isnan(ma5) and close > ma5:
        trend_score += 1
    if not np.isnan(ma5) and not np.isnan(ma20) and ma5 > ma20:
        trend_score += 1
    if not np.isnan(ma20) and not np.isnan(ma60) and ma20 > ma60:
        trend_score += 1

    momentum_score = 0
    if not np.isnan(k) and not np.isnan(d) and k > d:
        momentum_score += 1
    if not np.isnan(dif) and not np.isnan(macd) and dif > macd:
        momentum_score += 1
    if _to_float(last.get("Ret5"), 0) > 0:
        momentum_score += 1

    volume_score = 0
    if not np.isnan(vol) and not np.isnan(vol20) and vol > vol20:
        volume_score += 1
    if not np.isnan(vol) and not np.isnan(_to_float(prev.get("Volume"))) and vol > _to_float(prev.get("Volume")):
        volume_score += 1

    score = trend_score + momentum_score + volume_score
    if score >= 6:
        stance = "偏多"
    elif score >= 3:
        stance = "中性偏多"
    elif score >= 1:
        stance = "中性"
    else:
        stance = "偏弱"

    return {
        "stance": stance,
        "score": score,
        "trend_score": trend_score,
        "momentum_score": momentum_score,
        "volume_score": volume_score,
        "close_above_ma20": bool(close > ma20) if not np.isnan(close) and not np.isnan(ma20) else None,
        "kd_golden": bool(last.get("KD_Golden", False)),
        "macd_golden": bool(last.get("MACD_Golden", False)),
    }


def _fallback_sr_snapshot(df: pd.DataFrame) -> dict:
    if len(df) == 0:
        return {}

    last = df.iloc[-1]
    latest_close = _to_float(last["Close"])

    recent20_high = _to_float(df["High"].tail(20).max())
    recent20_low = _to_float(df["Low"].tail(20).min())
    recent60_high = _to_float(df["High"].tail(60).max())
    recent60_low = _to_float(df["Low"].tail(60).min())
    ma20 = _to_float(last.get("MA20"))
    ma60 = _to_float(last.get("MA60"))

    support_candidates = [x for x in [recent20_low, recent60_low, ma20, ma60] if not np.isnan(x)]
    resistance_candidates = [x for x in [recent20_high, recent60_high, ma20, ma60] if not np.isnan(x)]

    supports = sorted(set(round(x, 2) for x in support_candidates if x <= latest_close + 999999), reverse=True)
    resistances = sorted(set(round(x, 2) for x in resistance_candidates if x >= 0))

    near_support = None
    near_resistance = None
    if supports:
        below = [x for x in supports if x <= latest_close]
        near_support = max(below) if below else min(supports)
    if resistances:
        above = [x for x in resistances if x >= latest_close]
        near_resistance = min(above) if above else max(resistances)

    return {
        "latest_close": latest_close,
        "near_support": near_support,
        "near_resistance": near_resistance,
        "supports": supports[:4],
        "resistances": resistances[:4],
    }


def _fallback_radar_scores(df: pd.DataFrame) -> dict:
    if len(df) == 0:
        return {}

    last = df.iloc[-1]
    close = _to_float(last["Close"])
    ma20 = _to_float(last.get("MA20"))
    ma60 = _to_float(last.get("MA60"))
    k = _to_float(last.get("K"))
    d = _to_float(last.get("D"))
    ret20 = _to_float(last.get("Ret20"), 0)
    vol = _to_float(last.get("Volume"))
    vol20 = _to_float(last.get("VolMA20"))
    vol20v = _to_float(last.get("Volatility20"))

    trend = 50
    if not np.isnan(ma20) and not np.isnan(ma60):
        trend = min(max(50 + (1 if ma20 > ma60 else -1) * 20 + (1 if close > ma20 else -1) * 15, 5), 95)

    momentum = min(max(50 + (k - d if not np.isnan(k) and not np.isnan(d) else 0), 5), 95)
    strength = min(max(50 + ret20 * 250, 5), 95)
    volume_score = 50 if np.isnan(vol) or np.isnan(vol20) or vol20 == 0 else min(max(50 + (vol / vol20 - 1) * 40, 5), 95)
    risk = 50 if np.isnan(vol20v) else min(max(90 - vol20v * 100, 5), 95)

    return {
        "趨勢": round(trend, 1),
        "動能": round(momentum, 1),
        "強度": round(strength, 1),
        "量能": round(volume_score, 1),
        "風險控管": round(risk, 1),
    }


def _get_signal_snapshot(df: pd.DataFrame) -> dict:
    try:
        result = compute_signal_snapshot(df.copy())
        if isinstance(result, dict) and result:
            return result
    except Exception:
        pass
    return _fallback_signal_snapshot(df)


def _get_sr_snapshot(df: pd.DataFrame) -> dict:
    try:
        result = compute_support_resistance_snapshot(df.copy())
        if isinstance(result, dict) and result:
            return result
    except Exception:
        pass
    return _fallback_sr_snapshot(df)


def _get_radar_scores(df: pd.DataFrame) -> dict:
    try:
        result = compute_radar_scores(df.copy())
        if isinstance(result, dict) and result:
            return result
    except Exception:
        pass
    return _fallback_radar_scores(df)


# =========================
# 事件偵測
# =========================
@st.cache_data(show_spinner=False, ttl=300)
def _build_event_panel(df: pd.DataFrame) -> list:
    if len(df) == 0:
        return []

    events = []

    for _, row in df.tail(120).iterrows():
        dt = pd.to_datetime(row["Date"]).strftime("%Y-%m-%d")
        close = _to_float(row["Close"])

        if bool(row.get("GoldenCross_5_20", False)):
            events.append(
                {
                    "date": dt,
                    "title": "短中期黃金交叉",
                    "desc": f"MA5 上穿 MA20，短線趨勢轉強訊號。收盤 {close:.2f}",
                    "tag": "trend_up",
                    "priority": 92,
                }
            )
        if bool(row.get("DeathCross_5_20", False)):
            events.append(
                {
                    "date": dt,
                    "title": "短中期死亡交叉",
                    "desc": f"MA5 下穿 MA20，短線趨勢轉弱訊號。收盤 {close:.2f}",
                    "tag": "trend_down",
                    "priority": 90,
                }
            )
        if bool(row.get("KD_Golden", False)):
            events.append(
                {
                    "date": dt,
                    "title": "KD 黃金交叉",
                    "desc": f"K 值上穿 D 值，短波段動能改善。",
                    "tag": "kd_up",
                    "priority": 80,
                }
            )
        if bool(row.get("KD_Death", False)):
            events.append(
                {
                    "date": dt,
                    "title": "KD 死亡交叉",
                    "desc": f"K 值下穿 D 值，短波段動能轉弱。",
                    "tag": "kd_down",
                    "priority": 79,
                }
            )
        if bool(row.get("MACD_Golden", False)):
            events.append(
                {
                    "date": dt,
                    "title": "MACD 黃金交叉",
                    "desc": "DIF 上穿 MACD，波段動能偏多。",
                    "tag": "macd_up",
                    "priority": 86,
                }
            )
        if bool(row.get("MACD_Death", False)):
            events.append(
                {
                    "date": dt,
                    "title": "MACD 死亡交叉",
                    "desc": "DIF 下穿 MACD，波段動能偏空。",
                    "tag": "macd_down",
                    "priority": 85,
                }
            )

    # 突破 / 跌破
    for i in range(20, len(df)):
        row = df.iloc[i]
        prev20 = df.iloc[max(0, i - 20):i]
        dt = pd.to_datetime(row["Date"]).strftime("%Y-%m-%d")

        prev_high = _to_float(prev20["High"].max())
        prev_low = _to_float(prev20["Low"].min())
        close = _to_float(row["Close"])
        vol = _to_float(row["Volume"])
        vol20 = _to_float(row.get("VolMA20"))

        if not np.isnan(close) and not np.isnan(prev_high) and close > prev_high:
            extra = "，且量能放大" if (not np.isnan(vol) and not np.isnan(vol20) and vol > vol20) else ""
            events.append(
                {
                    "date": dt,
                    "title": "20日高點突破",
                    "desc": f"收盤突破前 20 日高點 {prev_high:.2f}{extra}。",
                    "tag": "breakout",
                    "priority": 95,
                }
            )
        if not np.isnan(close) and not np.isnan(prev_low) and close < prev_low:
            events.append(
                {
                    "date": dt,
                    "title": "20日低點跌破",
                    "desc": f"收盤跌破前 20 日低點 {prev_low:.2f}。",
                    "tag": "breakdown",
                    "priority": 94,
                }
            )

    # 去重保留重要事件
    uniq = {}
    for e in events:
        key = (e["date"], e["title"])
        if key not in uniq or e["priority"] > uniq[key]["priority"]:
            uniq[key] = e

    out = list(uniq.values())
    out = sorted(out, key=lambda x: (x["date"], x["priority"]), reverse=True)
    return out[:24]


def _build_focus_summary(df: pd.DataFrame, events: list, signal: dict, sr: dict) -> str:
    if len(df) == 0:
        return "目前無資料"

    last = df.iloc[-1]
    close = _to_float(last["Close"])
    ret1 = _to_float(last.get("Ret1"), 0) * 100
    ma20 = _to_float(last.get("MA20"))
    k = _to_float(last.get("K"))
    d = _to_float(last.get("D"))
    dif = _to_float(last.get("DIF"))
    macd = _to_float(last.get("MACD"))
    stance = _safe_text(signal.get("stance"), "中性")

    bits = [f"最新收盤 {close:.2f}", f"單日 {ret1:+.2f}%", f"技術面判斷：{stance}"]

    if not np.isnan(ma20):
        bits.append("站上 MA20" if close > ma20 else "跌落 MA20 下方")
    if not np.isnan(k) and not np.isnan(d):
        bits.append(f"KD {k:.1f}/{d:.1f}")
    if not np.isnan(dif) and not np.isnan(macd):
        bits.append("MACD 偏多" if dif > macd else "MACD 偏弱")

    if events:
        bits.append(f"最近事件：{events[0]['date']} {events[0]['title']}")

    ns = sr.get("near_support")
    nr = sr.get("near_resistance")
    if ns is not None and nr is not None:
        bits.append(f"近支撐 {ns:.2f} / 近壓力 {nr:.2f}")

    return "｜".join(bits)


def _build_price_summary(sr: dict) -> str:
    if not sr:
        return "關鍵價位不足"

    latest_close = sr.get("latest_close")
    near_support = sr.get("near_support")
    near_resistance = sr.get("near_resistance")
    supports = sr.get("supports", [])
    resistances = sr.get("resistances", [])

    bits = []
    if latest_close is not None:
        bits.append(f"現價 {latest_close:.2f}")
    if near_support is not None:
        bits.append(f"近支撐 {near_support:.2f}")
    if near_resistance is not None:
        bits.append(f"近壓力 {near_resistance:.2f}")
    if supports:
        bits.append("支撐帶 " + " / ".join(f"{x:.2f}" for x in supports[:3]))
    if resistances:
        bits.append("壓力帶 " + " / ".join(f"{x:.2f}" for x in resistances[:3]))
    return "｜".join(bits)


# =========================
# 策略
# =========================
def _build_strategy_plan(df: pd.DataFrame, signal: dict, sr: dict) -> dict:
    if len(df) == 0:
        return {}

    last = df.iloc[-1]
    close = _to_float(last["Close"])
    ma20 = _to_float(last.get("MA20"))
    ma60 = _to_float(last.get("MA60"))
    k = _to_float(last.get("K"))
    d = _to_float(last.get("D"))
    dif = _to_float(last.get("DIF"))
    macd = _to_float(last.get("MACD"))
    vol = _to_float(last.get("Volume"))
    vol20 = _to_float(last.get("VolMA20"))
    stance = _safe_text(signal.get("stance"), "中性")

    near_support = sr.get("near_support")
    near_resistance = sr.get("near_resistance")

    trend_ok = (not np.isnan(close) and not np.isnan(ma20) and close > ma20) and (
        np.isnan(ma60) or close > ma60
    )
    momentum_ok = (not np.isnan(k) and not np.isnan(d) and k > d) and (
        not np.isnan(dif) and not np.isnan(macd) and dif >= macd
    )
    volume_ok = np.isnan(vol) or np.isnan(vol20) or vol >= vol20

    strategy = "觀望"
    reason = []
    action = []
    risk = []

    if trend_ok and momentum_ok and volume_ok:
        strategy = "偏多順勢"
        reason.append("股價站穩中期均線，趨勢結構完整")
        reason.append("KD / MACD 同步偏多")
        if near_resistance is not None:
            action.append(f"若有效突破 {near_resistance:.2f} 並帶量，可考慮順勢追蹤")
        if near_support is not None:
            action.append(f"回測 {near_support:.2f} 附近不破，可分批布局")
        if near_support is not None:
            risk.append(f"跌破 {near_support:.2f} 應保守處理")
    elif stance in ["中性偏多", "偏多"]:
        strategy = "區間偏多"
        reason.append("整體技術面尚可，但未完全共振")
        if near_support is not None:
            action.append(f"靠近 {near_support:.2f} 留意承接")
        if near_resistance is not None:
            action.append(f"接近 {near_resistance:.2f} 留意壓力與量價是否續強")
        if near_support is not None:
            risk.append(f"失守 {near_support:.2f} 轉弱")
    else:
        strategy = "保守觀望"
        reason.append("趨勢或動能未明顯站回優勢")
        if near_resistance is not None:
            action.append(f"需先觀察是否站上 {near_resistance:.2f} 後再評估")
        if near_support is not None:
            risk.append(f"若跌破 {near_support:.2f}，下行風險增加")

    return {
        "strategy": strategy,
        "reason": reason,
        "action": action,
        "risk": risk,
    }


def _build_executable_plan(df: pd.DataFrame, signal: dict, sr: dict) -> dict:
    if len(df) == 0:
        return {}

    last = df.iloc[-1]
    close = _to_float(last["Close"])
    near_support = sr.get("near_support")
    near_resistance = sr.get("near_resistance")

    buy_zone = None
    chase_zone = None
    stop_loss = None
    take_profit_1 = None
    take_profit_2 = None

    if near_support is not None:
        buy_zone = (near_support * 0.995, near_support * 1.015)
        stop_loss = near_support * 0.97

    if near_resistance is not None:
        chase_zone = (near_resistance * 1.003, near_resistance * 1.02)
        take_profit_1 = near_resistance * 1.05
        take_profit_2 = near_resistance * 1.10

    return {
        "current_price": close,
        "buy_zone": buy_zone,
        "chase_zone": chase_zone,
        "stop_loss": stop_loss,
        "tp1": take_profit_1,
        "tp2": take_profit_2,
    }


# =========================
# 圖表
# =========================
def _make_main_chart(df: pd.DataFrame, code: str, name: str):
    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.56, 0.14, 0.14, 0.16],
        specs=[[{"secondary_y": False}], [{}], [{}], [{}]],
    )

    up_mask = df["Close"] >= df["Open"]
    down_mask = ~up_mask

    # K線
    fig.add_trace(
        go.Candlestick(
            x=df["Date"],
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name="K線",
        ),
        row=1,
        col=1,
    )

    # 均線
    for ma in ["MA5", "MA10", "MA20", "MA60", "MA120"]:
        if ma in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df["Date"],
                    y=df[ma],
                    mode="lines",
                    name=ma,
                    line=dict(width=1.6),
                ),
                row=1,
                col=1,
            )

    # 量能
    fig.add_trace(
        go.Bar(
            x=df.loc[up_mask, "Date"],
            y=df.loc[up_mask, "Volume"],
            name="量能↑",
            opacity=0.65,
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=df.loc[down_mask, "Date"],
            y=df.loc[down_mask, "Volume"],
            name="量能↓",
            opacity=0.45,
        ),
        row=2,
        col=1,
    )
    if "VolMA5" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["Date"],
                y=df["VolMA5"],
                mode="lines",
                name="VolMA5",
                line=dict(width=1.2),
            ),
            row=2,
            col=1,
        )
    if "VolMA20" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["Date"],
                y=df["VolMA20"],
                mode="lines",
                name="VolMA20",
                line=dict(width=1.2),
            ),
            row=2,
            col=1,
        )

    # KD
    fig.add_trace(
        go.Scatter(x=df["Date"], y=df["K"], mode="lines", name="K", line=dict(width=1.8)),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["Date"], y=df["D"], mode="lines", name="D", line=dict(width=1.8)),
        row=3,
        col=1,
    )
    fig.add_hline(y=80, line_width=1, line_dash="dot", row=3, col=1)
    fig.add_hline(y=20, line_width=1, line_dash="dot", row=3, col=1)

    # MACD
    fig.add_trace(
        go.Bar(x=df["Date"], y=df["OSC"], name="OSC", opacity=0.6),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["Date"], y=df["DIF"], mode="lines", name="DIF", line=dict(width=1.8)),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["Date"], y=df["MACD"], mode="lines", name="MACD", line=dict(width=1.8)),
        row=4,
        col=1,
    )

    fig.update_layout(
        title=f"{code} {name}｜歷史K線分析",
        template="plotly_dark",
        height=980,
        xaxis_rangeslider_visible=False,
        margin=dict(l=18, r=18, t=50, b=18),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
        hovermode="x unified",
        barmode="overlay",
    )
    fig.update_yaxes(title_text="價格", row=1, col=1)
    fig.update_yaxes(title_text="成交量", row=2, col=1)
    fig.update_yaxes(title_text="KD", row=3, col=1)
    fig.update_yaxes(title_text="MACD", row=4, col=1)
    return fig


def _make_radar_chart(radar_scores: dict):
    if not radar_scores:
        return None
    labels = list(radar_scores.keys())
    values = list(radar_scores.values())
    labels_closed = labels + [labels[0]]
    values_closed = values + [values[0]]

    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=values_closed,
            theta=labels_closed,
            fill="toself",
            name="雷達評分",
        )
    )
    fig.update_layout(
        template="plotly_dark",
        height=430,
        margin=dict(l=20, r=20, t=30, b=20),
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=False,
    )
    return fig


# =========================
# 資料抓取
# =========================
@st.cache_data(show_spinner=False, ttl=300)
def _fetch_history_with_fallback(code: str, start_date: date, end_date: date) -> pd.DataFrame:
    """
    先用現有 utils.get_history_data
    若失敗，補做 .TWO / .TW fallback 邏輯
    """
    candidates = []
    code = _normalize_code(code)

    if not code:
        return pd.DataFrame()

    # 原始代號優先
    candidates.append(code)

    # 台股常見 fallback
    if not code.endswith(".TW") and not code.endswith(".TWO"):
        candidates.append(f"{code}.TW")
        candidates.append(f"{code}.TWO")

    tried = []
    for c in candidates:
        tried.append(c)
        try:
            df = get_history_data(c, start_date, end_date)
            df = _normalize_history_df(df)
            if len(df) > 0:
                return df
        except Exception:
            pass

    return pd.DataFrame()


# =========================
# 主畫面
# =========================
def main():
    _ensure_ss_defaults()

    render_pro_hero(
        "歷史K線分析",
        "股神版技術分析終端｜搜尋同步群組｜事件面板｜焦點摘要｜K線/KD/MACD｜雷達/訊號/支撐壓力｜策略執行區"
    )

    group_map = _extract_group_map_from_watchlist()
    search_records = _build_search_records(group_map)
    _sync_search_group_stock(group_map, search_records)

    code, name = _pick_code_name()

    # -------------------------
    # 控制列
    # -------------------------
    render_pro_section("條件設定")

    c1, c2, c3, c4 = st.columns([1.25, 1.1, 1.25, 1.3])

    with c1:
        st.text_input(
            "快速搜尋股票",
            key=SS_SEARCH_KEY,
            placeholder="輸入代號或名稱，例如 2330 / 台積電",
            help="搜尋命中後，會自動同步群組與群組股票",
        )

    with c2:
        groups = list(group_map.keys())
        if st.session_state[SS_GROUP_KEY] not in groups:
            st.session_state[SS_GROUP_KEY] = groups[0] if groups else "全部股票"

        st.selectbox(
            "群組",
            options=groups,
            key=SS_GROUP_KEY,
            on_change=_on_group_change,
            args=(group_map,),
        )

    with c3:
        current_group = st.session_state.get(SS_GROUP_KEY, "全部股票")
        stock_items = group_map.get(current_group, [])
        stock_labels = [f'{x["code"]} {_safe_text(x["name"])}'.strip() for x in stock_items]

        if st.session_state.get(SS_STOCK_KEY, "") not in stock_labels:
            if stock_labels:
                st.session_state[SS_STOCK_KEY] = stock_labels[0]
                st.session_state[SS_CODE_KEY] = stock_items[0]["code"]

        st.selectbox(
            "群組股票",
            options=stock_labels if stock_labels else [""],
            key=SS_STOCK_KEY,
            on_change=_on_stock_change,
        )

    with c4:
        d1, d2 = st.columns(2)
        with d1:
            st.date_input("開始日期", key=SS_START_KEY)
        with d2:
            st.date_input("結束日期", key=SS_END_KEY)

    # 搜尋結果快捷區
    search_kw = _safe_text(st.session_state.get(SS_SEARCH_KEY, ""))
    matches = _search_stock_records(search_records, search_kw, limit=10) if search_kw else []

    if search_kw and matches:
        st.markdown("##### 搜尋快捷選單")
        mcols = st.columns(min(5, len(matches)))
        for i, m in enumerate(matches[:5]):
            with mcols[i]:
                if st.button(
                    f'{m["code"]} {_safe_text(m["name"])}',
                    key=f"search_hit_{m['code']}",
                    use_container_width=True,
                ):
                    hit_group = m.get("group") if m.get("group") in group_map else "全部股票"
                    st.session_state[SS_GROUP_KEY] = hit_group
                    st.session_state[SS_CODE_KEY] = m["code"]
                    st.session_state[SS_STOCK_KEY] = f'{m["code"]} {_safe_text(m["name"])}'.strip()
                    st.session_state[SS_SEARCH_KEY] = st.session_state[SS_STOCK_KEY]
                    st.rerun()

    code, name = _pick_code_name()

    if not code:
        st.warning("目前沒有可分析的股票，請先確認自選股或群組資料。")
        return

    start_date = st.session_state.get(SS_START_KEY, DEFAULT_START)
    end_date = st.session_state.get(SS_END_KEY, DEFAULT_END)

    if start_date > end_date:
        st.error("開始日期不可大於結束日期。")
        return

    # -------------------------
    # 抓資料
    # -------------------------
    with st.spinner(f"讀取 {code} {name} 歷史資料中..."):
        hist_df_raw = _fetch_history_with_fallback(code, start_date, end_date)

    if hist_df_raw is None or len(hist_df_raw) == 0:
        st.error(f"查無 {code} {name} 的歷史資料。已嘗試上市 / 上櫃 fallback。")
        return

    df = _prepare_tech_df(hist_df_raw)
    if len(df) == 0:
        st.error("歷史資料格式異常，無法完成技術分析。")
        return

    signal = _get_signal_snapshot(df)
    sr = _get_sr_snapshot(df)
    radar_scores = _get_radar_scores(df)
    events = _build_event_panel(df)
    focus_summary = _build_focus_summary(df, events, signal, sr)
    price_summary = _build_price_summary(sr)
    strategy_plan = _build_strategy_plan(df, signal, sr)
    exec_plan = _build_executable_plan(df, signal, sr)

    # -------------------------
    # 基本KPI
    # -------------------------
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last

    last_close = _to_float(last["Close"])
    prev_close = _to_float(prev["Close"])
    chg = last_close - prev_close if not np.isnan(last_close) and not np.isnan(prev_close) else np.nan
    chg_pct = (chg / prev_close * 100) if prev_close not in [0, np.nan] and not np.isnan(prev_close) else np.nan
    day_high = _to_float(last["High"])
    day_low = _to_float(last["Low"])
    volume = _to_float(last["Volume"])

    render_pro_kpi_row(
        [
            ("股票", f"{code} {name}"),
            ("收盤", f"{last_close:.2f}" if not np.isnan(last_close) else "—"),
            ("漲跌", f"{chg:+.2f}" if not np.isnan(chg) else "—"),
            ("漲跌幅", f"{chg_pct:+.2f}%" if not np.isnan(chg_pct) else "—"),
            ("區間高低", f"{day_low:.2f} ~ {day_high:.2f}" if not np.isnan(day_low) and not np.isnan(day_high) else "—"),
            ("成交量", format_number(volume) if not np.isnan(volume) else "—"),
        ]
    )

    # -------------------------
    # 摘要條
    # -------------------------
    csum1, csum2 = st.columns(2)

    with csum1:
        st.markdown(
            f"""
            <div class="hist-top-bar">
                <div class="hist-top-title">目前焦點事件摘要條</div>
                <div class="hist-top-value">{focus_summary}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with csum2:
        st.markdown(
            f"""
            <div class="hist-top-bar">
                <div class="hist-top-title">關鍵價位摘要條</div>
                <div class="hist-top-value">{price_summary}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # -------------------------
    # 左事件 / 右主圖
    # -------------------------
    left, right = st.columns([0.95, 2.7], gap="large")

    with left:
        render_pro_section("左側事件面板")

        if events:
            for e in events[:12]:
                st.markdown(
                    f"""
                    <div class="event-card">
                        <div class="event-date">{e['date']}</div>
                        <div class="event-title">{e['title']}</div>
                        <div class="event-desc">{e['desc']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
        else:
            render_pro_info_card("近期事件", "目前未偵測到關鍵事件。")

    with right:
        render_pro_section("K線 / KD / MACD")
        main_fig = _make_main_chart(df, code, name)
        st.plotly_chart(main_fig, use_container_width=True, config={"displaylogo": False})

    # -------------------------
    # 雷達 / 訊號 / 支撐壓力
    # -------------------------
    c_radar, c_signal, c_sr = st.columns([1.15, 1, 1], gap="large")

    with c_radar:
        render_pro_section("雷達評分")
        radar_fig = _make_radar_chart(radar_scores)
        if radar_fig is not None:
            st.plotly_chart(radar_fig, use_container_width=True, config={"displaylogo": False})
        else:
            render_pro_info_card("雷達評分", "目前無法產出雷達資料。")

    with c_signal:
        render_pro_section("訊號快照")
        stance = _safe_text(signal.get("stance"), "中性")
        score = signal.get("score", "—")

        stance_cls = "state-good" if "多" in stance else ("state-bad" if "弱" in stance else "state-warn")
        st.markdown(
            f"""
            <div class="strategy-box">
                <div class="strategy-title">技術面判斷</div>
                <div class="strategy-line">整體立場：<span class="{stance_cls}">{stance}</span></div>
                <div class="strategy-line">綜合分數：{score}</div>
                <div class="strategy-line">趨勢分數：{signal.get("trend_score", "—")}</div>
                <div class="strategy-line">動能分數：{signal.get("momentum_score", "—")}</div>
                <div class="strategy-line">量能分數：{signal.get("volume_score", "—")}</div>
                <div class="strategy-line">MA20 狀態：{"站上" if signal.get("close_above_ma20") else "未站上"}</div>
                <div class="strategy-line">KD 黃金交叉：{"是" if signal.get("kd_golden") else "否"}</div>
                <div class="strategy-line">MACD 黃金交叉：{"是" if signal.get("macd_golden") else "否"}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c_sr:
        render_pro_section("支撐 / 壓力")
        supports = sr.get("supports", [])
        resistances = sr.get("resistances", [])
        near_support = sr.get("near_support")
        near_resistance = sr.get("near_resistance")

        st.markdown(
            f"""
            <div class="strategy-box">
                <div class="strategy-title">關鍵價位</div>
                <div class="strategy-line">近支撐：{f"{near_support:.2f}" if near_support is not None else "—"}</div>
                <div class="strategy-line">近壓力：{f"{near_resistance:.2f}" if near_resistance is not None else "—"}</div>
                <div class="strategy-line">支撐帶：{" / ".join(f"{x:.2f}" for x in supports[:4]) if supports else "—"}</div>
                <div class="strategy-line">壓力帶：{" / ".join(f"{x:.2f}" for x in resistances[:4]) if resistances else "—"}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # -------------------------
    # 策略區
    # -------------------------
    render_pro_section("策略區")
    s1, s2 = st.columns(2, gap="large")

    with s1:
        strategy = strategy_plan.get("strategy", "觀望")
        reasons = strategy_plan.get("reason", [])
        actions = strategy_plan.get("action", [])
        risks = strategy_plan.get("risk", [])

        reason_html = "".join([f'<div class="strategy-line">• {x}</div>' for x in reasons]) or '<div class="strategy-line">• 無</div>'
        action_html = "".join([f'<div class="strategy-line">• {x}</div>' for x in actions]) or '<div class="strategy-line">• 無</div>'
        risk_html = "".join([f'<div class="strategy-line">• {x}</div>' for x in risks]) or '<div class="strategy-line">• 無</div>'

        st.markdown(
            f"""
            <div class="strategy-box">
                <div class="strategy-title">策略判讀：{strategy}</div>
                <div class="mini-muted">為技術面推演，不代表投資建議</div>
                <div class="strategy-line" style="margin-top:10px;font-weight:700;">判讀依據</div>
                {reason_html}
                <div class="strategy-line" style="margin-top:10px;font-weight:700;">操作重點</div>
                {action_html}
                <div class="strategy-line" style="margin-top:10px;font-weight:700;">風險提醒</div>
                {risk_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

    with s2:
        buy_zone = exec_plan.get("buy_zone")
        chase_zone = exec_plan.get("chase_zone")
        stop_loss = exec_plan.get("stop_loss")
        tp1 = exec_plan.get("tp1")
        tp2 = exec_plan.get("tp2")
        current_price = exec_plan.get("current_price")

        st.markdown(
            f"""
            <div class="strategy-box">
                <div class="strategy-title">可執行策略區</div>
                <div class="mini-muted">給你直接看的執行區間，不改原功能，只做強化</div>
                <div class="strategy-line">目前價格：{f"{current_price:.2f}" if current_price is not None else "—"}</div>
                <div class="strategy-line">承接區：{f"{buy_zone[0]:.2f} ~ {buy_zone[1]:.2f}" if buy_zone else "—"}</div>
                <div class="strategy-line">突破追價區：{f"{chase_zone[0]:.2f} ~ {chase_zone[1]:.2f}" if chase_zone else "—"}</div>
                <div class="strategy-line">防守點：{f"{stop_loss:.2f}" if stop_loss is not None else "—"}</div>
                <div class="strategy-line">第一目標：{f"{tp1:.2f}" if tp1 is not None else "—"}</div>
                <div class="strategy-line">第二目標：{f"{tp2:.2f}" if tp2 is not None else "—"}</div>
                <div class="strategy-line" style="margin-top:10px;">執行原則：</div>
                <div class="strategy-line">• 靠近支撐優先看承接是否有效</div>
                <div class="strategy-line">• 突破壓力需搭配量能確認，不帶量不追</div>
                <div class="strategy-line">• 進場前先看風險報酬比，再看趨勢是否延續</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # -------------------------
    # 原始資料表
    # -------------------------
    with st.expander("檢視技術分析明細資料", expanded=False):
        show_cols = [
            "Date", "Open", "High", "Low", "Close", "Volume",
            "MA5", "MA10", "MA20", "MA60", "MA120",
            "K", "D", "J", "DIF", "MACD", "OSC",
            "Ret1", "Ret5", "Ret20", "Volatility20",
        ]
        show_cols = [c for c in show_cols if c in df.columns]
        preview = df[show_cols].tail(120).copy()
        st.dataframe(preview, use_container_width=True, height=420)

    st.caption("已優化：搜尋/群組/股票同步、session_state 控制、上櫃 fallback、技術分析快取、事件面板與策略執行區保留並強化。")


# =========================
# 執行
# =========================
try:
    main()
except Exception as e:
    st.error("歷史K線分析頁面執行失敗")
    st.code(traceback.format_exc())
