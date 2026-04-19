from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
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
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
)

PAGE_TITLE = "股神推薦"
PFX = "godpick_"


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


def _normalize_code(v: Any) -> str:
    text = _safe_str(v)
    if not text:
        return ""
    if text.isdigit():
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits
    return text


def _normalize_category(v: Any) -> str:
    text = _safe_str(v)
    if not text:
        return ""
    return text.replace("　", " ").strip()


def _score_clip(v: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, v))


def _avg_safe(values: list[float | None], default: float = 0.0) -> float:
    clean = [float(x) for x in values if x is not None]
    if not clean:
        return default
    return sum(clean) / len(clean)


def _infer_category_from_name(name: str) -> str:
    n = _safe_str(name)
    if not n:
        return "其他"

    rules = [
        ("半導體", ["台積", "聯電", "世界", "創意", "世芯", "力旺", "晶心科", "智原", "矽力", "日月光", "采鈺", "京元電", "頎邦", "南亞科", "華邦電", "旺宏", "群聯", "聯發科", "瑞昱", "聯詠", "敦泰", "力積電", "穩懋", "環球晶", "中美晶", "IC", "晶圓", "封測", "半導體"]),
        ("AI", ["AI", "人工智慧", "伺服器", "Server", "GPU", "ASIC", "CPO", "光通訊", "散熱", "機殼", "高速傳輸", "交換器"]),
        ("電子", ["電子", "電腦", "主機板", "顯卡", "NB", "網通", "通訊", "電源", "零組件", "光電", "面板", "組裝", "模組"]),
        ("金融", ["金控", "銀行", "證券", "保險"]),
        ("航運", ["航運", "海運", "貨櫃", "航空"]),
        ("鋼鐵", ["鋼", "鋼鐵"]),
        ("塑化", ["塑膠", "塑化", "化工"]),
        ("生技醫療", ["生技", "醫療", "藥", "製藥", "檢測", "疫苗"]),
        ("汽車", ["車電", "汽車", "車用", "電動車"]),
        ("綠能", ["太陽能", "風電", "儲能", "綠能", "能源"]),
        ("營建", ["營建", "建設"]),
        ("食品", ["食品", "餐飲"]),
        ("觀光", ["觀光", "旅遊", "飯店"]),
        ("紡織", ["紡織", "成衣"]),
        ("通信網路", ["網路", "網通", "通訊", "電信"]),
        ("電機機械", ["機電", "機械", "自動化", "工具機"]),
    ]

    lower_n = n.lower()
    for category, keywords in rules:
        for kw in keywords:
            if kw.lower() in lower_n:
                return category

    return "其他"


# =========================================================
# watchlist / 主檔
# =========================================================
def _load_watchlist_map() -> dict[str, list[dict[str, str]]]:
    raw = st.session_state.get("watchlist_data")
    if not isinstance(raw, dict) or not raw:
        raw = get_normalized_watchlist()

    result: dict[str, list[dict[str, str]]] = {}

    if isinstance(raw, dict):
        for group_name, items in raw.items():
            g = _safe_str(group_name)
            if not g:
                continue

            rows = []
            seen = set()

            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue

                    code = _normalize_code(item.get("code"))
                    name = _safe_str(item.get("name")) or code
                    market = _safe_str(item.get("market")) or "上市"
                    category = _normalize_category(item.get("category"))
                    if not category:
                        category = _infer_category_from_name(name)

                    if not code or code in seen:
                        continue
                    seen.add(code)

                    rows.append(
                        {
                            "code": code,
                            "name": name,
                            "market": market,
                            "category": category,
                            "label": f"{code} {name}",
                        }
                    )

            result[g] = rows

    return result


@st.cache_data(ttl=1800, show_spinner=False)
def _load_master_df() -> pd.DataFrame:
    dfs = []
    category_candidates = [
        "category", "industry", "sector", "theme",
        "類別", "產業別", "產業", "主題",
    ]

    for market_arg in ["", "上市", "上櫃", "興櫃"]:
        try:
            df = get_all_code_name_map(market_arg)
            if isinstance(df, pd.DataFrame) and not df.empty:
                temp = df.copy()

                mapping = {
                    "證券代號": "code",
                    "證券名稱": "name",
                    "市場別": "market",
                    "code": "code",
                    "name": "name",
                    "market": "market",
                }
                temp = temp.rename(columns=mapping)

                found_category_col = None
                for col in temp.columns:
                    if str(col).strip() in category_candidates:
                        found_category_col = col
                        break
                if found_category_col:
                    temp = temp.rename(columns={found_category_col: "category"})

                for col in ["code", "name", "market"]:
                    if col not in temp.columns:
                        temp[col] = ""
                if "category" not in temp.columns:
                    temp["category"] = ""

                temp["code"] = temp["code"].map(_normalize_code)
                temp["name"] = temp["name"].map(_safe_str)
                temp["market"] = temp["market"].map(_safe_str)
                temp["category"] = temp["category"].map(_normalize_category)

                if market_arg in ["上市", "上櫃", "興櫃"]:
                    temp["market"] = temp["market"].replace("", market_arg)

                temp["category"] = temp.apply(
                    lambda r: _normalize_category(r.get("category")) or _infer_category_from_name(r.get("name")),
                    axis=1,
                )

                dfs.append(temp[["code", "name", "market", "category"]])
        except Exception:
            pass

    if not dfs:
        return pd.DataFrame(columns=["code", "name", "market", "category"])

    out = pd.concat(dfs, ignore_index=True)
    out["code"] = out["code"].map(_normalize_code)
    out["name"] = out["name"].map(_safe_str)
    out["market"] = out["market"].map(_safe_str).replace("", "上市")
    out["category"] = out["category"].map(_normalize_category)
    out = out[out["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out


def _find_name_market_category(
    code: str,
    manual_name: str,
    manual_market: str,
    manual_category: str,
    master_df: pd.DataFrame,
) -> tuple[str, str, str]:
    code = _normalize_code(code)
    manual_name = _safe_str(manual_name)
    manual_market = _safe_str(manual_market)
    manual_category = _normalize_category(manual_category)

    if isinstance(master_df, pd.DataFrame) and not master_df.empty:
        matched = master_df[master_df["code"].astype(str) == code]
        if not matched.empty:
            row = matched.iloc[0]
            final_name = _safe_str(row.get("name")) or manual_name or code
            final_market = _safe_str(row.get("market")) or manual_market or "上市"
            final_category = _normalize_category(row.get("category")) or manual_category
            if not final_category:
                final_category = _infer_category_from_name(final_name)
            return final_name, final_market, final_category

    final_name = manual_name or code
    final_market = manual_market or "上市"
    final_category = manual_category or _infer_category_from_name(final_name)
    return final_name, final_market, final_category


def _parse_manual_codes(text: str, master_df: pd.DataFrame) -> list[dict[str, str]]:
    rows = []
    seen = set()

    raw_lines = [x.strip() for x in _safe_str(text).replace("，", "\n").replace(",", "\n").splitlines() if x.strip()]
    for raw in raw_lines:
        txt = _safe_str(raw)
        code = _normalize_code(txt)
        name = ""
        market = "上市"
        category = ""

        if not code:
            if isinstance(master_df, pd.DataFrame) and not master_df.empty:
                matched = master_df[master_df["name"].astype(str).str.contains(txt, case=False, na=False)]
                if not matched.empty:
                    row = matched.iloc[0]
                    code = _normalize_code(row.get("code"))
                    name = _safe_str(row.get("name"))
                    market = _safe_str(row.get("market")) or "上市"
                    category = _normalize_category(row.get("category"))

        if code and not name:
            name, market, category = _find_name_market_category(code, "", market, category, master_df)

        if code and code not in seen:
            seen.add(code)
            rows.append(
                {
                    "code": code,
                    "name": name or code,
                    "market": market or "上市",
                    "category": category,
                    "label": f"{code} {name or code}",
                }
            )
    return rows


def _build_universe_from_market(
    master_df: pd.DataFrame,
    market_mode: str,
    limit_count: int,
    selected_categories: list[str],
) -> list[dict[str, str]]:
    if master_df is None or master_df.empty:
        return []

    work = master_df.copy()
    market_mode = _safe_str(market_mode)

    if market_mode == "上市":
        work = work[work["market"].astype(str) == "上市"].copy()
    elif market_mode == "上櫃":
        work = work[work["market"].astype(str) == "上櫃"].copy()

    clean_categories = [_normalize_category(x) for x in selected_categories if _normalize_category(x) and x != "全部"]
    if clean_categories:
        work = work[work["category"].astype(str).isin(clean_categories)].copy()

    work = work.head(limit_count).copy()

    rows = []
    for _, row in work.iterrows():
        code = _normalize_code(row.get("code"))
        name = _safe_str(row.get("name")) or code
        market = _safe_str(row.get("market")) or "上市"
        category = _normalize_category(row.get("category")) or _infer_category_from_name(name)
        if code:
            rows.append(
                {
                    "code": code,
                    "name": name,
                    "market": market,
                    "category": category,
                    "label": f"{code} {name}",
                }
            )
    return rows


def _collect_all_categories(master_df: pd.DataFrame, watchlist_map: dict[str, list[dict[str, str]]]) -> list[str]:
    cats = set()

    if isinstance(master_df, pd.DataFrame) and not master_df.empty:
        for _, row in master_df.iterrows():
            name = _safe_str(row.get("name"))
            cat = _normalize_category(row.get("category")) or _infer_category_from_name(name)
            if cat:
                cats.add(cat)

    if isinstance(watchlist_map, dict):
        for _, items in watchlist_map.items():
            for item in items:
                name = _safe_str(item.get("name"))
                cat = _normalize_category(item.get("category")) or _infer_category_from_name(name)
                if cat:
                    cats.add(cat)

    return sorted(list(cats))


# =========================================================
# 歷史資料
# =========================================================
def _prepare_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    temp = df.copy()
    if "日期" not in temp.columns:
        return pd.DataFrame()

    temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
    temp = temp.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]:
        if col in temp.columns:
            temp[col] = pd.to_numeric(temp[col], errors="coerce")

    if "收盤價" not in temp.columns:
        return pd.DataFrame()

    temp = temp.dropna(subset=["收盤價"]).copy()
    if temp.empty:
        return pd.DataFrame()

    close = temp["收盤價"]
    high = temp["最高價"] if "最高價" in temp.columns else close
    low = temp["最低價"] if "最低價" in temp.columns else close
    vol = pd.to_numeric(temp["成交股數"], errors="coerce") if "成交股數" in temp.columns else pd.Series(index=temp.index, dtype=float)

    for n in [5, 10, 20, 60, 120, 240]:
        temp[f"MA{n}"] = close.rolling(n).mean()

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    temp["ATR14"] = tr.rolling(14).mean()

    temp["VOL5"] = vol.rolling(5).mean()
    temp["VOL20"] = vol.rolling(20).mean()

    temp["RET5"] = close.pct_change(5) * 100
    temp["RET20"] = close.pct_change(20) * 100
    temp["RET60"] = close.pct_change(60) * 100

    temp["UP_DAY"] = (close > close.shift(1)).astype(float)

    return temp


@st.cache_data(ttl=300, show_spinner=False)
def _get_history_smart(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date) -> tuple[pd.DataFrame, str]:
    tried = []
    for mk in [market_type, "上市", "上櫃", "興櫃", ""]:
        mk = _safe_str(mk)
        if mk not in tried:
            tried.append(mk)

    for mk in tried:
        try:
            df = get_history_data(
                stock_no=stock_no,
                stock_name=stock_name,
                market_type=mk,
                start_date=start_date,
                end_date=end_date,
            )
        except TypeError:
            try:
                df = get_history_data(
                    stock_no=stock_no,
                    stock_name=stock_name,
                    market_type=mk,
                    start_dt=start_date,
                    end_dt=end_date,
                )
            except Exception:
                df = pd.DataFrame()
        except Exception:
            df = pd.DataFrame()

        df = _prepare_history_df(df)
        if not df.empty:
            return df, (mk or market_type or "未知")

    return pd.DataFrame(), (_safe_str(market_type) or "未知")


# =========================================================
# 分析 bundle
# =========================================================
def _build_auto_factor_scores(
    df: pd.DataFrame,
    signal_snapshot: dict,
    sr_snapshot: dict,
    radar: dict,
) -> dict[str, Any]:
    last = df.iloc[-1]

    close_now = _safe_float(last.get("收盤價"))
    ma20 = _safe_float(last.get("MA20"))
    ma60 = _safe_float(last.get("MA60"))
    ma120 = _safe_float(last.get("MA120"))
    atr14 = _safe_float(last.get("ATR14"))
    vol5 = _safe_float(last.get("VOL5"))
    vol20 = _safe_float(last.get("VOL20"))
    ret20 = _safe_float(last.get("RET20"))
    ret60 = _safe_float(last.get("RET60"))

    signal_score = _safe_float(signal_snapshot.get("score"), 0) or 0
    radar_trend = _safe_float(radar.get("trend"), 50) or 50
    radar_momentum = _safe_float(radar.get("momentum"), 50) or 50
    radar_volume = _safe_float(radar.get("volume"), 50) or 50
    radar_structure = _safe_float(radar.get("structure"), 50) or 50

    sup20 = _safe_float(sr_snapshot.get("sup_20"))

    eps_proxy = 50.0
    if close_now not in [None, 0]:
        trend_bonus = 0.0
        if ma120 is not None and close_now > ma120:
            trend_bonus += 18
        if ma60 is not None and close_now > ma60:
            trend_bonus += 12
        if ma20 is not None and close_now > ma20:
            trend_bonus += 8

        vol_penalty = 0.0
        if atr14 is not None:
            atr_pct = atr14 / close_now * 100
            if atr_pct <= 2.5:
                vol_penalty = 0
            elif atr_pct <= 5:
                vol_penalty = 6
            else:
                vol_penalty = 12

        eps_proxy = _score_clip(30 + trend_bonus + radar_structure * 0.25 + radar_trend * 0.20 - vol_penalty)

    revenue_proxy = _score_clip(
        25
        + (_safe_float(ret20, 0) or 0) * 0.9
        + (_safe_float(ret60, 0) or 0) * 0.35
        + radar_momentum * 0.30
        + radar_volume * 0.20
    )

    profit_proxy = _score_clip(
        30
        + signal_score * 6
        + radar_trend * 0.28
        + radar_structure * 0.22
        + (_safe_float(ret60, 0) or 0) * 0.35
    )

    lock_proxy = 45.0
    if close_now not in [None, 0]:
        vol_ratio = None
        if vol5 not in [None, 0] and vol20 not in [None, 0]:
            vol_ratio = vol5 / vol20

        atr_pct = None
        if atr14 is not None:
            atr_pct = atr14 / close_now * 100

        lock_bonus = 0.0
        if ma20 is not None and close_now >= ma20:
            lock_bonus += 12
        if sup20 is not None and close_now >= sup20:
            lock_bonus += 10
        if vol_ratio is not None:
            if 0.7 <= vol_ratio <= 1.15:
                lock_bonus += 12
            elif vol_ratio < 0.7:
                lock_bonus += 8
        if atr_pct is not None:
            if atr_pct <= 2.5:
                lock_bonus += 14
            elif atr_pct <= 4:
                lock_bonus += 8

        lock_proxy = _score_clip(20 + lock_bonus + radar_structure * 0.24)

    recent = df.tail(5).copy()
    up_days_5 = int(recent["UP_DAY"].sum()) if "UP_DAY" in recent.columns else 0
    inst_proxy = _score_clip(
        20
        + up_days_5 * 10
        + signal_score * 5
        + radar_momentum * 0.25
        + radar_volume * 0.20
    )

    factor_summary = (
        f"EPS代理 {format_number(eps_proxy,1)} / "
        f"營收動能代理 {format_number(revenue_proxy,1)} / "
        f"獲利代理 {format_number(profit_proxy,1)} / "
        f"大戶鎖碼代理 {format_number(lock_proxy,1)} / "
        f"法人連買代理 {format_number(inst_proxy,1)}"
    )

    return {
        "auto_factor_total": _avg_safe([eps_proxy, revenue_proxy, profit_proxy, lock_proxy, inst_proxy], 0),
        "eps_proxy": eps_proxy,
        "revenue_proxy": revenue_proxy,
        "profit_proxy": profit_proxy,
        "lock_proxy": lock_proxy,
        "inst_proxy": inst_proxy,
        "factor_summary": factor_summary,
    }


def _build_trade_plan(df: pd.DataFrame, sr_snapshot: dict, signal_snapshot: dict) -> dict[str, Any]:
    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"), 0) or 0
    atr14 = _safe_float(last.get("ATR14"), 0) or max(close_now * 0.03, 1.0)
    ma20 = _safe_float(last.get("MA20"))
    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))
    res60 = _safe_float(sr_snapshot.get("res_60"))
    score = _safe_float(signal_snapshot.get("score"), 0) or 0

    breakout_buy = res20 if res20 is not None else close_now
    pullback_buy = ma20 if ma20 is not None else (sup20 if sup20 is not None else close_now)
    stop_price = sup20 if sup20 is not None else max(close_now - atr14, 0)
    sell_target_1 = res20 if res20 is not None and res20 > close_now else close_now + atr14 * 1.5
    sell_target_2 = res60 if res60 is not None and res60 > sell_target_1 else sell_target_1 + atr14 * 1.2

    if score >= 4:
        launch_tag = "強勢起漲候選"
    elif score >= 2:
        launch_tag = "偏多轉強候選"
    elif score <= -2:
        launch_tag = "不建議追價"
    else:
        launch_tag = "等待表態"

    def _rr(entry: float, stop: float, target: float) -> str:
        risk = entry - stop
        reward = target - entry
        if risk <= 0:
            return "—"
        return f"1 : {reward / risk:.2f}"

    rr1 = _rr(pullback_buy, stop_price, sell_target_1) if pullback_buy and stop_price is not None and sell_target_1 else "—"
    rr2 = _rr(breakout_buy, stop_price, sell_target_2) if breakout_buy and stop_price is not None and sell_target_2 else "—"

    return {
        "launch_tag": launch_tag,
        "breakout_buy": breakout_buy,
        "pullback_buy": pullback_buy,
        "stop_price": stop_price,
        "sell_target_1": sell_target_1,
        "sell_target_2": sell_target_2,
        "rr1": rr1,
        "rr2": rr2,
    }


@st.cache_data(ttl=300, show_spinner=False)
def _analyze_stock_bundle(stock_no: str, stock_name: str, market_type: str, start_dt: date, end_dt: date) -> dict[str, Any]:
    hist_df, used_market = _get_history_smart(
        stock_no=stock_no,
        stock_name=stock_name,
        market_type=market_type,
        start_date=start_dt,
        end_date=end_dt,
    )
    if hist_df.empty:
        return {}

    signal_snapshot = compute_signal_snapshot(hist_df)
    sr_snapshot = compute_support_resistance_snapshot(hist_df)
    radar = compute_radar_scores(hist_df)
    auto_factor = _build_auto_factor_scores(hist_df, signal_snapshot, sr_snapshot, radar)
    trade_plan = _build_trade_plan(hist_df, sr_snapshot, signal_snapshot)

    last = hist_df.iloc[-1]
    first = hist_df.iloc[0]

    close_now = _safe_float(last.get("收盤價"))
    close_first = _safe_float(first.get("收盤價"))
    period_pct = None
    if close_now is not None and close_first not in [None, 0]:
        period_pct = ((close_now / close_first) - 1) * 100

    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))
    pressure_dist = None
    support_dist = None
    if close_now is not None and res20 not in [None, 0]:
        pressure_dist = ((res20 - close_now) / res20) * 100
    if close_now is not None and sup20 not in [None, 0]:
        support_dist = ((close_now - sup20) / sup20) * 100

    radar_avg = _avg_safe(
        [
            _safe_float(radar.get("trend")),
            _safe_float(radar.get("momentum")),
            _safe_float(radar.get("volume")),
            _safe_float(radar.get("position")),
            _safe_float(radar.get("structure")),
        ],
        50.0,
    )

    return {
        "used_market": used_market,
        "signal_snapshot": signal_snapshot,
        "sr_snapshot": sr_snapshot,
        "radar": radar,
        "auto_factor": auto_factor,
        "trade_plan": trade_plan,
        "close_now": close_now,
        "period_pct": period_pct,
        "pressure_dist": pressure_dist,
        "support_dist": support_dist,
        "radar_avg": radar_avg,
    }


@st.cache_data(ttl=300, show_spinner=False)
def _build_recommend_df(
    universe_items: list[dict[str, str]],
    master_df: pd.DataFrame,
    start_dt: date,
    end_dt: date,
    min_total_score: float,
    min_signal_score: float,
    selected_categories: list[str],
) -> pd.DataFrame:
    clean_categories = [_normalize_category(x) for x in selected_categories if _normalize_category(x) and x != "全部"]
    rows = []

    for item in universe_items:
        code = _normalize_code(item.get("code"))
        manual_name = _safe_str(item.get("name"))
        manual_market = _safe_str(item.get("market"))
        manual_category = _normalize_category(item.get("category"))
        if not code:
            continue

        stock_name, market_type, category = _find_name_market_category(
            code, manual_name, manual_market, manual_category, master_df
        )

        if clean_categories and category not in clean_categories:
            continue

        bundle = _analyze_stock_bundle(
            stock_no=code,
            stock_name=stock_name,
            market_type=market_type,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        if not bundle:
            continue

        signal_score = _safe_float(bundle["signal_snapshot"].get("score"), 0) or 0
        if signal_score < min_signal_score:
            continue

        auto_factor_total = _safe_float(bundle["auto_factor"].get("auto_factor_total"), 0) or 0
        technical_score = _score_clip(signal_score * 12 + (_safe_float(bundle["radar_avg"], 50) or 50) * 0.45)

        position_bonus = 0.0
        if bundle["pressure_dist"] is not None and 0 <= bundle["pressure_dist"] <= 8:
            position_bonus += 8.0
        if bundle["support_dist"] is not None and 0 <= bundle["support_dist"] <= 6:
            position_bonus += 6.0

        composite = (
            technical_score * 0.44
            + auto_factor_total * 0.40
            + position_bonus
            + (_safe_float(bundle["period_pct"], 0) * 0.10 if bundle["period_pct"] is not None else 0)
        )
        composite = _score_clip(composite)

        if composite < min_total_score:
            continue

        recommendation = "觀察"
        if composite >= 82:
            recommendation = "強烈關注"
        elif composite >= 70:
            recommendation = "優先觀察"
        elif composite >= 58:
            recommendation = "可列追蹤"

        rows.append(
            {
                "股票代號": code,
                "股票名稱": stock_name,
                "市場別": bundle["used_market"],
                "類別": category or _infer_category_from_name(stock_name),
                "最新價": bundle["close_now"],
                "區間漲跌幅%": bundle["period_pct"],
                "訊號分數": signal_score,
                "雷達均分": bundle["radar_avg"],
                "自動因子總分": auto_factor_total,
                "EPS代理分數": bundle["auto_factor"]["eps_proxy"],
                "營收動能代理分數": bundle["auto_factor"]["revenue_proxy"],
                "獲利代理分數": bundle["auto_factor"]["profit_proxy"],
                "大戶鎖碼代理分數": bundle["auto_factor"]["lock_proxy"],
                "法人連買代理分數": bundle["auto_factor"]["inst_proxy"],
                "20日壓力距離%": bundle["pressure_dist"],
                "20日支撐距離%": bundle["support_dist"],
                "推薦總分": composite,
                "推薦等級": recommendation,
                "起漲判斷": bundle["trade_plan"]["launch_tag"],
                "推薦買點_突破": bundle["trade_plan"]["breakout_buy"],
                "推薦買點_拉回": bundle["trade_plan"]["pullback_buy"],
                "停損價": bundle["trade_plan"]["stop_price"],
                "賣出目標1": bundle["trade_plan"]["sell_target_1"],
                "賣出目標2": bundle["trade_plan"]["sell_target_2"],
                "風險報酬_拉回": bundle["trade_plan"]["rr1"],
                "風險報酬_突破": bundle["trade_plan"]["rr2"],
                "自動因子摘要": bundle["auto_factor"]["factor_summary"],
                "雷達摘要": _safe_str(bundle["radar"].get("summary")) or "—",
            }
        )

    return pd.DataFrame(rows)


def _format_df(df: pd.DataFrame) -> pd.DataFrame:
    show = df.copy()
    price_cols = ["最新價", "推薦買點_突破", "推薦買點_拉回", "停損價", "賣出目標1", "賣出目標2"]
    pct_cols = ["區間漲跌幅%", "20日壓力距離%", "20日支撐距離%"]
    score_cols = [
        "訊號分數", "雷達均分", "自動因子總分",
        "EPS代理分數", "營收動能代理分數", "獲利代理分數",
        "大戶鎖碼代理分數", "法人連買代理分數", "推薦總分"
    ]

    for c in price_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 2) if pd.notna(x) else "")
    for c in pct_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: f"{x:,.2f}%" if pd.notna(x) else "")
    for c in score_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 1) if pd.notna(x) else "")

    return show


# =========================================================
# 主畫面
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    watchlist_map = _load_watchlist_map()
    master_df = _load_master_df()
    today = date.today()

    if _k("universe_mode") not in st.session_state:
        st.session_state[_k("universe_mode")] = "自選群組"
    if _k("group") not in st.session_state:
        groups = list(watchlist_map.keys())
        st.session_state[_k("group")] = groups[0] if groups else ""
    if _k("days") not in st.session_state:
        st.session_state[_k("days")] = 120
    if _k("top_n") not in st.session_state:
        st.session_state[_k("top_n")] = 20
    if _k("manual_codes") not in st.session_state:
        st.session_state[_k("manual_codes")] = ""
    if _k("scan_limit") not in st.session_state:
        st.session_state[_k("scan_limit")] = 200
    if _k("selected_categories") not in st.session_state:
        st.session_state[_k("selected_categories")] = ["全部"]
    if _k("min_total_score") not in st.session_state:
        st.session_state[_k("min_total_score")] = 55.0
    if _k("min_signal_score") not in st.session_state:
        st.session_state[_k("min_signal_score")] = -2.0

    render_pro_hero(
        title="股神推薦｜全自動因子版",
        subtitle="已補每支股票類別，現在可直接用類別做推薦判斷與篩選。",
    )

    if st.session_state.get("watchlist_version"):
        st.caption(
            f"自選股同步狀態：watchlist_version = {st.session_state.get('watchlist_version', 0)}"
            + (
                f" / 最後更新：{_safe_str(st.session_state.get('watchlist_last_saved_at', ''))}"
                if _safe_str(st.session_state.get("watchlist_last_saved_at", ""))
                else ""
            )
        )

    render_pro_section("掃描設定")

    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    with c1:
        st.selectbox("掃描範圍", ["自選群組", "手動輸入", "全市場", "上市", "上櫃"], key=_k("universe_mode"))
    with c2:
        group_options = list(watchlist_map.keys()) if watchlist_map else [""]
        if st.session_state.get(_k("group"), "") not in group_options:
            st.session_state[_k("group")] = group_options[0] if group_options else ""
        st.selectbox("自選群組", group_options, key=_k("group"))
    with c3:
        st.selectbox("觀察天數", [60, 90, 120, 180, 240], key=_k("days"))
    with c4:
        st.selectbox("輸出 Top N", [10, 20, 30, 50], key=_k("top_n"))

    d1, d2 = st.columns([2, 2])
    with d1:
        st.selectbox("掃描上限筆數", [100, 200, 300, 500], key=_k("scan_limit"))
    with d2:
        st.text_area(
            "手動輸入股票（可代碼 / 名稱，一行一檔）",
            key=_k("manual_codes"),
            height=110,
            placeholder="2330\n2454\n3548\n台積電",
        )

    all_categories = _collect_all_categories(master_df, watchlist_map)
    category_options = ["全部"] + all_categories if all_categories else ["全部"]

    if any(x not in category_options for x in st.session_state.get(_k("selected_categories"), [])):
        st.session_state[_k("selected_categories")] = ["全部"]

    render_pro_section("類別篩選")
    st.multiselect(
        "選擇類別（可多選）",
        options=category_options,
        key=_k("selected_categories"),
        help="若選擇『全部』或未選，則不限制類別。",
    )

    render_pro_section("推薦門檻")
    f1, f2 = st.columns(2)
    with f1:
        st.number_input("推薦總分下限", key=_k("min_total_score"), step=1.0)
    with f2:
        st.number_input("訊號分數下限", key=_k("min_signal_score"), step=1.0)

    render_pro_info_card(
        "類別邏輯",
        [
            ("優先順序", "主檔類別 → 自選股類別 → 名稱關鍵字推論 → 其他。", ""),
            ("篩選用途", "類別可直接納入掃描池與推薦結果判斷。", ""),
            ("後續升級", "下一步可加類股強度與類股輪動分數。", ""),
        ],
        chips=["類別已補齊", "可多選", "股神版"],
    )

    selected_categories = st.session_state.get(_k("selected_categories"), ["全部"])
    universe_mode = _safe_str(st.session_state.get(_k("universe_mode"), ""))

    if universe_mode == "自選群組":
        universe_items = watchlist_map.get(_safe_str(st.session_state.get(_k("group"), "")), [])
    elif universe_mode == "手動輸入":
        universe_items = _parse_manual_codes(st.session_state.get(_k("manual_codes"), ""), master_df)
    else:
        universe_items = _build_universe_from_market(
            master_df=master_df,
            market_mode=universe_mode,
            limit_count=int(st.session_state.get(_k("scan_limit"), 200)),
            selected_categories=selected_categories,
        )

    if not universe_items:
        st.warning("目前掃描池沒有股票。")
        st.stop()

    start_dt = today - timedelta(days=int(st.session_state.get(_k("days"), 120)))
    end_dt = today

    with st.spinner("股神推薦計算中..."):
        rec_df = _build_recommend_df(
            universe_items=universe_items,
            master_df=master_df,
            start_dt=start_dt,
            end_dt=end_dt,
            min_total_score=float(st.session_state.get(_k("min_total_score"), 55.0)),
            min_signal_score=float(st.session_state.get(_k("min_signal_score"), -2.0)),
            selected_categories=selected_categories,
        )

    if rec_df.empty:
        st.error("掃描完成，但沒有符合條件的股票。")
        st.stop()

    rec_df = rec_df.sort_values(["推薦總分", "訊號分數", "區間漲跌幅%"], ascending=[False, False, False]).reset_index(drop=True)
    top_n = int(st.session_state.get(_k("top_n"), 20))
    top_df = rec_df.head(top_n).copy()

    strong_count = int((rec_df["推薦等級"] == "強烈關注").sum())
    good_count = int((rec_df["推薦等級"] == "優先觀察").sum())
    avg_score = _avg_safe([_safe_float(x) for x in rec_df["推薦總分"].tolist()], 0)

    render_pro_kpi_row(
        [
            {"label": "掃描股票數", "value": len(rec_df), "delta": universe_mode, "delta_class": "pro-kpi-delta-flat"},
            {"label": "強烈關注", "value": strong_count, "delta": "最高等級", "delta_class": "pro-kpi-delta-flat"},
            {"label": "優先觀察", "value": good_count, "delta": "次高等級", "delta_class": "pro-kpi-delta-flat"},
            {"label": "平均總分", "value": format_number(avg_score, 1), "delta": "含類別判斷", "delta_class": "pro-kpi-delta-flat"},
        ]
    )

    render_pro_section("本輪精華推薦")
    st.dataframe(
        _format_df(
            top_df[
                [
                    "股票代號",
                    "股票名稱",
                    "市場別",
                    "類別",
                    "推薦等級",
                    "推薦總分",
                    "起漲判斷",
                    "最新價",
                    "推薦買點_拉回",
                    "推薦買點_突破",
                    "停損價",
                    "賣出目標1",
                    "賣出目標2",
                    "風險報酬_拉回",
                    "風險報酬_突破",
                    "自動因子摘要",
                    "雷達摘要",
                ]
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )

    pick_options = top_df["股票代號"].astype(str).tolist()
    code_to_row = {str(r["股票代號"]): r for _, r in rec_df.iterrows()}

    render_pro_section("單股股神劇本")
    selected_code = st.selectbox(
        "選擇推薦股",
        options=pick_options,
        format_func=lambda x: f"{x} {code_to_row.get(str(x), {}).get('股票名稱', '')}",
        key=_k("focus_code"),
    )

    focus_row = code_to_row.get(str(selected_code))
    if focus_row is not None:
        render_pro_info_card(
            "股神推薦結論",
            [
                ("股票", f"{_safe_str(focus_row.get('股票代號'))} {_safe_str(focus_row.get('股票名稱'))}", ""),
                ("類別", _safe_str(focus_row.get("類別")), ""),
                ("推薦等級", _safe_str(focus_row.get("推薦等級")), ""),
                ("推薦總分", format_number(focus_row.get("推薦總分"), 1), ""),
                ("起漲判斷", _safe_str(focus_row.get("起漲判斷")), ""),
                ("推薦買點（拉回）", format_number(focus_row.get("推薦買點_拉回"), 2), ""),
                ("推薦買點（突破）", format_number(focus_row.get("推薦買點_突破"), 2), ""),
                ("停損價", format_number(focus_row.get("停損價"), 2), ""),
                ("賣出目標1", format_number(focus_row.get("賣出目標1"), 2), ""),
                ("賣出目標2", format_number(focus_row.get("賣出目標2"), 2), ""),
                ("風險報酬（拉回）", _safe_str(focus_row.get("風險報酬_拉回")), ""),
                ("風險報酬（突破）", _safe_str(focus_row.get("風險報酬_突破")), ""),
                ("自動因子摘要", _safe_str(focus_row.get("自動因子摘要")), ""),
                ("雷達摘要", _safe_str(focus_row.get("雷達摘要")), ""),
            ],
            chips=[
                _safe_str(focus_row.get("推薦等級")),
                _safe_str(focus_row.get("起漲判斷")),
                _safe_str(focus_row.get("市場別")),
                _safe_str(focus_row.get("類別")),
            ],
        )

    tabs = st.tabs(["完整推薦表", "自動因子榜", "鎖碼 / 法人代理榜", "類別分布", "操作說明"])

    with tabs[0]:
        st.dataframe(_format_df(rec_df), use_container_width=True, hide_index=True)

    with tabs[1]:
        factor_rank = rec_df.sort_values(
            ["自動因子總分", "EPS代理分數", "營收動能代理分數", "獲利代理分數"],
            ascending=[False, False, False, False]
        ).reset_index(drop=True)
        st.dataframe(
            _format_df(
                factor_rank[
                    [
                        "股票代號",
                        "股票名稱",
                        "類別",
                        "自動因子總分",
                        "EPS代理分數",
                        "營收動能代理分數",
                        "獲利代理分數",
                        "自動因子摘要",
                    ]
                ].head(top_n)
            ),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[2]:
        money_rank = rec_df.sort_values(
            ["大戶鎖碼代理分數", "法人連買代理分數", "推薦總分"],
            ascending=[False, False, False]
        ).reset_index(drop=True)
        st.dataframe(
            _format_df(
                money_rank[
                    [
                        "股票代號",
                        "股票名稱",
                        "類別",
                        "大戶鎖碼代理分數",
                        "法人連買代理分數",
                        "推薦總分",
                        "推薦等級",
                        "自動因子摘要",
                    ]
                ].head(top_n)
            ),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[3]:
        category_df = (
            rec_df.groupby("類別", dropna=False)
            .agg(股票數=("股票代號", "count"), 平均總分=("推薦總分", "mean"))
            .reset_index()
            .sort_values(["股票數", "平均總分"], ascending=[False, False])
        )
        category_df["平均總分"] = category_df["平均總分"].apply(lambda x: format_number(x, 1) if pd.notna(x) else "")
        st.dataframe(category_df, use_container_width=True, hide_index=True)

    with tabs[4]:
        render_pro_info_card(
            "模組邏輯",
            [
                ("類別已補齊", "每支股票都會盡量補到類別，不再只剩未分類。", ""),
                ("推薦核心", "保留全自動因子、起漲判斷、推薦買點、停損、目標價。", ""),
                ("下一步", "可再加類股強度、類股熱度、同類股領先分數。", ""),
            ],
            chips=["類別可篩選", "股神版", "可持續升級"],
        )


if __name__ == "__main__":
    main()
