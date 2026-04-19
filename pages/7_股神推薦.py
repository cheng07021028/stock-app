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


def _score_clip(v: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, v))


def _avg_safe(values: list[float | None], default: float = 0.0) -> float:
    clean = [float(x) for x in values if x is not None]
    if not clean:
        return default
    return sum(clean) / len(clean)


# =========================================================
# 讀取自選股 / 主檔
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
                    if not code or code in seen:
                        continue
                    seen.add(code)
                    rows.append(
                        {
                            "code": code,
                            "name": name,
                            "market": market,
                            "label": f"{code} {name}",
                        }
                    )

            result[g] = rows

    return result


@st.cache_data(ttl=1800, show_spinner=False)
def _load_master_df() -> pd.DataFrame:
    dfs = []
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

                for col in ["code", "name", "market"]:
                    if col not in temp.columns:
                        temp[col] = ""

                temp["code"] = temp["code"].map(_normalize_code)
                temp["name"] = temp["name"].map(_safe_str)
                temp["market"] = temp["market"].map(_safe_str)
                if market_arg in ["上市", "上櫃", "興櫃"]:
                    temp["market"] = temp["market"].replace("", market_arg)

                dfs.append(temp[["code", "name", "market"]])
        except Exception:
            pass

    if not dfs:
        return pd.DataFrame(columns=["code", "name", "market"])

    out = pd.concat(dfs, ignore_index=True)
    out["code"] = out["code"].map(_normalize_code)
    out["name"] = out["name"].map(_safe_str)
    out["market"] = out["market"].map(_safe_str).replace("", "上市")
    out = out[out["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out


def _find_name_market(code: str, manual_name: str, manual_market: str, master_df: pd.DataFrame) -> tuple[str, str]:
    code = _normalize_code(code)
    manual_name = _safe_str(manual_name)
    manual_market = _safe_str(manual_market)

    if isinstance(master_df, pd.DataFrame) and not master_df.empty:
        matched = master_df[master_df["code"].astype(str) == code]
        if not matched.empty:
            row = matched.iloc[0]
            return (
                _safe_str(row.get("name")) or manual_name or code,
                _safe_str(row.get("market")) or manual_market or "上市",
            )

    return manual_name or code, manual_market or "上市"


def _parse_manual_codes(text: str, master_df: pd.DataFrame) -> list[dict[str, str]]:
    rows = []
    seen = set()

    raw_lines = [x.strip() for x in _safe_str(text).replace("，", "\n").replace(",", "\n").splitlines() if x.strip()]
    for raw in raw_lines:
        text = _safe_str(raw)
        code = _normalize_code(text)
        name = ""
        market = "上市"

        if not code:
            if isinstance(master_df, pd.DataFrame) and not master_df.empty:
                matched = master_df[master_df["name"].astype(str).str.contains(text, case=False, na=False)]
                if not matched.empty:
                    row = matched.iloc[0]
                    code = _normalize_code(row.get("code"))
                    name = _safe_str(row.get("name"))
                    market = _safe_str(row.get("market")) or "上市"

        if code and not name:
            name, market = _find_name_market(code, "", market, master_df)

        if code and code not in seen:
            seen.add(code)
            rows.append(
                {
                    "code": code,
                    "name": name or code,
                    "market": market or "上市",
                    "label": f"{code} {name or code}",
                }
            )

    return rows


# =========================================================
# 因子資料
# 可上傳 CSV，欄位可包含：
# code,name,eps,pred_revenue_yoy,revenue_yoy,profit_yoy,major_holder_ratio,
# inst_buy_days,foreign_buy_days,trust_buy_days,dealer_buy_days
# =========================================================
def _normalize_factor_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    temp = df.copy()
    temp.columns = [str(c).strip().lower() for c in temp.columns]

    rename_map = {
        "stock_no": "code",
        "stock_code": "code",
        "證券代號": "code",
        "股票代號": "code",
        "symbol": "code",
        "ticker": "code",
        "證券名稱": "name",
        "股票名稱": "name",
        "stock_name": "name",
        "eps(元)": "eps",
        "eps_ttm": "eps",
        "預估營收yoy": "pred_revenue_yoy",
        "forecast_revenue_yoy": "pred_revenue_yoy",
        "預測營收yoy": "pred_revenue_yoy",
        "營收yoy": "revenue_yoy",
        "revenue_growth": "revenue_yoy",
        "獲利yoy": "profit_yoy",
        "profit_growth": "profit_yoy",
        "大戶持股比": "major_holder_ratio",
        "major_holder": "major_holder_ratio",
        "大戶鎖碼": "major_holder_ratio",
        "法人連買天數": "inst_buy_days",
        "inst_buy_days": "inst_buy_days",
        "外資連買天數": "foreign_buy_days",
        "foreign_buy_days": "foreign_buy_days",
        "投信連買天數": "trust_buy_days",
        "trust_buy_days": "trust_buy_days",
        "自營商連買天數": "dealer_buy_days",
        "dealer_buy_days": "dealer_buy_days",
    }

    real_map = {}
    for c in temp.columns:
        real_map[c] = rename_map.get(c, c)
    temp = temp.rename(columns=real_map)

    if "code" not in temp.columns:
        return pd.DataFrame()

    temp["code"] = temp["code"].map(_normalize_code)
    if "name" not in temp.columns:
        temp["name"] = ""

    numeric_cols = [
        "eps",
        "pred_revenue_yoy",
        "revenue_yoy",
        "profit_yoy",
        "major_holder_ratio",
        "inst_buy_days",
        "foreign_buy_days",
        "trust_buy_days",
        "dealer_buy_days",
    ]
    for col in numeric_cols:
        if col not in temp.columns:
            temp[col] = None
        temp[col] = pd.to_numeric(temp[col], errors="coerce")

    temp["name"] = temp["name"].map(_safe_str)
    temp = temp[temp["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return temp


def _get_factor_df(uploaded_file) -> pd.DataFrame:
    if uploaded_file is None:
        return pd.DataFrame()
    try:
        df = pd.read_csv(uploaded_file)
        return _normalize_factor_df(df)
    except Exception:
        try:
            df = pd.read_excel(uploaded_file)
            return _normalize_factor_df(df)
        except Exception:
            return pd.DataFrame()


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
# 推薦邏輯
# =========================================================
def _factor_score_from_row(row: pd.Series) -> dict[str, float | str]:
    eps = _safe_float(row.get("eps"))
    pred_revenue_yoy = _safe_float(row.get("pred_revenue_yoy"))
    revenue_yoy = _safe_float(row.get("revenue_yoy"))
    profit_yoy = _safe_float(row.get("profit_yoy"))
    major_holder_ratio = _safe_float(row.get("major_holder_ratio"))
    inst_buy_days = _safe_float(row.get("inst_buy_days"))
    foreign_buy_days = _safe_float(row.get("foreign_buy_days"))
    trust_buy_days = _safe_float(row.get("trust_buy_days"))
    dealer_buy_days = _safe_float(row.get("dealer_buy_days"))

    eps_score = None if eps is None else _score_clip(eps * 8)
    rev_score = None if pred_revenue_yoy is None and revenue_yoy is None else _score_clip(_avg_safe([pred_revenue_yoy, revenue_yoy], 0) * 2)
    profit_score = None if profit_yoy is None else _score_clip(profit_yoy * 2)
    major_score = None if major_holder_ratio is None else _score_clip((major_holder_ratio - 40) * 2.2)
    inst_score = _score_clip(_avg_safe([inst_buy_days, foreign_buy_days, trust_buy_days, dealer_buy_days], 0) * 8)

    total = _avg_safe([eps_score, rev_score, profit_score, major_score, inst_score], 0)

    summary_bits = []
    if eps is not None:
        summary_bits.append(f"EPS {format_number(eps, 2)}")
    if pred_revenue_yoy is not None:
        summary_bits.append(f"預估營收YoY {format_number(pred_revenue_yoy, 1)}%")
    elif revenue_yoy is not None:
        summary_bits.append(f"營收YoY {format_number(revenue_yoy, 1)}%")
    if profit_yoy is not None:
        summary_bits.append(f"獲利YoY {format_number(profit_yoy, 1)}%")
    if major_holder_ratio is not None:
        summary_bits.append(f"大戶持股 {format_number(major_holder_ratio, 1)}%")
    if inst_buy_days is not None:
        summary_bits.append(f"法人連買 {format_number(inst_buy_days, 0)}天")

    return {
        "fundamental_score": total,
        "eps_score": eps_score if eps_score is not None else 0.0,
        "rev_score": rev_score if rev_score is not None else 0.0,
        "profit_score": profit_score if profit_score is not None else 0.0,
        "major_score": major_score if major_score is not None else 0.0,
        "inst_score": inst_score if inst_score is not None else 0.0,
        "factor_summary": " / ".join(summary_bits) if summary_bits else "未提供基本面因子",
    }


def _build_trade_plan(df: pd.DataFrame, sr_snapshot: dict, signal_snapshot: dict) -> dict[str, Any]:
    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"), 0) or 0
    atr14 = _safe_float(last.get("ATR14"), 0) or max(close_now * 0.03, 1.0)
    ma20 = _safe_float(last.get("MA20"))
    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))
    res60 = _safe_float(sr_snapshot.get("res_60"))
    sup60 = _safe_float(sr_snapshot.get("sup_60"))
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
        "support_60": sup60,
        "pressure_60": res60,
    }


@st.cache_data(ttl=300, show_spinner=False)
def _build_recommend_df(
    universe_items: list[dict[str, str]],
    factor_df: pd.DataFrame,
    master_df: pd.DataFrame,
    start_dt: date,
    end_dt: date,
) -> pd.DataFrame:
    factor_map = {}
    if isinstance(factor_df, pd.DataFrame) and not factor_df.empty:
        factor_map = {str(row["code"]): row for _, row in factor_df.iterrows()}

    rows = []

    for item in universe_items:
        code = _normalize_code(item.get("code"))
        manual_name = _safe_str(item.get("name"))
        manual_market = _safe_str(item.get("market"))
        if not code:
            continue

        stock_name, market_type = _find_name_market(code, manual_name, manual_market, master_df)
        hist_df, used_market = _get_history_smart(
            stock_no=code,
            stock_name=stock_name,
            market_type=market_type,
            start_date=start_dt,
            end_date=end_dt,
        )

        if hist_df.empty:
            continue

        signal_snapshot = compute_signal_snapshot(hist_df)
        sr_snapshot = compute_support_resistance_snapshot(hist_df)
        radar = compute_radar_scores(hist_df)
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

        factor_row = factor_map.get(code)
        factor_scores = _factor_score_from_row(factor_row) if factor_row is not None else _factor_score_from_row(pd.Series(dtype=object))
        trade_plan = _build_trade_plan(hist_df, sr_snapshot, signal_snapshot)

        technical_score = _score_clip((_safe_float(signal_snapshot.get("score"), 0) or 0) * 12 + radar_avg * 0.45)
        position_bonus = 0.0
        if pressure_dist is not None and 0 <= pressure_dist <= 8:
            position_bonus += 8.0
        if support_dist is not None and 0 <= support_dist <= 6:
            position_bonus += 6.0

        composite = (
            technical_score * 0.42
            + factor_scores["fundamental_score"] * 0.38
            + position_bonus
            + (_safe_float(factor_scores["inst_score"], 0) * 0.10)
            + (_safe_float(period_pct, 0) * 0.08 if period_pct is not None else 0)
        )
        composite = _score_clip(composite)

        recommendation = "觀察"
        if composite >= 80:
            recommendation = "強烈關注"
        elif composite >= 68:
            recommendation = "優先觀察"
        elif composite >= 55:
            recommendation = "可列追蹤"

        rows.append(
            {
                "股票代號": code,
                "股票名稱": stock_name,
                "市場別": used_market,
                "最新價": close_now,
                "區間漲跌幅%": period_pct,
                "訊號分數": _safe_float(signal_snapshot.get("score"), 0),
                "雷達均分": radar_avg,
                "基本面分數": factor_scores["fundamental_score"],
                "EPS分數": factor_scores["eps_score"],
                "營收分數": factor_scores["rev_score"],
                "獲利分數": factor_scores["profit_score"],
                "大戶分數": factor_scores["major_score"],
                "法人分數": factor_scores["inst_score"],
                "20日壓力距離%": pressure_dist,
                "20日支撐距離%": support_dist,
                "推薦總分": composite,
                "推薦等級": recommendation,
                "起漲判斷": trade_plan["launch_tag"],
                "推薦買點_突破": trade_plan["breakout_buy"],
                "推薦買點_拉回": trade_plan["pullback_buy"],
                "停損價": trade_plan["stop_price"],
                "賣出目標1": trade_plan["sell_target_1"],
                "賣出目標2": trade_plan["sell_target_2"],
                "風險報酬_拉回": trade_plan["rr1"],
                "風險報酬_突破": trade_plan["rr2"],
                "基本面摘要": factor_scores["factor_summary"],
                "雷達摘要": _safe_str(radar.get("summary")) or "—",
            }
        )

    return pd.DataFrame(rows)


def _format_df(df: pd.DataFrame) -> pd.DataFrame:
    show = df.copy()
    price_cols = ["最新價", "推薦買點_突破", "推薦買點_拉回", "停損價", "賣出目標1", "賣出目標2"]
    pct_cols = ["區間漲跌幅%", "20日壓力距離%", "20日支撐距離%"]
    score_cols = ["訊號分數", "雷達均分", "基本面分數", "EPS分數", "營收分數", "獲利分數", "大戶分數", "法人分數", "推薦總分"]

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

    render_pro_hero(
        title="股神推薦｜精華選股模組",
        subtitle="以技術面 + 基本面 + 法人 / 大戶因子做綜合評分，輸出起漲判斷、推薦買點、停損與賣出目標。",
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
        st.selectbox("掃描範圍", ["自選群組", "手動輸入"], key=_k("universe_mode"))
    with c2:
        group_options = list(watchlist_map.keys()) if watchlist_map else [""]
        if st.session_state.get(_k("group"), "") not in group_options:
            st.session_state[_k("group")] = group_options[0] if group_options else ""
        st.selectbox("自選群組", group_options, key=_k("group"))
    with c3:
        st.selectbox("觀察天數", [60, 90, 120, 180, 240], key=_k("days"))
    with c4:
        st.selectbox("輸出 Top N", [10, 20, 30, 50], key=_k("top_n"))

    st.text_area(
        "手動輸入股票（可代碼 / 名稱，一行一檔）",
        key=_k("manual_codes"),
        height=110,
        placeholder="2330\n2454\n3548\n台積電",
    )

    render_pro_section("因子資料匯入")
    uploaded = st.file_uploader(
        "上傳基本面 / 法人 / 大戶因子 CSV 或 Excel（可選）",
        type=["csv", "xlsx", "xls"],
        key=_k("factor_upload"),
    )
    factor_df = _get_factor_df(uploaded)

    render_pro_info_card(
        "建議因子欄位",
        [
            ("股票代號", "code", ""),
            ("EPS", "eps", ""),
            ("預測營收YoY", "pred_revenue_yoy", ""),
            ("營收YoY", "revenue_yoy", ""),
            ("獲利YoY", "profit_yoy", ""),
            ("大戶持股比", "major_holder_ratio", ""),
            ("法人連買天數", "inst_buy_days", ""),
        ],
        chips=["CSV可選", "未提供則自動降級為技術推薦"],
    )

    if st.session_state.get(_k("universe_mode")) == "自選群組":
        universe_items = watchlist_map.get(_safe_str(st.session_state.get(_k("group"), "")), [])
    else:
        universe_items = _parse_manual_codes(st.session_state.get(_k("manual_codes"), ""), master_df)

    if not universe_items:
        st.warning("目前掃描池沒有股票。")
        st.stop()

    start_dt = today - timedelta(days=int(st.session_state.get(_k("days"), 120)))
    end_dt = today

    with st.spinner("股神推薦計算中..."):
        rec_df = _build_recommend_df(
            universe_items=universe_items,
            factor_df=factor_df,
            master_df=master_df,
            start_dt=start_dt,
            end_dt=end_dt,
        )

    if rec_df.empty:
        st.error("掃描完成，但沒有可用資料。")
        st.stop()

    rec_df = rec_df.sort_values(["推薦總分", "訊號分數", "區間漲跌幅%"], ascending=[False, False, False]).reset_index(drop=True)

    top_n = int(st.session_state.get(_k("top_n"), 20))
    top_df = rec_df.head(top_n).copy()

    strong_count = int((rec_df["推薦等級"] == "強烈關注").sum())
    good_count = int((rec_df["推薦等級"] == "優先觀察").sum())
    with_factor_count = int((rec_df["基本面摘要"] != "未提供基本面因子").sum())
    avg_score = _avg_safe([_safe_float(x) for x in rec_df["推薦總分"].tolist()], 0)

    render_pro_kpi_row(
        [
            {"label": "掃描股票數", "value": len(rec_df), "delta": _safe_str(st.session_state.get(_k("universe_mode"), "")), "delta_class": "pro-kpi-delta-flat"},
            {"label": "強烈關注", "value": strong_count, "delta": "最高等級", "delta_class": "pro-kpi-delta-flat"},
            {"label": "優先觀察", "value": good_count, "delta": "次高等級", "delta_class": "pro-kpi-delta-flat"},
            {"label": "平均總分", "value": format_number(avg_score, 1), "delta": f"含因子 {with_factor_count} 檔", "delta_class": "pro-kpi-delta-flat"},
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
                    "基本面摘要",
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
                ("基本面摘要", _safe_str(focus_row.get("基本面摘要")), ""),
                ("雷達摘要", _safe_str(focus_row.get("雷達摘要")), ""),
            ],
            chips=[
                _safe_str(focus_row.get("推薦等級")),
                _safe_str(focus_row.get("起漲判斷")),
                _safe_str(focus_row.get("市場別")),
            ],
        )

    tabs = st.tabs(["完整推薦表", "基本面分數榜", "法人 / 大戶榜", "操作說明"])

    with tabs[0]:
        st.dataframe(_format_df(rec_df), use_container_width=True, hide_index=True)

    with tabs[1]:
        fundamental_rank = rec_df.sort_values(["基本面分數", "EPS分數", "營收分數", "獲利分數"], ascending=[False, False, False, False]).reset_index(drop=True)
        st.dataframe(
            _format_df(
                fundamental_rank[
                    [
                        "股票代號",
                        "股票名稱",
                        "基本面分數",
                        "EPS分數",
                        "營收分數",
                        "獲利分數",
                        "基本面摘要",
                    ]
                ].head(top_n)
            ),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[2]:
        money_rank = rec_df.sort_values(["大戶分數", "法人分數", "推薦總分"], ascending=[False, False, False]).reset_index(drop=True)
        st.dataframe(
            _format_df(
                money_rank[
                    [
                        "股票代號",
                        "股票名稱",
                        "大戶分數",
                        "法人分數",
                        "推薦總分",
                        "推薦等級",
                        "基本面摘要",
                    ]
                ].head(top_n)
            ),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[3]:
        render_pro_info_card(
            "模組邏輯",
            [
                ("核心精神", "技術面先選強，再用 EPS / 營收 / 獲利 / 大戶 / 法人因子強化排序。", ""),
                ("起漲判斷", "依訊號分數、雷達均分、支撐壓力位置與結構狀態綜合判定。", ""),
                ("推薦買點", "同時提供拉回買點與突破買點，不強迫單一劇本。", ""),
                ("停損 / 賣點", "用 20 日支撐、20 / 60 日壓力與 ATR 推估。", ""),
                ("速度考量", "全市場全掃會很重，這版先鎖自選群組 / 手動池，穩定優先。", ""),
            ],
            chips=["股神版", "推薦模組", "穩定優先"],
        )


if __name__ == "__main__":
    main()
