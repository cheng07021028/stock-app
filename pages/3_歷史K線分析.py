from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import plotly.graph_objects as go
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
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
    save_last_query_state,
    score_to_badge,
)

PAGE_TITLE = "歷史K線分析｜升級完整版"
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


def _safe_mapping(v: Any) -> dict:
    return v if isinstance(v, dict) else {}


def _get_handoff_stock_code() -> str:
    return _safe_str(st.session_state.get("kline_focus_stock_code", ""))


def _get_handoff_stock_name() -> str:
    return _safe_str(st.session_state.get("kline_focus_stock_name", ""))


def _get_handoff_watch_group() -> str:
    return _safe_str(st.session_state.get("godpick_last_watch_group", ""))


def _get_handoff_watch_codes() -> list[str]:
    raw = st.session_state.get("godpick_last_watch_codes", [])
    if not isinstance(raw, list):
        return []
    return [_safe_str(x) for x in raw if _safe_str(x)]


def _apply_external_focus_if_any(group_map: dict[str, list[dict[str, str]]]) -> bool:
    focus_code = _get_handoff_stock_code()
    focus_name = _get_handoff_stock_name()
    if not focus_code:
        return False

    for group_name, items in group_map.items():
        for item in items:
            if _safe_str(item.get("code")) == focus_code:
                st.session_state[_k("group")] = group_name
                st.session_state[_k("stock_code")] = focus_code
                st.session_state[_k("focus_event_idx")] = -1
                save_last_query_state(
                    quick_group=group_name,
                    quick_stock_code=focus_code,
                    home_start=st.session_state.get(_k("start_date")),
                    home_end=st.session_state.get(_k("end_date")),
                )
                return True

    if group_map:
        group_name = list(group_map.keys())[0]
        items = group_map.get(group_name, [])
        if items:
            st.session_state[_k("group")] = group_name
            st.session_state[_k("stock_code")] = focus_code
            st.session_state[_k("focus_event_idx")] = -1
            if focus_name:
                # hint for UI caption only
                st.session_state[_k("external_focus_name")] = focus_name
            save_last_query_state(
                quick_group=group_name,
                quick_stock_code=focus_code,
                home_start=st.session_state.get(_k("start_date")),
                home_end=st.session_state.get(_k("end_date")),
            )
            return True

    return False

def _metric_text(source: Any, key: str, default: str = "—") -> str:
    data = _safe_mapping(source)
    val = data.get(key, default)
    if isinstance(val, (list, tuple)):
        if len(val) >= 1:
            return _safe_str(val[0]) or default
        return default
    if isinstance(val, dict):
        return _safe_str(val.get("label") or val.get("text") or default)
    return _safe_str(val) or default


def _metric_number(source: Any, key: str, default: float | int | None = None):
    data = _safe_mapping(source)
    val = data.get(key, default)
    if isinstance(val, (list, tuple)):
        if len(val) >= 1:
            return _safe_float(val[0], default)
        return default
    if isinstance(val, dict):
        for candidate in ["value", "score", "number"]:
            if candidate in val:
                return _safe_float(val.get(candidate), default)
        return default
    return _safe_float(val, default)


def _default_signal_snapshot() -> dict[str, Any]:
    return {
        "score": 0,
        "ma_trend": ("整理", ""),
        "kd_cross": ("無新交叉", ""),
        "macd_trend": ("整理", ""),
        "price_vs_ma20": ("接近 MA20", ""),
        "breakout_20d": ("區間內", ""),
        "volume_state": ("量能普通", ""),
    }


def _default_sr_snapshot() -> dict[str, Any]:
    return {
        "res_20": None,
        "sup_20": None,
        "res_60": None,
        "sup_60": None,
        "pressure_signal": ("—", ""),
        "support_signal": ("—", ""),
        "break_signal": ("區間內", ""),
    }


def _default_radar_snapshot() -> dict[str, Any]:
    return {
        "trend": 50,
        "momentum": 50,
        "volume": 50,
        "position": 50,
        "structure": 50,
        "summary": "訊號中性，等待更明確方向。",
    }


def _compute_god_signal(df: pd.DataFrame, signal_snapshot: dict, sr_snapshot: dict, radar: dict) -> dict[str, Any]:
    if df is None or df.empty:
        return {
            "phase": "整理",
            "action": "觀察",
            "confidence": 50,
            "summary": "資料不足，先觀察。",
            "reason": [],
        }

    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"))
    ma20 = _safe_float(last.get("MA20"))
    ma60 = _safe_float(last.get("MA60"))
    k_val = _safe_float(last.get("K"))
    d_val = _safe_float(last.get("D"))
    dif = _safe_float(last.get("DIF"))
    dea = _safe_float(last.get("DEA"))
    score = _metric_number(signal_snapshot, "score", 0) or 0
    trend = _metric_number(radar, "trend", 50) or 50
    momentum = _metric_number(radar, "momentum", 50) or 50
    structure = _metric_number(radar, "structure", 50) or 50
    volume_score = _metric_number(radar, "volume", 50) or 50
    break_text = _metric_text(sr_snapshot, "break_signal", "區間內")

    reasons = []
    bull_points = 0
    bear_points = 0

    if close_now is not None and ma20 is not None and close_now > ma20:
        bull_points += 1
        reasons.append("股價站上 MA20")
    elif close_now is not None and ma20 is not None and close_now < ma20:
        bear_points += 1
        reasons.append("股價跌破 MA20")

    if close_now is not None and ma60 is not None and close_now > ma60:
        bull_points += 1
        reasons.append("股價位於 MA60 上方")
    elif close_now is not None and ma60 is not None and close_now < ma60:
        bear_points += 1
        reasons.append("股價位於 MA60 下方")

    if k_val is not None and d_val is not None and k_val > d_val:
        bull_points += 1
        reasons.append("KD 偏多")
    elif k_val is not None and d_val is not None and k_val < d_val:
        bear_points += 1
        reasons.append("KD 偏弱")

    if dif is not None and dea is not None and dif > dea:
        bull_points += 1
        reasons.append("MACD 偏多")
    elif dif is not None and dea is not None and dif < dea:
        bear_points += 1
        reasons.append("MACD 偏弱")

    if "突破" in break_text:
        bull_points += 2
        reasons.append("結構突破")
    elif "跌破" in break_text:
        bear_points += 2
        reasons.append("結構跌破")

    if score >= 3:
        bull_points += 1
    elif score <= -3:
        bear_points += 1

    confidence = int(max(0, min(100, round((trend + momentum + structure + volume_score) / 4))))
    diff = bull_points - bear_points

    if diff >= 3:
        phase = "主升候選"
        action = "偏多"
        summary = "多方條件同時成立，重點改看是否能延續量價與守住突破位。"
    elif diff <= -3:
        phase = "主跌候選"
        action = "偏空"
        summary = "空方條件較完整，反彈若無法站回關鍵位，弱勢延續機率較高。"
    elif diff >= 1:
        phase = "轉強觀察"
        action = "偏多觀察"
        summary = "已有轉強跡象，但仍要確認量能與突破有效性。"
    elif diff <= -1:
        phase = "轉弱觀察"
        action = "偏空觀察"
        summary = "已有轉弱跡象，但仍要確認跌破是否有效。"
    else:
        phase = "整理"
        action = "觀察"
        summary = "多空訊號混合，先等市場表態再提高勝率。"

    return {
        "phase": phase,
        "action": action,
        "confidence": confidence,
        "summary": summary,
        "reason": reasons[:6],
    }




def _grade_level(score: float, levels: list[tuple[float, str]]) -> str:
    for threshold, label in levels:
        if score >= threshold:
            return label
    return levels[-1][1] if levels else "一般"


def _compute_main_uptrend_signal(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or df.empty or len(df) < 80:
        return {"score": 0, "level": "資料不足", "summary": "資料不足，無法判斷主升段。", "items": []}

    last = df.iloc[-1]
    prev5 = df.iloc[max(0, len(df) - 6)]
    prev10 = df.iloc[max(0, len(df) - 11)]
    score = 0
    items = []

    close_now = _safe_float(last.get("收盤價"))
    ma20 = _safe_float(last.get("MA20"))
    ma60 = _safe_float(last.get("MA60"))
    ma120 = _safe_float(last.get("MA120"))
    ma20_prev = _safe_float(prev5.get("MA20"))
    ma60_prev = _safe_float(prev10.get("MA60"))
    dif = _safe_float(last.get("DIF"))
    dea = _safe_float(last.get("DEA"))
    vol = _safe_float(last.get("成交股數"), 0) or 0
    vol20 = _safe_float(last.get("VOL20"), 0) or 0

    if close_now is not None and ma20 is not None and close_now > ma20:
        score += 18
        items.append(("站上MA20", "是", ""))
    else:
        items.append(("站上MA20", "否", ""))

    if close_now is not None and ma60 is not None and close_now > ma60:
        score += 18
        items.append(("站上MA60", "是", ""))
    else:
        items.append(("站上MA60", "否", ""))

    if ma20 is not None and ma20_prev is not None and ma20 > ma20_prev:
        score += 16
        items.append(("MA20上彎", "是", ""))
    else:
        items.append(("MA20上彎", "否", ""))

    if ma60 is not None and ma60_prev is not None and ma60 > ma60_prev:
        score += 14
        items.append(("MA60上彎", "是", ""))
    else:
        items.append(("MA60上彎", "否", ""))

    if ma120 is not None and ma60 is not None and ma20 is not None and ma20 >= ma60 >= ma120:
        score += 12
        items.append(("均線多頭排列", "是", ""))
    else:
        items.append(("均線多頭排列", "否", ""))

    recent_low = _safe_float(df.tail(20)["最低價"].min())
    if close_now is not None and recent_low is not None and close_now >= recent_low * 1.08:
        score += 10
        items.append(("脫離近20日低點", "是", ""))
    else:
        items.append(("脫離近20日低點", "否", ""))

    if dif is not None and dea is not None and dif > dea:
        score += 6
        items.append(("MACD偏多", "是", ""))
    else:
        items.append(("MACD偏多", "否", ""))

    if dif is not None and dif > 0:
        score += 6
        items.append(("DIF站上0軸", "是", ""))
    else:
        items.append(("DIF站上0軸", "否", ""))

    if vol20 > 0 and vol >= vol20:
        score += 8
        items.append(("量能不弱於20日均量", "是", ""))
    else:
        items.append(("量能不弱於20日均量", "否", ""))

    level = _grade_level(score, [(78, "主升段明確"), (60, "主升段候選"), (40, "轉強觀察"), (0, "尚未成立")])
    summary = f"主升段評分 {score}，判定：{level}。"
    return {"score": score, "level": level, "summary": summary, "items": items}


def _compute_false_break_signal(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or df.empty or len(df) < 25:
        return {"score": 0, "direction": "無", "level": "資料不足", "summary": "資料不足，無法判斷真假突破。", "items": []}

    score = 0
    direction = "無"
    level = "一般"
    summary = "目前沒有明顯假突破 / 假跌破訊號。"
    items = []

    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev3 = df.iloc[-4:-1].copy()
    close_now = _safe_float(last.get("收盤價"))
    vol_now = _safe_float(last.get("成交股數"), 0) or 0
    vol20 = _safe_float(last.get("VOL20"), 0) or 0
    high20_prev = _safe_float(df.iloc[-21:-1]["最高價"].max())
    low20_prev = _safe_float(df.iloc[-21:-1]["最低價"].min())

    if close_now is not None and high20_prev is not None:
        if _safe_float(prev.get("收盤價")) is not None and _safe_float(prev.get("收盤價")) > high20_prev and close_now < high20_prev:
            direction = "假突破"
            score += 45
            items.append(("跌回前20日高下方", "是", ""))
        else:
            items.append(("跌回前20日高下方", "否", ""))

    if close_now is not None and low20_prev is not None:
        if _safe_float(prev.get("收盤價")) is not None and _safe_float(prev.get("收盤價")) < low20_prev and close_now > low20_prev:
            direction = "假跌破"
            score += 45
            items.append(("站回前20日低上方", "是", ""))
        else:
            items.append(("站回前20日低上方", "否", ""))

    if direction == "假突破":
        if vol20 > 0 and vol_now < vol20:
            score += 15
            items.append(("回落量縮", "是", ""))
        if prev3 is not None and not prev3.empty and all(prev3["收盤價"].fillna(0) <= (high20_prev or 0) * 1.03):
            score += 10
            items.append(("前段未能有效連續站穩", "是", ""))
        level = _grade_level(score, [(65, "強假突破"), (45, "中假突破"), (25, "弱假突破"), (0, "一般")])
        summary = f"真假突破評分 {score}，判定：{level}。突破後無法續強，需防追價陷阱。"
    elif direction == "假跌破":
        if vol20 > 0 and vol_now >= vol20:
            score += 15
            items.append(("站回帶量", "是", ""))
        if prev3 is not None and not prev3.empty and all(prev3["收盤價"].fillna(0) >= (low20_prev or 0) * 0.97):
            score += 10
            items.append(("前段未能有效連續跌深", "是", ""))
        level = _grade_level(score, [(65, "強假跌破"), (45, "中假跌破"), (25, "弱假跌破"), (0, "一般")])
        summary = f"真假突破評分 {score}，判定：{level}。跌破後又站回，需防空方陷阱。"

    return {"score": score, "direction": direction, "level": level, "summary": summary, "items": items}


def _compute_divergence_signal(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or df.empty or len(df) < 50:
        return {"score": 0, "type": "無", "level": "資料不足", "summary": "資料不足，無法判斷背離。", "items": []}

    peak_idx, trough_idx = _detect_pivots_smart(df, window=3, min_gap=5)
    items = []
    score = 0
    dtype = "無"
    level = "一般"
    summary = "目前沒有明顯背離。"

    if len(trough_idx) >= 2:
        i1, i2 = trough_idx[-2], trough_idx[-1]
        p1, p2 = df.iloc[i1], df.iloc[i2]
        low1, low2 = _safe_float(p1.get("最低價")), _safe_float(p2.get("最低價"))
        k1, k2 = _safe_float(p1.get("K")), _safe_float(p2.get("K"))
        dif1, dif2 = _safe_float(p1.get("DIF")), _safe_float(p2.get("DIF"))
        v1, v2 = _safe_float(p1.get("成交股數"), 0) or 0, _safe_float(p2.get("成交股數"), 0) or 0
        if low1 is not None and low2 is not None and low2 < low1:
            if k1 is not None and k2 is not None and k2 > k1:
                score += 28
                items.append(("KD底背離", "是", ""))
            if dif1 is not None and dif2 is not None and dif2 > dif1:
                score += 28
                items.append(("MACD底背離", "是", ""))
            if v2 <= v1:
                score += 12
                items.append(("破底量未放大", "是", ""))
            if score > 0:
                dtype = "多方背離"
                level = _grade_level(score, [(60, "強"), (40, "中"), (20, "弱"), (0, "一般")])
                summary = f"背離評分 {score}，判定：{level}多方背離。"

    bear_score = 0
    bear_items = []
    if len(peak_idx) >= 2:
        i1, i2 = peak_idx[-2], peak_idx[-1]
        p1, p2 = df.iloc[i1], df.iloc[i2]
        h1, h2 = _safe_float(p1.get("最高價")), _safe_float(p2.get("最高價"))
        k1, k2 = _safe_float(p1.get("K")), _safe_float(p2.get("K"))
        dif1, dif2 = _safe_float(p1.get("DIF")), _safe_float(p2.get("DIF"))
        v1, v2 = _safe_float(p1.get("成交股數"), 0) or 0, _safe_float(p2.get("成交股數"), 0) or 0
        if h1 is not None and h2 is not None and h2 > h1:
            if k1 is not None and k2 is not None and k2 < k1:
                bear_score += 28
                bear_items.append(("KD頂背離", "是", ""))
            if dif1 is not None and dif2 is not None and dif2 < dif1:
                bear_score += 28
                bear_items.append(("MACD頂背離", "是", ""))
            if v2 <= v1:
                bear_score += 12
                bear_items.append(("過高量未放大", "是", ""))

    if bear_score > score and bear_score > 0:
        score = bear_score
        dtype = "空方背離"
        level = _grade_level(score, [(60, "強"), (40, "中"), (20, "弱"), (0, "一般")])
        items = bear_items
        summary = f"背離評分 {score}，判定：{level}空方背離。"

    return {"score": score, "type": dtype, "level": level, "summary": summary, "items": items}


def _compute_god_table_signal(df: pd.DataFrame, signal_snapshot: dict, sr_snapshot: dict, radar: dict, god_signal: dict, main_up: dict, false_break: dict, divergence: dict) -> dict[str, Any]:
    score = 0
    reasons = []
    safe_signal_score = _metric_number(signal_snapshot, "score", 0) or 0
    radar_trend = _metric_number(radar, "trend", 50) or 50
    radar_structure = _metric_number(radar, "structure", 50) or 50
    break_text = _metric_text(sr_snapshot, "break_signal", "區間內")

    score += safe_signal_score * 4
    score += (radar_trend - 50) * 0.6
    score += (radar_structure - 50) * 0.5
    score += (_safe_float(main_up.get("score"), 0) or 0) * 0.5

    if _safe_str(false_break.get("direction")) == "假突破":
        score -= (_safe_float(false_break.get("score"), 0) or 0) * 0.9
        reasons.append("出現假突破風險")
    elif _safe_str(false_break.get("direction")) == "假跌破":
        score += (_safe_float(false_break.get("score"), 0) or 0) * 0.5
        reasons.append("出現假跌破收復")

    if _safe_str(divergence.get("type")) == "多方背離":
        score += (_safe_float(divergence.get("score"), 0) or 0) * 0.45
        reasons.append("多方背離加分")
    elif _safe_str(divergence.get("type")) == "空方背離":
        score -= (_safe_float(divergence.get("score"), 0) or 0) * 0.45
        reasons.append("空方背離扣分")

    if "突破" in break_text:
        score += 10
        reasons.append("結構突破")
    elif "跌破" in break_text:
        score -= 10
        reasons.append("結構跌破")

    phase = _safe_str(god_signal.get("phase", "整理"))
    if "主升" in phase:
        score += 10
        reasons.append("主升候選")
    elif "主跌" in phase:
        score -= 10
        reasons.append("主跌候選")

    if score >= 85:
        status = "可偏多追蹤"
    elif score >= 60:
        status = "可試單"
    elif score >= 35:
        status = "可觀察"
    else:
        status = "暫不出手"

    return {
        "score": round(score, 1),
        "status": status,
        "summary": f"股神總表評分 {round(score,1)}，判定：{status}。",
        "reasons": reasons[:6],
    }


def _render_signal_summary_table(god_table: dict, main_up: dict, false_break: dict, divergence: dict):
    rows = [
        {"模組": "股神總表", "結果": _safe_str(god_table.get("status", "暫不出手")), "分數": _safe_float(god_table.get("score"), 0), "摘要": _safe_str(god_table.get("summary", "—"))},
        {"模組": "主升段確認", "結果": _safe_str(main_up.get("level", "—")), "分數": _safe_float(main_up.get("score"), 0), "摘要": _safe_str(main_up.get("summary", "—"))},
        {"模組": "真假突破", "結果": _safe_str(false_break.get("level", "—")), "分數": _safe_float(false_break.get("score"), 0), "摘要": _safe_str(false_break.get("summary", "—"))},
        {"模組": "背離強弱", "結果": f"{_safe_str(divergence.get('level', '—'))}{_safe_str(divergence.get('type', ''))}", "分數": _safe_float(divergence.get("score"), 0), "摘要": _safe_str(divergence.get("summary", "—"))},
    ]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
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



def _init_applied_query_state():
    if _k("applied_group") not in st.session_state:
        st.session_state[_k("applied_group")] = _safe_str(st.session_state.get(_k("group"), ""))
    if _k("applied_stock_code") not in st.session_state:
        st.session_state[_k("applied_stock_code")] = _safe_str(st.session_state.get(_k("stock_code"), ""))
    if _k("applied_start_date") not in st.session_state:
        st.session_state[_k("applied_start_date")] = _to_date(
            st.session_state.get(_k("start_date")),
            date.today() - timedelta(days=365),
        )
    if _k("applied_end_date") not in st.session_state:
        st.session_state[_k("applied_end_date")] = _to_date(
            st.session_state.get(_k("end_date")),
            date.today(),
        )
    if _k("runtime_history_cache") not in st.session_state:
        st.session_state[_k("runtime_history_cache")] = {}


def _set_applied_query(group: str, stock_code: str, start_date: date, end_date: date):
    st.session_state[_k("applied_group")] = _safe_str(group)
    st.session_state[_k("applied_stock_code")] = _safe_str(stock_code)
    st.session_state[_k("applied_start_date")] = _to_date(start_date, date.today() - timedelta(days=365))
    st.session_state[_k("applied_end_date")] = _to_date(end_date, date.today())


def _get_applied_query() -> tuple[str, str, date, date]:
    return (
        _safe_str(st.session_state.get(_k("applied_group"), "")),
        _safe_str(st.session_state.get(_k("applied_stock_code"), "")),
        _to_date(st.session_state.get(_k("applied_start_date")), date.today() - timedelta(days=365)),
        _to_date(st.session_state.get(_k("applied_end_date")), date.today()),
    )


def _load_history_runtime_cached(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date) -> tuple[pd.DataFrame, str, str]:
    cache = st.session_state.get(_k("runtime_history_cache"), {})
    if not isinstance(cache, dict):
        cache = {}

    sig = "|".join([
        _safe_str(stock_no),
        _safe_str(stock_name),
        _safe_str(market_type),
        _to_date(start_date, date.today()).isoformat(),
        _to_date(end_date, date.today()).isoformat(),
    ])

    cached = cache.get(sig)
    if isinstance(cached, dict):
        df = cached.get("df")
        actual_market = _safe_str(cached.get("actual_market", market_type or "未知"))
        data_source = _safe_str(cached.get("data_source", "none"))
        if isinstance(df, pd.DataFrame):
            return df.copy(), actual_market, data_source

    df, actual_market, data_source = _get_history_data_smart(
        stock_no=stock_no,
        stock_name=stock_name,
        market_type=market_type,
        start_date=start_date,
        end_date=end_date,
    )
    cache[sig] = {
        "df": df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(),
        "actual_market": actual_market,
        "data_source": data_source,
    }
    if len(cache) > 12:
        keys = list(cache.keys())
        for old_key in keys[:-12]:
            cache.pop(old_key, None)
    st.session_state[_k("runtime_history_cache")] = cache
    return df, actual_market, data_source


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
def _get_history_data_smart(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date) -> tuple[pd.DataFrame, str, str]:
    stock_no = _safe_str(stock_no)
    stock_name = _safe_str(stock_name)
    market_type = _safe_str(market_type)

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
            if not df.empty:
                return df, (_safe_str(try_market) or "未標示"), "utils"
        except Exception:
            pass

    try:
        df2 = _get_tpex_history_data(stock_no, start_date, end_date)
        df2 = _prepare_history_df(df2)
        if not df2.empty:
            return df2, "上櫃", "tpex"
    except Exception:
        pass

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

        if i >= 20 and all(c in df.columns for c in ["成交股數", "VOL20", "MA20", "MA60", "收盤價", "最高價", "最低價", "K", "D", "DIF", "DEA"]):
            vol20 = _safe_float(cur.get("VOL20"))
            vol_now = _safe_float(cur.get("成交股數"))
            ma20 = _safe_float(cur.get("MA20"))
            ma60 = _safe_float(cur.get("MA60"))
            close_price = _safe_float(cur.get("收盤價"))
            prev_high20 = _safe_float(df.iloc[max(0, i - 20): i]["最高價"].max())
            prev_low20 = _safe_float(df.iloc[max(0, i - 20): i]["最低價"].min())

            if all(v is not None for v in [vol20, vol_now, ma20, ma60, close_price, prev_high20]):
                if close_price > ma20 and close_price > ma60 and vol_now >= vol20 * 1.3 and close_price >= prev_high20:
                    rows.append({"日期": d, "事件分類": "主升段", "事件": "主升段啟動候選", "說明": "股價站上中期均線且放量突破，疑似主升段起點。"})
            if all(v is not None for v in [close_price, prev_high20]):
                if close_price < prev_high20 and _safe_float(prev.get("收盤價")) is not None and _safe_float(prev.get("收盤價")) >= prev_high20:
                    rows.append({"日期": d, "事件分類": "假突破", "事件": "突破失敗", "說明": "前一交易日突破後無法站穩，需防假突破回落。"})
            if all(v is not None for v in [close_price, prev_low20]):
                if close_price > prev_low20 and _safe_float(prev.get("收盤價")) is not None and _safe_float(prev.get("收盤價")) <= prev_low20:
                    rows.append({"日期": d, "事件分類": "假突破", "事件": "跌破失敗", "說明": "前一交易日跌破後快速站回，需防假跌破反彈。"})

        if i >= 10 and all(c in df.columns for c in ["收盤價", "K", "DIF"]):
            price_now = _safe_float(cur.get("收盤價"))
            price_prev5 = _safe_float(df.iloc[max(0, i - 5)].get("收盤價"))
            k_now = _safe_float(cur.get("K"))
            k_prev5 = _safe_float(df.iloc[max(0, i - 5)].get("K"))
            dif_now = _safe_float(cur.get("DIF"))
            dif_prev5 = _safe_float(df.iloc[max(0, i - 5)].get("DIF"))
            if all(v is not None for v in [price_now, price_prev5, k_now, k_prev5]):
                if price_now < price_prev5 and k_now > k_prev5:
                    rows.append({"日期": d, "事件分類": "背離", "事件": "KD 底背離候選", "說明": "價格創較弱位置，但 KD 未同步轉弱，留意止跌。"})
                elif price_now > price_prev5 and k_now < k_prev5:
                    rows.append({"日期": d, "事件分類": "背離", "事件": "KD 頂背離候選", "說明": "價格走高但 KD 未同步走強，留意追價風險。"})
            if all(v is not None for v in [price_now, price_prev5, dif_now, dif_prev5]):
                if price_now < price_prev5 and dif_now > dif_prev5:
                    rows.append({"日期": d, "事件分類": "背離", "事件": "MACD 底背離候選", "說明": "價格偏弱但 MACD 動能未再惡化，留意轉折。"})
                elif price_now > price_prev5 and dif_now < dif_prev5:
                    rows.append({"日期": d, "事件分類": "背離", "事件": "MACD 頂背離候選", "說明": "價格續漲但 MACD 動能走弱，留意假強。"})

    if not rows:
        return pd.DataFrame(columns=["日期", "事件分類", "事件", "說明"])

    return pd.DataFrame(rows).drop_duplicates(subset=["日期", "事件", "說明"]).sort_values("日期", ascending=False).reset_index(drop=True)


@st.cache_data(ttl=1800, show_spinner=False)
def _compute_analysis_bundle(df: pd.DataFrame) -> dict[str, Any]:
    signal_snapshot = _default_signal_snapshot()
    sr_snapshot = _default_sr_snapshot()
    radar = _default_radar_snapshot()

    try:
        signal_snapshot = _safe_mapping(compute_signal_snapshot(df)) or _default_signal_snapshot()
    except Exception:
        signal_snapshot = _default_signal_snapshot()

    try:
        sr_snapshot = _safe_mapping(compute_support_resistance_snapshot(df)) or _default_sr_snapshot()
    except Exception:
        sr_snapshot = _default_sr_snapshot()

    try:
        radar = _safe_mapping(compute_radar_scores(df)) or _default_radar_snapshot()
    except Exception:
        radar = _default_radar_snapshot()

    safe_score = _metric_number(signal_snapshot, "score", 0) or 0
    badge_text, _ = score_to_badge(safe_score)
    event_df = _build_event_df(df)
    peak_idx, trough_idx = _detect_pivots_smart(df, window=4, min_gap=6)
    god_signal = _compute_god_signal(df, signal_snapshot, sr_snapshot, radar)
    main_up = _compute_main_uptrend_signal(df)
    false_break = _compute_false_break_signal(df)
    divergence = _compute_divergence_signal(df)
    god_table = _compute_god_table_signal(df, signal_snapshot, sr_snapshot, radar, god_signal, main_up, false_break, divergence)
    buy_backtest = _compute_buy_point_backtest(df)

    return {
        "signal_snapshot": signal_snapshot,
        "sr_snapshot": sr_snapshot,
        "radar": radar,
        "badge_text": badge_text,
        "event_df": event_df,
        "peak_idx": peak_idx,
        "trough_idx": trough_idx,
        "god_signal": god_signal,
        "main_up": main_up,
        "false_break": false_break,
        "divergence": divergence,
        "god_table": god_table,
        "buy_backtest": buy_backtest,
    }


@st.cache_data(ttl=1800, show_spinner=False)
def _compute_buy_point_backtest(df: pd.DataFrame) -> dict[str, Any]:
    empty_overall = pd.DataFrame(columns=["分級", "訊號數", "5日勝率", "10日勝率", "20日勝率", "5日平均報酬", "10日平均報酬", "20日平均報酬", "最大20日報酬", "最大20日回撤"])
    empty_signals = pd.DataFrame(columns=["日期", "收盤價", "分級", "分數", "型態", "5日報酬", "10日報酬", "20日報酬", "最大20日報酬", "最大20日回撤", "理由"])
    if df is None or df.empty or len(df) < 80:
        return {
            "current": {"grade": "資料不足", "score": 0, "pattern": "—", "summary": "資料筆數不足，無法回測買點分級。", "reasons": []},
            "overall": empty_overall,
            "by_pattern": pd.DataFrame(columns=["型態", "訊號數", "5日勝率", "10日勝率", "20日勝率", "20日平均報酬"]),
            "signals": empty_signals,
        }

    work = df.copy().reset_index(drop=True)
    close = pd.to_numeric(work.get("收盤價"), errors="coerce")
    high = pd.to_numeric(work.get("最高價"), errors="coerce")
    low = pd.to_numeric(work.get("最低價"), errors="coerce")
    vol = pd.to_numeric(work.get("成交股數"), errors="coerce")

    signals = []

    def _grade(score: float) -> str:
        if score >= 85:
            return "S級買點"
        if score >= 75:
            return "A級買點"
        if score >= 65:
            return "B級買點"
        if score >= 55:
            return "C級觀察"
        return "未達標"

    def _ret(idx: int, days: int):
        if idx + days >= len(work):
            return None
        p0 = _safe_float(close.iloc[idx])
        p1 = _safe_float(close.iloc[idx + days])
        if p0 in [None, 0] or p1 is None:
            return None
        return (p1 / p0 - 1) * 100

    def _max_gain_drawdown(idx: int, days: int = 20):
        if idx + 1 >= len(work):
            return None, None
        p0 = _safe_float(close.iloc[idx])
        if p0 in [None, 0]:
            return None, None
        window = work.iloc[idx + 1:min(len(work), idx + days + 1)]
        if window.empty:
            return None, None
        hh = _safe_float(pd.to_numeric(window["最高價"], errors="coerce").max())
        ll = _safe_float(pd.to_numeric(window["最低價"], errors="coerce").min())
        gain = ((hh / p0) - 1) * 100 if hh not in [None, 0] else None
        draw = ((ll / p0) - 1) * 100 if ll not in [None, 0] else None
        return gain, draw

    for i in range(60, len(work) - 1):
        row = work.iloc[i]
        prev = work.iloc[i - 1]
        price = _safe_float(row.get("收盤價"))
        ma20 = _safe_float(row.get("MA20"))
        ma60 = _safe_float(row.get("MA60"))
        ma120 = _safe_float(row.get("MA120"))
        vol20 = _safe_float(row.get("VOL20"))
        vol_now = _safe_float(row.get("成交股數"))
        k_now = _safe_float(row.get("K"))
        d_now = _safe_float(row.get("D"))
        dif = _safe_float(row.get("DIF"))
        dea = _safe_float(row.get("DEA"))
        atr14 = _safe_float(row.get("ATR14"), 0) or 0
        high20_prev = _safe_float(high.iloc[max(0, i - 20):i].max())
        low10_prev = _safe_float(low.iloc[max(0, i - 10):i].min())
        close_prev5 = _safe_float(close.iloc[i - 5])
        dif_prev5 = _safe_float(work.iloc[i - 5].get("DIF"))
        k_prev5 = _safe_float(work.iloc[i - 5].get("K"))

        score = 0
        reasons = []
        pattern = []

        if price is None:
            continue

        if ma20 is not None and price > ma20:
            score += 12
            reasons.append("站上MA20")
        if ma60 is not None and price > ma60:
            score += 12
            reasons.append("站上MA60")
        if ma20 is not None and ma60 is not None and ma20 > ma60:
            score += 8
            reasons.append("MA20在MA60上方")
        if ma120 is not None and ma60 is not None and ma60 > ma120:
            score += 6
            reasons.append("MA60在MA120上方")

        if all(v is not None for v in [vol_now, vol20]) and vol20 > 0:
            vr = vol_now / vol20
            if vr >= 1.8:
                score += 16
                reasons.append("量能明顯放大")
            elif vr >= 1.3:
                score += 10
                reasons.append("量能優於20日均量")

        if high20_prev is not None and price >= high20_prev:
            score += 18
            pattern.append("20日突破")
            reasons.append("突破前20日高")
        elif high20_prev is not None and price >= high20_prev * 0.985:
            score += 8
            reasons.append("逼近前20日高")

        if k_now is not None and d_now is not None and k_now > d_now:
            score += 8
            reasons.append("KD偏多")
        if dif is not None and dea is not None and dif > dea:
            score += 8
            reasons.append("MACD偏多")
        if dif is not None and dea is not None and dif > 0:
            score += 5
            reasons.append("DIF位於0軸上")

        if all(v is not None for v in [price, close_prev5]) and price > close_prev5:
            score += 5
            reasons.append("5日價格延續")

        if all(v is not None for v in [price, low10_prev]) and low10_prev > 0:
            pullback = (price / low10_prev - 1) * 100
            if 3 <= pullback <= 18:
                score += 6
                reasons.append("回檔後再轉強")
                pattern.append("回檔轉強")

        if all(v is not None for v in [price, close_prev5, dif, dif_prev5]) and price < close_prev5 and dif > dif_prev5:
            score += 6
            reasons.append("MACD底背離候選")
            pattern.append("底背離")

        if all(v is not None for v in [price, close_prev5, k_now, k_prev5]) and price < close_prev5 and k_now > k_prev5:
            score += 4
            reasons.append("KD底背離候選")
            if "底背離" not in pattern:
                pattern.append("底背離")

        if atr14 > 0 and ma20 is not None and abs(price - ma20) <= atr14 * 1.2:
            score += 4
            reasons.append("接近MA20風險較可控")

        if high20_prev is not None and _safe_float(prev.get("收盤價")) is not None and _safe_float(prev.get("收盤價")) >= high20_prev and price < high20_prev:
            score -= 12
            reasons.append("前一日突破後回落")

        if ma20 is not None and price < ma20:
            score -= 8
        if ma60 is not None and price < ma60:
            score -= 10
        if all(v is not None for v in [vol_now, vol20]) and vol20 > 0 and vol_now < vol20 * 0.7:
            score -= 6

        grade = _grade(score)
        if grade == "未達標":
            continue

        if not pattern:
            if high20_prev is not None and price >= high20_prev:
                pattern = ["突破追蹤"]
            elif ma20 is not None and ma60 is not None and price > ma20 > ma60:
                pattern = ["主升段延續"]
            else:
                pattern = ["整理轉強"]

        ret5 = _ret(i, 5)
        ret10 = _ret(i, 10)
        ret20 = _ret(i, 20)
        mg, md = _max_gain_drawdown(i, 20)

        signals.append({
            "日期": row.get("日期"),
            "收盤價": price,
            "分級": grade,
            "分數": round(score, 1),
            "型態": " / ".join(pattern),
            "5日報酬": ret5,
            "10日報酬": ret10,
            "20日報酬": ret20,
            "最大20日報酬": mg,
            "最大20日回撤": md,
            "理由": "、".join(reasons[:8]),
        })

    signals_df = pd.DataFrame(signals)
    if signals_df.empty:
        return {
            "current": {"grade": "尚無有效買點", "score": 0, "pattern": "—", "summary": "目前條件不足，尚未形成可回測買點。", "reasons": []},
            "overall": empty_overall,
            "by_pattern": pd.DataFrame(columns=["型態", "訊號數", "5日勝率", "10日勝率", "20日勝率", "20日平均報酬"]),
            "signals": empty_signals,
        }

    def _win_rate(series: pd.Series):
        s = pd.to_numeric(series, errors="coerce").dropna()
        if s.empty:
            return None
        return (s > 0).mean() * 100

    def _avg(series: pd.Series):
        s = pd.to_numeric(series, errors="coerce").dropna()
        return None if s.empty else s.mean()

    overall_rows = []
    for grade in ["S級買點", "A級買點", "B級買點", "C級觀察"]:
        sub = signals_df[signals_df["分級"] == grade].copy()
        if sub.empty:
            continue
        overall_rows.append({
            "分級": grade,
            "訊號數": int(len(sub)),
            "5日勝率": _win_rate(sub["5日報酬"]),
            "10日勝率": _win_rate(sub["10日報酬"]),
            "20日勝率": _win_rate(sub["20日報酬"]),
            "5日平均報酬": _avg(sub["5日報酬"]),
            "10日平均報酬": _avg(sub["10日報酬"]),
            "20日平均報酬": _avg(sub["20日報酬"]),
            "最大20日報酬": _avg(sub["最大20日報酬"]),
            "最大20日回撤": _avg(sub["最大20日回撤"]),
        })

    pattern_rows = []
    tmp = signals_df.copy()
    tmp["主型態"] = tmp["型態"].astype(str).str.split(" / ").str[0]
    for pat, sub in tmp.groupby("主型態"):
        pattern_rows.append({
            "型態": pat,
            "訊號數": int(len(sub)),
            "5日勝率": _win_rate(sub["5日報酬"]),
            "10日勝率": _win_rate(sub["10日報酬"]),
            "20日勝率": _win_rate(sub["20日報酬"]),
            "20日平均報酬": _avg(sub["20日報酬"]),
        })

    current = signals_df.iloc[-1].to_dict()
    overall_df = pd.DataFrame(overall_rows)
    if not overall_df.empty:
        overall_df = overall_df.sort_values("訊號數", ascending=False).reset_index(drop=True)
    pattern_df = pd.DataFrame(pattern_rows)
    if not pattern_df.empty:
        pattern_df = pattern_df.sort_values(["20日平均報酬", "訊號數"], ascending=[False, False]).reset_index(drop=True)

    current_summary = f"最近一次可回測買點為{_safe_str(current.get('分級'))}，型態偏{_safe_str(current.get('型態'))}，歷史同分級可用來看5/10/20日勝率。"

    return {
        "current": {
            "grade": _safe_str(current.get("分級")),
            "score": _safe_float(current.get("分數"), 0) or 0,
            "pattern": _safe_str(current.get("型態")),
            "summary": current_summary,
            "reasons": _safe_str(current.get("理由")).split("、") if _safe_str(current.get("理由")) else [],
            "ret5": current.get("5日報酬"),
            "ret10": current.get("10日報酬"),
            "ret20": current.get("20日報酬"),
        },
        "overall": overall_df,
        "by_pattern": pattern_df,
        "signals": signals_df.sort_values("日期", ascending=False).reset_index(drop=True),
    }


def _format_backtest_display(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    pct_cols = [c for c in out.columns if "勝率" in c or "報酬" in c or "回撤" in c]
    for col in pct_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").map(lambda x: None if pd.isna(x) else round(float(x), 2))
    if "收盤價" in out.columns:
        out["收盤價"] = pd.to_numeric(out["收盤價"], errors="coerce").map(lambda x: None if pd.isna(x) else round(float(x), 2))
    if "分數" in out.columns:
        out["分數"] = pd.to_numeric(out["分數"], errors="coerce").map(lambda x: None if pd.isna(x) else round(float(x), 1))
    return out


def _render_backtest_summary_cards(backtest: dict[str, Any]):
    current = backtest.get("current", {}) if isinstance(backtest, dict) else {}
    overall = backtest.get("overall") if isinstance(backtest, dict) else None

    top_win = None
    top_grade = "—"
    if isinstance(overall, pd.DataFrame) and not overall.empty and "20日勝率" in overall.columns:
        tmp = overall.dropna(subset=["20日勝率"]).sort_values("20日勝率", ascending=False)
        if not tmp.empty:
            top_win = _safe_float(tmp.iloc[0].get("20日勝率"))
            top_grade = _safe_str(tmp.iloc[0].get("分級"))

    render_pro_info_card(
        "可回測買點總覽",
        [
            ("最近買點分級", _safe_str(current.get("grade", "—")), ""),
            ("最近買點分數", _safe_str(current.get("score", 0)), ""),
            ("最近型態", _safe_str(current.get("pattern", "—")), ""),
            ("最佳歷史分級", top_grade, ""),
            ("最佳20日勝率", "—" if top_win is None else f"{top_win:.1f}%", ""),
            ("摘要", _safe_str(current.get("summary", "—")), ""),
        ],
        chips=["回測分級", "5/10/20日"],
    )


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
        "背離": {"bg": "#fff1f2", "border": "#e11d48", "tag": "#be123c", "text": "#881337"},
        "主升段": {"bg": "#ecfeff", "border": "#0891b2", "tag": "#0e7490", "text": "#164e63"},
        "假突破": {"bg": "#fff7ed", "border": "#ea580c", "tag": "#c2410c", "text": "#9a3412"},
    }
    return mapping.get(event_type, {"bg": "#f8fafc", "border": "#94a3b8", "tag": "#475569", "text": "#334155"})


def _event_direction_meta(event_name: str, event_type: str) -> dict[str, str]:
    name = _safe_str(event_name)
    typ = _safe_str(event_type)

    if typ in ["起漲點", "突破", "主升段"]:
        return {"arrow": "↑", "label": "偏多", "bg": "#dcfce7", "color": "#166534"}
    if typ in ["起跌點", "跌破", "假突破"]:
        return {"arrow": "↓", "label": "偏空", "bg": "#fee2e2", "color": "#991b1b"}
    if "黃金交叉" in name:
        return {"arrow": "↑", "label": "轉強", "bg": "#dbeafe", "color": "#1d4ed8"}
    if "死亡交叉" in name:
        return {"arrow": "↓", "label": "轉弱", "bg": "#fee2e2", "color": "#b91c1c"}
    return {"arrow": "→", "label": "觀察", "bg": "#e2e8f0", "color": "#334155"}


@st.cache_data(ttl=600, show_spinner=False)
def _build_candlestick_chart(df: pd.DataFrame, stock_label: str, show_ma: bool, show_pivots: bool, peak_idx: tuple[int, ...], trough_idx: tuple[int, ...]) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=df["日期"],
            open=df["開盤價"],
            high=df["最高價"],
            low=df["最低價"],
            close=df["收盤價"],
            name="K線",
        )
    )

    if show_ma:
        for n in [5, 10, 20, 60, 120, 240]:
            col = f"MA{n}"
            if col in df.columns:
                fig.add_trace(go.Scatter(x=df["日期"], y=df[col], mode="lines", name=col))

    if show_pivots:
        if trough_idx:
            idxs = [i for i in trough_idx if 0 <= i < len(df)]
            if idxs:
                sub = df.iloc[idxs].copy()
                fig.add_trace(go.Scatter(x=sub["日期"], y=sub["最低價"], mode="markers", name="起漲點", marker=dict(size=10, symbol="triangle-up")))
        if peak_idx:
            idxs = [i for i in peak_idx if 0 <= i < len(df)]
            if idxs:
                sub = df.iloc[idxs].copy()
                fig.add_trace(go.Scatter(x=sub["日期"], y=sub["最高價"], mode="markers", name="起跌點", marker=dict(size=10, symbol="triangle-down")))

    fig.update_layout(
        title=f"{stock_label}｜歷史K線分析",
        height=760,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="日期",
        yaxis_title="價格",
        xaxis_rangeslider_visible=False,
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
        trend_text = _metric_text(signal_snapshot, "ma_trend", "整理")
        kd_text = _metric_text(signal_snapshot, "kd_cross", "無新交叉")
        macd_text = _metric_text(signal_snapshot, "macd_trend", "整理")
        break_text = _metric_text(sr_snapshot, "break_signal", "區間內")

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
    structure_text = _metric_text(sr_snapshot, "break_signal", "區間內")

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
    score = int(_metric_number(signal_snapshot, "score", 0) or 0)
    structure_text = _metric_text(sr_snapshot, "break_signal", "區間內")
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
    score = _metric_number(signal_snapshot, "score", 0) or 0

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

    pressure_text = _metric_text(sr_snapshot, "pressure_signal", "—")
    support_text = _metric_text(sr_snapshot, "support_signal", "—")
    break_text = _metric_text(sr_snapshot, "break_signal", "—")

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
    _apply_external_focus_if_any(group_map)

    if _apply_watchlist_sync_if_needed(group_map):
        group_map = _build_group_stock_map()
        flat_rows = _flatten_group_map(group_map)
        _repair_state(group_map)
        _apply_external_focus_if_any(group_map)

    render_pro_hero(
        title="歷史K線分析｜升級完整版",
        subtitle="承接 7_股神推薦 / 4_自選股中心 的股票焦點，保留原功能並補強單股決策終端。",
    )

    watchlist_version = st.session_state.get("watchlist_version", 0)
    watchlist_saved_at = _safe_str(st.session_state.get("watchlist_last_saved_at", ""))
    if watchlist_version or watchlist_saved_at:
        st.caption(
            f"自選股同步狀態：watchlist_version = {watchlist_version}"
            + (f" / 最後更新：{watchlist_saved_at}" if watchlist_saved_at else "")
        )

    handoff_code = _get_handoff_stock_code()
    handoff_group = _get_handoff_watch_group()
    if handoff_code:
        st.caption(
            f"外部承接焦點：股票 {handoff_code}"
            + (f" / 群組 {handoff_group}" if handoff_group else "")
            + (f" / 名稱 {_get_handoff_stock_name()}" if _get_handoff_stock_name() else "")
        )


    _init_applied_query_state()

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

    with st.form(key=_k("query_form"), clear_on_submit=False):
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

        action_cols = st.columns(4)
        apply_query = False
        handoff_applied = False

        with action_cols[0]:
            apply_query = st.form_submit_button("套用查詢 / 重新載入", use_container_width=True, type="primary")

        with action_cols[1]:
            if st.form_submit_button("承接 4頁送來的股票", use_container_width=True):
                changed = _apply_external_focus_if_any(group_map)
                if changed:
                    _set_applied_query(
                        _safe_str(st.session_state.get(_k("group"), "")),
                        _safe_str(st.session_state.get(_k("stock_code"), "")),
                        _to_date(st.session_state.get(_k("start_date")), date.today() - timedelta(days=365)),
                        _to_date(st.session_state.get(_k("end_date")), date.today()),
                    )
                    handoff_applied = True
                    st.session_state[_k("focus_event_idx")] = -1

        with action_cols[2]:
            if st.form_submit_button("清除外部焦點", use_container_width=True):
                st.session_state["kline_focus_stock_code"] = ""
                st.session_state["kline_focus_stock_name"] = ""

        with action_cols[3]:
            st.caption("只有按『套用查詢』才會重抓歷史資料")

    st.caption("可直接承接 4_自選股中心 / 7_股神推薦 送來的焦點股票")
    st.info("已改成手動載入模式：切換群組、股票、日期時不會立刻重抓資料，避免歷史資料頁卡住。")

    if apply_query or handoff_applied:
        _set_applied_query(
            _safe_str(st.session_state.get(_k("group"), "")),
            _safe_str(st.session_state.get(_k("stock_code"), "")),
            _to_date(st.session_state.get(_k("start_date")), date.today() - timedelta(days=365)),
            _to_date(st.session_state.get(_k("end_date")), date.today()),
        )
        st.session_state[_k("focus_event_idx")] = -1

    applied_group, applied_code, start_date, end_date = _get_applied_query()

    if applied_group and applied_group in group_map:
        selected_group = applied_group
    else:
        selected_group = _safe_str(st.session_state.get(_k("group"), ""))

    selected_items = group_map.get(selected_group, [])
    selected_code_to_item = {x["code"]: x for x in selected_items}

    if start_date > end_date:
        st.error("開始日期不可大於結束日期。")
        st.stop()

    if not applied_code or applied_code not in selected_code_to_item:
        fallback_code = _safe_str(st.session_state.get(_k("stock_code"), ""))
        if fallback_code in selected_code_to_item:
            applied_code = fallback_code
            _set_applied_query(selected_group, applied_code, start_date, end_date)
        else:
            st.warning("請先選擇股票。")
            st.stop()

    selected_item = selected_code_to_item[applied_code]
    selected_code = applied_code
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
        df, actual_market, data_source = _load_history_runtime_cached(
            stock_no=selected_code,
            stock_name=stock_name,
            market_type=market_type,
            start_date=start_date,
            end_date=end_date,
        )

    st.caption(
        f"目前實際查詢值：群組【{selected_group}】 / 股票【{stock_label}】 / 自選市場【{market_type}】 / 實際市場【{actual_market}】 / 資料源【{data_source}】"
    )
    render_pro_section("互動控制")
    i1, i2, i3, i4 = st.columns([2, 2, 2, 2])

    with i1:
        st.selectbox("事件篩選", options=["全部", "起漲點", "起跌點", "MA", "KD", "MACD", "突破", "跌破", "背離", "主升段", "假突破"], key=_k("event_filter"))
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
                "delta": f"分數 {_metric_number(signal_snapshot, 'score', 0)}",
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


    focus_hint_rows = []
    if _get_handoff_stock_code():
        focus_hint_rows.append(("外部焦點股票", _get_handoff_stock_code(), ""))
    if _get_handoff_stock_name():
        focus_hint_rows.append(("外部焦點名稱", _get_handoff_stock_name(), ""))
    if _get_handoff_watch_group():
        focus_hint_rows.append(("來源群組", _get_handoff_watch_group(), ""))
    if focus_hint_rows:
        render_pro_info_card("跨頁承接資訊", focus_hint_rows, chips=["4頁", "7頁", "焦點股票"])

    left, right = st.columns([1.15, 2.85])

    with left:
        _render_left_event_panel(filtered_event_df)
        recent_pairs = [(pd.to_datetime(r["日期"]).strftime("%Y-%m-%d"), _safe_str(r["事件"]), "") for _, r in filtered_event_df.head(6).iterrows()] if not filtered_event_df.empty else [("最近事件", "無明確新事件", "")]
        render_pro_info_card("最近事件摘要", recent_pairs, chips=[badge_text, actual_market])

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
        )

    tabs = st.tabs(["KD / MACD", "雷達 / 訊號", "股神進階判斷", "可回測買點", "策略區", "最近事件", "原始資料"])

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
                    ("趨勢", _metric_number(radar, "trend", 50), ""),
                    ("動能", _metric_number(radar, "momentum", 50), ""),
                    ("量能", _metric_number(radar, "volume", 50), ""),
                    ("位置", _metric_number(radar, "position", 50), ""),
                    ("結構", _metric_number(radar, "structure", 50), ""),
                    ("摘要", _metric_text(radar, "summary", "—"), ""),
                ],
                chips=[badge_text],
            )
            render_pro_info_card(
                "訊號燈號",
                [
                    ("均線趨勢", _metric_text(signal_snapshot, "ma_trend", "—"), ""),
                    ("KD交叉", _metric_text(signal_snapshot, "kd_cross", "—"), ""),
                    ("MACD趨勢", _metric_text(signal_snapshot, "macd_trend", "—"), ""),
                    ("價位狀態", _metric_text(signal_snapshot, "price_vs_ma20", "—"), ""),
                    ("突破狀態", _metric_text(signal_snapshot, "breakout_20d", "—"), ""),
                    ("量能狀態", _metric_text(signal_snapshot, "volume_state", "—"), ""),
                ],
            )
        with r2:
            render_pro_info_card(
                "支撐壓力",
                [
                    ("20日壓力", format_number(_metric_number(sr_snapshot, "res_20"), 2), ""),
                    ("20日支撐", format_number(_metric_number(sr_snapshot, "sup_20"), 2), ""),
                    ("60日壓力", format_number(_metric_number(sr_snapshot, "res_60"), 2), ""),
                    ("60日支撐", format_number(_metric_number(sr_snapshot, "sup_60"), 2), ""),
                    ("壓力訊號", _metric_text(sr_snapshot, "pressure_signal", "—"), ""),
                    ("支撐訊號", _metric_text(sr_snapshot, "support_signal", "—"), ""),
                    ("區間判斷", _metric_text(sr_snapshot, "break_signal", "—"), ""),
                ],
            )
            render_pro_info_card(
                "股神分析觀點",
                _build_master_commentary(df, signal_snapshot, sr_snapshot, radar, filtered_event_df if not filtered_event_df.empty else event_df),
                chips=[actual_market, badge_text],
            )

    with tabs[2]:
        top_l, top_r = st.columns(2)
        with top_l:
            render_pro_info_card(
                "股神總表",
                [
                    ("判定", _safe_str(god_table.get("status", "暫不出手")), ""),
                    ("總分", _safe_str(god_table.get("score", 0)), ""),
                    ("市場階段", _safe_str(god_signal.get("phase", "整理")), ""),
                    ("操作動作", _safe_str(god_signal.get("action", "觀察")), ""),
                    ("訊號信心", f"{_safe_float(god_signal.get('confidence'), 50):.0f}", ""),
                    ("核心摘要", _safe_str(god_table.get("summary", "—")), ""),
                ],
                chips=[badge_text, actual_market],
            )
            render_pro_info_card(
                "主升段確認",
                [("判定", _safe_str(main_up.get("level", "—")), ""), ("分數", _safe_str(main_up.get("score", 0)), ""), ("摘要", _safe_str(main_up.get("summary", "—")), "")] + list(main_up.get("items", []))[:6],
                chips=["主升段", "趨勢確認"],
            )
        with top_r:
            render_pro_info_card(
                "真假突破 / 假跌破",
                [("方向", _safe_str(false_break.get("direction", "無")), ""), ("判定", _safe_str(false_break.get("level", "—")), ""), ("分數", _safe_str(false_break.get("score", 0)), ""), ("摘要", _safe_str(false_break.get("summary", "—")), "")] + list(false_break.get("items", []))[:6],
                chips=["真假突破", "過濾假訊號"],
            )
            render_pro_info_card(
                "背離強弱分級",
                [("型態", _safe_str(divergence.get("type", "無")), ""), ("強弱", _safe_str(divergence.get("level", "—")), ""), ("分數", _safe_str(divergence.get("score", 0)), ""), ("摘要", _safe_str(divergence.get("summary", "—")), "")] + list(divergence.get("items", []))[:6],
                chips=["KD", "MACD", "背離"],
            )

        render_pro_info_card(
            "更接近股神的執行重點",
            [
                ("1", "突破當天不算完成，至少再看 2~3 根是否站穩。", ""),
                ("2", "主升段不是單看漲，而是均線、價位、量能一起確認。", ""),
                ("3", "背離只當輔助，必須搭配結構收復或失守。", ""),
                ("4", "先看失效位，再決定是否進場，這樣才更像做交易。", ""),
            ],
            chips=["主升段", "假突破", "背離", "風控"],
        )
        _render_signal_summary_table(god_table, main_up, false_break, divergence)

    with tabs[3]:
        _render_backtest_summary_cards(buy_backtest)
        b1, b2 = st.columns(2)
        with b1:
            overall_df = _format_backtest_display(buy_backtest.get("overall"))
            if isinstance(overall_df, pd.DataFrame) and not overall_df.empty:
                st.markdown("#### 分級勝率表")
                st.dataframe(overall_df, use_container_width=True, hide_index=True)
            else:
                st.info("目前沒有足夠的回測買點樣本。")
        with b2:
            pattern_df = _format_backtest_display(buy_backtest.get("by_pattern"))
            if isinstance(pattern_df, pd.DataFrame) and not pattern_df.empty:
                st.markdown("#### 型態勝率表")
                st.dataframe(pattern_df, use_container_width=True, hide_index=True)
            else:
                st.info("目前沒有足夠的型態樣本。")

        current = buy_backtest.get("current", {}) if isinstance(buy_backtest, dict) else {}
        reasons = current.get("reasons", []) if isinstance(current, dict) else []
        render_pro_info_card(
            "最近一次可回測買點",
            [
                ("分級", _safe_str(current.get("grade", "—")), ""),
                ("分數", _safe_str(current.get("score", 0)), ""),
                ("型態", _safe_str(current.get("pattern", "—")), ""),
                ("5日報酬", format_number(current.get("ret5"), 2) + "%" if current.get("ret5") is not None else "—", ""),
                ("10日報酬", format_number(current.get("ret10"), 2) + "%" if current.get("ret10") is not None else "—", ""),
                ("20日報酬", format_number(current.get("ret20"), 2) + "%" if current.get("ret20") is not None else "—", ""),
            ] + [(f"理由{i+1}", _safe_str(r), "") for i, r in enumerate(reasons[:6])],
            chips=["歷史回測", "最近訊號"],
        )

        signals_df = _format_backtest_display(buy_backtest.get("signals"))
        if isinstance(signals_df, pd.DataFrame) and not signals_df.empty:
            st.markdown("#### 歷史買點明細")
            st.dataframe(signals_df.head(80), use_container_width=True, hide_index=True)

    with tabs[4]:
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

    with tabs[5]:
        if filtered_event_df.empty:
            st.info("目前沒有符合條件的事件。")
        else:
            st.dataframe(filtered_event_df, use_container_width=True, hide_index=True)

    with tabs[6]:
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
        st.write("8. 新增股神進階判斷：真假突破、主升段確認、背離強弱分級、股神訊號總表。")
        st.write("9. 新增可回測買點分級：統計 S/A/B/C 分級在 5 / 10 / 20 日的勝率與平均報酬。")
        st.write("10. 可承接 4_自選股中心 / 7_股神推薦 送來的焦點股票，不必重新搜尋。")


if __name__ == "__main__":
    main()
