# -*- coding: utf-8 -*-
"""V191-H34 daily 1~3 safe recommendation admission layer.

Goal
----
Improve actionable recall without turning the recommendation engine into a quota filler.
The layer runs *after* the existing formal engine and only backfills when the current
Formal/A- list is below the market-regime target.

Principles
----------
1. Target count: bullish=3, neutral/selective=2, defensive=1, hard-risk=0.
2. Existing Formal/A- recommendations always have priority.
3. Hard vetoes are immutable: stale K-line, extreme/LOCKDOWN market, illiquidity,
   excessive stop/chase risk, insufficient RR, Emerging market, explicit block labels.
4. H41 preserves Formal/V188 authority. Near-miss rows may become an independent daily conditional pick, never silently rewritten into Formal/A-.
5. Conditional picks must have a reachable executable path and always require trigger/pullback + guard confirmation; no trigger means NO-TRADE.
6. If no stock is safe enough, output 0. Never fabricate a pick to satisfy the quota.
7. H32 forecasts are optional ranking evidence, never a hard guarantee.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h47_daily_safe_selection_leader_stage_20260821"

H34_COLUMNS = [
    "H34每日精選", "H34每日精選排名", "H34每日目標檔數", "H34安全精選分",
    "H34精選等級", "H34精選理由", "H34阻擋原因", "H34操作原則", "H34機率來源", "H34版本",
    "H41推薦漏斗模式", "H41安全可執行候選數", "H41硬風控淘汰數", "H41推薦漏斗健康",
    "H41原始正式分區", "H41每日條件精選", "H41條件操作許可",
    "H41實戰觸發距離%", "H41最近可執行距離%", "V191_H41推薦架構",
]

_BLANK = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}


def _s(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    t = str(v).strip()
    return "" if t.lower() in _BLANK else t


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        text = str(v).strip().replace(",", "").replace("％", "%")
        if text.endswith("%"):
            text = text[:-1].strip()
        if not text or text.lower() in _BLANK:
            return float(default)
        x = float(text)
        return x if math.isfinite(x) else float(default)
    except Exception:
        return float(default)


def _first_num(row: pd.Series, names: Iterable[str], default: float = 0.0, positive: bool = False) -> float:
    fallback = None
    for name in names:
        if name not in row.index:
            continue
        raw = row.get(name)
        if not _s(raw):
            continue
        x = _f(raw, float("nan"))
        if math.isnan(x):
            continue
        if fallback is None:
            fallback = x
        if not positive or x > 0:
            return float(x)
    return float(default if fallback is None else fallback)


def _first_text(row: pd.Series, names: Iterable[str], default: str = "") -> str:
    for name in names:
        if name in row.index:
            text = _s(row.get(name))
            if text:
                return text
    return default


def _blob(row: pd.Series, names: Iterable[str]) -> str:
    return "｜".join(_s(row.get(name)) for name in names if name in row.index and _s(row.get(name)))


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(x)))


def _market_profile(frame: pd.DataFrame) -> dict[str, Any]:
    """Build one authoritative market profile without double-counting breadth risk.

    H38 aggregated every market text column into one blob.  That made the lower
    level breadth signal ``廣度紅燈`` override the formal engine's authoritative
    ``黃燈/橘燈`` classification and forced the strict READY-R washout path every
    day.  H41 gives explicit authority-level fields precedence:
    LOCKDOWN wins globally; an authority red level stays strict; breadth-red with
    neutral/yellow/orange authority becomes *defensive selective* (target 1), not
    a total ban.
    """
    if frame is None or frame.empty:
        return {"target": 0, "hard_block": True, "red_light": False, "defensive_selective": False, "score": 0.0, "regime": "NO-DATA"}

    text_parts: list[str] = []
    authority_levels: list[str] = []
    breadth_parts: list[str] = []
    bridge_parts: list[str] = []
    scores: list[float] = []
    score_columns = ["大盤橋接分數", "大盤可參考分數", "大盤多空分數", "市場環境分數"]
    for _, row in frame.iterrows():
        level = _first_text(row, ["大盤風控層級", "大盤策略模式", "大盤橋接風控"])
        breadth = _first_text(row, ["大盤風險燈號", "今日大盤結論"])
        bridge = _blob(row, ["極端市場LOCKDOWN", "大盤原始橋接狀態", "大盤橋接狀態", "大盤策略建議", "大盤風控建議", "大盤橋接風控"])
        if level:
            authority_levels.append(level)
            text_parts.append(level)
        if breadth:
            breadth_parts.append(breadth)
            text_parts.append(breadth)
        if bridge:
            bridge_parts.append(bridge)
            text_parts.append(bridge)
        # H42：同一列可能同時有「大盤橋接分」與「市場環境分」。
        # 舊版 _first_num 只取第一個，8/18 因 30.9 蓋掉 66.7 而
        # 全市場被錯判 LOCKDOWN。現在收集所有獨立有效分數做共識。
        for score_col in score_columns:
            if score_col not in row.index or not _s(row.get(score_col)):
                continue
            value = _f(row.get(score_col), float("nan"))
            if math.isfinite(value) and all(abs(float(value) - seen) > 1e-6 for seen in scores):
                scores.append(float(value))

    all_text = "｜".join(text_parts)
    level_text = "｜".join(authority_levels)
    breadth_text = "｜".join(breadth_parts)
    bridge_text = "｜".join(bridge_parts)
    score = float(pd.Series(scores, dtype="float64").median()) if scores else 50.0
    low_score_votes = sum(1 for value in scores if value < 42)

    hard = any(k in all_text for k in ["LOCKDOWN", "全面禁買", "極端風險", "崩跌後冷卻", "極端崩跌", "禁止所有新倉", "全面停買"])
    # H42：單一低分不可再獨自製造 LOCKDOWN。只有至少兩個獨立權威
    # 分數都低於42，才讓分數本身成為硬風險證據；明確LOCKDOWN文字仍優先。
    if score < 42 and low_score_votes >= 2:
        hard = True
    if hard:
        return {"target": 0, "hard_block": True, "red_light": True, "defensive_selective": False, "score": score, "regime": "LOCKDOWN/極端風險"}

    # Only the authoritative level / bridge can create a strict red regime.
    # ``廣度紅燈`` alone is a breadth warning and must not erase all candidates.
    authority_red = any(k in level_text for k in ["紅燈｜權威", "全面防守", "僅准條件逆勢"]) or any(
        k in bridge_text for k in ["空方", "風險急升", "禁止進攻", "全面防守"]
    )
    breadth_red = any(k in breadth_text for k in ["廣度紅燈", "候選廣度偏弱", "防守"])
    selective_level = any(k in level_text for k in ["橘燈", "黃燈", "防守｜縮小倉位"])
    neutral_bridge = any(k in bridge_text for k in ["中性", "震盪", "盤整", "選股", "輪動", "正常"])

    if authority_red:
        return {"target": 1, "hard_block": False, "red_light": True, "defensive_selective": False, "score": score, "regime": "紅燈/僅嚴格逆勢"}

    if selective_level or breadth_red or (score < 50 and neutral_bridge):
        return {"target": 1, "hard_block": False, "red_light": False, "defensive_selective": True, "score": score, "regime": "防守精選/最多1檔"}

    if score >= 65 or any(k in (level_text + "｜" + bridge_text) for k in ["趨勢多頭", "強勢多頭", "攻擊模式", "綠燈"]):
        target, regime = 3, "攻擊/偏多"
    elif score >= 50:
        target, regime = 2, "中性/精選"
    else:
        target, regime = 1, "防守/只選最強"
    return {"target": target, "hard_block": False, "red_light": False, "defensive_selective": bool(target == 1), "score": score, "regime": regime}


def _probability_with_source(row: pd.Series) -> tuple[float, str]:
    # H34 is a decision layer, so it must prefer the probability that has already
    # passed SuperAI/T+1 calibration.  H32 is a forecast/reporting layer and is
    # only a fallback when it genuinely existed before this decision.
    for col in [
        "SuperAI校準後隔日上漲機率%", "H32隔日上漲機率%",
        "SuperAI隔日上漲機率%", "模型隔日上漲機率%",
    ]:
        raw = row.get(col)
        if not _s(raw):
            continue
        value = _f(raw, float("nan"))
        if math.isfinite(value) and 0.0 <= value <= 100.0:
            return float(value), col
    return 50.0, "預設50%"


def _metrics(row: pd.Series) -> dict[str, float]:
    rr = _first_num(row, ["路徑風險報酬比", "風險報酬比", "實戰風險報酬比", "保守風報比"], 0.0, positive=True)
    stop = _first_num(row, ["停損距離_隔日%", "隔日有效風控距離%", "實戰停損距離%", "停損距離%"], 0.0, positive=True)
    amount = _first_num(row, ["流動性參考成交額百萬", "成交額百萬", "20日均成交額百萬"], 0.0, positive=True)
    gap = _first_num(row, ["距最近可執行買點%", "觸發距離%"], 99.0)
    latest = _first_num(row, ["最新價", "收盤價", "價格"], 0.0, positive=True)
    trigger = _first_num(row, ["實戰觸發價", "觸發價", "建議觸發價"], 0.0, positive=True)
    trigger_distance = ((trigger / latest) - 1.0) * 100.0 if latest > 0 and trigger > 0 else gap
    valid_gap = gap if math.isfinite(gap) and 0 <= gap < 90 else float("nan")
    valid_trigger_distance = trigger_distance if math.isfinite(trigger_distance) and trigger_distance >= 0 else float("nan")
    if math.isfinite(valid_gap) and math.isfinite(valid_trigger_distance):
        execution_distance = min(valid_gap, valid_trigger_distance)
    elif math.isfinite(valid_gap):
        execution_distance = valid_gap
    elif math.isfinite(valid_trigger_distance):
        execution_distance = valid_trigger_distance
    else:
        execution_distance = 99.0
    # V188 是正式決策的優先來源；但舊快取/單元測試若尚未帶入 V188 欄位，
    # 不可把 Trade/Alpha 默認成0而把整個推薦漏斗誤清空。此 fallback 只維持
    # 相容性，Page07 正式流程仍會在 H34 前完成 V188。
    priority = _first_num(row, ["V188股神作戰優先分", "股神推薦優先分", "正式推薦排序分", "AI綜合決策分"], 50.0)
    trade = _first_num(row, ["SuperAI Trade分", "SuperAI交易分", "實戰操作品質分", "可操作分", "進場可執行分"], 50.0)
    alpha = _first_num(row, ["SuperAI Alpha分", "AI Alpha品質分", "候選強度分", "AI綜合決策分", "股神推薦優先分"], 50.0)
    prob, prob_source = _probability_with_source(row)
    return {
        "entry": _first_num(row, ["Entry進場買點分", "Entry進場分", "進場買點分", "買進分數"], 0.0),
        "risk": _first_num(row, ["Risk風控安全分", "Risk風控分", "風控安全分"], 0.0),
        "buy": _first_num(row, ["買進分數"], 0.0),
        "op": _first_num(row, ["可操作分", "實戰操作品質分", "進場可執行分"], 0.0),
        "ready": _first_num(row, ["進場可執行分", "隔日可執行優先分"], 0.0),
        "rr": rr,
        "stop": stop,
        "amount": amount,
        "gap": gap,
        "chase": _first_num(row, ["追價風險分", "追高風險分數_決策", "追價風險分數"], 55.0),
        "mainstream": _first_num(row, ["主流資金分"], 50.0),
        "sector": max(
            _first_num(row, ["族群攻擊強度"], 0.0),
            _first_num(row, ["族群輪動分"], 0.0),
            _first_num(row, ["類股熱度分數"], 0.0),
        ),
        "mainwave": _first_num(row, ["H45主流波段分"], 0.0),
        "sector_main": _first_num(row, ["H45族群主流分"], 0.0),
        "onset": _first_num(row, ["H45起漲結構分"], 0.0),
        "mainwave_status": _first_text(row, ["H45主流波段狀態"]),
        "h47_status": _first_text(row, ["H47主流領先狀態", "H47波段階段"]),
        "h47_trade_status": _first_text(row, ["H47交易候選狀態"]),
        "h47_start": _first_num(row, ["H47起漲優先分"], 0.0),
        "h47_rs": _first_num(row, ["H47個股相對強度分"], 0.0),
        "priority": priority,
        "trade": trade,
        "alpha": alpha,
        "trigger_distance": trigger_distance,
        "execution_distance": execution_distance,
        "ai": _first_num(row, ["AI綜合決策分", "SuperAI 最終決策分", "SuperAI最終決策分"], 50.0),
        "prob": prob,
        "prob_source": prob_source,
        "t1": _first_num(row, ["H32隔日預估漲跌幅%", "模型預估超額報酬%"], 0.0),
        "swing": _first_num(row, ["H32後續波段預估漲幅%", "H32_10日預估報酬%"], 0.0),
    }


def _hard_veto(row: pd.Series, market: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if market.get("hard_block"):
        reasons.append("大盤LOCKDOWN/極端風險")
    market_type = _s(row.get("市場別")).replace(" ", "")
    if market_type in {"興櫃", "Emerging"}:
        reasons.append("興櫃不列每日作戰推薦")

    k_lag_text = _first_text(row, ["K線落後交易日"])
    k_lag = _f(k_lag_text, float("nan")) if k_lag_text else float("nan")
    k_fresh = _first_text(row, ["K線資料新鮮度", "股神資料總新鮮度"])
    k_positive = any(k in k_fresh for k in ["最新", "對齊", "PASS", "已驗證"])
    k_negative = any(k in k_fresh for k in ["過期", "嚴重落後", "待更新", "未驗證", "未知"])
    if (math.isfinite(k_lag) and k_lag != 0) or k_negative or (not math.isfinite(k_lag) and not k_positive):
        reasons.append("K線不是最近完成交易日")

    o_lag_text = _first_text(row, ["官方因子落後交易日"])
    o_lag = _f(o_lag_text, float("nan")) if o_lag_text else float("nan")
    o_fresh = _first_text(row, ["官方因子新鮮度", "官方因子資料狀態"])
    o_positive = any(k in o_fresh for k in ["最新", "T-1已驗證", "降級可用", "PASS", "已驗證"])
    o_negative = any(k in o_fresh for k in ["無法驗證", "過期", "失效", "嚴重落後"])
    if (math.isfinite(o_lag) and o_lag > 1) or o_negative or (not math.isfinite(o_lag) and not o_positive):
        reasons.append("官方因子過期/無法驗證")

    text = _blob(row, [
        "正式推薦排除原因", "風控否決旗標", "真禁買原因", "硬否決原因",
        "紅燈觸發管制", "模型預測限制", "推薦輪動狀態", "掃描品質狀態",
    ])
    hard_words = [
        "LOCKDOWN", "極端", "全面禁買", "低流動性", "冷門禁追", "興櫃",
        "停損距離過大", "風控失衡", "假強", "假突破", "PRED-BLOCK",
        "STICKY-BLOCK", "禁止升格", "過熱禁買",
    ]
    for word in hard_words:
        if word in text:
            reasons.append(f"硬風控：{word}")
            break

    m = _metrics(row)
    if m["amount"] < 150:
        reasons.append("成交額不足1.5億元")
    if m["stop"] <= 0 or m["stop"] > 8.0:
        reasons.append(f"停損距離{m['stop']:.1f}%不合格")
    if m["rr"] < 1.05:
        reasons.append(f"RR {m['rr']:.2f}<1.05")
    if m["chase"] > 68:
        reasons.append(f"追價風險{m['chase']:.0f}>68")

    # H42：防守/震盪盤特別防「盤中沖高、收盤被打回低檔」的假強勢。
    # 8/18 中美晶/台化都有極低收盤位置＋極長上影，舊H41只看Entry/RR
    # 仍可能把它們排成每日精選第一名。這種結構可留研究雷達，但不能在
    # 防守市場直接取得每日條件精選資格。
    close_pos = _first_num(row, ["當日收盤位置%"], 50.0)
    upper_shadow = _first_num(row, ["上影線比例%"], 20.0)
    if bool(market.get("defensive_selective")) and close_pos < 20 and upper_shadow > 60:
        reasons.append(f"防守盤沖高回落：收盤位置{close_pos:.0f}%／上影{upper_shadow:.0f}%")
    return list(dict.fromkeys(reasons))


def _quality_score(m: dict[str, float]) -> float:
    rr_score = _clamp(m["rr"] / 2.5 * 100.0)
    gap_score = 100.0 if m["gap"] <= 1.5 else 90.0 if m["gap"] <= 2.5 else 78.0 if m["gap"] <= 4 else 62.0 if m["gap"] <= 5.5 else 35.0
    forecast_score = _clamp(50.0 + m["t1"] * 8.0 + m["swing"] * 1.8)
    trigger_reach_score = 100.0 if m["execution_distance"] <= 1.5 else 90.0 if m["execution_distance"] <= 3.0 else 78.0 if m["execution_distance"] <= 4.5 else 62.0 if m["execution_distance"] <= 5.5 else 35.0 if m["execution_distance"] <= 7.0 else 10.0
    # H47: selection score must reward current leadership / early-stage fit, while
    # execution safety remains a separate veto.  This prevents a safe but weak
    # stock from outranking the real market leaders merely because Risk is high.
    h47_available = bool(m.get("h47_status") or m.get("h47_start", 0) > 0 or m.get("h47_rs", 0) > 0)
    if h47_available:
        h47_start = m.get("h47_start", 0.0) or 50.0
        h47_rs = m.get("h47_rs", 0.0) or 50.0
        sector_main = m.get("sector_main", 0.0) or 50.0
        score = (
            h47_start * 0.17 + h47_rs * 0.13 + sector_main * 0.08
            + m["priority"] * 0.10 + m["trade"] * 0.08 + m["entry"] * 0.09
            + m["risk"] * 0.11 + m["op"] * 0.06 + rr_score * 0.08
            + (100.0 - m["chase"]) * 0.04 + m["prob"] * 0.02
            + trigger_reach_score * 0.02 + gap_score * 0.01 + forecast_score * 0.01
        )
    else:
        mainwave = m.get("mainwave", 0.0) or 50.0
        sector_main = m.get("sector_main", 0.0) or 50.0
        onset = m.get("onset", 0.0) or 50.0
        score = (
            m["priority"] * 0.12 + m["trade"] * 0.08 + m["ai"] * 0.04
            + m["entry"] * 0.11 + m["risk"] * 0.12 + m["op"] * 0.07
            + rr_score * 0.10 + (100.0 - m["chase"]) * 0.05
            + m["mainstream"] * 0.03 + m["sector"] * 0.02 + m["prob"] * 0.03
            + forecast_score * 0.01 + gap_score * 0.01 + trigger_reach_score * 0.02
            + mainwave * 0.11 + sector_main * 0.05 + onset * 0.03
        )
    return round(_clamp(score), 2)

def _gate(row: pd.Series, market: dict[str, Any]) -> dict[str, Any]:
    veto = _hard_veto(row, market)
    m = _metrics(row)
    score = _quality_score(m)
    h47_available = bool(m.get("h47_status") or m.get("h47_start", 0) > 0 or m.get("h47_rs", 0) > 0)
    h47_status = str(m.get("h47_status") or "")
    h47_trade = str(m.get("h47_trade_status") or "")
    h47_market_ok = h47_status.startswith(("L-EARLY", "L-PULLBACK", "L-LEADER"))
    h47_extended = h47_status.startswith("L-EXTENDED")
    h47_trade_ready = h47_trade.startswith("T-READY")
    h45_available = bool(m.get("mainwave_status") or m.get("mainwave", 0) > 0)
    h45_ready = str(m.get("mainwave_status") or "").startswith("M-READY")
    h45_prep = str(m.get("mainwave_status") or "").startswith("M-PREP")
    h45_watch = str(m.get("mainwave_status") or "").startswith("M-WATCH")
    h45_mainline_ok = bool(h45_ready or h45_prep or (h45_watch and m.get("mainwave", 0) >= 63))
    mainline_ok = h47_market_ok if h47_available else h45_mainline_ok
    if veto:
        return {"eligible": False, "formal": False, "hard_veto": veto, "score": score, "reason": "、".join(veto[:5]), "m": m}
    if h47_available and h47_extended:
        return {"eligible": False, "formal": False, "hard_veto": [], "score": score,
                "reason": "H47主流領漲但已延伸｜禁止追價，只等回測重新取得T-READY", "m": m}

    # H38：紅燈市場不能用一般 Entry/Risk/RR 條件繞過正式引擎。
    # 唯一可進 H34 的例外是正式紅燈逆勢模組已判定 READY-R；WATCH-R/R2
    # 與 BLOCK-R 一律只保留雷達，不得被 H34 補位升格。
    if bool(market.get("red_light")):
        red_status = _first_text(row, ["紅燈逆勢反轉判定"])
        if not red_status.startswith("READY-R"):
            reason = red_status or "紅燈市場未取得 READY-R 逆勢反轉許可"
            return {
                "eligible": False, "formal": False, "hard_veto": [], "score": score,
                "reason": f"紅燈限制：{reason}", "m": m,
            }

    # H41：防守精選不是紅燈全面封鎖。它允許最多1檔，但回補候選必須
    # 同時具有 V188 交易品質與更高安全邊際，避免把一般雷達硬湊成推薦。
    defensive_ok = True
    if bool(market.get("defensive_selective")):
        defensive_ok = bool(
            score >= 65 and m["priority"] >= 62 and m["trade"] >= 62
            and m["risk"] >= 58 and m["rr"] >= 1.25 and m["chase"] <= 62
            and m["execution_distance"] <= 5.5
            # H47存在時，防守盤每日安全精選只接受真正主流市場結構，
            # 且必須已取得T-READY。領漲但過熱者可列主流核心，不可列每日買進精選。
            and ((h47_market_ok and h47_trade_ready) if h47_available else ((not h45_available) or h45_ready or h45_prep))
        )

    # H34-F: safe near-miss allowed to become a formal recommendation.
    formal = bool(
        m["entry"] >= 66 and m["risk"] >= 63 and m["buy"] >= 58
        and m["op"] >= 66 and m["rr"] >= 1.50 and 0 < m["stop"] <= 6.5
        and m["amount"] >= 250 and m["chase"] <= 52 and m["gap"] <= 5.0
        and ((h47_market_ok and m.get("h47_start",0) >= 60) if h47_available else ((h45_mainline_ok and m.get("mainwave", 0) >= 62) if h45_available else (m["mainstream"] >= 55 or m["sector"] >= 55)))
        and m["prob"] >= 52 and score >= 69
    )
    # H41-A：把「條件精選」從八個硬 AND 門檻改成「核心風控 + 品質柱」。
    # 歷史 T+1 顯示雷達在 *觸發且守價後* 的可執行績效有價值，因此 Entry、
    # 買進分、主流/族群、機率等應作為多因子證據，不應任一差 0.1 就把整檔
    # 清空。硬風控仍維持 RR/停損/流動性/追價；至少 4/6 品質柱才可升 A-。
    quality_pillars = [
        m["entry"] >= 58,
        m["buy"] >= 52,
        m["gap"] <= 6.5,
        (h47_market_ok if h47_available else (h45_mainline_ok if h45_available else (m["mainstream"] >= 50 or m["sector"] >= 50))),
        m["prob"] >= 48,
        m["alpha"] >= 55,
    ]
    pillar_count = sum(1 for ok in quality_pillars if ok)
    a_minus_core = bool(
        m["risk"] >= 56 and m["op"] >= 60 and m["rr"] >= 1.20
        and 0 < m["stop"] <= 7.5 and m["amount"] >= 180 and m["chase"] <= 62
        and m["priority"] >= 60 and m["trade"] >= 60 and m["execution_distance"] <= 6.0 and score >= 62
    )
    a_minus = bool(a_minus_core and pillar_count >= 4)
    m["quality_pillars"] = pillar_count
    eligible = bool((formal or a_minus) and defensive_ok)
    if not eligible:
        failed = []
        checks = [
            (m["risk"] >= 56, f"Risk {m['risk']:.1f}<56"),
            (m["op"] >= 60, f"可操作{m['op']:.1f}<60"),
            (m["rr"] >= 1.20, f"RR {m['rr']:.2f}<1.20"),
            (m["chase"] <= 62, f"追價風險{m['chase']:.0f}>62"),
            (m["priority"] >= 60, f"V188作戰優先{m['priority']:.1f}<60"),
            (m["trade"] >= 60, f"SuperAI Trade {m['trade']:.1f}<60"),
            (m["execution_distance"] <= 6.0, f"最近可執行距離{m['execution_distance']:.1f}%>6.0"),
            (pillar_count >= 4, f"H45品質柱{pillar_count}/6<4"),
            ((h47_market_ok if h47_available else ((not h45_available) or h45_mainline_ok)), f"H47/H45非主流起漲｜{m.get('h47_status') or m.get('mainwave_status') or '無'}"),
            (score >= 62, f"H34安全精選分{score:.1f}<62"),
            (defensive_ok, f"防守精選品質不足｜V188 {m['priority']:.1f}/Trade {m['trade']:.1f}/Risk {m['risk']:.1f}/RR {m['rr']:.2f}/追價 {m['chase']:.0f}"),
        ]
        failed = [msg for ok, msg in checks if not ok]
        reason = "、".join(failed[:5]) or "未達H34安全精選條件"
    else:
        reason = ""
    return {"eligible": eligible, "formal": formal, "hard_veto": [], "score": score, "reason": reason, "m": m}


def _existing_bucket(row: pd.Series) -> bool:
    bucket = _s(row.get("正式推薦分區"))
    return bucket in {"正式下週主推薦", "A-｜準主推薦小量試單"}


def _conditional_permission(row: pd.Series) -> str:
    """Return an execution instruction that respects the upstream V188 route."""
    v188 = _first_text(row, ["V188交易許可", "操作許可"])
    path = _first_text(row, ["主要進場路徑", "進場路徑", "進場時機"])
    blob = f"{v188}｜{path}"
    if "WAIT-PULLBACK" in blob or "回測" in blob or "承接" in blob:
        ref = _first_num(row, ["回測承接參考價", "守價回測參考價", "回測承接價", "主要進場參考價"], 0.0, positive=True)
        ref_text = f"{ref:.2f}" if ref > 0 else "系統回測承接區"
        return f"PULLBACK-COND｜只等回測/承接 {ref_text} 附近守價成立後小量；禁止突破追價；未成立=NO-TRADE"
    trigger = _first_num(row, ["實戰觸發價", "觸發價"], 0.0, positive=True)
    guard = _first_num(row, ["觸發後守價", "守價", "守價回測參考價"], 0.0, positive=True)
    trigger_text = f"{trigger:.2f}" if trigger > 0 else "實戰觸發價"
    guard_text = f"{guard:.2f}" if guard > 0 else "守價"
    return f"TRIGGER-COND｜突破 {trigger_text} 且守住 {guard_text} 後小量；未觸發/失守=NO-TRADE"


def apply_daily_safe_selection(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame(columns=H34_COLUMNS)
    out = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame(frame)
    # H36: these authority/audit columns are textual even when an imported Excel
    # inferred a bool dtype.  Cast once before writing labels to avoid pandas
    # future assignment failures (e.g. True/False -> "是/否").
    for _text_col in [
        "是否正式推薦", "正式推薦分區", "正式推薦資格", "正式推薦等級",
        "準主推薦等級", "操作許可", "下週是否可直接買", "正式推薦判定來源",
        "正式推薦排除原因", "正式推薦動作", "最終操作結論", "推薦資格路徑",
    ]:
        if _text_col in out.columns:
            out[_text_col] = out[_text_col].astype(object)
    for col, default in {
        "H34每日精選": "否", "H34每日精選排名": 0, "H34每日目標檔數": 0,
        "H34安全精選分": 0.0, "H34精選等級": "", "H34精選理由": "",
        "H34阻擋原因": "", "H34操作原則": "", "H34機率來源": "", "H34版本": VERSION,
    }.items():
        out[col] = default
    if out.empty:
        return out

    market = _market_profile(out)
    target = int(market["target"])
    out["H34每日目標檔數"] = target
    out["H41推薦漏斗模式"] = str(market.get("regime") or "")
    out["H41原始正式分區"] = out["正式推薦分區"].astype(str) if "正式推薦分區" in out.columns else ""
    out["H41每日條件精選"] = "否"
    out["H41條件操作許可"] = ""
    out["H41實戰觸發距離%"] = 0.0
    out["H41最近可執行距離%"] = 0.0
    out["V191_H41推薦架構"] = "是"
    if target <= 0:
        out["H34阻擋原因"] = "大盤LOCKDOWN/極端風險：H34允許0檔，不為名額犧牲風控"
        return out

    # Score everyone for auditability.
    gates: dict[Any, dict[str, Any]] = {}
    for idx, row in out.iterrows():
        gate = _gate(row, market)
        gates[idx] = gate
        out.at[idx, "H34安全精選分"] = gate["score"]
        out.at[idx, "H34阻擋原因"] = gate["reason"]
        out.at[idx, "H34機率來源"] = gate["m"].get("prob_source", "")
        out.at[idx, "H41實戰觸發距離%"] = round(float(gate["m"].get("trigger_distance", 99.0)), 2)
        out.at[idx, "H41最近可執行距離%"] = round(float(gate["m"].get("execution_distance", 99.0)), 2)

    eligible_count = sum(1 for gate in gates.values() if gate.get("eligible") and not gate.get("hard_veto"))
    hard_veto_count = sum(1 for gate in gates.values() if gate.get("hard_veto"))
    out["H41安全可執行候選數"] = int(eligible_count)
    out["H41硬風控淘汰數"] = int(hard_veto_count)

    existing = [idx for idx, row in out.iterrows() if _existing_bucket(row)]
    existing.sort(key=lambda idx: gates[idx]["score"], reverse=True)

    # Existing Formal/A- has priority only while it still passes the immutable
    # H34 hard-risk/data vetoes.  The old code selected every existing bucket
    # before checking its gate, so a stale K-line formal pick could be labelled
    # ``H34每日精選=是`` and its blocking reason was subsequently cleared.
    existing_safe = [
        idx for idx in existing
        if not gates[idx].get("hard_veto")
        and (not bool(market.get("red_light")) or bool(gates[idx].get("eligible")))
        and (not bool(market.get("defensive_selective")) or bool(gates[idx].get("eligible")))
    ]
    selected: list[Any] = existing_safe[:target]

    # H41：Formal/A- 不足時提供「每日條件精選」，但不得再篡改 Formal/V188
    # 的原始推薦分區。過去 H34 會把一般 RADAR 直接改成 A-，造成權威層互相
    # 打架，也讓績效回饋無法分清「Formal推薦」與「H34條件精選」。
    # 現在 H34 只建立獨立的每日條件層：有明確觸發價、守價與停損才有資格；
    # 未觸發就是 NO-TRADE，不把觀察股冒充直接買進推薦。
    if len(selected) < target:
        pool = [idx for idx in out.index if idx not in existing and gates[idx]["eligible"]]
        pool.sort(key=lambda idx: gates[idx]["score"], reverse=True)
        for idx in pool[: max(0, target - len(selected))]:
            gate = gates[idx]
            out.at[idx, "H41每日條件精選"] = "是"
            out.at[idx, "H41條件操作許可"] = _conditional_permission(out.loc[idx])
            out.at[idx, "H34精選等級"] = "H41-C｜每日條件精選"
            out.at[idx, "H34操作原則"] = "每日條件精選不是開盤買進；只做觸發後守價，未觸發不交易，跌破守價/停損立即取消。"
            out.at[idx, "推薦資格路徑"] = "H41｜Formal/A-不足時的獨立每日條件精選；不改寫原始推薦分區"
            selected.append(idx)

    # Mark selected daily list. Existing standard picks receive their original grade.
    selected = selected[:target]
    if target <= 0:
        funnel_health = "真空手｜LOCKDOWN/極端風險，策略允許0檔"
    elif selected:
        funnel_health = f"PASS｜本輪{len(selected)}檔每日精選／安全可執行候選{eligible_count}檔／目標上限{target}檔"
    elif eligible_count <= 0:
        funnel_health = f"真空手｜非配額問題；{len(out)}檔候選均未同時通過交易品質與硬風控"
    else:
        funnel_health = f"CHECK｜有{eligible_count}檔安全候選但最終0檔，請檢查分流/快取一致性"
    out["H41推薦漏斗健康"] = funnel_health
    for idx, gate in gates.items():
        if idx not in selected and gate.get("eligible") and not _s(out.at[idx, "H34阻擋原因"]):
            out.at[idx, "H34阻擋原因"] = (
                f"符合H34安全條件，但受{market.get('regime','市場')}目標{target}檔上限，依安全分排序未入選"
            )
    for rank, idx in enumerate(selected, start=1):
        out.at[idx, "H34每日精選"] = "是"
        out.at[idx, "H34每日精選排名"] = rank
        if not _s(out.at[idx, "H34精選等級"]):
            bucket = _s(out.at[idx, "正式推薦分區"])
            out.at[idx, "H34精選等級"] = "標準正式推薦" if bucket == "正式下週主推薦" else "標準A-準主推薦"
        m = gates[idx]["m"]
        out.at[idx, "H34精選理由"] = (
            f"安全分{gates[idx]['score']:.1f}｜Entry {m['entry']:.0f}｜Risk {m['risk']:.0f}｜"
            f"RR {m['rr']:.2f}｜可操作{m['op']:.0f}｜最近可執行距離{m['execution_distance']:.1f}%｜"
            f"突破觸發距離{m['trigger_distance']:.1f}%｜追價{m['chase']:.0f}｜隔日機率{m['prob']:.1f}%({m.get('prob_source','')})"
        )
        out.at[idx, "H34操作原則"] = "只做觸發後守價；禁止開盤追價；停損失守立即取消；未觸發不交易。"
        out.at[idx, "H34阻擋原因"] = ""

    return out


_INSTALLED = False
_ORIGINAL = None


def install_daily_safe_selection_guard() -> bool:
    """Wrap the formal engine once, without editing its large source file."""
    global _INSTALLED, _ORIGINAL
    if _INSTALLED:
        return True
    try:
        import godpick_formal_recommendation_engine as engine
    except Exception:
        return False
    original = getattr(engine, "apply_formal_recommendation_engine", None)
    if not callable(original):
        return False
    if getattr(original, "_h34_daily_safe_selection", False):
        _INSTALLED = True
        return True
    _ORIGINAL = original

    def wrapped(df):
        base = original(df)
        # H38：正式引擎執行時 SuperAI/V188 尚未產生校準機率與交易品質分，
        # 此時禁止提早套 H34。Page07 / V188 cache repair 會在 SuperAI 完成後
        # 明確執行 H34，確保 H34機率來源與最終決策資料完全一致。
        post_v188_ready = any(
            col in base.columns
            for col in ["V188股神作戰優先分", "SuperAI Trade分", "SuperAI校準後隔日上漲機率%"]
        )
        return apply_daily_safe_selection(base) if post_v188_ready else base

    wrapped.__name__ = getattr(original, "__name__", "apply_formal_recommendation_engine")
    wrapped.__doc__ = getattr(original, "__doc__", None)
    wrapped._h34_daily_safe_selection = True
    wrapped._h34_original = original
    engine.apply_formal_recommendation_engine = wrapped
    _INSTALLED = True
    return True
