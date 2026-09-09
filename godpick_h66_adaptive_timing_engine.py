# -*- coding: utf-8 -*-
"""V191-H66 Adaptive Alpha Ranking × T+1 Timing Truth.

H66 is a *research ranking / timing* overlay.  It never weakens H64 Formal,
H56 pre-open authority, Entry/hold-price or RR execution gates.

Why H66 exists
---------------
H65 solved the "Formal=0 must not mean no answer" problem by adding a broad
multi-factor observation radar.  H66 solves the next problem: the strongest
business/structural candidate is not always the best *next-session* candidate.
It therefore separates:

* Quality / swing value (fundamental + mainstream + holder trend)
* T+1 timing (close quality + institutional acceleration + ignition + entry)
* Contradictory evidence (high opportunity score while price/flow says sell)
* Good-news/no-price-response risk (sell-the-news / distribution)
* Market breadth regime (index strength without broad participation)
* Adaptive learning from matured H66 T+1 truth (bounded, sample-aware)

The output remains research-only unless the existing H64/H56/Entry/RR chain
independently authorises a trade.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h66_adaptive_alpha_t1_timing_truth_20260909"

# These are T+1 timing weights, not long-term valuation weights.  Fundamentals
# stay in H66 quality/swing score and only have a small direct effect on T+1.
BASE_TIMING_WEIGHTS = {
    "收盤品質": 24.0,
    "法人加速度": 20.0,
    "主流點火": 18.0,
    "技術買點": 16.0,
    "量能流動性": 10.0,
    "結構品質": 8.0,
    "短線動能": 4.0,
}

H66_COLUMNS = [
    "H66結構品質分", "H66T5波段品質分", "H66收盤品質分", "H66法人加速度分",
    "H66主流點火分", "H66技術買點分", "H66量能流動性分", "H66短線動能分",
    "H66矛盾訊號扣分", "H66利多不漲扣分", "H66市場廣度調整", "H66歷史學習調整",
    "H66學習樣本數", "H66學習狀態", "H66T1時機原始分", "H66T1自適應排序分",
    "H66T1全市場百分位%", "H66T1全市場順位", "H66T1層級", "H66T1觀察推薦",
    "H66適合週期", "H66T1升級缺口", "H66T1理由", "H66權威邊界", "H66版本",
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


def _f(v: Any, default: float | None = None) -> float | None:
    try:
        t = str(v).strip().replace(",", "").replace("％", "%")
        if t.endswith("%"):
            t = t[:-1].strip()
        if not t or t.lower() in _BLANK:
            return default
        x = float(t)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _clip(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(x)))


def _row_num(row: pd.Series | dict[str, Any], names: Iterable[str], default: float | None = None) -> float | None:
    idx = row.index if isinstance(row, pd.Series) else row.keys()
    for c in names:
        if c in idx:
            x = _f(row.get(c), None)
            if x is not None:
                return x
    return default


def _row_txt(row: pd.Series | dict[str, Any], names: Iterable[str], default: str = "") -> str:
    idx = row.index if isinstance(row, pd.Series) else row.keys()
    for c in names:
        if c in idx:
            t = _s(row.get(c))
            if t:
                return t
    return default


def _has_any(row: pd.Series | dict[str, Any], names: Iterable[str]) -> bool:
    idx = row.index if isinstance(row, pd.Series) else row.keys()
    return any(c in idx and bool(_s(row.get(c))) for c in names)


def _weighted(items: list[tuple[float | None, float]], default: float = 50.0) -> float:
    valid = [(float(v), float(w)) for v, w in items if v is not None and math.isfinite(float(v)) and w > 0]
    if not valid:
        return float(default)
    den = sum(w for _, w in valid)
    return _clip(sum(v * w for v, w in valid) / den)


def _first_col(frame: pd.DataFrame, names: Iterable[str]) -> str | None:
    for c in names:
        if c in frame.columns:
            return c
    return None


def _numeric_percentile(frame: pd.DataFrame, names: Iterable[str], invert: bool = False) -> pd.Series:
    col = _first_col(frame, names)
    if not col:
        return pd.Series([float("nan")] * len(frame), index=frame.index, dtype="float64")
    raw = frame[col].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False)
    s = pd.to_numeric(raw, errors="coerce")
    out = s.rank(method="average", pct=True).mul(100.0)
    return 100.0 - out if invert else out


def _pct_value(series: pd.Series, idx: Any) -> float | None:
    try:
        v = series.get(idx, float("nan"))
        return float(v) if pd.notna(v) else None
    except Exception:
        return None


def _close_position_score(v: float | None) -> float | None:
    if v is None:
        return None
    # close position: 0 = near low, 100 = near high
    if v >= 88:
        return 96.0
    if v >= 75:
        return 88.0
    if v >= 60:
        return 78.0
    if v >= 45:
        return 62.0
    if v >= 30:
        return 45.0
    if v >= 15:
        return 28.0
    return 12.0


def _upper_shadow_score(v: float | None) -> float | None:
    if v is None:
        return None
    if v <= 8:
        return 92.0
    if v <= 18:
        return 80.0
    if v <= 30:
        return 62.0
    if v <= 45:
        return 42.0
    return 22.0


def _daily_return_timing_score(v: float | None) -> float | None:
    if v is None:
        return None
    # For T+1, mild strength is better than collapse or extreme chase.
    if 1.0 <= v <= 5.5:
        return 90.0
    if 0.0 <= v < 1.0:
        return 72.0
    if 5.5 < v <= 7.5:
        return 72.0
    if -1.5 <= v < 0:
        return 55.0
    if -3.0 <= v < -1.5:
        return 38.0
    if v < -3.0:
        return 20.0
    if 7.5 < v <= 9.5:
        return 48.0
    return 30.0


def _stage_bonus(text: str, mapping: dict[str, float], default: float = 50.0) -> float:
    t = _s(text)
    for prefix, val in mapping.items():
        if t.startswith(prefix):
            return float(val)
    return float(default)


def _corr_weight_adjustments(rows: list[dict[str, Any]]) -> tuple[dict[str, float], int, str]:
    """Learn bounded component multipliers from matured H66 truth.

    This intentionally requires >=30 usable H66 snapshots.  Before that, H66
    uses fixed base timing weights plus older H57/H60 cohort evidence.  It never
    learns from the current day's unknown future return.
    """
    factors = {
        "收盤品質": "H66收盤品質分",
        "法人加速度": "H66法人加速度分",
        "主流點火": "H66主流點火分",
        "技術買點": "H66技術買點分",
        "量能流動性": "H66量能流動性分",
        "結構品質": "H66結構品質分",
        "短線動能": "H66短線動能分",
    }
    usable = []
    for r in rows:
        if not isinstance(r, dict) or not bool(r.get("T1成熟")):
            continue
        alpha = _f(r.get("Selection Alpha%"), None)
        if alpha is None:
            continue
        snap = {k: _f(r.get(col), None) for k, col in factors.items()}
        if sum(v is not None for v in snap.values()) >= 4:
            usable.append((snap, alpha))
    n = len(usable)
    if n < 30:
        return {k: 1.0 for k in factors}, n, "EARLY｜H66成熟樣本不足30，固定權重"

    df = pd.DataFrame([{**snap, "alpha": alpha} for snap, alpha in usable])
    mult: dict[str, float] = {}
    for k in factors:
        sub = df[[k, "alpha"]].dropna()
        if len(sub) < 24 or sub[k].nunique() < 4:
            mult[k] = 1.0
            continue
        # Spearman rank relation is more robust to scale differences/outliers.
        corr = sub[k].rank(pct=True).corr(sub["alpha"].rank(pct=True))
        corr = 0.0 if corr is None or not math.isfinite(float(corr)) else float(corr)
        # Conservative bounded adaptation: +/-15% only.
        mult[k] = max(0.85, min(1.15, 1.0 + 0.30 * corr))
    return mult, n, "SUPPORTED" if n < 100 else "MATURE"


def _legacy_cohort_profile(truth_rows: list[dict[str, Any]]) -> dict[str, float]:
    """Precompute small bootstrap adjustments once per H66 run.

    Avoids scanning the whole T+1 truth store for every stock in a 1,700-name
    universe.  Each cohort must have >=12 mature samples and contributes at
    most +/-1.5 points.
    """
    buckets: dict[str, list[float]] = defaultdict(list)
    for r in truth_rows:
        if not isinstance(r, dict) or not bool(r.get("T1成熟")):
            continue
        a = _f(r.get("Selection Alpha%"), None)
        if a is None:
            continue
        if _s(r.get("H60主升階段")).startswith("MR1"):
            buckets["MR1"].append(a)
        if _s(r.get("H60三因子層級")).startswith("T3"):
            buckets["T3"].append(a)
        if _s(r.get("H60雪球股層級")).startswith("SB1"):
            buckets["SB1"].append(a)
        if _s(r.get("H57前兆階段")).startswith("PI3"):
            buckets["PI3"].append(a)
    profile: dict[str, float] = {}
    for key, vals in buckets.items():
        if len(vals) >= 12:
            avg = sum(vals) / len(vals)
            profile[key] = max(-1.5, min(1.5, avg * 0.6))
    return profile


def _legacy_cohort_adjustment(row: pd.Series, profile: dict[str, float]) -> float:
    adj = 0.0
    if _row_txt(row, ["H60主升階段"]).startswith("MR1"):
        adj += profile.get("MR1", 0.0)
    if _row_txt(row, ["H60三因子層級"]).startswith("T3"):
        adj += profile.get("T3", 0.0)
    if _row_txt(row, ["H60雪球股層級"]).startswith("SB1"):
        adj += profile.get("SB1", 0.0)
    if _row_txt(row, ["H57前兆階段"]).startswith("PI3"):
        adj += profile.get("PI3", 0.0)
    return max(-3.0, min(3.0, adj))


def _load_learning_rows_best_effort(limit: int = 600) -> list[dict[str, Any]]:
    try:
        from godpick_t1_trade_truth import load_t1_truth_rows
        rows = load_t1_truth_rows(limit=limit)
        return [dict(r) for r in rows if isinstance(r, dict)]
    except Exception:
        return []


def apply_h66_adaptive_timing(frame: pd.DataFrame, truth_rows: Any | None = None) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()

    work = frame.copy().reset_index(drop=True)
    # H66 builds on H65 but does not replace it.
    try:
        from godpick_h65_multifactor_observation_engine import VERSION as H65_VERSION, apply_h65_multifactor_observation
        h65v = work.get("H65版本", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
        if not h65v.eq(H65_VERSION).all():
            work = apply_h65_multifactor_observation(work)
    except Exception:
        pass

    if truth_rows is None:
        learning_rows = _load_learning_rows_best_effort()
    elif isinstance(truth_rows, pd.DataFrame):
        learning_rows = truth_rows.to_dict(orient="records")
    elif isinstance(truth_rows, list):
        learning_rows = [dict(x) for x in truth_rows if isinstance(x, dict)]
    elif isinstance(truth_rows, dict):
        raw = truth_rows.get("records") or truth_rows.get("rows") or truth_rows.get("data") or []
        learning_rows = [dict(x) for x in raw if isinstance(x, dict)]
    else:
        learning_rows = []

    factor_mult, learning_n, learning_state = _corr_weight_adjustments(learning_rows)
    adaptive_weights = {
        k: BASE_TIMING_WEIGHTS[k] * factor_mult.get(k, 1.0) for k in BASE_TIMING_WEIGHTS
    }
    # Renormalise to exactly 100 for transparent interpretation.
    den = sum(adaptive_weights.values()) or 100.0
    adaptive_weights = {k: v / den * 100.0 for k, v in adaptive_weights.items()}
    legacy_profile = _legacy_cohort_profile(learning_rows)

    pct = {
        "foreign1": _numeric_percentile(work, ["外資近1日買賣超"]),
        "foreign3": _numeric_percentile(work, ["外資近3日買賣超"]),
        "foreign5": _numeric_percentile(work, ["外資近5日買賣超"]),
        "trust1": _numeric_percentile(work, ["投信近1日買賣超"]),
        "trust3": _numeric_percentile(work, ["投信近3日買賣超"]),
        "trust5": _numeric_percentile(work, ["投信近5日買賣超"]),
        "inst1": _numeric_percentile(work, ["三大法人近1日合計"]),
        "inst3": _numeric_percentile(work, ["三大法人近3日合計"]),
        "inst5": _numeric_percentile(work, ["三大法人近5日合計"]),
        "ret1": _numeric_percentile(work, ["今日漲幅%", "當日漲跌幅%", "今日漲跌幅%", "漲跌幅%"]),
        "ret5": _numeric_percentile(work, ["近5日漲幅%"]),
        "amount": _numeric_percentile(work, ["流動性參考成交額百萬", "成交額百萬", "平均成交額百萬"]),
        "rev": _numeric_percentile(work, ["月營收YoY%", "單月營收YoY%", "營收年增率%"]),
        "eps": _numeric_percentile(work, ["EPS成長分數", "EPS成長率%", "估算EPS"]),
    }

    interim: list[dict[str, Any]] = []
    for idx, row in work.iterrows():
        day_ret = _row_num(row, ["今日漲幅%", "當日漲跌幅%", "今日漲跌幅%", "漲跌幅%"], None)
        close_pos = _row_num(row, ["當日收盤位置%"], None)
        upper_shadow = _row_num(row, ["上影線比例%"], None)
        exhaust = _row_num(row, ["H54耗竭風險分", "隔日耗竭風險分"], 50.0) or 50.0
        chase = _row_num(row, ["追價風險分", "追價風險分數"], 50.0) or 50.0
        risk_safe = _row_num(row, ["Risk風控安全分", "AI Risk風控分"], 50.0) or 50.0

        close_quality = _weighted([
            (_close_position_score(close_pos), 0.46),
            (_upper_shadow_score(upper_shadow), 0.20),
            (_daily_return_timing_score(day_ret), 0.20),
            (_clip(100.0 - max(exhaust, chase)), 0.08),
            (_clip(risk_safe), 0.06),
        ])

        # Institutional acceleration: current 1/3 day evidence matters more than
        # a large 5-day legacy total.  This catches sudden distribution quickly.
        inst_accel = _weighted([
            (_pct_value(pct["foreign1"], idx), 0.22),
            (_pct_value(pct["trust1"], idx), 0.12),
            (_pct_value(pct["inst1"], idx), 0.20),
            (_pct_value(pct["foreign3"], idx), 0.10),
            (_pct_value(pct["trust3"], idx), 0.07),
            (_pct_value(pct["inst3"], idx), 0.09),
            (_pct_value(pct["inst5"], idx), 0.06),
            (_row_num(row, ["法人籌碼官方分數", "法人籌碼分數"], None), 0.08),
            (_clip(45.0 + (_row_num(row, ["法人連買天數"], 0.0) or 0.0) * 8.0), 0.04),
            (_clip(50.0 + (_row_num(row, ["法人買超占量比%"], 0.0) or 0.0) * 2.5) if _has_any(row, ["法人買超占量比%"] ) else None, 0.02),
        ])

        mainstream_ignition = _weighted([
            (_row_num(row, ["H60主升段分"], None), 0.20),
            (_row_num(row, ["H60三因子共振分"], None), 0.12),
            (_row_num(row, ["H57飆股發動前兆分"], None), 0.18),
            (_row_num(row, ["H57資金加速度分"], None), 0.14),
            (_row_num(row, ["H57主流形成前兆分"], None), 0.12),
            (_row_num(row, ["H62新領漲分"], None), 0.10),
            (_row_num(row, ["H65主流分"], None), 0.07),
            (_row_num(row, ["H65趨勢分"], None), 0.07),
        ])
        # Stage evidence is categorical and deliberately modest, so it cannot
        # override a bad close / heavy selling by itself.
        mainstream_ignition = _clip(mainstream_ignition * 0.88 + _weighted([
            (_stage_bonus(_row_txt(row, ["H60主升階段"]), {"MR1": 94, "MR2": 80, "MR3": 69, "MR0": 45}), 0.45),
            (_stage_bonus(_row_txt(row, ["H57前兆階段"]), {"PI3": 92, "PI2": 80, "PI1": 68, "IG1": 88, "PI0": 40}), 0.35),
            (_stage_bonus(_row_txt(row, ["H60三因子層級"]), {"T3": 94, "T2": 78, "T1": 64, "T0": 38}), 0.20),
        ]) * 0.12)

        technical = _weighted([
            (_row_num(row, ["Entry進場買點分", "進場可執行分", "實戰買點分數"], None), 0.34),
            (_row_num(row, ["H51Pivot起漲分"], None), 0.18),
            (_row_num(row, ["H51量價確認分"], None), 0.16),
            (_row_num(row, ["H57相對強度轉折分"], None), 0.12),
            (_row_num(row, ["H65技術分"], None), 0.20),
        ])

        liquidity = _weighted([
            (_row_num(row, ["H65量能流動性分", "H51流動性分"], None), 0.56),
            (_pct_value(pct["amount"], idx), 0.28),
            (_row_num(row, ["H51量價確認分"], None), 0.16),
        ])

        structure_quality = _weighted([
            (_row_num(row, ["H65多因子觀察分"], None), 0.28),
            (_row_num(row, ["H65主流分"], None), 0.16),
            (_row_num(row, ["H65趨勢分"], None), 0.14),
            (_row_num(row, ["H65大戶分", "H60大戶鎖碼真相分"], None), 0.13),
            (_row_num(row, ["H65營收分", "營收成長官方分數", "營收成長分數"], None), 0.10),
            (_row_num(row, ["H65獲利EPS分", "官方基本面成長分數", "基本面成長分數"], None), 0.10),
            (_row_num(row, ["H65估值分"], None), 0.04),
            (_row_num(row, ["Risk風控安全分"], None), 0.05),
        ])

        # T+5/swing quality gives fundamentals and holder trend more influence.
        swing_quality = _weighted([
            (structure_quality, 0.32),
            (_row_num(row, ["H65營收分", "營收成長官方分數", "營收成長分數"], None), 0.16),
            (_row_num(row, ["H65獲利EPS分", "官方基本面成長分數", "基本面成長分數"], None), 0.16),
            (_row_num(row, ["H65大戶分", "H60大戶鎖碼真相分"], None), 0.14),
            (_row_num(row, ["H65主流分", "H51族群主線分"], None), 0.12),
            (_row_num(row, ["H65趨勢分", "H64真強勢分"], None), 0.10),
        ])

        short_momentum = _weighted([
            (_pct_value(pct["ret1"], idx), 0.35),
            (_pct_value(pct["ret5"], idx), 0.30),
            (_row_num(row, ["H57壓縮轉擴張分"], None), 0.15),
            (_row_num(row, ["H57提前視窗分"], None), 0.20),
        ])

        # Contradiction governance: high opportunity must not overrule current
        # distribution / poor close / absent ignition.
        contradiction = 0.0
        h62 = _row_num(row, ["H62增量機會分"], 0.0) or 0.0
        inst1 = _row_num(row, ["三大法人近1日合計"], None)
        foreign1 = _row_num(row, ["外資近1日買賣超"], None)
        h60_stage = _row_txt(row, ["H60主升階段"])
        h57_phase = _row_txt(row, ["H57前兆階段"])
        h60_tier = _row_txt(row, ["H60三因子層級"])
        holder_delta = _row_num(row, ["H60千張大戶週變化pp"], None)
        entry = _row_num(row, ["Entry進場買點分"], None)

        if h62 >= 62 and close_quality < 38:
            contradiction += 7.0
        if h62 >= 62 and inst1 is not None and inst1 < 0:
            contradiction += 5.0
        if h62 >= 62 and foreign1 is not None and foreign1 < 0:
            contradiction += 3.0
        if h62 >= 62 and not h60_stage.startswith(("MR1", "MR2", "MR3")) and not h57_phase.startswith(("PI2", "PI3", "IG1")):
            contradiction += 4.0
        if day_ret is not None and day_ret <= -3.0 and close_pos is not None and close_pos < 30:
            contradiction += 6.0
        if inst1 is not None and inst1 < 0 and close_pos is not None and close_pos < 25:
            contradiction += 4.0
        if holder_delta is not None and holder_delta < -0.25 and inst1 is not None and inst1 < 0:
            contradiction += 4.0
        if entry is not None and entry < 55 and close_quality < 48:
            contradiction += 3.0
        if h60_tier.startswith("T0") and h57_phase.startswith("PI0") and h62 >= 60:
            contradiction += 4.0
        contradiction = min(28.0, contradiction)

        # Sell-the-news / good-news-no-price-response.  Strong reported growth is
        # valuable for swing quality, but if the market rejects it today, T+1 is
        # penalised rather than boosted.
        news_reject = 0.0
        rev_yoy = _row_num(row, ["月營收YoY%", "單月營收YoY%", "營收年增率%"], None)
        eps_growth = _row_num(row, ["EPS成長分數", "EPS成長率%"], None)
        strong_fund = ((rev_yoy is not None and rev_yoy >= 20) or (eps_growth is not None and eps_growth >= 75) or ((_pct_value(pct["rev"], idx) or 0) >= 80) or ((_pct_value(pct["eps"], idx) or 0) >= 80))
        if strong_fund and day_ret is not None and day_ret < -1.5:
            news_reject += 4.0
        if strong_fund and close_pos is not None and close_pos < 35:
            news_reject += 3.0
        if strong_fund and inst1 is not None and inst1 < 0:
            news_reject += 3.0
        if strong_fund and upper_shadow is not None and upper_shadow > 35:
            news_reject += 2.0
        news_reject = min(12.0, news_reject)

        # Market breadth.  Prefer actual breadth fields; if absent, do not invent
        # breadth from index direction.  A narrow index rally is explicitly worse.
        adv = _row_num(row, ["市場上漲家數", "上漲家數", "大盤上漲家數"], None)
        dec = _row_num(row, ["市場下跌家數", "下跌家數", "大盤下跌家數"], None)
        breadth = _row_num(row, ["市場廣度%", "大盤廣度%", "上漲家數占比%"], None)
        if breadth is None and adv is not None and dec is not None and (adv + dec) > 0:
            breadth = adv / (adv + dec) * 100.0
        market_score = _row_num(row, ["市場環境分數", "H42市場共識分", "SuperAI市場情境分"], None)
        breadth_adj = 0.0
        if breadth is not None:
            if breadth >= 62:
                breadth_adj = 3.0
            elif breadth >= 53:
                breadth_adj = 1.0
            elif breadth < 42 and (market_score or 0) >= 60:
                breadth_adj = -5.0  # index strong, breadth weak
            elif breadth < 42:
                breadth_adj = -3.0
            elif breadth < 48:
                breadth_adj = -1.5

        components = {
            "收盤品質": close_quality,
            "法人加速度": inst_accel,
            "主流點火": mainstream_ignition,
            "技術買點": technical,
            "量能流動性": liquidity,
            "結構品質": structure_quality,
            "短線動能": short_momentum,
        }
        raw_t1 = sum(components[k] * adaptive_weights[k] for k in components) / 100.0
        legacy_adj = _legacy_cohort_adjustment(row, legacy_profile)
        # If H66 itself has matured, correlation-adjusted weights are already the
        # main learning mechanism.  Legacy cohort boost is kept small.
        history_adj = legacy_adj
        final = _clip(raw_t1 - contradiction - news_reject + breadth_adj + history_adj)

        risk_text = "｜".join([
            _row_txt(row, ["正式推薦排除原因"]), _row_txt(row, ["高分禁買旗標"]),
            _row_txt(row, ["K線資料新鮮度"]), _row_txt(row, ["股神資料總新鮮度"]),
            _row_txt(row, ["V188交易許可"]), _row_txt(row, ["H57交易保護狀態"]),
        ]).upper()
        stale = any(k in risk_text for k in ["過期", "落後", "待更新", "STALE"])
        hard_block = any(k in risk_text for k in ["禁買", "BLOCK", "正式排除"])
        if stale:
            final = min(final, 48.0)
        if hard_block:
            final = min(final, 62.0)  # visible research, never a top timing tier

        interim.append({
            "structure": structure_quality, "swing": swing_quality, "close": close_quality,
            "inst": inst_accel, "ignite": mainstream_ignition, "technical": technical,
            "liquidity": liquidity, "momentum": short_momentum, "contradiction": contradiction,
            "news_reject": news_reject, "breadth_adj": breadth_adj, "history_adj": history_adj,
            "raw": raw_t1, "final": final, "stale": stale, "hard_block": hard_block,
            "close_pos": close_pos, "day_ret": day_ret, "inst1": inst1, "learning_n": learning_n,
            "learning_state": learning_state,
        })

    final_series = pd.Series([m["final"] for m in interim], index=work.index, dtype="float64")
    percentile = final_series.rank(method="average", pct=True).mul(100.0)
    # Rank 1 = best. Stable method preserves source ordering on exact ties.
    ranks = final_series.rank(method="first", ascending=False).astype(int)

    rows: list[dict[str, Any]] = []
    for pos, idx in enumerate(work.index):
        row = work.loc[idx]
        m = interim[pos]
        pctv = float(percentile.loc[idx])
        rankv = int(ranks.loc[idx])
        score = float(m["final"])
        h64_eff = _row_txt(row, ["H64有效權威", "H63有效權威", "H62有效權威"])

        if score >= 72 and pctv >= 85 and m["close"] >= 58 and m["inst"] >= 55 and m["contradiction"] < 10 and not m["stale"] and not m["hard_block"]:
            tier = "A1｜T+1優先觀察"
            rec = "是｜T+1重點觀察；仍須原Formal/Entry/RR權威"
        elif score >= 64 and pctv >= 70 and m["contradiction"] < 15 and not m["stale"]:
            tier = "A2｜T+1次優先"
            rec = "是｜T+1次優先；等待收盤/法人/點火再確認"
        elif score >= 56 and pctv >= 50 and not m["stale"]:
            tier = "B1｜T+1等待確認"
            rec = "是｜觀察但不追價；等矛盾訊號解除"
        else:
            tier = "R0｜T+1僅研究"
            rec = "否｜相對研究，不構成T+1觀察推薦"

        if m["hard_block"] and tier.startswith(("A1", "A2")):
            tier = "B1｜T+1等待確認"
            rec = "是｜上游BLOCK存在，只觀察不升級"
        if m["stale"]:
            tier = "R0｜資料待更新"
            rec = "否｜資料待更新"

        # Separate holding-period recommendation.
        if tier.startswith("A1") and m["swing"] >= 62:
            horizon = "T+1優先／可續看T+3~5"
        elif tier.startswith(("A1", "A2")):
            horizon = "T+1短線觀察"
        elif m["swing"] >= 68 and score < 62:
            horizon = "T+3~5／波段較佳，T+1時機不足"
        elif m["swing"] >= 60:
            horizon = "波段研究／等待短線觸發"
        else:
            horizon = "等待"

        gaps: list[str] = []
        if m["close"] < 58:
            gaps.append(f"收盤品質{m['close']:.0f}偏弱")
        if m["inst"] < 55:
            gaps.append(f"法人加速度{m['inst']:.0f}不足")
        if m["ignite"] < 58:
            gaps.append(f"主流點火{m['ignite']:.0f}未完成")
        if m["technical"] < 58:
            gaps.append(f"技術買點{m['technical']:.0f}不足")
        if m["contradiction"] >= 6:
            gaps.append(f"矛盾扣分{m['contradiction']:.1f}")
        if m["news_reject"] >= 4:
            gaps.append(f"利多不漲扣分{m['news_reject']:.1f}")
        if h64_eff != "EFFECTIVE-FORMAL" and len(gaps) < 4:
            gaps.append("H64仍非有效Formal")
        if not gaps:
            gaps.append("等待H56盤前＋Entry守價＋RR確認")

        reasons = [
            f"T1={score:.1f}/P{pctv:.1f}/Rank{rankv}",
            f"收盤{m['close']:.0f}", f"法人{m['inst']:.0f}", f"點火{m['ignite']:.0f}",
            f"技術{m['technical']:.0f}", f"矛盾-{m['contradiction']:.1f}",
        ]
        if m["news_reject"] > 0:
            reasons.append(f"利多不漲-{m['news_reject']:.1f}")
        if m["breadth_adj"]:
            reasons.append(f"廣度{m['breadth_adj']:+.1f}")
        if m["history_adj"]:
            reasons.append(f"學習{m['history_adj']:+.1f}")

        rows.append({
            "H66結構品質分": round(m["structure"], 2),
            "H66T5波段品質分": round(m["swing"], 2),
            "H66收盤品質分": round(m["close"], 2),
            "H66法人加速度分": round(m["inst"], 2),
            "H66主流點火分": round(m["ignite"], 2),
            "H66技術買點分": round(m["technical"], 2),
            "H66量能流動性分": round(m["liquidity"], 2),
            "H66短線動能分": round(m["momentum"], 2),
            "H66矛盾訊號扣分": round(m["contradiction"], 2),
            "H66利多不漲扣分": round(m["news_reject"], 2),
            "H66市場廣度調整": round(m["breadth_adj"], 2),
            "H66歷史學習調整": round(m["history_adj"], 2),
            "H66學習樣本數": int(m["learning_n"]),
            "H66學習狀態": m["learning_state"],
            "H66T1時機原始分": round(m["raw"], 2),
            "H66T1自適應排序分": round(score, 2),
            "H66T1全市場百分位%": round(pctv, 2),
            "H66T1全市場順位": rankv,
            "H66T1層級": tier,
            "H66T1觀察推薦": rec,
            "H66適合週期": horizon,
            "H66T1升級缺口": "；".join(gaps[:4]),
            "H66T1理由": "；".join(reasons) + f"；H64={h64_eff or '未授權'}。",
            "H66權威邊界": "H66只重排研究/觀察與T+1時機；正式推薦仍須H64有效Formal＋H56盤前＋Entry/守價＋RR。",
            "H66版本": VERSION,
        })

    addon = pd.DataFrame(rows, index=work.index)
    for c in H66_COLUMNS:
        work[c] = addon[c]
    return work


def _tier_priority(t: str) -> int:
    s = _s(t)
    if s.startswith("A1"):
        return 40
    if s.startswith("A2"):
        return 30
    if s.startswith("B1"):
        return 20
    return 10


def build_h66_t1_timing_table(frame: pd.DataFrame, max_rows: int = 20, max_per_sector: int = 3, truth_rows: Any | None = None) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame({
            "H66T1層級": ["R0｜目前沒有可排序候選"],
            "H66T1觀察推薦": ["否"],
            "H66T1升級缺口": ["請先完成全市場掃描與資料更新"],
        })
    v = frame.get("H66版本", pd.Series([""] * len(frame), index=frame.index)).fillna("").astype(str)
    work = frame if v.eq(VERSION).all() else apply_h66_adaptive_timing(frame, truth_rows=truth_rows)
    out = work.copy()
    out["_H66tier"] = out.get("H66T1層級", pd.Series([""] * len(out), index=out.index)).astype(str).map(_tier_priority)
    for c in ["H66T1自適應排序分", "H66T1全市場百分位%", "H66收盤品質分", "H66法人加速度分"]:
        if c not in out.columns:
            out[c] = 0.0
    out.sort_values(["_H66tier", "H66T1自適應排序分", "H66T1全市場百分位%", "H66收盤品質分", "H66法人加速度分"], ascending=False, kind="mergesort", inplace=True)

    target = max(1, int(max_rows or 20))
    cap = max(1, int(max_per_sector or 3))
    chosen: list[pd.Series] = []
    seen: set[str] = set()
    counts: dict[str, int] = {}
    for _, r in out.iterrows():
        code = _row_txt(r, ["股票代號", "代號"])
        if not code or code in seen:
            continue
        sector = _row_txt(r, ["類別", "族群名稱", "產業"], "未分類")
        if counts.get(sector, 0) >= cap:
            continue
        chosen.append(r); seen.add(code); counts[sector] = counts.get(sector, 0) + 1
        if len(chosen) >= target:
            break
    if len(chosen) < target:
        for _, r in out.iterrows():
            code = _row_txt(r, ["股票代號", "代號"])
            if not code or code in seen:
                continue
            chosen.append(r); seen.add(code)
            if len(chosen) >= target:
                break
    res = pd.DataFrame(chosen).reset_index(drop=True) if chosen else out.head(target).copy().reset_index(drop=True)
    res["H66T1觀察順位"] = range(1, len(res) + 1)
    res.drop(columns=["_H66tier"], errors="ignore", inplace=True)
    front = [c for c in [
        "H66T1觀察順位", "股票代號", "股票名稱", "類別", "H66T1層級", "H66T1觀察推薦",
        "H66T1自適應排序分", "H66T1全市場百分位%", "H66T1全市場順位", "H66適合週期",
        "H66收盤品質分", "H66法人加速度分", "H66主流點火分", "H66技術買點分",
        "H66量能流動性分", "H66短線動能分", "H66結構品質分", "H66T5波段品質分",
        "H66矛盾訊號扣分", "H66利多不漲扣分", "H66市場廣度調整", "H66歷史學習調整",
        "H66學習樣本數", "H66學習狀態", "H66T1升級缺口", "H66T1理由",
        "H65觀察層級", "H65多因子觀察分", "H64研究層級", "H64有效權威",
        "當日收盤位置%", "上影線比例%", "外資近1日買賣超", "投信近1日買賣超", "三大法人近1日合計",
        "外資近3日買賣超", "投信近3日買賣超", "三大法人近3日合計", "法人連買天數",
        "H60主升階段", "H60三因子層級", "H57前兆階段", "H62增量機會分", "H66權威邊界",
    ] if c in res.columns]
    rest = [c for c in res.columns if c not in front]
    return res[front + rest]


def build_h66_learning_governance_table(truth_rows: Any) -> pd.DataFrame:
    if isinstance(truth_rows, pd.DataFrame):
        rows = truth_rows.to_dict(orient="records")
    elif isinstance(truth_rows, list):
        rows = [dict(x) for x in truth_rows if isinstance(x, dict)]
    elif isinstance(truth_rows, dict):
        raw = truth_rows.get("records") or truth_rows.get("rows") or []
        rows = [dict(x) for x in raw if isinstance(x, dict)]
    else:
        rows = []
    mult, n, state = _corr_weight_adjustments(rows)
    out = []
    for k, base in BASE_TIMING_WEIGHTS.items():
        out.append({
            "H66學習項目": k,
            "T+1基礎權重%": base,
            "成熟樣本數": n,
            "學習狀態": state,
            "自適應倍率": round(mult.get(k, 1.0), 4),
            "治理限制": "成熟樣本<30不調權；成熟後每因子最多±15%，Formal權威不受H66修改",
        })
    return pd.DataFrame(out)


__all__ = [
    "VERSION", "BASE_TIMING_WEIGHTS", "H66_COLUMNS", "apply_h66_adaptive_timing",
    "build_h66_t1_timing_table", "build_h66_learning_governance_table",
]
