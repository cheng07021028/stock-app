# -*- coding: utf-8 -*-
"""V191-H60 Main-rise x Holder-lock x Snowball compound research engine.

H60 is a research/selection layer only.  It may identify main-rise candidates,
actual/proxy large-holder locking, and compounding/snowball quality, but it
never grants Formal/V188/H51/H56 trading permission and never overrides Entry,
Stop or execution RR.
"""
from __future__ import annotations

from typing import Any
import math
import pandas as pd

VERSION = "v191_h60_mainrise_holder_snowball_truth_20260904"

H60_COLUMNS = [
    "H60主升段分", "H60主升階段",
    "H60鎖碼來源", "H60大戶資料日期", "H60千張大戶持股比%", "H60千張大戶週變化pp",
    "H60大戶鎖碼真相分", "H60大戶鎖碼層級",
    "H60雪球複利分", "H60雪球股層級",
    "H60三因子共振分", "H60三因子層級", "H60研究結論", "H60版本",
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


def _first_num(row: pd.Series, names: list[str], default: float = 0.0) -> float:
    for c in names:
        if c in row.index:
            x = _f(row.get(c))
            if x is not None:
                return float(x)
    return float(default)


def _avg_available(row: pd.Series, names: list[str], default: float = 50.0) -> float:
    vals = []
    for c in names:
        if c in row.index:
            x = _f(row.get(c))
            if x is not None and 0 <= x <= 100:
                vals.append(float(x))
    return sum(vals) / len(vals) if vals else float(default)


def _mainrise_score(row: pd.Series) -> tuple[float, str]:
    legacy = _first_num(row, ["主流主升優先分", "主升起漲分", "主升分數"], 0.0)
    ignition = _first_num(row, ["H51發動潛力分"], 50.0)
    resonance = _first_num(row, ["H53族群共振分"], 50.0)
    continuation = _first_num(row, ["H55主線延續路徑分"], 50.0)
    pre_main = _first_num(row, ["H57主流形成前兆分", "H57飆股發動前兆分"], 50.0)
    exhaust = _first_num(row, ["H54耗竭風險分"], 50.0)
    ret5 = _first_num(row, ["近5日漲幅%", "5日漲幅%"], 0.0)
    ret20 = _first_num(row, ["近20日漲幅%", "20日漲幅%"], 0.0)
    if legacy > 0:
        base = legacy * 0.38 + ignition * 0.20 + resonance * 0.15 + continuation * 0.15 + pre_main * 0.12
    else:
        base = ignition * 0.28 + resonance * 0.22 + continuation * 0.22 + pre_main * 0.18 + _first_num(row, ["H51個股領漲品質分"], 50.0) * 0.10
    penalty = max(0.0, exhaust - 58.0) * 0.38 + max(0.0, ret5 - 16.0) * 0.45 + max(0.0, ret20 - 32.0) * 0.18
    score = _clip(base - penalty)
    market = _s(row.get("H51市場地位"))
    if score >= 78 and exhaust < 76 and not any(k in market for k in ["EXTENDED", "MATURE", "NO-CHASE"]):
        stage = "MR1｜主升起漲"
    elif score >= 68:
        stage = "MR2｜主升蓄勢"
    elif score >= 58:
        stage = "MR3｜主升研究"
    else:
        stage = "MR0｜非主升優先"
    return score, stage


def _holder_lock_score(row: pd.Series) -> tuple[str, str, float | None, float | None, float, str]:
    actual_status = _s(row.get("TDCC大戶資料狀態")).upper()
    ratio = _f(row.get("TDCC千張大戶持股比%"))
    delta = _f(row.get("TDCC千張大戶週變化pp"))
    data_date = _s(row.get("TDCC大戶資料日期"))
    if actual_status == "ACTUAL" and ratio is not None:
        score = 48.0 + (ratio - 40.0) * 0.75
        if delta is not None:
            score += delta * 10.0
            if delta < -1.5:
                score -= abs(delta + 1.5) * 5.0
        score = _clip(score)
        source = "ACTUAL｜TDCC千張大戶真實持股"
    else:
        proxy = _avg_available(row, [
            "大戶鎖碼分數", "大戶鎖碼代理分數", "大戶承接分", "投信鎖碼分", "籌碼續航分", "籌碼續航", "法人籌碼分數",
        ], 50.0)
        score = _clip(proxy)
        source = "PROXY｜量價/法人/承接代理，非千張大戶真實持股"
        ratio = None
        delta = None
        data_date = ""
    if score >= 78 and source.startswith("ACTUAL") and (delta is None or delta >= 0):
        level = "LK1｜真實鎖碼強"
    elif score >= 70:
        level = "LK2｜鎖碼偏強"
    elif score >= 58:
        level = "LK3｜鎖碼觀察"
    else:
        level = "LK0｜鎖碼不足"
    return source, data_date, ratio, delta, score, level


def _trend_score(row: pd.Series) -> float:
    tech = _avg_available(row, ["技術趨勢分數", "趨勢分數", "波段潛力分數", "H45趨勢延續分"], 50.0)
    r5 = _first_num(row, ["近5日漲幅%", "5日漲幅%"], 0.0)
    r20 = _first_num(row, ["近20日漲幅%", "20日漲幅%"], 0.0)
    r60 = _first_num(row, ["近60日漲幅%", "60日漲幅%"], 0.0)
    controlled = 50.0
    controlled += 10.0 if 1 <= r5 <= 12 else -8.0 if r5 > 20 or r5 < -8 else 0.0
    controlled += 12.0 if 4 <= r20 <= 30 else -10.0 if r20 > 45 or r20 < -15 else 0.0
    controlled += 8.0 if 8 <= r60 <= 60 else -6.0 if r60 > 90 or r60 < -25 else 0.0
    return _clip(tech * 0.60 + _clip(controlled) * 0.40)


def _snowball_score(row: pd.Series, holder_score: float) -> tuple[float, str]:
    growth = _avg_available(row, [
        "基本面成長分數", "官方基本面成長分數", "營收成長分數", "營收成長官方分數", "EPS成長分數", "營收動能代理分數", "EPS代理分數",
    ], 50.0)
    chip = _avg_available(row, ["籌碼續航分", "籌碼續航", "大戶承接分", "投信鎖碼分", "法人籌碼分數"], holder_score)
    trend = _trend_score(row)
    quality = _first_num(row, ["H51基本面資金分"], growth)
    exhaust = _first_num(row, ["H54耗竭風險分"], 50.0)
    chase = _first_num(row, ["追價風險分", "追價風險"], 50.0)
    score = growth * 0.28 + trend * 0.27 + chip * 0.20 + holder_score * 0.15 + quality * 0.10
    score -= max(0.0, exhaust - 65.0) * 0.25 + max(0.0, chase - 70.0) * 0.18
    score = _clip(score)
    if score >= 78 and growth >= 65 and trend >= 65:
        level = "SB1｜雪球複利核心"
    elif score >= 68:
        level = "SB2｜雪球成長候選"
    elif score >= 58:
        level = "SB3｜雪球觀察"
    else:
        level = "SB0｜非雪球優先"
    return score, level


def apply_h60_compound_engine(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    out = frame.copy()
    values = {c: [] for c in H60_COLUMNS}
    for _, row in out.iterrows():
        main_score, main_stage = _mainrise_score(row)
        lock_source, holder_date, ratio, delta, lock_score, lock_level = _holder_lock_score(row)
        snow_score, snow_level = _snowball_score(row, lock_score)
        resonance = _clip(main_score * 0.38 + lock_score * 0.28 + snow_score * 0.34)
        actual = lock_source.startswith("ACTUAL")
        if main_stage.startswith("MR1") and lock_level.startswith("LK1") and snow_level.startswith("SB1"):
            tri = "T3-A｜主升×真實鎖碼×雪球核心"
        elif main_stage.startswith("MR1") and snow_level.startswith("SB1") and lock_score >= 65:
            tri = "T3-P｜主升×代理鎖碼×雪球核心"
        elif resonance >= 72 and (main_stage.startswith(("MR1", "MR2")) or snow_level.startswith(("SB1", "SB2"))):
            tri = "T2｜雙因子以上共振"
        elif resonance >= 60:
            tri = "T1｜研究共振"
        else:
            tri = "T0｜尚未形成共振"
        conclusion = (
            f"主升={main_stage}({main_score:.1f})；鎖碼={lock_level}({lock_score:.1f},{'官方TDCC' if actual else '代理'})；"
            f"雪球={snow_level}({snow_score:.1f})；三因子={tri}({resonance:.1f})。H60只做研究，不改Formal/V188/H56交易權威。"
        )
        rowvals = {
            "H60主升段分": round(main_score, 2), "H60主升階段": main_stage,
            "H60鎖碼來源": lock_source, "H60大戶資料日期": holder_date,
            "H60千張大戶持股比%": round(ratio, 4) if ratio is not None else None,
            "H60千張大戶週變化pp": round(delta, 4) if delta is not None else None,
            "H60大戶鎖碼真相分": round(lock_score, 2), "H60大戶鎖碼層級": lock_level,
            "H60雪球複利分": round(snow_score, 2), "H60雪球股層級": snow_level,
            "H60三因子共振分": round(resonance, 2), "H60三因子層級": tri,
            "H60研究結論": conclusion, "H60版本": VERSION,
        }
        for c in H60_COLUMNS:
            values[c].append(rowvals[c])
    for c, vals in values.items():
        out[c] = vals
    return out


__all__ = ["VERSION", "H60_COLUMNS", "apply_h60_compound_engine"]
