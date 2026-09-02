# -*- coding: utf-8 -*-
"""V191-H57 Pre-Ignition engine.

Purpose
-------
Find stocks that are becoming *abnormal before the obvious breakout* without
creating trade authority.  The engine deliberately separates discovery from
execution:

    discovery (H57) -> H51/V188/Formal authority -> H56 pre-open confirmation

H57 focuses on five things that were previously scattered across many engines:
1. capital / volume acceleration;
2. volatility compression and compression-to-expansion transition;
3. relative-strength / momentum inflection;
4. early sector formation instead of only mature mainstream confirmation;
5. early-window quality (near pivot, not already extended).

It NEVER rewrites H51交易許可, Formal, V188, entry, stop or route RR.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h57_pre_ignition_acceleration_engine_20260902"

H57_COLUMNS = [
    "H57資金加速度分", "H57波動壓縮分", "H57壓縮轉擴張分", "H57相對強度轉折分",
    "H57提前視窗分", "H57族群點火廣度分", "H57主流形成前兆分", "H57前兆證據完整度",
    "H57飆股發動前兆分", "H57全市場前兆百分位%", "H57精選雷達層級", "H57前兆階段", "H57研究優先層級", "H57交易保護狀態",
    "H57前兆理由", "H57版本",
]

_BLANK = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}
_BROAD_PARENT_BUCKETS = {"半導體業", "電子零組件業", "光電業", "其他電子業", "電腦及週邊設備業"}


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
        t = str(v).strip().replace(",", "").replace("％", "%")
        if t.endswith("%"):
            t = t[:-1].strip()
        if not t or t.lower() in _BLANK:
            return float(default)
        x = float(t)
        return x if math.isfinite(x) else float(default)
    except Exception:
        return float(default)


def _clip(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(x)))


def _first_num(row: pd.Series, names: Iterable[str], default: float = 0.0) -> float:
    for c in names:
        if c in row.index and _s(row.get(c)):
            return _f(row.get(c), default)
    return float(default)


def _first_text(row: pd.Series, names: Iterable[str], default: str = "") -> str:
    for c in names:
        if c in row.index:
            t = _s(row.get(c))
            if t:
                return t
    return default


def _accel_score(v: float) -> float:
    """Acceleration is best when clearly positive but not blow-off extreme."""
    if v <= -35: return 12.0
    if v <= -15: return 25.0
    if v <= 0: return 42.0 + (v + 15.0) * (8.0 / 15.0)
    if v <= 15: return 50.0 + v * 1.25
    if v <= 35: return 68.75 + (v - 15.0) * 0.75
    if v <= 70: return 83.75 + (v - 35.0) * 0.32
    if v <= 120: return 95.0
    if v <= 220: return 95.0 - (v - 120.0) * 0.10
    return 82.0


def _compression_score(ratio: float) -> float:
    """5d/prior-20d realized-vol ratio: lower = tighter base, but too dead is weaker."""
    if ratio <= 0: return 50.0
    if ratio < 0.25: return 72.0
    if ratio <= 0.45: return 94.0
    if ratio <= 0.65: return 100.0
    if ratio <= 0.85: return 86.0
    if ratio <= 1.05: return 66.0
    if ratio <= 1.30: return 45.0
    return 25.0


def _expansion_score(row: pd.Series) -> float:
    prior_compress = _first_num(row, ["前5日波動壓縮比", "波動壓縮比"], 1.0)
    range_expand = _first_num(row, ["當日區間擴張倍數"], 1.0)
    day_vr = _first_num(row, ["當日量比", "均量比"], 1.0)
    day_ret = _first_num(row, ["今日漲幅%", "當日漲跌幅%"], 0.0)
    close_loc = _first_num(row, ["當日收盤位置%"], 50.0)
    upper = _first_num(row, ["上影線比例%"], 20.0)
    # Compression -> first controlled range/volume expansion is ideal.  A huge
    # one-day move is already ignition/extension, not an early signal.
    compression = _compression_score(prior_compress)
    range_s = 45.0
    if range_expand >= 1.8: range_s = 86.0
    elif range_expand >= 1.35: range_s = 94.0
    elif range_expand >= 1.10: range_s = 82.0
    elif range_expand >= 0.90: range_s = 62.0
    else: range_s = 42.0
    volume_s = 42.0 if day_vr < 0.95 else 62.0 if day_vr < 1.15 else 82.0 if day_vr < 1.55 else 92.0
    price_s = 92.0 if 0.5 <= day_ret <= 4.5 else 78.0 if -0.8 <= day_ret < 0.5 else 68.0 if 4.5 < day_ret <= 6.5 else 38.0
    close_s = _clip(35.0 + close_loc * 0.65 - max(0.0, upper - 25.0) * 0.8)
    return _clip(compression * 0.28 + range_s * 0.22 + volume_s * 0.22 + price_s * 0.16 + close_s * 0.12)


def _rs_inflection_score(row: pd.Series) -> float:
    rs = _first_num(row, ["H47個股相對強度分", "H45個股領先分", "相對強度分"], 50.0)
    accel = _first_num(row, ["3日動能加速度百分點"], 0.0)
    close3 = _first_num(row, ["3日平均收盤位置%", "當日收盤位置%"], 50.0)
    trend = _first_num(row, ["均線轉強分", "動能翻多分"], 50.0)
    accel_s = _clip(50.0 + accel * 8.0)
    close_s = _clip(25.0 + close3 * 0.75)
    return _clip(rs * 0.38 + accel_s * 0.27 + close_s * 0.18 + trend * 0.17)


def _early_window_score(row: pd.Series) -> float:
    ret1 = _first_num(row, ["今日漲幅%", "當日漲跌幅%"], 0.0)
    ret5 = _first_num(row, ["近5日漲幅%", "5日績效%"], 0.0)
    ret20 = _first_num(row, ["近20日漲幅%", "20日績效%"], 0.0)
    dist_high = _first_num(row, ["距20日高點%"], 9.0)
    breakout = _first_num(row, ["突破20日高點%"], 0.0)
    close_ma20 = _first_num(row, ["收盤距MA20%"], 0.0)

    one = 95.0 if -0.8 <= ret1 <= 3.5 else 82.0 if 3.5 < ret1 <= 5.5 else 65.0 if -2.0 <= ret1 < -0.8 else 38.0
    five = 98.0 if -1.0 <= ret5 <= 7.0 else 86.0 if 7.0 < ret5 <= 10.0 else 68.0 if -5.0 <= ret5 < -1.0 else 35.0
    twenty = 92.0 if -4.0 <= ret20 <= 16.0 else 75.0 if 16.0 < ret20 <= 23.0 else 55.0 if -10.0 <= ret20 < -4.0 else 30.0
    near = 100.0 if 0.0 <= dist_high <= 3.5 else 86.0 if dist_high <= 6.0 else 66.0 if dist_high <= 9.0 else 42.0
    # Already >3% above the old 20d high is less "pre" and more ignition/extension.
    if breakout > 3.0:
        near -= min(35.0, (breakout - 3.0) * 6.0)
    ma = 92.0 if -1.5 <= close_ma20 <= 6.0 else 75.0 if 6.0 < close_ma20 <= 10.0 else 48.0
    return _clip(one * 0.20 + five * 0.25 + twenty * 0.15 + near * 0.25 + ma * 0.15)


def _evidence_completeness(row: pd.Series) -> float:
    groups = [
        ["成交額3日加速度%", "成交量3日加速度%"],
        ["波動壓縮比", "前5日波動壓縮比"],
        ["當日區間擴張倍數"],
        ["3日動能加速度百分點"],
        ["距20日高點%", "突破20日高點%"],
        ["當日收盤位置%"],
        ["成交額百萬", "20日均成交額百萬"],
    ]
    present = 0
    for names in groups:
        if any(c in row.index and _s(row.get(c)) for c in names):
            present += 1
    return round(present / len(groups) * 100.0, 2)


def _hard_protection(row: pd.Series) -> tuple[bool, str]:
    kfresh = _first_text(row, ["K線資料新鮮度"], "")
    shock = _first_text(row, ["H51急跌收復狀態", "H51市場地位"], "")
    blocked = "｜".join(_first_text(row, [c], "") for c in ["V188交易許可", "操作許可", "正式推薦排除原因", "進場阻擋原因"])
    amount = _first_num(row, ["成交額百萬", "20日均成交額百萬"], 0.0)
    liq = _first_num(row, ["H51流動性分"], 50.0)
    reasons = []
    if any(x in kfresh for x in ["過期", "落後", "待更新"]): reasons.append("K線非最新")
    if "SHOCK" in shock.upper() or "RECLAIM" in shock.upper(): reasons.append("急跌後需先收復")
    if any(x in blocked.upper() for x in ["LOCKDOWN", "禁止所有新倉", "全面禁買", "WAIT-DATA"]): reasons.append("上游硬風控")
    if amount > 0 and amount < 80: reasons.append("成交額偏低")
    if liq < 45: reasons.append("流動性不足")
    return bool(reasons), "、".join(reasons) if reasons else "研究保護通過｜仍不代表買進許可"


def apply_pre_ignition_engine(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    out = frame.copy()

    interim: list[dict[str, float]] = []
    for _, row in out.iterrows():
        amount3 = _first_num(row, ["成交額3日加速度%"], 0.0)
        amount5 = _first_num(row, ["成交額5日加速度%"], amount3)
        volume3 = _first_num(row, ["成交量3日加速度%"], 0.0)
        volume5 = _first_num(row, ["成交量5日加速度%"], volume3)
        inst = _first_num(row, ["法人連買代理分數", "法人籌碼分數", "法人籌碼官方分數"], 50.0)
        capital = _clip(
            _accel_score(amount3) * 0.38 + _accel_score(amount5) * 0.16
            + _accel_score(volume3) * 0.24 + _accel_score(volume5) * 0.10 + inst * 0.12
        )
        compression = _compression_score(_first_num(row, ["前5日波動壓縮比", "波動壓縮比"], 1.0))
        expansion = _expansion_score(row)
        rs_turn = _rs_inflection_score(row)
        early = _early_window_score(row)
        completeness = _evidence_completeness(row)
        interim.append({
            "capital": capital, "compression": compression, "expansion": expansion,
            "rs": rs_turn, "early": early, "completeness": completeness,
        })

    # Cross-sectional early-sector formation: multiple stocks accelerating before
    # mature H53/H55 mainstream confirmation is more valuable than one isolated spike.
    sectors = out.get("類別", pd.Series(["未分類"] * len(out), index=out.index)).fillna("未分類").astype(str).str.strip()
    base = pd.DataFrame(interim, index=out.index)
    sector_ctx: dict[str, dict[str, float]] = {}
    for sector, idxs in sectors.groupby(sectors).groups.items():
        idxs = list(idxs)
        g = base.loc[idxs]
        n = max(1, len(g))
        qualifying = ((g["capital"] >= 66) & (g["rs"] >= 64) & (g["early"] >= 65)).mean() * 100.0
        pressure = ((g["compression"] >= 70) | (g["expansion"] >= 72)).mean() * 100.0
        top_pre = (g["capital"] * 0.30 + g["rs"] * 0.25 + g["early"] * 0.25 + g["expansion"] * 0.20).nlargest(min(3, n)).mean()
        conf = _clip(35.0 + min(n, 8) * 8.125)
        shrink = conf / 100.0
        breadth = _clip(50.0 + ((_clip(qualifying * 0.58 + pressure * 0.42)) - 50.0) * shrink)
        if sector in _BROAD_PARENT_BUCKETS and n >= 5:
            breadth = _clip(breadth - 7.0)
        sector_ctx[sector or "未分類"] = {"breadth": breadth, "top_pre": float(top_pre), "conf": conf}

    rows: dict[str, list[Any]] = {c: [] for c in H57_COLUMNS}
    for pos, (_, row) in enumerate(out.iterrows()):
        sc = interim[pos]
        sector = _first_text(row, ["類別", "族群名稱"], "未分類")
        ctx = sector_ctx.get(sector, {"breadth": 50.0, "top_pre": 50.0, "conf": 35.0})
        h53attack = _first_num(row, ["H53族群攻擊分", "族群攻擊強度"], 50.0)
        h53res = _first_num(row, ["H53族群共振分"], 50.0)
        h55cat = _first_num(row, ["H55催化代理分", "局部題材火種分"], 50.0)
        sector_form = _clip(
            float(ctx["breadth"]) * 0.36 + float(ctx["top_pre"]) * 0.18
            + h53attack * 0.20 + h53res * 0.12 + h55cat * 0.14
        )
        freshness = _first_num(row, ["今日訊號新鮮分", "H51主線新鮮分"], 50.0)
        liquidity = _first_num(row, ["H51流動性分"], 50.0)
        exhaustion = _first_num(row, ["H54耗竭風險分", "隔日耗竭風險分"], 50.0)
        chase = _first_num(row, ["追價風險分", "追價風險分數"], 50.0)
        ret1 = _first_num(row, ["今日漲幅%"], 0.0)
        ret5 = _first_num(row, ["近5日漲幅%"], 0.0)
        ret20 = _first_num(row, ["近20日漲幅%"], 0.0)
        upper = _first_num(row, ["上影線比例%"], 20.0)
        breakout = _first_num(row, ["突破20日高點%"], 0.0)

        penalty = 0.0
        penalty += max(0.0, ret1 - 5.0) * 2.8
        penalty += max(0.0, ret5 - 11.0) * 1.25
        penalty += max(0.0, ret20 - 24.0) * 0.55
        penalty += max(0.0, exhaustion - 62.0) * 0.42
        penalty += max(0.0, chase - 68.0) * 0.32
        penalty += max(0.0, upper - 38.0) * 0.20
        penalty += max(0.0, breakout - 4.0) * 1.8

        pre = _clip(
            sc["capital"] * 0.22 + sc["compression"] * 0.14 + sc["expansion"] * 0.15
            + sc["rs"] * 0.17 + sc["early"] * 0.12 + sector_form * 0.12
            + freshness * 0.04 + liquidity * 0.04 - penalty
        )
        blocked, protection_reason = _hard_protection(row)

        already_ignition = bool(
            (ret1 >= 4.5 or breakout >= 1.0)
            and _first_num(row, ["當日量比", "均量比"], 1.0) >= 1.25
            and _first_num(row, ["當日收盤位置%"], 50.0) >= 70
        )
        extended = bool(exhaustion >= 72 or ret5 >= 17 or ret20 >= 34 or chase >= 80)

        if blocked:
            phase = "BX｜DATA-RISK-BLOCKED｜前兆可研究但資料/風控未通過"
            tier = "X0｜BLOCKED-RESEARCH"
        elif extended:
            phase = "EX1｜EXTENDED｜已延伸/耗竭，不再視為發動前兆"
            tier = "X1｜NO-CHASE"
        elif already_ignition and pre >= 68:
            phase = "IG1｜IGNITION｜已開始點火，轉入突破/回測執行觀察"
            tier = "S2｜IGNITION-WATCH"
        elif pre >= 79 and sc["capital"] >= 70 and sc["rs"] >= 68 and sc["early"] >= 72 and (sc["compression"] >= 68 or sc["expansion"] >= 72):
            phase = "PI3｜PRE-IGNITION｜高品質1-3日發動前兆"
            tier = "S1｜PRE-IGNITION-PRIME"
        elif pre >= 70 and sc["capital"] >= 62 and sc["rs"] >= 62:
            phase = "PI2｜PRESSURE-BUILDING｜資金/結構正在蓄勢"
            tier = "S3｜PRESSURE-BUILDING"
        elif pre >= 62:
            phase = "PI1｜EARLY-SIGNAL｜早期異常，持續觀察"
            tier = "S4｜EARLY-SIGNAL"
        else:
            phase = "PI0｜NORMAL｜尚未形成足夠發動前兆"
            tier = "W2｜RESEARCH"

        protection = protection_reason
        reason = (
            f"前兆{pre:.1f}；資金加速{sc['capital']:.1f}/壓縮{sc['compression']:.1f}/"
            f"壓縮轉擴張{sc['expansion']:.1f}/RS轉折{sc['rs']:.1f}/提前視窗{sc['early']:.1f}；"
            f"族群點火廣度{float(ctx['breadth']):.1f}/主流形成前兆{sector_form:.1f}/證據完整{sc['completeness']:.0f}%；"
            f"耗竭{exhaustion:.1f}/追價{chase:.1f}/延伸扣分{penalty:.1f}。{protection_reason}。"
            "H57只負責提早發現，不建立買進權限；正式執行仍由Formal/V188/H51/H56與觸發/RR決定。"
        )
        values = {
            "H57資金加速度分": round(sc["capital"], 2),
            "H57波動壓縮分": round(sc["compression"], 2),
            "H57壓縮轉擴張分": round(sc["expansion"], 2),
            "H57相對強度轉折分": round(sc["rs"], 2),
            "H57提前視窗分": round(sc["early"], 2),
            "H57族群點火廣度分": round(float(ctx["breadth"]), 2),
            "H57主流形成前兆分": round(sector_form, 2),
            "H57前兆證據完整度": round(sc["completeness"], 2),
            "H57飆股發動前兆分": round(pre, 2),
            "H57全市場前兆百分位%": 0.0,
            "H57精選雷達層級": "",
            "H57前兆階段": phase,
            "H57研究優先層級": tier,
            "H57交易保護狀態": protection,
            "H57前兆理由": reason,
            "H57版本": VERSION,
        }
        for c in H57_COLUMNS:
            rows[c].append(values[c])

    for c, vals in rows.items():
        out[c] = vals

    # Cross-sectional elite radar.  The raw PI3 pool may intentionally be broad
    # enough for recall; E1 compresses it to the very top tail so the user gets a
    # practical 10-30 stock research list in a ~1,700-stock universe.
    score_series = pd.to_numeric(out.get("H57飆股發動前兆分", 0), errors="coerce").fillna(0.0)
    pct = score_series.rank(method="average", pct=True) * 100.0
    out["H57全市場前兆百分位%"] = pct.round(2)
    n = len(out)
    phase_series = out.get("H57前兆階段", pd.Series([""] * n, index=out.index)).fillna("").astype(str)
    evidence = pd.to_numeric(out.get("H57前兆證據完整度", 0), errors="coerce").fillna(0.0)
    sector_form = pd.to_numeric(out.get("H57主流形成前兆分", 0), errors="coerce").fillna(0.0)
    if n >= 100:
        elite_mask = phase_series.str.startswith("PI3") & pct.ge(98.5) & evidence.ge(70) & sector_form.ge(50)
    else:
        elite_mask = phase_series.str.startswith("PI3") & score_series.ge(82) & evidence.ge(70) & sector_form.ge(50)
    ignition_elite = phase_series.str.startswith("IG1") & pct.ge(99.0) & evidence.ge(70)
    out["H57精選雷達層級"] = ""
    out.loc[elite_mask, "H57精選雷達層級"] = "E1｜ELITE-PRE-IGNITION｜全市場頂級發動前兆"
    out.loc[ignition_elite, "H57精選雷達層級"] = "E2｜ELITE-IGNITION｜全市場頂級已點火"
    out.loc[elite_mask, "H57研究優先層級"] = "S0｜ELITE-PRE-IGNITION"
    return out


__all__ = ["VERSION", "H57_COLUMNS", "apply_pre_ignition_engine"]
