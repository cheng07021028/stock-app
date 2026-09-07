# -*- coding: utf-8 -*-
"""V191-H62 incremental opportunity / effective-formal truth engine.

H61 solved attention cost for research rows, but intentionally kept every raw
Formal candidate on the first screen.  That preserved authority provenance but
could still make familiar large caps look like today's best recommendation even
when recent Selection Alpha and remaining upside had decayed.

H62 separates two truths:
  * RAW FORMAL: historical/upstream authority evidence (never deleted).
  * EFFECTIVE FORMAL: raw Formal that still earns today's execution attention.

A raw Formal can become FORMAL-HOLD when recent mature alpha / incremental
headroom / RR / freshness do not justify scarce front-page attention.  This is
not a permanent blacklist and never rewrites the raw Formal columns.  A future
recovery can restore EFFECTIVE-FORMAL automatically.

Non-formal candidates compete cross-sectionally for scarce research slots.  New
leaders with strong incremental upside can be N1/N2, but N1/N2 are research only
and never manufacture BUY permission.
"""
from __future__ import annotations

from typing import Any
import math
import pandas as pd

VERSION = "v191_h62_incremental_opportunity_effective_formal_truth_20260907"

H62_COLUMNS = [
    "H62原始權威", "H62有效權威", "H62近期證明分", "H62增量上漲空間分", "H62新領漲分",
    "H62熟面孔衰退扣分", "H62增量機會分", "H62全市場機會百分位%", "H62機會層級",
    "H62前排資格", "H62正式作戰資格", "H62決策理由", "H62版本",
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


def _num(row: pd.Series, names: list[str], default: float = 0.0) -> float:
    for c in names:
        if c in row.index and _s(row.get(c)):
            return _f(row.get(c), default)
    return float(default)


def _txt(row: pd.Series, names: list[str], default: str = "") -> str:
    for c in names:
        if c in row.index:
            t = _s(row.get(c))
            if t:
                return t
    return default


def _raw_authority(row: pd.Series) -> str:
    # H60 fixed the A- vs Formal parser. Prefer the normalized H56 authority.
    h56 = _txt(row, ["H56上游權威層級", "H56上游權威"], "").upper()
    if h56 in {"FORMAL", "A-MINUS", "RADAR", "RESTRICTED", "UNKNOWN"}:
        return h56
    formal = _txt(row, ["是否正式推薦"], "").lower()
    bucket = _txt(row, ["正式推薦分區", "正式推薦等級"], "")
    if formal.startswith(("是", "true", "1")) or bucket == "正式下週主推薦":
        return "FORMAL"
    if "A-" in bucket or "準主推薦" in bucket or "條件推薦" in bucket:
        return "A-MINUS"
    if "雷達" in bucket:
        return "RADAR"
    return "UNKNOWN"


def apply_h62_incremental_opportunity_engine(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy()

    # H62 expects H61 evidence but is safe when a partial/legacy frame is used.
    if "H61機會價值分" not in work.columns:
        try:
            from godpick_h61_opportunity_cost_engine import apply_h61_opportunity_cost_engine
            work = apply_h61_opportunity_cost_engine(work)
        except Exception:
            pass

    rows: list[dict[str, Any]] = []
    for _, row in work.iterrows():
        auth = _raw_authority(row)
        recent_n = int(round(_num(row, ["H61近期成熟樣本"], 0)))
        recent_alpha = _num(row, ["H61近期SelectionAlpha%"], 0.0)
        pos_alpha = _num(row, ["H61近期正Alpha率%"], 50.0)
        h61_opp = _num(row, ["H61機會價值分"], 50.0)
        headroom = _num(row, ["H61上漲空間分"], 50.0)
        rrq = _num(row, ["H61RR品質分"], 50.0)
        expected10 = _num(row, ["H32_10日預估報酬%", "10日預估報酬%"], 0.0)
        h57pct = _num(row, ["H57全市場前兆百分位%"], 50.0)
        h57 = _num(row, ["H57飆股發動前兆分"], 50.0)
        h57cash = _num(row, ["H57資金加速度分"], 50.0)
        h57rs = _num(row, ["H57相對強度轉折分"], 50.0)
        h57early = _num(row, ["H57提前視窗分"], 50.0)
        h60main = _num(row, ["H60主升段分"], 50.0)
        h60triple = _num(row, ["H60三因子共振分"], 50.0)
        h53 = _num(row, ["H53族群共振分"], 50.0)
        h55 = _num(row, ["H55雙路徑隔日分"], 50.0)
        near5 = int(round(_num(row, ["近5次入榜次數"], 0)))
        consecutive = int(round(_num(row, ["連續入榜次數"], 0)))
        h61tier = _txt(row, ["H61機會層級"], "")

        # Recent proof deliberately gives neutral credit when there are too few
        # mature samples; H62 is not allowed to punish a genuinely new stock for
        # missing history.
        if recent_n <= 0:
            recent_proof = 55.0
        else:
            alpha_component = _clip(50.0 + recent_alpha * 16.0)
            pos_component = _clip(pos_alpha)
            confidence = min(1.0, 0.48 + 0.18 * recent_n)
            recent_proof = 55.0 * (1.0 - confidence) + (alpha_component * 0.72 + pos_component * 0.28) * confidence

        # Incremental upside asks whether there is still enough reward left now,
        # not whether the company is historically excellent.
        exp_score = _clip(38.0 + expected10 * 8.0) if expected10 > 0 else 35.0
        incremental_upside = _clip(headroom * 0.58 + exp_score * 0.42)

        # New-leader evidence rewards market-wide freshness and acceleration.
        new_leader = _clip(
            h57pct * 0.24 + h57 * 0.18 + h57cash * 0.16 + h57rs * 0.12 + h57early * 0.10
            + h60main * 0.08 + h60triple * 0.05 + h53 * 0.04 + h55 * 0.03
        )

        familiar_pen = 0.0
        if near5 >= 2:
            familiar_pen += min(12.0, (near5 - 1) * 3.0)
        if consecutive >= 2:
            familiar_pen += min(9.0, (consecutive - 1) * 3.0)
        if recent_n >= 2 and recent_alpha <= 0:
            familiar_pen += min(22.0, 7.0 + abs(recent_alpha) * 8.0)
        if h61tier.startswith(("R0", "L0")):
            familiar_pen += 7.0
        if expected10 > 0 and expected10 < 3.0:
            familiar_pen += 5.0
        familiar_pen = _clip(familiar_pen, 0.0, 38.0)

        incremental = _clip(
            h61_opp * 0.24 + recent_proof * 0.22 + incremental_upside * 0.20 + rrq * 0.12 + new_leader * 0.22
            - familiar_pen * 0.72
        )

        # Effective Formal is a current-run action concept, not a rewrite of raw
        # Formal history. Strong raw Formal remains effective. Familiar Formal
        # with mature negative alpha + modest opportunity is put on HOLD until it
        # re-proves itself. One weak observation alone is never enough to hold.
        effective = auth
        formal_action = "否"
        if auth == "FORMAL":
            hold_evidence = (
                (recent_n >= 2 and recent_alpha <= -0.45 and h61_opp < 58.0)
                or (recent_n >= 3 and recent_alpha <= 0.0 and incremental < 52.0)
                or (recent_n >= 2 and h61tier.startswith(("R0", "L0")) and incremental_upside < 62.0 and incremental < 56.0)
            )
            if hold_evidence:
                effective = "FORMAL-HOLD"
                formal_action = "否｜本輪暫停"
            else:
                effective = "EFFECTIVE-FORMAL"
                formal_action = "是｜仍需H56/觸發"

        # Row-level opportunity tier; cross-sectional percentile is added later.
        if effective == "FORMAL-HOLD":
            tier = "D0｜熟面孔Formal重新證明"
            front = "否｜Formal暫停前排"
        elif effective == "EFFECTIVE-FORMAL":
            tier = "F1｜有效Formal"
            front = "是｜有效Formal保留"
        elif incremental >= 75 and h57pct >= 90 and familiar_pen < 12:
            tier = "N1｜全市場新領漲機會"
            front = "是｜新機會研究"
        elif incremental >= 66 and h57pct >= 80:
            tier = "N2｜高增量新機會"
            front = "是｜新機會研究"
        elif (_txt(row, ["H55參考層級"], "").startswith("R2") or _txt(row, ["H57前兆階段"], "").startswith(("PI2", "PI3", "IG1"))) and new_leader >= 68.0 and familiar_pen < 12.0:
            # Preserve genuinely fresh H55/H57 discovery recall even when H61
            # lacks mature headroom/alpha history. H62 is a scarce-attention
            # guard, not a reason to hide new ignition/pre-ignition candidates.
            tier = "N2｜高增量新機會"
            front = "是｜新機會研究"
        elif h61tier.startswith(("R0", "L0")) or (recent_n >= 2 and recent_alpha <= -0.5 and incremental < 58):
            tier = "D0｜熟面孔/低增量降權"
            front = "否｜不占前排"
        elif incremental >= 60:
            tier = "W1｜一般增量研究"
            front = "是｜一般研究"
        else:
            tier = "W0｜低增量研究"
            front = "否｜不占前排"

        reason = (
            f"原始權威={auth}/有效權威={effective}；近期成熟{recent_n}筆、Alpha{recent_alpha:+.2f}pp/正Alpha{pos_alpha:.0f}%；"
            f"10日預估{expected10:+.2f}%/增量上漲空間{incremental_upside:.1f}；RR品質{rrq:.1f}；"
            f"新領漲{new_leader:.1f}/H57百分位{h57pct:.1f}；近5次{near5}/連續{consecutive}；"
            f"熟面孔衰退扣{familiar_pen:.1f}；增量機會{incremental:.1f}。"
        )
        rows.append({
            "H62原始權威": auth,
            "H62有效權威": effective,
            "H62近期證明分": round(recent_proof, 2),
            "H62增量上漲空間分": round(incremental_upside, 2),
            "H62新領漲分": round(new_leader, 2),
            "H62熟面孔衰退扣分": round(familiar_pen, 2),
            "H62增量機會分": round(incremental, 2),
            "H62全市場機會百分位%": 0.0,
            "H62機會層級": tier,
            "H62前排資格": front,
            "H62正式作戰資格": formal_action,
            "H62決策理由": reason,
            "H62版本": VERSION,
        })

    addon = pd.DataFrame(rows, index=work.index)
    # Percentile is intentionally cross-sectional across the current scan, so a
    # familiar name must compete with today's entire market instead of yesterday.
    score = pd.to_numeric(addon["H62增量機會分"], errors="coerce").fillna(0.0)
    if len(addon) >= 2:
        pct = score.rank(method="average", pct=True) * 100.0
    else:
        pct = pd.Series([100.0] * len(addon), index=addon.index)
    addon["H62全市場機會百分位%"] = pct.round(2)

    # Percentile can promote genuinely fresh research names, but never changes
    # raw/effective Formal. Conversely low-percentile familiar names are kept out
    # of scarce research slots.
    for idx in addon.index:
        eff = _s(addon.at[idx, "H62有效權威"])
        tier = _s(addon.at[idx, "H62機會層級"])
        pctv = _f(addon.at[idx, "H62全市場機會百分位%"])
        inc = _f(addon.at[idx, "H62增量機會分"])
        pen = _f(addon.at[idx, "H62熟面孔衰退扣分"])
        if eff not in {"EFFECTIVE-FORMAL", "FORMAL-HOLD"}:
            if pctv >= 97.0 and inc >= 68.0 and pen < 14.0:
                addon.at[idx, "H62機會層級"] = "N1｜全市場新領漲機會"
                addon.at[idx, "H62前排資格"] = "是｜新機會研究"
            elif pctv >= 90.0 and inc >= 62.0 and not tier.startswith("D0"):
                addon.at[idx, "H62機會層級"] = "N2｜高增量新機會"
                addon.at[idx, "H62前排資格"] = "是｜新機會研究"
            elif pctv < 70.0 and tier.startswith("W1"):
                addon.at[idx, "H62機會層級"] = "W0｜低增量研究"
                addon.at[idx, "H62前排資格"] = "否｜不占前排"

    for c in H62_COLUMNS:
        work[c] = addon[c]
    return work


__all__ = ["VERSION", "H62_COLUMNS", "apply_h62_incremental_opportunity_engine"]
