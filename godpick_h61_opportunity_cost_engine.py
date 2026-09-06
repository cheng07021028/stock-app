# -*- coding: utf-8 -*-
"""V191-H61 opportunity-cost / repeated-favorite / recent-alpha truth engine.

Purpose
-------
The upstream engines are good at finding strong, liquid, mainstream names.  A
side-effect is that familiar large-cap leaders can repeatedly occupy the first
research rows even when their *incremental* upside has become modest or their
recent Selection Alpha has decayed.

H61 answers a different question:
    "Is this stock still worth spending one of today's scarce attention slots on
     versus a fresher alternative?"

It is a research-ranking layer only.  It NEVER removes/changes Formal/V188/H56
trading authority, Entry, Stop, or RR.  Formal rows remain visible even if H61
marks them low opportunity.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable
import math
import re
import pandas as pd

VERSION = "v191_h61_opportunity_cost_repeat_alpha_truth_20260906"

H61_COLUMNS = [
    "H61近期成熟樣本", "H61近期SelectionAlpha%", "H61近期正Alpha率%", "H61歷史成熟樣本",
    "H61上漲空間分", "H61近期Alpha分", "H61RR品質分", "H61新鮮機會分",
    "H61重複慣性扣分", "H61機會成本扣分", "H61機會價值分",
    "H61機會層級", "H61前排資格", "H61決策理由", "H61版本",
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


def _first_num(row: pd.Series, names: Iterable[str], default: float = 0.0) -> float:
    for c in names:
        if c not in row.index:
            continue
        t = _s(row.get(c))
        if not t:
            continue
        x = _f(t, float("nan"))
        if math.isfinite(x):
            return float(x)
    return float(default)


def _first_text(row: pd.Series, names: Iterable[str], default: str = "") -> str:
    for c in names:
        if c in row.index:
            t = _s(row.get(c))
            if t:
                return t
    return default


def _norm_code(v: Any) -> str:
    s = _s(v)
    if not s:
        return ""
    m = re.search(r"(\d{4,6})", s)
    return m.group(1) if m else s.upper()


def _date_key(v: Any) -> str:
    s = _s(v).replace("/", "-").replace(".", "-")
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    m = re.search(r"(20\d{2})[-/]?(\d{1,2})[-/]?(\d{1,2})", s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return s


def _load_truth_rows_safely(limit: int = 1200) -> list[dict[str, Any]]:
    try:
        from godpick_t1_trade_truth import load_t1_truth_rows
        rows = load_t1_truth_rows(limit=limit)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def _truth_metrics(truth_rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in truth_rows or []:
        if not isinstance(r, dict):
            continue
        mature = r.get("T1成熟")
        mature_text = _s(mature).lower()
        if not (mature is True or mature_text in {"true", "1", "yes", "是"}):
            continue
        code = _norm_code(r.get("股票代號") or r.get("代號"))
        if not code:
            continue
        grouped.setdefault(code, []).append(r)

    out: dict[str, dict[str, float]] = {}
    for code, items in grouped.items():
        items = sorted(items, key=lambda x: _date_key(x.get("推薦日期") or x.get("推薦批次日期")), reverse=True)
        usable = []
        for r in items:
            a = r.get("Selection Alpha%")
            if _s(a):
                usable.append((_f(a), r))
        recent = usable[:3]
        weights = [1.0, 0.70, 0.50]
        if recent:
            ww = weights[:len(recent)]
            weighted = sum(x[0] * w for x, w in zip(recent, ww)) / sum(ww)
            pos = sum(1 for x, _ in recent if x > 0) / len(recent) * 100.0
        else:
            weighted, pos = 0.0, 50.0
        out[code] = {
            "recent_n": float(len(recent)),
            "recent_alpha": float(weighted),
            "recent_pos": float(pos),
            "history_n": float(len(usable)),
        }
    return out


def _headroom_score(expected10: float, h49: float, wave: float) -> float:
    # Expected 10-day return is deliberately influential, but not a hard gate:
    # it is a model estimate and can be low-confidence. H49/wave preserve the
    # structural upside view.
    x = expected10
    if x <= 0: pred = 25.0
    elif x < 1.0: pred = 30.0 + x * 12.0
    elif x < 2.0: pred = 42.0 + (x - 1.0) * 12.0
    elif x < 3.5: pred = 54.0 + (x - 2.0) * 10.0
    elif x < 5.0: pred = 69.0 + (x - 3.5) * 7.0
    elif x < 8.0: pred = 79.5 + (x - 5.0) * 4.0
    else: pred = min(100.0, 91.5 + (x - 8.0) * 1.5)
    structural = h49 if h49 > 0 else (wave if wave > 0 else 50.0)
    if wave > 0 and h49 > 0:
        structural = h49 * 0.70 + wave * 0.30
    return _clip(pred * 0.62 + structural * 0.38)


def _rr_score(rr: float) -> float:
    if rr <= 0: return 15.0
    if rr < 0.8: return 20.0 + rr * 18.0
    if rr < 1.0: return 34.4 + (rr - 0.8) * 28.0
    if rr < 1.3: return 40.0 + (rr - 1.0) * 50.0
    if rr < 1.5: return 55.0 + (rr - 1.3) * 50.0
    if rr < 2.0: return 65.0 + (rr - 1.5) * 30.0
    if rr < 3.0: return 80.0 + (rr - 2.0) * 15.0
    return min(100.0, 95.0 + (rr - 3.0) * 2.0)


def _alpha_score(alpha: float, n: int) -> float:
    if n <= 0:
        return 50.0
    # Each 1 percentage point of recent Selection Alpha moves 14 score pts.
    # This makes repeated -1%~-2% favorites visibly pay an opportunity cost.
    confidence = min(1.0, 0.55 + 0.18 * n)
    raw = _clip(50.0 + alpha * 14.0)
    return 50.0 * (1.0 - confidence) + raw * confidence


def apply_h61_opportunity_cost_engine(frame: pd.DataFrame, truth_rows: list[dict[str, Any]] | None = None) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy()
    truth_rows = _load_truth_rows_safely() if truth_rows is None else truth_rows
    truth = _truth_metrics(truth_rows)

    rows = []
    for _, row in work.iterrows():
        code = _norm_code(row.get("股票代號") or row.get("代號"))
        tm = truth.get(code, {"recent_n": 0.0, "recent_alpha": 0.0, "recent_pos": 50.0, "history_n": 0.0})
        recent_n = int(tm["recent_n"])
        recent_alpha = float(tm["recent_alpha"])
        recent_pos = float(tm["recent_pos"])
        history_n = int(tm["history_n"])

        expected10 = _first_num(row, ["H32_10日預估報酬%", "10日預估報酬%"], 0.0)
        h49 = _first_num(row, ["H49上漲潛力分", "選股潛力分", "Alpha選股潛力分"], 0.0)
        wave = _first_num(row, ["波段潛力分數", "H49波段潛力分"], 0.0)
        rr = _first_num(row, ["H51路徑RR", "路徑風險報酬比", "SuperAI執行風報比"], 0.0)
        h57 = _first_num(row, ["H57飆股發動前兆分"], 50.0)
        h57_pct = _first_num(row, ["H57全市場前兆百分位%"], 50.0)
        h60 = _first_num(row, ["H60三因子共振分"], 50.0)
        h60_main = _first_num(row, ["H60主升段分"], 50.0)
        h51 = _first_num(row, ["H51專業參考分"], 50.0)
        near5 = int(round(_first_num(row, ["近5次入榜次數"], 0.0)))
        consecutive = int(round(_first_num(row, ["連續入榜次數"], 0.0)))
        auth = _first_text(row, ["H56上游權威層級"], "")
        is_formal = auth == "FORMAL" or _first_text(row, ["是否正式推薦"], "").startswith(("是", "True", "true"))

        headroom = _headroom_score(expected10, h49, wave)
        alpha_score = _alpha_score(recent_alpha, recent_n)
        rr_score = _rr_score(rr)
        fresh_score = _clip(h57 * 0.35 + h57_pct * 0.25 + h60 * 0.16 + h60_main * 0.12 + h51 * 0.12)

        # Repeated favorites only pay a strong penalty after actual mature T+1
        # evidence says they are not producing fresh alpha. New evidence alone
        # cannot zero this penalty anymore.
        repeat_pen = 0.0
        exposure = max(near5, min(5, history_n))
        if exposure >= 2:
            repeat_pen += min(8.0, (exposure - 1) * 2.0)
        if consecutive >= 2:
            repeat_pen += min(7.0, (consecutive - 1) * 2.5)
        if recent_n >= 2 and recent_alpha <= 0:
            repeat_pen += min(18.0, abs(recent_alpha) * 6.0 + 5.0)
        elif recent_n >= 1 and recent_alpha < -0.5:
            repeat_pen += min(9.0, abs(recent_alpha) * 4.0)
        repeat_pen = _clip(repeat_pen, 0.0, 30.0)

        # Opportunity-cost penalty asks whether scarce attention is being spent
        # on a name with only modest incremental upside / poor RR / decayed alpha.
        opp_pen = 0.0
        if expected10 < 1.5: opp_pen += 12.0
        elif expected10 < 3.0: opp_pen += 7.0
        elif expected10 < 4.0: opp_pen += 3.0
        if rr < 0.8: opp_pen += 14.0
        elif rr < 1.2: opp_pen += 8.0
        elif rr < 1.5: opp_pen += 4.0
        if recent_n >= 2 and recent_alpha < -0.5: opp_pen += min(12.0, 4.0 + abs(recent_alpha) * 3.0)
        if near5 >= 3 and expected10 < 4.0: opp_pen += 5.0
        opp_pen = _clip(opp_pen, 0.0, 35.0)

        opportunity = _clip(
            headroom * 0.27 + alpha_score * 0.24 + rr_score * 0.18 + fresh_score * 0.21 + h60 * 0.10
            - repeat_pen * 0.55 - opp_pen * 0.65
        )

        # Human-readable tier. Formal is never hidden/demoted by this label;
        # `front` controls only research priority.
        if recent_n >= 2 and recent_alpha <= -0.5 and opportunity < 58:
            tier = "R0｜重複低效觀察"
        elif expected10 < 1.5 and rr < 1.2:
            tier = "L0｜低漲幅空間"
        elif opportunity >= 76 and (near5 <= 1 or recent_alpha > 0.3):
            tier = "O1｜高潛力新機會"
        elif opportunity >= 66:
            tier = "O2｜可追蹤新機會"
        elif opportunity >= 60 and recent_n > 0 and recent_alpha > 0:
            tier = "R1｜重複但仍有新Alpha證據"
        else:
            tier = "W1｜一般研究"

        front = "是"
        if not is_formal and (tier.startswith(("R0", "L0")) or opportunity < 56):
            front = "否｜不占前排"
        elif is_formal:
            front = "是｜Formal保留"

        reason = (
            f"10日預估{expected10:+.2f}%/上漲空間{headroom:.1f}；RR{rr:.2f}/{rr_score:.1f}；"
            f"近期成熟{recent_n}筆、SelectionAlpha{recent_alpha:+.2f}pp、正Alpha率{recent_pos:.0f}%；"
            f"近5次入榜{near5}/連續{consecutive}/歷史成熟{history_n}；"
            f"重複慣性扣{repeat_pen:.1f}/機會成本扣{opp_pen:.1f}；機會價值{opportunity:.1f}。"
        )
        rows.append({
            "H61近期成熟樣本": recent_n,
            "H61近期SelectionAlpha%": round(recent_alpha, 4),
            "H61近期正Alpha率%": round(recent_pos, 2),
            "H61歷史成熟樣本": history_n,
            "H61上漲空間分": round(headroom, 2),
            "H61近期Alpha分": round(alpha_score, 2),
            "H61RR品質分": round(rr_score, 2),
            "H61新鮮機會分": round(fresh_score, 2),
            "H61重複慣性扣分": round(repeat_pen, 2),
            "H61機會成本扣分": round(opp_pen, 2),
            "H61機會價值分": round(opportunity, 2),
            "H61機會層級": tier,
            "H61前排資格": front,
            "H61決策理由": reason,
            "H61版本": VERSION,
        })
    addon = pd.DataFrame(rows, index=work.index)
    for c in H61_COLUMNS:
        work[c] = addon[c]
    return work


__all__ = ["VERSION", "H61_COLUMNS", "apply_h61_opportunity_cost_engine"]
