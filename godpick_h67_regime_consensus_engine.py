# -*- coding: utf-8 -*-
"""V191-H67 Market Regime × Sector Flow × Signal Consensus Governance.

H67 is a research-priority overlay on top of H66.  It exists because a high
H66 T+1 score must not become a broad A1 list when the market regime is weak,
the stock's sector is losing money-flow, or the key timing pillars disagree.

Authority boundary
------------------
H67 NEVER creates or upgrades H64 Formal authority.  P1/P2/C1 are research
priority labels only. Execution still requires the existing H64/H56/Entry/RR
chain. H67 also makes next-session pre-open revalidation explicit.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h67_regime_sector_consensus_preopen_truth_20260910"

H67_COLUMNS = [
    "H67市場Regime分", "H67市場Regime調整", "H67族群資金分", "H67族群資金調整",
    "H67關鍵訊號一致性分", "H67一致性調整", "H67追價耗竭扣分", "H67盤前再確認狀態",
    "H67T1治理原始分", "H67T1治理分", "H67全市場百分位%", "H67全市場順位",
    "H67研究優先層級", "H67研究建議", "H67升級缺口", "H67治理理由", "H67權威邊界", "H67版本",
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


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(v)))


def _num(row: pd.Series | dict[str, Any], names: Iterable[str], default: float | None = None) -> float | None:
    idx = row.index if isinstance(row, pd.Series) else row.keys()
    for c in names:
        if c in idx:
            x = _f(row.get(c), None)
            if x is not None:
                return x
    return default


def _txt(row: pd.Series | dict[str, Any], names: Iterable[str], default: str = "") -> str:
    idx = row.index if isinstance(row, pd.Series) else row.keys()
    for c in names:
        if c in idx:
            t = _s(row.get(c))
            if t:
                return t
    return default


def _weighted(values: list[tuple[float | None, float]], default: float = 50.0) -> float:
    vals = [(float(v), float(w)) for v, w in values if v is not None and math.isfinite(float(v)) and w > 0]
    if not vals:
        return default
    den = sum(w for _, w in vals)
    return _clip(sum(v * w for v, w in vals) / den)


def _market_regime(row: pd.Series) -> tuple[float, float, str]:
    base = _weighted([
        (_num(row, ["大盤橋接分數"], None), 0.34),
        (_num(row, ["市場環境分數"], None), 0.26),
        (_num(row, ["SuperAI市場情境分"], None), 0.18),
        (_num(row, ["H42市場共識分"], None), 0.12),
        (_num(row, ["市場廣度%", "大盤廣度%", "上漲家數占比%"], None), 0.10),
    ], 50.0)
    text = "｜".join([
        _txt(row, ["大盤策略模式"]), _txt(row, ["大盤風險燈號"]), _txt(row, ["大盤風險等級"]),
        _txt(row, ["市場環境"]), _txt(row, ["AI市場狀態"]), _txt(row, ["H42市場情境"]),
        _txt(row, ["極端市場LOCKDOWN"]), _txt(row, ["大盤風控層級"]),
    ])
    adj = 0.0
    if base >= 68: adj += 3.0
    elif base >= 58: adj += 1.0
    elif base < 35: adj -= 10.0
    elif base < 43: adj -= 7.0
    elif base < 50: adj -= 4.0
    elif base < 55: adj -= 2.0
    risk_terms = ["中高風險", "高風險", "偏空", "弱勢", "黃燈", "紅燈", "LOCKDOWN", "RISK-OFF", "保守"]
    if any(k.upper() in text.upper() for k in risk_terms):
        adj -= 3.0
    if any(k in text for k in ["紅燈", "LOCKDOWN", "極高風險"]):
        adj -= 4.0
    if any(k in text for k in ["低風險", "偏多", "綠燈", "攻擊"]):
        adj += 1.5
    return _clip(base), max(-14.0, min(5.0, adj)), text


def _sector_flow(row: pd.Series) -> tuple[float, float, str]:
    score = _weighted([
        (_num(row, ["族群資金流分數"], None), 0.30),
        (_num(row, ["H53族群資金分"], None), 0.20),
        (_num(row, ["H53族群共振分"], None), 0.15),
        (_num(row, ["H51族群主線分"], None), 0.15),
        (_num(row, ["H50族群可買主流分"], None), 0.12),
        (_num(row, ["族群輪動分"], None), 0.08),
    ], 50.0)
    text = "｜".join([
        _txt(row, ["強勢族群等級"]), _txt(row, ["族群輪動狀態"]), _txt(row, ["族群策略建議"]),
        _txt(row, ["族群資金流說明"]), _txt(row, ["資金流熱門族群"]), _txt(row, ["資金輪動角色"]),
    ])
    adj = 0.0
    if score >= 70: adj += 4.0
    elif score >= 62: adj += 2.0
    elif score < 38: adj -= 9.0
    elif score < 45: adj -= 6.0
    elif score < 52: adj -= 3.0
    bad = ["資金退潮", "弱勢", "資金不足", "流出", "退燒", "非主流", "轉弱"]
    good = ["資金流入", "強勢", "主流確認", "續強", "領漲"]
    if any(k in text for k in bad): adj -= 5.0
    if any(k in text for k in ["資金退潮", "明顯流出"]): adj -= 3.0
    if any(k in text for k in good): adj += 2.0
    return _clip(score), max(-15.0, min(6.0, adj)), text


def _consensus(row: pd.Series) -> tuple[float, float, list[str]]:
    pillars = {
        "收盤": _num(row, ["H66收盤品質分"], None),
        "法人": _num(row, ["H66法人加速度分"], None),
        "點火": _num(row, ["H66主流點火分"], None),
        "技術": _num(row, ["H66技術買點分"], None),
        "量能": _num(row, ["H66量能流動性分"], None),
        "結構": _num(row, ["H66結構品質分"], None),
    }
    vals = [(k, v) for k, v in pillars.items() if v is not None]
    if not vals:
        return 50.0, 0.0, []
    score = sum(v for _, v in vals) / len(vals)
    strong = sum(1 for _, v in vals if v >= 65)
    weak = sum(1 for _, v in vals if v < 50)
    critical_weak = [k for k, v in vals if k in {"收盤", "法人", "點火", "技術"} and v < 52]
    adj = 0.0
    if strong >= 5 and weak == 0: adj += 4.0
    elif strong >= 4 and weak <= 1: adj += 2.0
    if weak >= 2: adj -= 5.0
    if len(critical_weak) >= 2: adj -= 4.0
    if score < 55: adj -= 3.0
    return _clip(score), max(-10.0, min(5.0, adj)), critical_weak


def _chase_exhaustion_penalty(row: pd.Series) -> tuple[float, list[str]]:
    day_ret = _num(row, ["今日漲幅%", "當日漲跌幅%", "今日漲跌幅%", "漲跌幅%"], None)
    chase = _num(row, ["追價風險分", "追價風險分數", "追高風險分數_決策"], 50.0) or 50.0
    exhaust = _num(row, ["H54耗竭風險分", "隔日耗竭風險分", "隔日耗竭風險分數"], 50.0) or 50.0
    ext = _num(row, ["H49延伸風險扣分"], 0.0) or 0.0
    close_pos = _num(row, ["當日收盤位置%"], None)
    pen = 0.0; reasons = []
    if day_ret is not None:
        if day_ret >= 9.0: pen += 7.0; reasons.append(f"單日+{day_ret:.1f}%追價")
        elif day_ret >= 7.0: pen += 5.0; reasons.append(f"單日+{day_ret:.1f}%延伸")
        elif day_ret >= 5.5: pen += 2.5
    if chase >= 75: pen += 5.0; reasons.append(f"追價風險{chase:.0f}")
    elif chase >= 65: pen += 3.0
    if exhaust >= 75: pen += 5.0; reasons.append(f"耗竭{exhaust:.0f}")
    elif exhaust >= 65: pen += 3.0
    if ext >= 6: pen += min(4.0, ext * 0.4); reasons.append(f"延伸扣分{ext:.1f}")
    if day_ret is not None and day_ret >= 5.5 and close_pos is not None and close_pos < 55:
        pen += 3.0; reasons.append("大漲但收盤位置不佳")
    return min(18.0, pen), reasons


def _preopen_status(row: pd.Series) -> tuple[str, bool]:
    t1 = _num(row, ["H56T1確認分"], None)
    text = "｜".join([
        _txt(row, ["H56最終參考層級", "H56盤前狀態"]), _txt(row, ["H56盤前重驗需求"]),
        _txt(row, ["盤中二段確認要求"]), _txt(row, ["盤中觸發確認條件"]), _txt(row, ["H56隔夜證據狀態"]),
    ])
    upper = text.upper()
    if any(k in upper for k in ["BLOCK", "禁買", "LOCKDOWN"]):
        return "BLOCK｜盤前不得升級", True
    if (t1 is not None and t1 >= 72) and not any(k in text for k in ["重驗", "待確認", "二段確認"]):
        return "CONFIRMED｜已有H56確認，但次日仍需價格守價", False
    return "RECHECK｜次日盤前必須重驗大盤/族群/Entry/RR", False


def apply_h67_regime_consensus(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy().reset_index(drop=True)
    try:
        from godpick_h66_adaptive_timing_engine import VERSION as H66_VERSION, apply_h66_adaptive_timing
        hv = work.get("H66版本", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
        if not hv.eq(H66_VERSION).all():
            work = apply_h66_adaptive_timing(work)
    except Exception:
        pass

    metrics = []
    for _, row in work.iterrows():
        h66 = _num(row, ["H66T1自適應排序分"], 50.0) or 50.0
        mscore, madj, mtxt = _market_regime(row)
        sscore, sadj, stxt = _sector_flow(row)
        cscore, cadj, cweak = _consensus(row)
        chase_pen, chase_reasons = _chase_exhaustion_penalty(row)
        preopen, pre_block = _preopen_status(row)
        raw = _clip(h66 + madj + sadj + cadj - chase_pen)
        score = raw
        # Structural ceilings: weak market + weak sector must not remain top priority.
        if madj <= -7 and sadj <= -6:
            score = min(score, 66.0)
        if cscore < 55 or len(cweak) >= 2:
            score = min(score, 68.0)
        if pre_block:
            score = min(score, 54.0)
        if chase_pen >= 10:
            score = min(score, 64.0)
        metrics.append({
            "market_score": mscore, "market_adj": madj, "market_text": mtxt,
            "sector_score": sscore, "sector_adj": sadj, "sector_text": stxt,
            "consensus": cscore, "consensus_adj": cadj, "critical_weak": cweak,
            "chase": chase_pen, "chase_reasons": chase_reasons,
            "preopen": preopen, "pre_block": pre_block, "raw": raw, "score": _clip(score),
        })

    scores = pd.Series([m["score"] for m in metrics], index=work.index, dtype="float64")
    pct = scores.rank(method="average", pct=True).mul(100.0)
    ranks = scores.rank(method="first", ascending=False).astype(int)
    out_rows = []
    for pos, idx in enumerate(work.index):
        row = work.loc[idx]; m = metrics[pos]
        p = float(pct.loc[idx]); rank = int(ranks.loc[idx]); score = float(m["score"])
        h66tier = _txt(row, ["H66T1層級"])
        h64eff = _txt(row, ["H64有效權威", "H63有效權威", "H62有效權威"])
        if score >= 76 and p >= 90 and m["consensus"] >= 66 and m["market_adj"] > -7 and m["sector_adj"] > -6 and m["chase"] < 8 and h66tier.startswith(("A1", "A2")):
            tier = "P1｜次日優先研究"
            rec = "優先｜盤前重驗通過後再看Entry/RR；非Formal"
        elif score >= 68 and p >= 72 and m["consensus"] >= 58 and m["market_adj"] > -11 and m["sector_adj"] > -11:
            tier = "P2｜次日次優先研究"
            rec = "次優先｜等待大盤/族群/價格確認；非Formal"
        elif score >= 58 and p >= 45:
            tier = "C1｜條件觀察"
            rec = "條件式觀察｜不追價"
        else:
            tier = "R0｜僅研究"
            rec = "低優先｜等待新證據"
        if m["pre_block"]:
            tier = "R0｜上游BLOCK"
            rec = "否｜盤前權威BLOCK"

        gaps = []
        if m["market_adj"] <= -4: gaps.append(f"大盤Regime偏弱({m['market_score']:.0f})")
        if m["sector_adj"] <= -4: gaps.append(f"族群資金不足({m['sector_score']:.0f})")
        if m["consensus"] < 62: gaps.append(f"訊號一致性{m['consensus']:.0f}不足")
        if m["critical_weak"]: gaps.append("關鍵弱項:" + "/".join(m["critical_weak"][:3]))
        if m["chase"] >= 5: gaps.append(f"追價耗竭-{m['chase']:.1f}")
        if h64eff != "EFFECTIVE-FORMAL": gaps.append("H64非有效Formal")
        if not gaps: gaps.append("只差次日H56/Entry/RR實際確認")
        reasons = [
            f"H66={_num(row,['H66T1自適應排序分'],50.0):.1f}", f"市場{m['market_adj']:+.1f}",
            f"族群{m['sector_adj']:+.1f}", f"一致性{m['consensus']:.0f}({m['consensus_adj']:+.1f})", f"追價-{m['chase']:.1f}",
            m["preopen"],
        ]
        out_rows.append({
            "H67市場Regime分": round(m["market_score"], 2), "H67市場Regime調整": round(m["market_adj"], 2),
            "H67族群資金分": round(m["sector_score"], 2), "H67族群資金調整": round(m["sector_adj"], 2),
            "H67關鍵訊號一致性分": round(m["consensus"], 2), "H67一致性調整": round(m["consensus_adj"], 2),
            "H67追價耗竭扣分": round(m["chase"], 2), "H67盤前再確認狀態": m["preopen"],
            "H67T1治理原始分": round(m["raw"], 2), "H67T1治理分": round(score, 2),
            "H67全市場百分位%": round(p, 2), "H67全市場順位": rank,
            "H67研究優先層級": tier, "H67研究建議": rec,
            "H67升級缺口": "；".join(gaps[:5]), "H67治理理由": "；".join(reasons) + f"；H64={h64eff or '未授權'}",
            "H67權威邊界": "P1/P2/C1只代表研究優先序；不得升級H64 Formal，執行仍須H56盤前＋Entry守價＋RR。",
            "H67版本": VERSION,
        })
    addon = pd.DataFrame(out_rows, index=work.index)
    for c in H67_COLUMNS:
        work[c] = addon[c]
    return work


def _tier_priority(v: str) -> int:
    t = _s(v)
    if t.startswith("P1"): return 40
    if t.startswith("P2"): return 30
    if t.startswith("C1"): return 20
    return 10


def build_h67_priority_table(frame: pd.DataFrame, max_rows: int = 20, max_per_sector: int = 2) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame({"H67研究優先層級": ["R0｜目前沒有候選"], "H67研究建議": ["請先完成全市場掃描"]})
    v = frame.get("H67版本", pd.Series([""] * len(frame), index=frame.index)).fillna("").astype(str)
    work = frame if v.eq(VERSION).all() else apply_h67_regime_consensus(frame)
    out = work.copy()
    out["_H67tier"] = out.get("H67研究優先層級", pd.Series([""] * len(out), index=out.index)).astype(str).map(_tier_priority)
    out.sort_values(["_H67tier", "H67T1治理分", "H67全市場百分位%", "H67關鍵訊號一致性分"], ascending=False, kind="mergesort", inplace=True)
    target = max(1, int(max_rows or 20)); cap = max(1, int(max_per_sector or 2))
    chosen=[]; seen=set(); counts={}
    for _, r in out.iterrows():
        code=_txt(r,["股票代號","代號"]); sector=_txt(r,["類別","族群名稱","產業"],"未分類")
        if not code or code in seen or counts.get(sector,0)>=cap: continue
        chosen.append(r); seen.add(code); counts[sector]=counts.get(sector,0)+1
        if len(chosen)>=target: break
    if len(chosen)<target:
        for _, r in out.iterrows():
            code=_txt(r,["股票代號","代號"])
            if not code or code in seen: continue
            chosen.append(r); seen.add(code)
            if len(chosen)>=target: break
    res=pd.DataFrame(chosen).reset_index(drop=True) if chosen else out.head(target).copy().reset_index(drop=True)
    res["H67研究順位"] = range(1,len(res)+1)
    res.drop(columns=["_H67tier"], errors="ignore", inplace=True)
    front=[c for c in [
        "H67研究順位","股票代號","股票名稱","類別","H67研究優先層級","H67研究建議","H67T1治理分","H67全市場百分位%","H67全市場順位",
        "H67市場Regime分","H67市場Regime調整","H67族群資金分","H67族群資金調整","H67關鍵訊號一致性分","H67一致性調整","H67追價耗竭扣分","H67盤前再確認狀態","H67升級缺口","H67治理理由",
        "H66T1層級","H66T1自適應排序分","H66收盤品質分","H66法人加速度分","H66主流點火分","H66技術買點分","H64有效權威","H67權威邊界"
    ] if c in res.columns]
    return res[front + [c for c in res.columns if c not in front]]


def build_h67_governance_table(frame: pd.DataFrame) -> pd.DataFrame:
    work = apply_h67_regime_consensus(frame) if isinstance(frame, pd.DataFrame) and not frame.empty else pd.DataFrame()
    if work.empty:
        return pd.DataFrame({"治理項目":["H67"],"目前狀態":["無資料"]})
    tier=work["H67研究優先層級"].fillna("").astype(str)
    return pd.DataFrame([
        {"治理項目":"P1次日優先研究", "目前狀態":int(tier.str.startswith("P1").sum()), "治理原則":"弱大盤/弱族群/低一致性不得大量進P1"},
        {"治理項目":"P2次日次優先", "目前狀態":int(tier.str.startswith("P2").sum()), "治理原則":"仍須次日盤前重驗"},
        {"治理項目":"市場Regime中位調整", "目前狀態":round(float(pd.to_numeric(work["H67市場Regime調整"],errors="coerce").median()),2), "治理原則":"弱環境負向、強環境僅小幅加分"},
        {"治理項目":"族群資金中位調整", "目前狀態":round(float(pd.to_numeric(work["H67族群資金調整"],errors="coerce").median()),2), "治理原則":"資金退潮直接降研究優先"},
        {"治理項目":"一致性中位數", "目前狀態":round(float(pd.to_numeric(work["H67關鍵訊號一致性分"],errors="coerce").median()),2), "治理原則":"收盤/法人/點火/技術至少不可同時多項偏弱"},
        {"治理項目":"Formal權威", "目前狀態":"UNCHANGED", "治理原則":"H67不可建立/升級Formal，只治理研究排序"},
    ])


__all__ = ["VERSION","H67_COLUMNS","apply_h67_regime_consensus","build_h67_priority_table","build_h67_governance_table"]
