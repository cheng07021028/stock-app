# -*- coding: utf-8 -*-
"""V191-H64 strong-mainstream-holder core truth engine.

H64 fixes a structural problem exposed after H62/H63: a candidate could earn a
high incremental-opportunity rank from recent Alpha / headroom / acceleration
while *simultaneously* failing the older H42/H47 current-strong-mainstream gate.
That made research pages look busy with stocks that were not actually current
leaders.

H64 therefore separates three current-run truths and requires alignment:
  1) TRUE STRENGTH: current relative strength / leader quality / price action.
  2) TRUE MAINSTREAM: fine-grained sector resonance / fresh-mainline evidence.
  3) HOLDER LOCK TREND: TDCC actual holding must have a prior snapshot before it
     can be called "locking". High absolute ownership without a delta is only
     "ownership known, trend unconfirmed".

H64 never manufactures BUY authority. It can only tighten an H62 effective
Formal into FORMAL-QUALITY-HOLD, or promote non-formal names to a research tier.
"""
from __future__ import annotations

from typing import Any
import math
import pandas as pd

VERSION = "v191_h64_strong_mainstream_holder_core_truth_20260908"

H64_COLUMNS = [
    "H64真強勢分", "H64真強勢狀態", "H64主流真相分", "H64主流真相狀態",
    "H64鎖碼趨勢狀態", "H64鎖碼確認分", "H64核心共振分", "H64品質閘門",
    "H64有效權威", "H64研究層級", "H64前排資格", "H64正式作戰資格",
    "H64決策理由", "H64版本",
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


def _f(v: Any, default: float | None = 0.0) -> float | None:
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


def _num(row: pd.Series, names: list[str], default: float = 0.0) -> float:
    for c in names:
        if c in row.index and _s(row.get(c)):
            return float(_f(row.get(c), default) or default)
    return float(default)


def _txt(row: pd.Series, names: list[str], default: str = "") -> str:
    for c in names:
        if c in row.index:
            t = _s(row.get(c))
            if t:
                return t
    return default


def _strength_truth(row: pd.Series) -> tuple[float, str, bool, bool]:
    h42 = _num(row, ["H42強勢分"], 50.0)
    h42_status = _txt(row, ["H42強勢狀態"], "")
    rs = _num(row, ["H47個股相對強度分"], 50.0)
    leader = _num(row, ["H51個股領漲品質分"], 50.0)
    cash = _num(row, ["H57資金加速度分"], 50.0)
    rs_turn = _num(row, ["H57相對強度轉折分"], 50.0)
    ignition = _num(row, ["H51發動潛力分", "H57飆股發動前兆分"], 50.0)
    close_pos = _num(row, ["當日收盤位置%"], 50.0)
    ret1 = _num(row, ["今日漲幅%", "當日漲跌幅%"], 0.0)
    phase = _txt(row, ["H57前兆階段"], "")

    score = _clip(
        h42 * 0.22 + rs * 0.20 + leader * 0.18 + cash * 0.12 + rs_turn * 0.10
        + ignition * 0.10 + close_pos * 0.08
        - max(0.0, -ret1 - 4.0) * 2.0
    )
    legacy_pass = bool(h42_status) and not h42_status.startswith("S-NO") and h42 >= 58 and rs >= 58 and leader >= 55
    early_pass = phase.startswith(("PI2", "PI3", "IG1")) and cash >= 65 and rs_turn >= 60 and leader >= 52 and close_pos >= 58
    hard = legacy_pass or early_pass
    if legacy_pass and score >= 66:
        status = "S1｜當前強勢領漲確認"
    elif early_pass and score >= 62:
        status = "S2｜新強勢正在形成"
    elif score >= 60:
        status = "S3｜強度接近但未過門"
    else:
        status = "S0｜非當前強勢優先"
    return score, status, hard, early_pass


def _mainstream_truth(row: pd.Series) -> tuple[float, str, bool, bool]:
    sector = _num(row, ["H51族群主線分"], 50.0)
    resonance = _num(row, ["H53族群共振分"], 50.0)
    breadth = _num(row, ["H53族群廣度分"], 50.0)
    attack = _num(row, ["H53族群攻擊分"], 50.0)
    premain = _num(row, ["H57主流形成前兆分"], 50.0)
    mainrise = _num(row, ["H60主升段分"], 50.0)
    market = _txt(row, ["H51市場地位"], "")
    main_stage = _txt(row, ["H60主升階段"], "")

    score = _clip(sector * 0.28 + resonance * 0.24 + breadth * 0.12 + attack * 0.10 + premain * 0.12 + mainrise * 0.14)
    explicit_main = (
        sector >= 60 and resonance >= 60 and
        (main_stage.startswith(("MR1", "MR2")) or market.startswith(("HM-EARLY", "HM-PULLBACK", "HM-LEADER")))
    )
    forming_main = premain >= 65 and resonance >= 56 and breadth >= 55 and attack >= 55
    hard = explicit_main or forming_main
    if explicit_main and score >= 64:
        status = "M1｜當前主流確認"
    elif forming_main and score >= 60:
        status = "M2｜新主流形成中"
    elif sector >= 58 or resonance >= 58:
        status = "M3｜次主流/輪動觀察"
    else:
        status = "M0｜非主流優先"
    return score, status, hard, forming_main


def _lock_truth(row: pd.Series) -> tuple[float, str, bool]:
    source = _txt(row, ["H60鎖碼來源"], "")
    ratio = _f(row.get("H60千張大戶持股比%"), None)
    delta = _f(row.get("H60千張大戶週變化pp"), None)
    proxy = _num(row, ["H60大戶鎖碼真相分"], 50.0)

    if source.startswith("ACTUAL"):
        if delta is None:
            # Absolute ownership is useful context but is NOT evidence of locking.
            score = _clip((ratio if ratio is not None else 50.0) * 0.55 + 22.0)
            return min(score, 64.0), "LU｜TDCC持股已知但缺前期，鎖碼趨勢未確認", False
        ratio_v = ratio if ratio is not None else 50.0
        score = _clip(48.0 + (ratio_v - 40.0) * 0.55 + delta * 14.0)
        if delta <= -0.50:
            return min(score, 48.0), "LD｜TDCC千張大戶減碼", False
        if delta >= 0.20 and ratio_v >= 50.0:
            return max(score, 72.0), "LC｜TDCC千張大戶增持鎖碼確認", True
        if -0.20 <= delta < 0.20 and ratio_v >= 65.0:
            return max(score, 68.0), "LS｜TDCC高持股穩定鎖碼", True
        return score, "LW｜TDCC持股趨勢普通，尚未確認鎖碼", False
    if source.startswith("PROXY"):
        return min(proxy, 62.0), "LP｜代理籌碼，非TDCC真實鎖碼", False
    return 45.0, "L0｜無可驗證大戶鎖碼資料", False


def apply_h64_core_truth(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy()
    if "H62有效權威" not in work.columns:
        try:
            from godpick_h62_incremental_opportunity_engine import apply_h62_incremental_opportunity_engine
            work = apply_h62_incremental_opportunity_engine(work)
        except Exception:
            pass

    rows: list[dict[str, Any]] = []
    for _, row in work.iterrows():
        sscore, sstate, strong_pass, early_strength = _strength_truth(row)
        mscore, mstate, main_pass, forming_main = _mainstream_truth(row)
        lscore, lstate, lock_pass = _lock_truth(row)
        rrq = _num(row, ["H61RR品質分"], 50.0)
        h62 = _num(row, ["H62增量機會分"], 50.0)
        expected10 = _num(row, ["H32_10日預估報酬%", "10日預估報酬%"], 0.0)
        exhaust = _num(row, ["H54耗竭風險分"], 50.0)
        auth = _txt(row, ["H62有效權威"], "UNKNOWN").upper()

        core = _clip(sscore * 0.31 + mscore * 0.31 + lscore * 0.18 + rrq * 0.10 + h62 * 0.10 - max(0.0, exhaust - 72.0) * 0.25)
        executable_quality = rrq >= 58 and (expected10 <= 0 or expected10 >= 1.5) and exhaust < 82
        core_pass = strong_pass and main_pass and lock_pass and executable_quality
        core_wait_lock = strong_pass and main_pass and executable_quality and not lock_pass
        early_main = (early_strength or forming_main) and (strong_pass or main_pass) and core >= 58

        if core_pass:
            gate = "PASS｜強勢×主流×TDCC鎖碼×RR對齊"
        elif core_wait_lock:
            gate = "WAIT-LOCK｜強勢主流成立，但大戶鎖碼趨勢未確認"
        elif not strong_pass:
            gate = "BLOCK-STRENGTH｜未通過當前強勢/相對強度"
        elif not main_pass:
            gate = "BLOCK-MAINSTREAM｜未通過當前主流/族群共振"
        else:
            gate = "BLOCK-EXEC｜RR/耗竭/增量空間未完成"

        if auth == "EFFECTIVE-FORMAL":
            if core_pass:
                eff = "EFFECTIVE-FORMAL"
                battle = "是｜H64核心正式作戰"
                tier = "C1｜核心強勢主流Formal"
                front = "是｜正式核心"
            else:
                eff = "FORMAL-QUALITY-HOLD"
                battle = "否｜H64品質暫停"
                tier = "CH｜Formal品質重驗"
                front = "否｜Formal品質暫停"
        elif auth in {"FORMAL-HOLD", "FORMAL-QUALITY-HOLD"}:
            eff = auth
            battle = "否｜暫停"
            tier = "CH｜Formal品質重驗"
            front = "否｜Formal暫停"
        else:
            eff = auth
            battle = "否"
            if core_pass:
                tier = "C2｜核心強勢主流研究"
                front = "是｜核心研究"
            elif core_wait_lock:
                tier = "C3｜強勢主流待鎖碼確認"
                front = "是｜待鎖碼研究"
            elif early_main and not gate.startswith("BLOCK-STRENGTH"):
                tier = "E1｜新主流形成研究"
                front = "是｜新主流研究"
            else:
                tier = "D0｜非核心候選降權"
                front = "否｜不占前排"

        reason = (
            f"強勢{sscore:.1f}/{sstate}；主流{mscore:.1f}/{mstate}；鎖碼{lscore:.1f}/{lstate}；"
            f"RR品質{rrq:.1f}/10日預估{expected10:+.2f}%/耗竭{exhaust:.1f}；"
            f"H62增量{h62:.1f}；品質閘門={gate}；H62權威={auth}→H64有效權威={eff}。"
        )
        rows.append({
            "H64真強勢分": round(sscore, 2), "H64真強勢狀態": sstate,
            "H64主流真相分": round(mscore, 2), "H64主流真相狀態": mstate,
            "H64鎖碼趨勢狀態": lstate, "H64鎖碼確認分": round(lscore, 2),
            "H64核心共振分": round(core, 2), "H64品質閘門": gate,
            "H64有效權威": eff, "H64研究層級": tier, "H64前排資格": front,
            "H64正式作戰資格": battle, "H64決策理由": reason, "H64版本": VERSION,
        })

    addon = pd.DataFrame(rows, index=work.index)
    for c in H64_COLUMNS:
        work[c] = addon[c]

    # Cross-sectional ranking is used only inside quality tiers, never to turn a
    # failing row into a core row.
    work["H64全市場核心百分位%"] = work["H64核心共振分"].rank(method="average", pct=True).mul(100).round(2)
    return work


def build_h64_single_decision_truth_table(frame: pd.DataFrame, max_rows: int = 10) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame({
            "H64唯一決策": ["NONE｜沒有核心強勢主流候選"],
            "H64是否可買": ["否"],
            "現在該做什麼": ["等待強勢、主流與可驗證大戶鎖碼重新對齊；不為推薦而推薦。"],
        })
    work = frame if ("H64版本" in frame.columns and frame["H64版本"].astype(str).eq(VERSION).all()) else apply_h64_core_truth(frame)
    tier = work.get("H64研究層級", pd.Series([""]*len(work), index=work.index)).fillna("").astype(str)
    eff = work.get("H64有效權威", pd.Series([""]*len(work), index=work.index)).fillna("").astype(str)
    keep = eff.eq("EFFECTIVE-FORMAL") | tier.str.startswith(("C2", "C3", "E1"))
    out = work.loc[keep].copy()
    if out.empty:
        return pd.DataFrame({
            "H64唯一決策": ["NONE｜沒有核心強勢主流候選"],
            "H64是否可買": ["否"],
            "現在該做什麼": ["本輪候選未同時通過當前強勢、主流共振與鎖碼真相；寧可空手等待。"],
        })
    formal = out["H64有效權威"].astype(str).eq("EFFECTIVE-FORMAL")
    decisions=[]; buys=[]; actions=[]; ranks=[]
    for idx in out.index:
        t=str(out.at[idx,"H64研究層級"])
        if bool(formal.loc[idx]):
            decisions.append("F1｜H64核心正式推薦")
            buys.append("否｜仍需H56盤前/觸發/守價/RR")
            actions.append("核心強勢主流Formal；盤前再確認，未觸發不買。")
            ranks.append(100)
        elif t.startswith("C2"):
            decisions.append("C2｜核心強勢主流研究")
            buys.append("否｜研究")
            actions.append("強勢×主流×鎖碼已對齊，但上游未正式授權；只列研究。")
            ranks.append(75)
        elif t.startswith("C3"):
            decisions.append("C3｜強勢主流待鎖碼")
            buys.append("否｜研究")
            actions.append("強勢與主流成立，但TDCC鎖碼趨勢尚未確認；等待下一期持股變化。")
            ranks.append(65)
        else:
            decisions.append("E1｜新主流形成研究")
            buys.append("否｜研究")
            actions.append("新主流/新強勢形成中；等待主流確認、鎖碼與Formal/V188完成。")
            ranks.append(55)
    out["H64唯一決策"] = decisions
    out["H64是否可買"] = buys
    out["現在該做什麼"] = actions
    out["_H64rank"] = ranks
    out["_H64formal"] = formal.astype(int)
    out.sort_values(["_H64formal", "_H64rank", "H64核心共振分", "H64全市場核心百分位%", "H62增量機會分"], ascending=False, inplace=True, kind="mergesort")
    formal_n=int(out["_H64formal"].sum())
    out=out.head(max(max_rows, formal_n, 1)).copy()
    out["唯一順位"] = range(1, len(out)+1)
    out.drop(columns=["_H64rank","_H64formal"], errors="ignore", inplace=True)
    front=[c for c in [
        "唯一順位","股票代號","股票名稱","類別","H64唯一決策","H64是否可買","現在該做什麼",
        "H64研究層級","H64品質閘門","H64真強勢狀態","H64真強勢分","H64主流真相狀態","H64主流真相分",
        "H64鎖碼趨勢狀態","H64鎖碼確認分","H64核心共振分","H64全市場核心百分位%","H64有效權威",
        "H62機會層級","H61近期SelectionAlpha%","H60主升階段","H60大戶鎖碼層級","H60千張大戶持股比%","H60千張大戶週變化pp",
        "H56盤前狀態","H51市場地位","H51交易許可","H51路徑RR","H64決策理由"
    ] if c in out.columns]
    rest=[c for c in out.columns if c not in front]
    return out.loc[:,front+rest]


def build_h64_core_research_table(frame: pd.DataFrame, max_rows: int = 30) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    work = frame if ("H64版本" in frame.columns and frame["H64版本"].astype(str).eq(VERSION).all()) else apply_h64_core_truth(frame)
    tier=work["H64研究層級"].fillna("").astype(str)
    out=work.loc[tier.str.startswith(("C1","C2","C3","E1","CH"))].copy()
    if out.empty:
        return out
    order=tier.loc[out.index].map(lambda x: 100 if x.startswith("C1") else 90 if x.startswith("C2") else 80 if x.startswith("C3") else 70 if x.startswith("E1") else 40)
    out["_H64order"]=order
    out.sort_values(["_H64order","H64核心共振分","H64全市場核心百分位%"], ascending=False, inplace=True, kind="mergesort")
    cols=[c for c in [
        "股票代號","股票名稱","類別","H64研究層級","H64品質閘門","H64有效權威","H64核心共振分","H64全市場核心百分位%",
        "H64真強勢狀態","H64真強勢分","H64主流真相狀態","H64主流真相分","H64鎖碼趨勢狀態","H64鎖碼確認分",
        "H60千張大戶持股比%","H60千張大戶週變化pp","H57前兆階段","H51市場地位","H51路徑RR","H64決策理由"
    ] if c in out.columns]
    return out.head(max(1,int(max_rows)))[cols].reset_index(drop=True)


__all__ = ["VERSION","H64_COLUMNS","apply_h64_core_truth","build_h64_single_decision_truth_table","build_h64_core_research_table"]
