# -*- coding: utf-8 -*-
"""V191-H76 Daily New-Alpha / Structural-Core split and repeat-evidence gate.

Why H76 exists
--------------
H74 correctly moved fresh money and current leadership ahead of stale quality, and H75
made Excel readable.  A remaining source of confusion was that structurally excellent
large-cap names could still appear at the top of older H72/H73 research tables on many
runs.  Users could reasonably read that as a *new daily recommendation* even when the
stock had no sufficiently new evidence that day.

H76 therefore separates two different questions:

1. DAILY NEW ALPHA: what has *new* cross-sectional evidence today and deserves scarce
   daily attention?
2. STRUCTURAL CORE: what remains a high-quality / institutional / long-run research name
   but does not have enough new evidence to consume today's new-alpha slots?

Safety / authority boundaries
-----------------------------
* Decision-time only. H76 uses only columns available in the current decision snapshot.
* No stock symbol/name is hard-coded. The same repeat-evidence rule applies to all names.
* A familiar stock is NOT permanently banned. It can return to Daily New Alpha only when
  fresh evidence renews (strength/mainstream/institution/capital/freshness/TDCC truth).
* H76 never creates Formal, never changes H64/H63 identity and never bypasses H68.
"""
from __future__ import annotations

from typing import Any
import math
import pandas as pd

VERSION = "v191_h76_daily_alpha_core_split_repeat_evidence_truth_20260918"

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


def _num(work: pd.DataFrame, col: str, default: float = 50.0, *, clip: bool = True) -> pd.Series:
    if col not in work.columns:
        return pd.Series([default] * len(work), index=work.index, dtype=float)
    s = pd.to_numeric(work[col], errors="coerce").fillna(default).astype(float)
    return s.clip(0, 100) if clip else s


def _first_num(work: pd.DataFrame, cols: list[str], default: float = 0.0, *, clip: bool = False) -> pd.Series:
    for c in cols:
        if c in work.columns:
            s = pd.to_numeric(work[c], errors="coerce")
            if s.notna().any():
                s = s.fillna(default).astype(float)
                return s.clip(0, 100) if clip else s
    return pd.Series([default] * len(work), index=work.index, dtype=float)


def _ensure_h75(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    try:
        from godpick_h75_executive_decision_export import VERSION as H75V, apply_h75_executive_decision
        v = frame.get("H75版本", pd.Series([""] * len(frame), index=frame.index)).fillna("").astype(str)
        if not v.eq(H75V).all():
            return apply_h75_executive_decision(frame)
    except Exception:
        pass
    return frame.copy()


def _confirmed_holder(state: str) -> bool:
    s = _s(state).upper()
    return s.startswith("LOCKING") or s.startswith("ACCUMULATING")


def apply_h76_daily_alpha_core_split(frame: pd.DataFrame) -> pd.DataFrame:
    """Apply H76 without altering upstream Formal / execution authority."""
    work = _ensure_h75(frame)
    if work.empty:
        return work
    work = work.copy().reset_index(drop=True)

    h74 = _num(work, "H74決策總分", 50.0)
    strength = _num(work, "H74強勢加速度分", 50.0)
    mainstream = _num(work, "H74主流新鮮度分", 50.0)
    inst = _num(work, "H74法人資金加速度分", 50.0)
    capital = _num(work, "H74成交資金加速度分", 50.0)
    fresh = _num(work, "H74訊號新鮮分", 50.0)
    core64 = _num(work, "H64核心共振分", 50.0)
    h72risk = _num(work, "H72風險調整分", 50.0)
    h72quality = _num(work, "H72品質獲利模型分", 50.0)
    h72growth = _num(work, "H72成長動能模型分", 50.0)
    h72inst = _num(work, "H72法人需求模型分", 50.0)
    rr = _num(work, "H61RR品質分", 50.0)
    incremental = _num(work, "H62增量機會分", 50.0)

    near5 = _first_num(work, ["近5次入榜次數", "H61近5次入榜次數"], 0.0).clip(lower=0)
    consecutive = _first_num(work, ["連續入榜次數", "H61連續入榜次數"], 0.0).clip(lower=0)
    repeat61 = _first_num(work, ["H61重複慣性扣分", "重複推薦慣性扣分"], 0.0).clip(0, 30)
    repeat74 = _num(work, "H74熟面孔慣性扣分", 0.0, clip=False).clip(0, 30)

    holder_state = work.get("H74大戶鎖碼狀態", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    auth = work.get("H64有效權威", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    h68 = work.get("H68次日執行狀態", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    h72tier = work.get("H72研究層級", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    h73tier = work.get("H73研究層級", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)

    # Familiarity is deliberately broad enough to catch the prior failure mode even
    # when one historical counter is not populated in a given runtime snapshot.
    familiar = (near5 >= 3) | (consecutive >= 2) | (repeat61 >= 8) | (repeat74 >= 8)

    evidence_flags = pd.DataFrame({
        "強勢再加速": strength >= 70,
        "主流新鮮": mainstream >= 62,
        "法人加速": inst >= 68,
        "成交資金加速": capital >= 68,
        "訊號新鮮": fresh >= 65,
        "TDCC真增持": holder_state.map(_confirmed_holder),
    }, index=work.index)
    evidence_count = evidence_flags.astype(int).sum(axis=1)

    # A repeated name needs a *renewal bundle*, not one isolated strong factor.
    renewed = (
        familiar
        & (evidence_count >= 3)
        & (fresh >= 62)
        & ((mainstream >= 60) | (capital >= 68))
        & ((inst >= 64) | (capital >= 70))
        & (h74 >= 65)
    )

    repeat_gate_penalty = pd.Series([0.0] * len(work), index=work.index)
    blocked_familiar = familiar & ~renewed
    repeat_gate_penalty.loc[blocked_familiar] = (
        10.0
        + (near5.loc[blocked_familiar] - 2).clip(lower=0) * 2.5
        + (consecutive.loc[blocked_familiar] - 1).clip(lower=0) * 2.0
        + repeat61.loc[blocked_familiar] * 0.35
        + repeat74.loc[blocked_familiar] * 0.35
    ).clip(10, 24)
    # Re-accelerating familiar names still carry a small scarcity cost so new names
    # with equally strong evidence are not crowded out every day.
    repeat_gate_penalty.loc[renewed] = (
        1.5 + repeat74.loc[renewed] * 0.12 + repeat61.loc[renewed] * 0.08
    ).clip(1.5, 5.0)

    evidence_bonus = ((evidence_count - 2).clip(lower=0) * 2.0).clip(0, 8)
    daily_score = (
        h74 * 0.50 + strength * 0.10 + mainstream * 0.10 + inst * 0.10 +
        capital * 0.10 + fresh * 0.07 + incremental * 0.03 + evidence_bonus - repeat_gate_penalty
    ).clip(0, 100)

    structural_score = (
        h72risk * 0.23 + h72quality * 0.18 + h72growth * 0.13 + h72inst * 0.18 +
        core64 * 0.16 + rr * 0.06 + incremental * 0.06
    ).clip(0, 100)

    # Daily-alpha qualification is intentionally stricter than structural research.
    # Familiar names must pass `renewed`; new names do not need a history that doesn't exist.
    eligible_history = (~familiar) | renewed
    d1 = (
        (daily_score >= 70) & (h74 >= 68) & (evidence_count >= 4) & (fresh >= 60)
        & ((mainstream >= 58) | (capital >= 68)) & eligible_history
    )
    d2 = (
        ~d1 & (daily_score >= 64) & (h74 >= 63) & (evidence_count >= 3) & (fresh >= 55)
        & ((mainstream >= 55) | (inst >= 68) | (capital >= 68)) & eligible_history
    )
    formal = auth.str.upper().eq("EFFECTIVE-FORMAL")
    structural_core = (~formal) & (~d1) & (~d2) & (
        (structural_score >= 68) | h72tier.str.startswith("E1") | h73tier.str.startswith("L1") | (familiar & (structural_score >= 62))
    )

    labels: list[str] = []
    history_states: list[str] = []
    evidence_summaries: list[str] = []
    gate_results: list[str] = []
    conclusions: list[str] = []
    daily_eligible: list[str] = []

    for i in work.index:
        ev = [name for name in evidence_flags.columns if bool(evidence_flags.at[i, name])]
        ev_text = "＋".join(ev) if ev else "尚無足夠新證據"
        if formal.iat[i]:
            label = "F0｜正式推薦權威（仍需H68/盤前重驗）"
            elig = "YES｜Formal權威"
        elif d1.iat[i]:
            label = "D1｜每日新Alpha核心"
            elig = "YES｜每日新Alpha"
        elif d2.iat[i]:
            label = "D2｜每日新Alpha候選"
            elig = "YES｜每日新Alpha"
        elif structural_core.iat[i]:
            label = "C1｜結構核心監控（非每日新推薦）"
            elig = "NO｜核心監控，不佔每日Alpha名額"
        else:
            label = "R0｜一般研究"
            elig = "NO｜一般研究"

        if familiar.iat[i] and renewed.iat[i]:
            hstate = "RENEWED｜熟面孔但新證據重新達標"
            gate = f"PASS-RENEWED｜新證據{int(evidence_count.iat[i])}項"
        elif familiar.iat[i]:
            hstate = "REPEAT-GATED｜熟面孔"
            gate = f"BLOCK-DAILY｜新證據僅{int(evidence_count.iat[i])}項/新鮮度{float(fresh.iat[i]):.0f}"
        else:
            hstate = "NEW/NORMAL｜未觸發熟面孔門檻"
            gate = f"PASS-HISTORY｜新證據{int(evidence_count.iat[i])}項"

        if formal.iat[i]:
            conclusion = f"Formal權威成立；每日Alpha分{float(daily_score.iat[i]):.1f}。仍須H68與盤前價格重驗。"
        elif d1.iat[i] or d2.iat[i]:
            conclusion = f"今日新Alpha；{ev_text}。每日Alpha分{float(daily_score.iat[i]):.1f}。非Formal，勿直接當買進訊號。"
        elif structural_core.iat[i]:
            why = "重複推薦未取得足夠新證據" if familiar.iat[i] and not renewed.iat[i] else "結構品質較強但今日Alpha不足"
            conclusion = f"結構核心監控；{why}。結構分{float(structural_score.iat[i]):.1f}，不佔每日新Alpha名額。"
        else:
            conclusion = f"一般研究；今日新證據不足。每日Alpha分{float(daily_score.iat[i]):.1f}。"

        labels.append(label)
        history_states.append(hstate)
        evidence_summaries.append(ev_text)
        gate_results.append(gate)
        conclusions.append(conclusion)
        daily_eligible.append(elig)

    work["H76分流層級"] = labels
    work["H76每日新Alpha分"] = daily_score.round(2)
    work["H76結構核心分"] = structural_score.round(2)
    work["H76近5次入榜"] = near5.round(0).astype("Int64")
    work["H76連續入榜"] = consecutive.round(0).astype("Int64")
    work["H76熟面孔狀態"] = history_states
    work["H76新證據數"] = evidence_count.astype("Int64")
    work["H76新證據摘要"] = evidence_summaries
    work["H76重複推薦門檻"] = gate_results
    work["H76每日榜資格"] = daily_eligible
    work["H76重複門檻扣分"] = repeat_gate_penalty.round(2)
    work["H76主管結論"] = conclusions
    work["H76權威邊界"] = "H76只分流每日新Alpha與結構核心；不得建立Formal，不得解除H64/H63/H68權威。熟面孔不是永久封殺，須以新證據重新取得每日榜資格。"
    work["H76版本"] = VERSION

    # Deterministic ranks inside each lane.
    alpha_mask = formal | d1 | d2
    alpha_rank = pd.Series([pd.NA] * len(work), index=work.index, dtype="Int64")
    if alpha_mask.any():
        vals = work.loc[alpha_mask, "H76每日新Alpha分"] + formal.loc[alpha_mask].astype(float) * 20.0
        alpha_rank.loc[alpha_mask] = vals.rank(method="first", ascending=False).astype("Int64")
    core_mask = structural_core
    core_rank = pd.Series([pd.NA] * len(work), index=work.index, dtype="Int64")
    if core_mask.any():
        core_rank.loc[core_mask] = work.loc[core_mask, "H76結構核心分"].rank(method="first", ascending=False).astype("Int64")
    work["H76每日新Alpha順位"] = alpha_rank
    work["H76結構核心順位"] = core_rank
    return work


def build_h76_daily_alpha_table(frame: pd.DataFrame, max_rows: int = 10, max_per_sector: int = 2) -> pd.DataFrame:
    w = apply_h76_daily_alpha_core_split(frame)
    if w.empty:
        return pd.DataFrame({"今日結論": ["本輪沒有可用候選；不為推薦而推薦。"]})
    lane = w["H76分流層級"].fillna("").astype(str)
    use = w.loc[lane.str.startswith(("F0", "D1", "D2"))].copy()
    if use.empty:
        return pd.DataFrame({"今日結論": ["今天沒有通過H76『每日新Alpha』證據門檻的股票；結構好股請看02_結構核心監控。"]})
    pri = use["H76分流層級"].map(lambda x: 60 if _s(x).startswith("F0") else 50 if _s(x).startswith("D1") else 40)
    use = use.assign(_p=pri).sort_values(
        ["_p", "H76每日新Alpha分", "H76新證據數", "H74決策總分", "H74成交資金加速度分"],
        ascending=False, kind="mergesort"
    )
    sec = next((c for c in ["族群名稱", "類別", "產業別"] if c in use.columns), None)
    picks: list[int] = []
    counts: dict[str, int] = {}
    for idx, row in use.iterrows():
        group = _s(row.get(sec)) if sec else "未分類"
        is_formal = _s(row.get("H76分流層級")).startswith("F0")
        if max_per_sector and counts.get(group, 0) >= max_per_sector and not is_formal:
            continue
        picks.append(idx)
        counts[group] = counts.get(group, 0) + 1
        if len(picks) >= max_rows:
            break
    use = use.loc[picks].copy()
    use.insert(0, "今日新Alpha順位", range(1, len(use) + 1))
    cols = [c for c in [
        "今日新Alpha順位", "股票代號", "股票名稱", "市場別", "族群名稱", "類別", "H76分流層級",
        "H76每日新Alpha分", "H76新證據數", "H76新證據摘要", "H76熟面孔狀態", "H76重複推薦門檻",
        "H74強勢加速度分", "H74主流新鮮度分", "H74法人資金加速度分", "H74成交資金加速度分",
        "H74大戶鎖碼狀態", "H74訊號新鮮分", "H64有效權威", "H68次日執行狀態", "H76主管結論"
    ] if c in use.columns]
    return use[cols].reset_index(drop=True)


def build_h76_structural_core_table(frame: pd.DataFrame, max_rows: int = 12) -> pd.DataFrame:
    w = apply_h76_daily_alpha_core_split(frame)
    if w.empty:
        return pd.DataFrame({"狀態": ["目前沒有結構核心監控資料。"]})
    lane = w["H76分流層級"].fillna("").astype(str)
    use = w.loc[lane.str.startswith("C1")].copy()
    if use.empty:
        return pd.DataFrame({"狀態": ["目前沒有被分流到結構核心監控的股票。"]})
    use = use.sort_values(
        ["H76結構核心分", "H76每日新Alpha分", "H72風險調整分"], ascending=False, kind="mergesort"
    ).head(max_rows).copy()
    use.insert(0, "核心監控順位", range(1, len(use) + 1))
    cols = [c for c in [
        "核心監控順位", "股票代號", "股票名稱", "市場別", "族群名稱", "類別", "H76分流層級",
        "H76結構核心分", "H76每日新Alpha分", "H76近5次入榜", "H76連續入榜", "H76熟面孔狀態",
        "H76新證據數", "H76新證據摘要", "H76重複推薦門檻", "H76重複門檻扣分",
        "H74主流新鮮度分", "H74法人資金加速度分", "H74成交資金加速度分", "H74訊號新鮮分",
        "H72品質獲利模型分", "H72成長動能模型分", "H72法人需求模型分", "H64有效權威", "H76主管結論"
    ] if c in use.columns]
    return use[cols].reset_index(drop=True)


def build_h76_evidence_table(frame: pd.DataFrame, max_rows: int = 20) -> pd.DataFrame:
    w = apply_h76_daily_alpha_core_split(frame)
    if w.empty:
        return pd.DataFrame({"狀態": ["目前沒有推薦證據。"]})
    pri = w["H76分流層級"].map(lambda x: 60 if _s(x).startswith("F0") else 50 if _s(x).startswith("D1") else 40 if _s(x).startswith("D2") else 30 if _s(x).startswith("C1") else 10)
    w = w.assign(_p=pri).sort_values(
        ["_p", "H76每日新Alpha分", "H76結構核心分", "H76新證據數"], ascending=False, kind="mergesort"
    ).head(max_rows).copy()
    w.insert(0, "證據順位", range(1, len(w) + 1))
    cols = [c for c in [
        "證據順位", "股票代號", "股票名稱", "族群名稱", "H76分流層級", "H76每日榜資格", "H76每日新Alpha分",
        "H76結構核心分", "H76熟面孔狀態", "H76近5次入榜", "H76連續入榜", "H76新證據數", "H76新證據摘要",
        "H76重複推薦門檻", "H76重複門檻扣分", "H74決策總分", "H74強勢加速度分", "H74主流新鮮度分",
        "H74法人資金加速度分", "H74成交資金加速度分", "H74大戶鎖碼狀態", "H74訊號新鮮分",
        "H64品質閘門", "H64有效權威", "H68次日執行狀態", "H76主管結論"
    ] if c in w.columns]
    return w[cols].reset_index(drop=True)


def build_h76_performance_health_summary(perf_df: pd.DataFrame, health_df: pd.DataFrame, max_rows: int = 60) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if isinstance(perf_df, pd.DataFrame) and not perf_df.empty:
        for _, r in perf_df.head(max_rows // 2).iterrows():
            item = _s(r.get("績效指標")) or _s(r.get("項目")) or "績效資訊"
            val = r.get("目前數值", r.get("數值", ""))
            interp = _s(r.get("主管解讀")) or "看滾動樣本，不用單日勝負調權。"
            rows.append({"區塊": "推薦績效", "項目": item, "數值": val, "主管解讀": interp})
    if isinstance(health_df, pd.DataFrame) and not health_df.empty:
        for _, r in health_df.head(max_rows // 2).iterrows():
            item = _s(r.get("項目")) or _s(r.get("類型")) or "系統健康"
            val = r.get("數值", r.get("目前數值", ""))
            interp = _s(r.get("主管解讀")) or "資料健康需正常，推薦才有解讀價值。"
            rows.append({"區塊": "系統健康", "項目": item, "數值": val, "主管解讀": interp})
    if not rows:
        return pd.DataFrame({"狀態": ["目前沒有績效/健康摘要。"]})
    return pd.DataFrame(rows).head(max_rows).reset_index(drop=True)


def build_h76_governance_summary(frame: pd.DataFrame) -> pd.DataFrame:
    w = apply_h76_daily_alpha_core_split(frame)
    if w.empty:
        return pd.DataFrame({"治理項目": ["H76"], "目前狀態": ["無資料"], "治理原則": ["不建立Formal"]})
    lane = w["H76分流層級"].fillna("").astype(str)
    familiar = w["H76熟面孔狀態"].fillna("").astype(str)
    return pd.DataFrame([
        {"治理項目": "每日新Alpha核心", "目前狀態": int(lane.str.startswith("D1").sum()), "治理原則": "今天真的有新證據才占每日前排"},
        {"治理項目": "每日新Alpha候選", "目前狀態": int(lane.str.startswith("D2").sum()), "治理原則": "仍非Formal，需盤前重驗"},
        {"治理項目": "結構核心監控", "目前狀態": int(lane.str.startswith("C1").sum()), "治理原則": "好公司/好結構不等於今天的新推薦"},
        {"治理項目": "熟面孔被每日榜阻擋", "目前狀態": int(familiar.str.startswith("REPEAT-GATED").sum()), "治理原則": "沒有新證據就不重複占每日Alpha名額"},
        {"治理項目": "熟面孔新證據續命", "目前狀態": int(familiar.str.startswith("RENEWED").sum()), "治理原則": "重新加速可回每日榜，不永久封殺"},
        {"治理項目": "Formal/Execution權威", "目前狀態": "UNCHANGED", "治理原則": "H64/H63 Formal＋H68執行否決仍唯一權威"},
    ])


__all__ = [
    "VERSION", "apply_h76_daily_alpha_core_split", "build_h76_daily_alpha_table",
    "build_h76_structural_core_table", "build_h76_evidence_table",
    "build_h76_performance_health_summary", "build_h76_governance_summary",
]
