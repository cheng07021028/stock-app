# -*- coding: utf-8 -*-
"""V191-H77 verified-delta, chase-risk and entry-quality governance.

H76 separated daily alpha from structural core.  H77 fixes the next observed
failure mode: an absolute high score is not automatically *new* evidence, and a
stock already near the daily limit must not be presented as the best fresh
opportunity merely because momentum and turnover scores are high.

Authority boundary: H77 is a research-ranking and display gate only.  It never
creates Formal, never changes H64/H63 identity, and never bypasses H68.
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from godpick_h76_daily_alpha_core_split import apply_h76_daily_alpha_core_split


VERSION = "v191_h77_verified_delta_chase_entry_performance_brake_20260918"
_BLANK = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}


def _s(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in _BLANK else text


def _raw_num(work: pd.DataFrame, col: str) -> pd.Series:
    if col not in work.columns:
        return pd.Series([float("nan")] * len(work), index=work.index, dtype=float)
    return pd.to_numeric(work[col], errors="coerce").astype(float)


def _score(work: pd.DataFrame, col: str, default: float = 50.0) -> pd.Series:
    return _raw_num(work, col).fillna(default).clip(0, 100)


def _date_valid(work: pd.DataFrame, col: str) -> pd.Series:
    if col not in work.columns:
        return pd.Series([False] * len(work), index=work.index, dtype=bool)
    return work[col].map(lambda value: bool(_s(value)))


def apply_h77_verified_delta_execution_gate(frame: pd.DataFrame) -> pd.DataFrame:
    """Add auditable H77 evidence, chase and entry gates to an H76 snapshot."""
    work = apply_h76_daily_alpha_core_split(frame)
    if work.empty:
        return work
    work = work.copy().reset_index(drop=True)

    momentum = _raw_num(work, "3日動能加速度百分點")
    turnover3 = _raw_num(work, "成交額3日加速度%")
    volume3 = _raw_num(work, "成交量3日加速度%")
    inst1 = _raw_num(work, "三大法人近1日合計")
    inst3 = _raw_num(work, "三大法人近3日合計")
    holder_delta = _raw_num(work, "TDCC千張大戶週變化pp")
    today = _raw_num(work, "今日漲幅%")
    ret5 = _raw_num(work, "近5日漲幅%")
    ma20_gap = _raw_num(work, "收盤距MA20%")
    close_pos = _raw_num(work, "當日收盤位置%")
    entry = _score(work, "Entry進場買點分", 50.0)
    rr = _score(work, "H61RR品質分", 50.0)

    momentum_cov = momentum.notna()
    capital_cov = turnover3.notna() | volume3.notna()
    institution_cov = inst1.notna() & inst3.notna()
    holder_cov = holder_delta.notna() & _date_valid(work, "TDCC大戶資料日期") & _date_valid(work, "TDCC大戶前期日期")

    momentum_delta = momentum_cov & momentum.ge(0.5)
    capital_delta = capital_cov & pd.concat([turnover3, volume3], axis=1).max(axis=1, skipna=True).ge(8.0)
    # Positive one-day flow plus a pace at least as strong as 80% of the prior
    # three-day daily average.  This is a direction test, not a large-cap level test.
    institution_delta = institution_cov & inst1.gt(0) & inst3.gt(0) & inst1.ge((inst3 / 3.0) * 0.8)
    holder_delta_ok = holder_cov & holder_delta.gt(0)

    flags = pd.DataFrame({
        "動能增量": momentum_delta,
        "量能增量": capital_delta,
        "法人流速增量": institution_delta,
        "TDCC真增持": holder_delta_ok,
    }, index=work.index)
    coverage_flags = pd.DataFrame({
        "動能": momentum_cov,
        "量能": capital_cov,
        "法人": institution_cov,
        "TDCC": holder_cov,
    }, index=work.index)
    verified_count = flags.astype(int).sum(axis=1)
    coverage_count = coverage_flags.astype(int).sum(axis=1)

    severe_chase = (
        today.ge(9.5) | ret5.ge(18.0) | ma20_gap.ge(20.0) | entry.lt(40.0)
    ).fillna(False)
    elevated_chase = (
        ~severe_chase
        & (today.ge(7.0) | ret5.ge(12.0) | ma20_gap.ge(12.0) | close_pos.ge(95.0))
    ).fillna(False)
    chase_penalty = pd.Series(0.0, index=work.index)
    chase_penalty.loc[elevated_chase] = 6.0
    chase_penalty.loc[severe_chase] = 14.0
    entry_penalty = ((55.0 - entry).clip(lower=0) * 0.22).clip(0, 5)
    rr_penalty = ((35.0 - rr).clip(lower=0) * 0.12).clip(0, 3)
    evidence_penalty = (2 - verified_count).clip(lower=0) * 3.0 + (2 - coverage_count).clip(lower=0) * 2.0
    verified_bonus = verified_count.clip(upper=4) * 1.5
    h77_score = (
        _score(work, "H76每日新Alpha分", 50.0)
        + verified_bonus - chase_penalty - entry_penalty - rr_penalty - evidence_penalty
    ).clip(0, 100)

    h76_lane = work["H76分流層級"].fillna("").astype(str)
    formal = h76_lane.str.startswith("F0")
    h76_daily = h76_lane.str.startswith(("D1", "D2"))
    a1 = (
        ~formal & h76_daily & verified_count.ge(2) & coverage_count.ge(3)
        & ~severe_chase & entry.ge(55) & h77_score.ge(65)
    )
    a2 = (
        ~formal & h76_daily & ~a1 & verified_count.ge(1) & coverage_count.ge(2)
        & ~severe_chase & entry.ge(45) & h77_score.ge(58)
    )
    wait = ~formal & h76_daily & ~a1 & ~a2
    core = ~formal & ~h76_daily & h76_lane.str.startswith("C1")

    lanes: list[str] = []
    evidence_texts: list[str] = []
    coverage_texts: list[str] = []
    chase_states: list[str] = []
    conclusions: list[str] = []
    for i in work.index:
        evidence_names = [name for name in flags.columns if bool(flags.at[i, name])]
        covered_names = [name for name in coverage_flags.columns if bool(coverage_flags.at[i, name])]
        evidence_text = "＋".join(evidence_names) if evidence_names else "沒有可驗證的增量證據"
        coverage_text = f"{int(coverage_count.iat[i])}/4｜" + ("＋".join(covered_names) if covered_names else "無原始增量欄位")
        if severe_chase.iat[i]:
            chase_state = "RED｜追價/延伸或進場品質不合格"
        elif elevated_chase.iat[i]:
            chase_state = "AMBER｜短線偏熱，限拉回確認"
        else:
            chase_state = "GREEN｜未觸發追價紅燈"

        if formal.iat[i]:
            lane = "F0｜正式權威（仍須H68/盤前重驗）"
            conclusion = "Formal身分保留；H77不改權威，只補增量與追價風險證據。"
        elif a1.iat[i]:
            lane = "A1｜驗證增量研究優先"
            conclusion = f"增量證據{int(verified_count.iat[i])}項，進場分{entry.iat[i]:.0f}；僅研究優先，非Formal。"
        elif a2.iat[i]:
            lane = "A2｜條件式研究候選"
            conclusion = f"增量證據{int(verified_count.iat[i])}項或進場品質尚未完整；等待拉回/下一批資料確認。"
        elif wait.iat[i]:
            lane = "W1｜追價或證據等待區"
            conclusion = f"原H76每日候選被H77降為等待；{chase_state}，增量證據{int(verified_count.iat[i])}項。"
        elif core.iat[i]:
            lane = "C1｜結構核心監控（非每日新推薦）"
            conclusion = "結構可追蹤，但不占今日驗證增量名額。"
        else:
            lane = "R0｜一般研究"
            conclusion = "未通過今日驗證增量與執行品質門檻。"

        lanes.append(lane)
        evidence_texts.append(evidence_text)
        coverage_texts.append(coverage_text)
        chase_states.append(chase_state)
        conclusions.append(conclusion)

    work["H77研究層級"] = lanes
    work["H77驗證增量分"] = h77_score.round(2)
    work["H77增量證據數"] = verified_count.astype("Int64")
    work["H77增量證據摘要"] = evidence_texts
    work["H77增量欄位覆蓋"] = coverage_texts
    work["H77增量欄位覆蓋數"] = coverage_count.astype("Int64")
    work["H77追價風險"] = chase_states
    work["H77追價扣分"] = chase_penalty.round(2)
    work["H77進場品質分"] = entry.round(2)
    work["H77RR品質分"] = rr.round(2)
    work["H77主管結論"] = conclusions
    work["H77權威邊界"] = "H77只治理研究排序、可驗證增量、追價與進場品質；Formal仍由H64/H63，執行仍由H68。"
    work["H77版本"] = VERSION

    priority = work["H77研究層級"].map(
        lambda x: 70 if _s(x).startswith("F0") else 60 if _s(x).startswith("A1") else 50 if _s(x).startswith("A2") else 0
    )
    rank_mask = priority.gt(0)
    rank = pd.Series([pd.NA] * len(work), index=work.index, dtype="Int64")
    if rank_mask.any():
        ordering = work.loc[rank_mask].assign(_priority=priority.loc[rank_mask]).sort_values(
            ["_priority", "H77驗證增量分", "H77增量證據數", "H77進場品質分"],
            ascending=False,
            kind="mergesort",
        )
        rank.loc[ordering.index] = pd.Series(range(1, len(ordering) + 1), index=ordering.index, dtype="Int64")
    work["H77研究順位"] = rank
    return work


def _diversified(frame: pd.DataFrame, max_rows: int, max_per_sector: int) -> pd.DataFrame:
    sector_col = next((c for c in ["族群名稱", "類別", "產業別"] if c in frame.columns), None)
    picks: list[int] = []
    counts: dict[str, int] = {}
    for idx, row in frame.iterrows():
        sector = _s(row.get(sector_col)) if sector_col else "未分類"
        is_formal = _s(row.get("H77研究層級")).startswith("F0")
        if max_per_sector and counts.get(sector, 0) >= max_per_sector and not is_formal:
            continue
        picks.append(idx)
        # Formal is an independent authority and must neither be blocked by nor
        # consume the research-list sector diversification quota.
        if not is_formal:
            counts[sector] = counts.get(sector, 0) + 1
        if len(picks) >= max_rows:
            break
    return frame.loc[picks].copy()


def build_h77_verified_alpha_table(frame: pd.DataFrame, max_rows: int = 8, max_per_sector: int = 2) -> pd.DataFrame:
    work = apply_h77_verified_delta_execution_gate(frame)
    if work.empty:
        return pd.DataFrame({"今日結論": ["本輪沒有可用候選；不為推薦而推薦。"]})
    lane = work["H77研究層級"].fillna("").astype(str)
    use = work.loc[lane.str.startswith(("F0", "A1", "A2"))].copy()
    if use.empty:
        return pd.DataFrame({"今日結論": ["本輪沒有通過H77驗證增量＋追價＋進場品質門檻的股票。"]})
    priority = use["H77研究層級"].map(lambda x: 70 if _s(x).startswith("F0") else 60 if _s(x).startswith("A1") else 50)
    use = use.assign(_priority=priority).sort_values(
        ["_priority", "H77驗證增量分", "H77增量證據數", "H77進場品質分"],
        ascending=False,
        kind="mergesort",
    )
    use = _diversified(use, max_rows=max_rows, max_per_sector=max_per_sector)
    use.insert(0, "今日驗證Alpha順位", range(1, len(use) + 1))
    cols = [c for c in [
        "今日驗證Alpha順位", "股票代號", "股票名稱", "市場別", "族群名稱", "類別", "H77研究層級",
        "H77驗證增量分", "H77增量證據數", "H77增量證據摘要", "H77增量欄位覆蓋", "H77追價風險",
        "H77進場品質分", "H77RR品質分", "今日漲幅%", "近5日漲幅%", "收盤距MA20%",
        "H76熟面孔狀態", "H76重複推薦門檻", "H64有效權威", "H68次日執行狀態", "H77主管結論",
    ] if c in use.columns]
    return use[cols].reset_index(drop=True)


def build_h77_wait_core_table(frame: pd.DataFrame, max_rows: int = 15) -> pd.DataFrame:
    work = apply_h77_verified_delta_execution_gate(frame)
    if work.empty:
        return pd.DataFrame({"狀態": ["目前沒有等待/核心監控資料。"]})
    lane = work["H77研究層級"].fillna("").astype(str)
    use = work.loc[lane.str.startswith(("W1", "C1"))].copy()
    if use.empty:
        return pd.DataFrame({"狀態": ["目前沒有等待/核心監控資料。"]})
    priority = use["H77研究層級"].map(lambda x: 60 if _s(x).startswith("W1") else 40)
    use = use.assign(_priority=priority).sort_values(
        ["_priority", "H77驗證增量分", "H76結構核心分"], ascending=False, kind="mergesort"
    ).head(max_rows)
    use.insert(0, "等待監控順位", range(1, len(use) + 1))
    cols = [c for c in [
        "等待監控順位", "股票代號", "股票名稱", "市場別", "族群名稱", "H77研究層級",
        "H77驗證增量分", "H77增量證據數", "H77增量證據摘要", "H77增量欄位覆蓋", "H77追價風險",
        "H77追價扣分", "H77進場品質分", "H77RR品質分", "今日漲幅%", "近5日漲幅%", "收盤距MA20%",
        "H76結構核心分", "H76熟面孔狀態", "H64有效權威", "H68次日執行狀態", "H77主管結論",
    ] if c in use.columns]
    return use[cols].reset_index(drop=True)


def build_h77_evidence_table(frame: pd.DataFrame, max_rows: int = 25) -> pd.DataFrame:
    work = apply_h77_verified_delta_execution_gate(frame)
    if work.empty:
        return pd.DataFrame({"狀態": ["目前沒有H77證據資料。"]})
    priority = work["H77研究層級"].map(
        lambda x: 70 if _s(x).startswith("F0") else 60 if _s(x).startswith("A1") else 50 if _s(x).startswith("A2") else 40 if _s(x).startswith("W1") else 30 if _s(x).startswith("C1") else 10
    )
    use = work.assign(_priority=priority).sort_values(
        ["_priority", "H77驗證增量分", "H77增量證據數", "H77進場品質分"],
        ascending=False,
        kind="mergesort",
    ).head(max_rows).copy()
    use.insert(0, "證據順位", range(1, len(use) + 1))
    cols = [c for c in [
        "證據順位", "股票代號", "股票名稱", "族群名稱", "H77研究層級", "H77驗證增量分",
        "H77增量證據數", "H77增量證據摘要", "H77增量欄位覆蓋", "H77追價風險", "H77追價扣分",
        "H77進場品質分", "H77RR品質分", "H76分流層級", "H76熟面孔狀態", "H76重複推薦門檻",
        "H64有效權威", "H68次日執行狀態", "H77主管結論",
    ] if c in use.columns]
    return use[cols].reset_index(drop=True)


def build_h77_performance_health_summary(perf_df: pd.DataFrame, health_df: pd.DataFrame, max_rows: int = 60) -> pd.DataFrame:
    metrics: dict[str, Any] = {}
    if isinstance(perf_df, pd.DataFrame) and not perf_df.empty and perf_df.shape[1] >= 2:
        metrics = dict(zip(perf_df.iloc[:, 0].astype(str), perf_df.iloc[:, 1]))
    samples = pd.to_numeric(pd.Series([metrics.get("executable_samples")]), errors="coerce").iloc[0]
    win = pd.to_numeric(pd.Series([metrics.get("executable_win_rate_pct")]), errors="coerce").iloc[0]
    alpha = pd.to_numeric(pd.Series([metrics.get("avg_selection_alpha_pct")]), errors="coerce").iloc[0]
    defensive = bool((pd.notna(samples) and samples >= 30) and ((pd.notna(win) and win < 45) or (pd.notna(alpha) and alpha < 0)))
    brake = "DEFENSIVE｜研究名單縮編；不得因分數高而放寬Formal/H68" if defensive else "NORMAL｜仍遵守Formal/H68權威"
    rows: list[dict[str, Any]] = [{
        "區塊": "H77績效煞車", "項目": "目前治理模式", "數值": brake,
        "主管解讀": f"可執行樣本={int(samples) if pd.notna(samples) else 'NA'}；勝率={win if pd.notna(win) else 'NA'}%；Selection Alpha={alpha if pd.notna(alpha) else 'NA'}%。",
    }]
    if isinstance(perf_df, pd.DataFrame) and not perf_df.empty:
        for _, row in perf_df.head(max_rows // 2).iterrows():
            rows.append({"區塊": "推薦績效", "項目": row.iloc[0], "數值": row.iloc[1], "主管解讀": "看成熟樣本與Alpha，不用單日勝負調權。"})
    if isinstance(health_df, pd.DataFrame) and not health_df.empty:
        for _, row in health_df.head(max_rows // 2).iterrows():
            rows.append({"區塊": "系統健康", "項目": row.get("項目", row.iloc[0]), "數值": row.get("數值", row.iloc[-1]), "主管解讀": "資料健康正常後，推薦才有解讀價值。"})
    return pd.DataFrame(rows).head(max_rows).reset_index(drop=True)


def build_h77_governance_summary(frame: pd.DataFrame) -> pd.DataFrame:
    work = apply_h77_verified_delta_execution_gate(frame)
    if work.empty:
        return pd.DataFrame({"治理項目": ["H77"], "目前狀態": ["無資料"], "治理原則": ["不建立Formal"]})
    lane = work["H77研究層級"].fillna("").astype(str)
    return pd.DataFrame([
        {"治理項目": "驗證增量研究優先", "目前狀態": int(lane.str.startswith("A1").sum()), "治理原則": "至少2項真實增量、3項原始欄位覆蓋、進場分>=55"},
        {"治理項目": "條件式研究候選", "目前狀態": int(lane.str.startswith("A2").sum()), "治理原則": "至少1項增量；仍非Formal"},
        {"治理項目": "追價/證據等待", "目前狀態": int(lane.str.startswith("W1").sum()), "治理原則": "接近漲停、過度延伸、低進場分或證據不足即降級"},
        {"治理項目": "TDCC真增持覆蓋", "目前狀態": int(work["H77增量證據摘要"].fillna("").str.contains("TDCC真增持", regex=False).sum()), "治理原則": "必須同時有本期、前期日期與週變化>0"},
        {"治理項目": "Formal/Execution權威", "目前狀態": "UNCHANGED", "治理原則": "H64/H63 Formal＋H68執行否決仍唯一權威"},
    ])


__all__ = [
    "VERSION", "apply_h77_verified_delta_execution_gate", "build_h77_verified_alpha_table",
    "build_h77_wait_core_table", "build_h77_evidence_table", "build_h77_performance_health_summary",
    "build_h77_governance_summary",
]
