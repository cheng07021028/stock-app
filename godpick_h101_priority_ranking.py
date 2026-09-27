# -*- coding: utf-8 -*-
"""V191-H101 Recommendation Priority Ranking.

Manager-facing ranking layer only.

Goals
-----
* Make the recommendation order obvious: rank Formal and Research separately.
* Build one concise ``00_推薦優先總覽`` table for UI/Excel.
* Use H96 opportunity quality + H89 selection/execution + H81 risk/technical +
  H79 cross-sectional strength + H99 net RR as a consensus score.
* Penalise overheat/no-chase, bad sector conflicts, weak risk, missing news and
  blocked execution instead of blindly sorting by one score.
* Never create or relax Formal/A-/R1 authority.

This module is intentionally deterministic and dataframe-only.  It does not
fetch market data and does not mutate trade gates.
"""
from __future__ import annotations

from typing import Any, Iterable
import math

import pandas as pd

VERSION = "v191_h101_recommendation_priority_ranking_20260925"

H101_COLUMNS = [
    "H101版本",
    "H101同層順位",
    "H101推薦優先分",
    "H101優先層級",
    "H101推薦層別",
    "H101排名信心",
    "H101建議動作",
    "H101主要加分",
    "H101主要扣分",
    "H101排名依據",
    "H101Formal權限",
    "H101決策摘要",
]

FRONT = [
    "H101同層順位", "H101推薦優先分", "H101優先層級", "H101推薦層別",
    "股票代號", "股票名稱", "市場別", "類別",
    "H101排名信心", "H101建議動作", "H101主要加分", "H101主要扣分",
    "H99目標交易日", "H99執行狀態", "H99主進場", "H99防守停損", "H99第一目標", "H99成本後RR",
    "H96核心研究等級", "H96核心機會分", "H89選股方向分", "H89執行品質分",
    "H81風險管理分", "H79強度百分位%", "H79族群百分位%",
    "H101排名依據", "H101Formal權限", "H101決策摘要",
]

OVERVIEW_COLUMNS = [
    "H101閱讀順位", "H101推薦層別", "H101同層順位", "股票代號", "股票名稱", "類別",
    "H101推薦優先分", "H101優先層級", "H101排名信心", "H96核心研究等級",
    "H99主進場", "H99防守停損", "H99第一目標", "H99成本後RR", "H99目標交易日",
    "H101建議動作", "H101主要加分", "H101主要扣分",
]


def _text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _num(v: Any, default: float | None = None) -> float | None:
    if v is None or isinstance(v, bool):
        return default
    if isinstance(v, str):
        v = v.replace(",", "").replace("％", "%").replace("%", "").replace("+", "").strip()
        if not v or v.lower() in {"nan", "none", "null", "--", "-", "<na>"}:
            return default
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _first_num(row: dict[str, Any], names: Iterable[str], default: float | None = None) -> float | None:
    for name in names:
        x = _num(row.get(name), None)
        if x is not None:
            return x
    return default


def _first_text(row: dict[str, Any], names: Iterable[str]) -> str:
    for name in names:
        s = _text(row.get(name))
        if s:
            return s
    return ""


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(v)))


def _code(v: Any) -> str:
    s = _text(v)
    return s[:-2] if s.endswith(".0") else s


def _rr_score(rr: float | None) -> float | None:
    if rr is None:
        return None
    # Bounded, continuous conversion.  RR=1.0 -> 45, 1.5 -> 75, 2.0 -> 100.
    if rr <= 0:
        return 0.0
    return _clip(45.0 + (rr - 1.0) * 60.0)


def _pool_label(pool: str) -> str:
    return {
        "actionable": "FORMAL｜正式可執行",
        "research": "RESEARCH｜核心研究（非買進）",
        "waiting": "WAITING｜等待候選",
        "audit": "AUDIT｜驗證候選",
        "emerging_watch": "EMERGING｜興櫃隔離研究",
    }.get(pool, pool.upper())


def _tier(pool: str, score: float, rank: int) -> str:
    if pool == "actionable":
        return "F1｜正式第一優先" if rank == 1 else "F2｜正式次優先"
    if pool == "research":
        if score >= 82:
            return "R1｜核心第一優先"
        if score >= 74:
            return "R2｜核心優先"
        return "R3｜條件研究"
    if pool == "waiting":
        return "W1｜等待前段" if score >= 68 else "W2｜等待"
    if pool == "audit":
        return "A1｜Audit前段" if score >= 72 else "A2｜Audit觀察"
    return "E｜隔離研究"


def analyze_candidate(row: dict[str, Any] | pd.Series, *, pool: str) -> dict[str, Any]:
    raw = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})

    h96 = _first_num(raw, ["H96核心機會分"])
    selection = _first_num(raw, ["H89選股方向分"])
    execution = _first_num(raw, ["H89執行品質分"])
    risk = _first_num(raw, ["H81風險管理分"])
    strength = _first_num(raw, ["H79強度百分位%", "H47個股相對強度分"])
    sector = _first_num(raw, ["H79族群百分位%", "H53族群共振分"])
    technical = _first_num(raw, ["H81技術多週期分"])
    coverage = _first_num(raw, ["H81資料覆蓋%", "H93資料覆蓋%"])
    rr = _first_num(raw, ["H99成本後RR", "H89成本後RR1", "H79成本後RR"])

    weighted = [
        (h96, 0.30, "H96高信念"),
        (selection, 0.15, "Selection"),
        (execution, 0.15, "Execution"),
        (risk, 0.10, "風控"),
        (strength, 0.10, "個股強度"),
        (sector, 0.05, "族群強度"),
        (technical, 0.05, "技術多週期"),
        (coverage, 0.05, "資料覆蓋"),
        (_rr_score(rr), 0.05, "成本後RR"),
    ]
    num = 0.0
    den = 0.0
    for value, weight, _ in weighted:
        if value is None:
            continue
        num += _clip(value) * weight
        den += weight
    base = (num / den) if den > 0 else 0.0

    bonuses: list[str] = []
    penalties: list[str] = []
    adjustment = 0.0

    h96_core = _first_text(raw, ["H96核心資格"])
    lead_count = _first_num(raw, ["H96領先證據數"], 0.0) or 0.0
    price_plan_ok = _first_text(raw, ["H89Formal價格計畫合格"])
    no_chase = _first_text(raw, ["H96不追價"])
    h96_exec = _first_text(raw, ["H96執行狀態"])
    h99_exec = _first_text(raw, ["H99執行狀態"])
    sector_state = _first_text(raw, ["H94族群狀態", "H96市場族群一致性"])
    leader_exception = _first_text(raw, ["H96領先例外"])
    exhaust = _first_text(raw, ["H94耗竭風險"])
    h82 = _first_num(raw, ["H82自適應加減分"])
    news = _first_text(raw, ["H94新聞證據狀態"])

    if h96_core == "CORE":
        adjustment += 4.0; bonuses.append("H96核心資格")
    if lead_count >= 4:
        adjustment += 2.0; bonuses.append(f"領先證據{int(lead_count)}項")
    if price_plan_ok == "是":
        adjustment += 2.0; bonuses.append("價格計畫完整")
    if rr is not None and rr >= 1.5:
        adjustment += 2.0; bonuses.append(f"NetRR {rr:.2f}")

    if no_chase == "是" or h96_exec.startswith("LEADER-NO-CHASE"):
        adjustment -= 8.0; penalties.append("不追價/延伸")
    bad_sector = any(t in sector_state for t in ["退潮", "弱勢", "高檔鈍化", "降溫"])
    if bad_sector and leader_exception != "是":
        adjustment -= 6.0; penalties.append("族群逆風")
    elif bad_sector and leader_exception == "是":
        penalties.append("族群逆風但屬領先例外")
    if exhaust in {"HIGH", "BLOCK"}:
        adjustment -= 5.0; penalties.append("耗竭風險")
    if risk is not None and risk < 50:
        adjustment -= 5.0; penalties.append(f"風控偏低{risk:.0f}")
    if h82 is not None and h82 <= -0.70:
        adjustment -= 4.0; penalties.append(f"成熟學習{h82:+.2f}")
    if "MISSING" in news:
        adjustment -= 3.0; penalties.append("新聞證據缺口")
    if rr is not None and rr < 1.20:
        adjustment -= 4.0; penalties.append(f"NetRR偏低{rr:.2f}")
    if h99_exec.startswith("BLOCK"):
        adjustment -= 8.0; penalties.append("執行價格計畫BLOCK")

    # Formal is ranked separately.  Do not let a pool label manufacture score.
    score = _clip(base + adjustment)

    available = sum(1 for value, _, _ in weighted if value is not None)
    confidence = "HIGH" if available >= 8 else "MEDIUM-HIGH" if available >= 6 else "MEDIUM" if available >= 4 else "LOW"

    if pool == "actionable":
        action = "Formal同層依順位檢查；盤前重驗、價格仍在H99計畫且原Formal治理未失效才執行。"
    elif pool == "research":
        if no_chase == "是" or h96_exec.startswith("LEADER-NO-CHASE"):
            action = "研究優先但不追價；等待H99進場區＋盤前重驗，再看是否取得原Formal授權。"
        else:
            action = "依研究順位優先追蹤；等待H99價格計畫與盤前重驗，研究股不得冒充買進。"
    elif pool == "waiting":
        action = "等待條件改善；未取得核心研究或Formal授權。"
    elif pool == "audit":
        action = "作為漏選/風險稽核參考，不是推薦清單。"
    else:
        action = "隔離研究，不占上市櫃主榜。"

    basis_parts = []
    for value, weight, label in weighted:
        if value is not None:
            basis_parts.append(f"{label}{float(value):.1f}×{int(weight*100)}%")
    basis = "；".join(basis_parts)
    summary = (
        f"優先分{score:.1f}｜{_pool_label(pool)}｜信心{confidence}；"
        "H101只排序閱讀/研究優先，不建立或放寬Formal。"
    )
    return {
        "H101版本": VERSION,
        "H101同層順位": None,
        "H101推薦優先分": round(score, 2),
        "H101優先層級": "",
        "H101推薦層別": _pool_label(pool),
        "H101排名信心": confidence,
        "H101建議動作": action,
        "H101主要加分": "；".join(bonuses) or "無額外加分",
        "H101主要扣分": "；".join(penalties) or "無重大扣分",
        "H101排名依據": basis,
        "H101Formal權限": "LOCKED｜H101只做優先排序；Formal仍由H64/H68＋新鮮度＋流動性＋成本後RR＋停損治理。",
        "H101決策摘要": summary,
    }


def apply_priority_overlay(frame: pd.DataFrame | None, *, pool: str) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)
    out = frame.copy(deep=True)
    if out.empty:
        for c in H101_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="object")
        return out

    # Placeholder conclusion rows are not securities and must never receive a fake rank.
    valid_mask = pd.Series([True] * len(out), index=out.index)
    if "股票代號" in out.columns:
        valid_mask = out["股票代號"].map(_code).ne("")
    else:
        valid_mask[:] = False

    for c in H101_COLUMNS:
        if c not in out.columns:
            out[c] = None

    valid = out.loc[valid_mask].copy()
    if not valid.empty:
        add = pd.DataFrame([analyze_candidate(r, pool=pool) for r in valid.to_dict("records")], index=valid.index)
        for c in H101_COLUMNS:
            if c in add.columns:
                out.loc[valid.index, c] = add[c]
        rank_frame = out.loc[valid.index].copy()
        rank_frame["__score"] = pd.to_numeric(rank_frame["H101推薦優先分"], errors="coerce")
        sort_cols = ["__score"]
        ascending = [False]
        for extra in ("H96核心機會分", "H89選股方向分"):
            if extra in rank_frame.columns:
                rank_frame[extra] = pd.to_numeric(rank_frame[extra], errors="coerce")
                sort_cols.append(extra); ascending.append(False)
        rank_order = rank_frame.sort_values(sort_cols, ascending=ascending, na_position="last").index.tolist()
        for rank, idx in enumerate(rank_order, start=1):
            score = _num(out.at[idx, "H101推薦優先分"], 0.0) or 0.0
            out.at[idx, "H101同層順位"] = rank
            out.at[idx, "H101優先層級"] = _tier(pool, score, rank)
        # Rank is the first sort key; preserve placeholder rows after ranked securities.
        ranked = out.loc[rank_order].copy() if rank_order else out.iloc[0:0].copy()
        other = out.loc[~out.index.isin(rank_order)].copy()
        out = pd.concat([ranked, other], ignore_index=True, sort=False)

    front = [c for c in FRONT if c in out.columns]
    rest = [c for c in out.columns if c not in front]
    return out.loc[:, front + rest].copy()


def build_priority_overview(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for name in ("actionable", "research"):
        df = tables.get(name, pd.DataFrame())
        if not isinstance(df, pd.DataFrame) or df.empty or "股票代號" not in df.columns:
            continue
        work = df[df["股票代號"].map(_code).ne("")].copy()
        if work.empty:
            continue
        # Formal always appears before Research; within a pool use H101 rank.
        work["__pool_order"] = 0 if name == "actionable" else 1
        work["__pool_rank"] = pd.to_numeric(work.get("H101同層順位"), errors="coerce").fillna(9999)
        frames.append(work)
    if not frames:
        return pd.DataFrame(columns=OVERVIEW_COLUMNS)
    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged = merged.sort_values(["__pool_order", "__pool_rank", "H101推薦優先分"], ascending=[True, True, False], na_position="last").reset_index(drop=True)
    merged["H101閱讀順位"] = range(1, len(merged) + 1)
    for col in OVERVIEW_COLUMNS:
        if col not in merged.columns:
            merged[col] = None
    return merged.loc[:, OVERVIEW_COLUMNS].copy()


def decorate_decision_tables(tables: dict[str, Any] | None) -> dict[str, pd.DataFrame]:
    src = tables if isinstance(tables, dict) else {}
    out: dict[str, pd.DataFrame] = {}
    pool_names = {
        "actionable": "actionable",
        "research": "research",
        "waiting": "waiting",
        "audit": "audit",
        "emerging_watch": "emerging_watch",
    }
    for name, value in src.items():
        frame = value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame(value) if value is not None else pd.DataFrame()
        if name in pool_names:
            frame = apply_priority_overlay(frame, pool=pool_names[name])
        out[name] = frame

    out["priority_overview"] = build_priority_overview(out)

    health = out.get("health", pd.DataFrame()).copy()
    if not health.empty and "項目" in health.columns:
        health = health.loc[~health["項目"].astype(str).str.startswith("H101")].copy()
    overview = out.get("priority_overview", pd.DataFrame())
    research = out.get("research", pd.DataFrame())
    formal = out.get("actionable", pd.DataFrame())
    top_text = "本輪Formal/Research皆無可排序股票"
    if isinstance(overview, pd.DataFrame) and not overview.empty:
        r = overview.iloc[0]
        top_text = f"#{int(_num(r.get('H101閱讀順位'), 1) or 1)} {_code(r.get('股票代號'))} {_text(r.get('股票名稱'))}｜{_text(r.get('H101推薦層別'))}｜優先分{_num(r.get('H101推薦優先分'),0):.1f}"
    rows = [
        {"項目": "H101版本", "數值": VERSION},
        {"項目": "H101主管第一閱讀", "數值": top_text},
        {"項目": "H101Formal排序列", "數值": int(len(formal[formal.get('股票代號', pd.Series(dtype=str)).map(_code).ne('')])) if isinstance(formal, pd.DataFrame) and '股票代號' in formal.columns else 0},
        {"項目": "H101Research排序列", "數值": int(len(research[research.get('股票代號', pd.Series(dtype=str)).map(_code).ne('')])) if isinstance(research, pd.DataFrame) and '股票代號' in research.columns else 0},
        {"項目": "H101Excel總覽", "數值": "00_推薦優先總覽：Formal在前、Research在後；各自保留同層順位，研究股不冒充買進。"},
        {"項目": "H101排名核心", "數值": "H96 30%＋H89 Selection 15%＋Execution 15%＋H81風控10%＋強度10%＋族群5%＋技術5%＋覆蓋5%＋NetRR5%，再做追價/族群/耗竭/資料風險扣分。"},
        {"項目": "H101Formal權限", "數值": "LOCKED"},
    ]
    out["health"] = pd.concat([health, pd.DataFrame(rows)], ignore_index=True, sort=False)
    return out


def export_contract_summary(tables: dict[str, pd.DataFrame] | None) -> dict[str, Any]:
    tables = tables if isinstance(tables, dict) else {}
    overview = tables.get("priority_overview", pd.DataFrame())
    research = tables.get("research", pd.DataFrame())
    actionable = tables.get("actionable", pd.DataFrame())
    ok = isinstance(overview, pd.DataFrame) and isinstance(research, pd.DataFrame) and isinstance(actionable, pd.DataFrame)
    if isinstance(research, pd.DataFrame) and not research.empty and "股票代號" in research.columns:
        has_real_research = research["股票代號"].map(_code).ne("").any()
        if has_real_research and "H101同層順位" not in research.columns:
            ok = False
    return {
        "version": VERSION,
        "ok": bool(ok),
        "overview_rows": int(len(overview)) if isinstance(overview, pd.DataFrame) else 0,
        "formal_rows": int(len(actionable)) if isinstance(actionable, pd.DataFrame) else 0,
        "research_rows": int(len(research)) if isinstance(research, pd.DataFrame) else 0,
    }
