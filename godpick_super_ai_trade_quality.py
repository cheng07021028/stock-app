# -*- coding: utf-8 -*-
"""V188 Super AI trade-quality governance.

This layer deliberately separates *stock quality* (Alpha) from *trade quality*
(Entry/Risk/RR/data evidence).  It is demotion-only: it may make an existing
recommendation stricter, but it never promotes a row that the legacy hard gates
had rejected.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import re

import pandas as pd

V188_VERSION = "super_ai_trade_quality_v188_20260812"

V188_COLUMNS = [
    "V188版本",
    "SuperAI Alpha分", "SuperAI Alpha等級",
    "SuperAI Trade分", "SuperAI Trade等級", "SuperAI最終作戰等級",
    "SuperAI執行風報比", "SuperAI風報比來源",
    "SuperAI校準後隔日上漲機率%", "SuperAI機率校準樣本數", "SuperAI機率校準幅度pp",
    "V188股神作戰優先分", "V188交易許可", "V188正式推薦資格",
    "V188RR治理", "V188T+1追價治理", "V188個股資料證據", "V188市場對齊治理",
    "V188類股集中治理", "V188類股集中扣分", "V188治理原因",
]

_BLANK = {"", "nan", "none", "null", "nat", "--", "-", "<na>", "n/a"}


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
        if v is None:
            return default
        if isinstance(v, (int, float)):
            x = float(v)
            return x if math.isfinite(x) else default
        t = str(v).strip().replace(",", "").replace("％", "%")
        if t.endswith("%"):
            t = t[:-1]
        x = float(t)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _first(row: dict[str, Any], names: Iterable[str], default: float = 0.0, positive: bool = False) -> tuple[float, str]:
    fallback: tuple[float, str] | None = None
    for name in names:
        if name not in row or not _s(row.get(name)):
            continue
        value = _f(row.get(name), float("nan"))
        if not math.isfinite(value):
            continue
        if fallback is None:
            fallback = (value, name)
        if not positive or value > 0:
            return value, name
    return fallback if fallback is not None else (default, "")


def _clip(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(x)))


def _contains(text: str, keys: Iterable[str]) -> bool:
    low = _s(text).lower()
    return any(str(k).lower() in low for k in keys)


def _grade(score: float) -> str:
    if score >= 90:
        return "S+"
    if score >= 84:
        return "S"
    if score >= 78:
        return "A+"
    if score >= 72:
        return "A"
    if score >= 66:
        return "B+"
    if score >= 60:
        return "B"
    if score >= 52:
        return "C"
    return "D"


def canonical_execution_rr(row: dict[str, Any]) -> tuple[float, str]:
    """Return the conservative/executable RR, never AI tactical RR first.

    ``AI戰術風報比`` is a model feature and can be much larger than the actual
    nearest executable reward/risk.  It is only a last-resort fallback.
    """
    return _first(
        row,
        ["路徑風險報酬比", "實戰風險報酬比", "風險報酬比", "AI戰術風報比"],
        0.0,
        positive=True,
    )


def _official_evidence(row: dict[str, Any]) -> tuple[bool, float, str]:
    date_text = _s(row.get("官方因子資料日期")) or _s(row.get("官方資料日期")) or _s(row.get("法人資料日期")) or _s(row.get("估值資料日期"))
    lag, _ = _first(row, ["官方因子落後交易日", "官方資料落後交易日"], 999.0)
    fresh = "｜".join(_s(row.get(k)) for k in ["官方因子新鮮度", "股神資料總新鮮度", "來源可信度狀態"] if _s(row.get(k)))
    trust, _ = _first(row, ["每日因子來源可信度", "因子來源可信度", "官方來源可信度"], 0.0)
    # V187 legacy migration sometimes represents trusted old rows as 0 before
    # re-materialisation.  Only explicit verified text can rescue that case.
    explicit_verified = _contains(fresh, ["已驗證", "最新", "t-1", "對齊"])
    date_ok = bool(date_text) and lag <= 1
    if not date_text and explicit_verified and lag <= 1:
        date_ok = True
    trust_ok = trust >= 70 or (trust == 0 and explicit_verified)
    if lag >= 2 or lag >= 999 or _contains(fresh, ["日期未驗證", "過期", "可信度不足"]):
        date_ok = False
    score = 100.0 if date_ok and trust_ok else 62.0 if date_ok else 30.0 if trust_ok else 15.0
    label = (
        f"PASS｜個股官方因子T-{int(max(lag,0)) if lag < 999 else '?'}已驗證｜來源可信{trust:.0f}"
        if date_ok and trust_ok
        else f"BLOCK｜個股官方資料證據不足｜日期={date_text or '空白'}｜lag={lag:.0f}｜可信={trust:.0f}"
    )
    return bool(date_ok and trust_ok), score, label


def _market_alignment(row: dict[str, Any]) -> tuple[bool, float, str]:
    status = "｜".join(_s(row.get(k)) for k in ["大盤與K線對齊狀態", "大盤資料新鮮度", "大盤橋接狀態", "大盤策略模式"] if _s(row.get(k)))
    market_date = _s(row.get("大盤資料日期"))
    lag, _ = _first(row, ["大盤資料落後交易日"], 999.0)
    if _contains(status, ["lockdown", "禁止所有新倉"]):
        return False, 0.0, "BLOCK｜大盤LOCKDOWN"
    unknown = (not market_date and not _contains(status, ["ready", "對齊", "最新", "已驗證"])) or _contains(status, ["wait", "未知", "日期未驗證", "待同步"])
    stale = lag >= 2 and lag < 999
    if unknown or lag >= 999:
        return False, 35.0, f"WAIT｜大盤未知/未對齊｜{status or '無狀態'}"
    if stale:
        return False, 40.0, f"BLOCK｜大盤落後{lag:.0f}交易日"
    return True, 100.0 if lag <= 0 else 88.0, f"PASS｜大盤與K線對齊｜lag={lag:.0f}"


def _rr_governance(rr: float) -> tuple[str, float, bool]:
    if rr <= 0:
        return "BLOCK｜缺少可驗證執行RR", 25.0, False
    if rr < 1.0:
        return f"BLOCK｜RR {rr:.2f}<1.00，不建立正式新倉", 30.0, False
    if rr < 1.3:
        return f"RADAR｜RR {rr:.2f} 僅供雷達，未達正式交易門檻1.30", 48.0, False
    if rr < 1.5:
        return f"PASS-LIMITED｜RR {rr:.2f} 達最低門檻，仍需其餘條件", 70.0, True
    return f"PASS｜RR {rr:.2f} 優良", min(100.0, 76.0 + (rr - 1.5) * 18.0), True


def _chase_governance(row: dict[str, Any]) -> tuple[str, float, bool]:
    ret1, _ = _first(row, ["今日漲幅%", "單日漲幅%"], 0.0)
    ret5, _ = _first(row, ["近5日漲幅%", "5日漲幅%"], 0.0)
    chase, _ = _first(row, ["追價風險分", "追高風險分數_決策", "追高風險分"], 50.0)
    exhaust, _ = _first(row, ["隔日耗竭風險分"], chase)
    hot_text = "｜".join(_s(row.get(k)) for k in ["AI過熱型態", "過熱原因", "股神追高風險"] if _s(row.get(k)))
    extreme = ret1 >= 7.0 or chase >= 80 or exhaust >= 80 or _contains(hot_text, ["極端過熱", "爆量長上影"])
    warm = ret1 >= 4.0 or ret5 >= 10.0 or chase >= 68 or exhaust >= 70 or _contains(hot_text, ["過熱", "追高"])
    if extreme:
        return f"PULLBACK-ONLY｜前日{ret1:+.2f}%/5日{ret5:+.2f}%｜極端過熱，禁止突破追價", 38.0, True
    if warm:
        return f"PULLBACK-ONLY｜前日{ret1:+.2f}%/5日{ret5:+.2f}%｜禁止追價，只准回測守價", 58.0, True
    return f"PASS｜追價風險可控｜前日{ret1:+.2f}%/5日{ret5:+.2f}%", 88.0 if chase <= 60 else 76.0, False


def _probability_calibration(raw_prob: float, experience: dict[str, Any]) -> tuple[float, int, float]:
    bins = experience.get("calibration_bins") if isinstance(experience, dict) else None
    if not isinstance(bins, list):
        return round(_clip(raw_prob, 5, 95), 1), 0, 0.0
    chosen = None
    for item in bins:
        if not isinstance(item, dict):
            continue
        low = _f(item.get("low"), -1)
        high = _f(item.get("high"), 101)
        if low <= raw_prob < high or (raw_prob == 100 and high >= 100):
            chosen = item
            break
    if not chosen:
        return round(_clip(raw_prob, 5, 95), 1), 0, 0.0
    n = int(_f(chosen.get("n"), 0))
    actual = _f(chosen.get("actual_up_rate_pct"), raw_prob)
    limit = 0.0 if n < 30 else 3.0 if n < 100 else 8.0
    if limit <= 0:
        return round(_clip(raw_prob, 5, 95), 1), n, 0.0
    weight = min(0.65, n / 150.0)
    delta = max(-limit, min(limit, (actual - raw_prob) * weight))
    return round(_clip(raw_prob + delta, 5, 95), 1), n, round(delta, 2)


def _legacy_formal_allowed(row: dict[str, Any]) -> bool:
    raw = _s(row.get("是否正式推薦")).lower()
    if raw in {"true", "1", "是", "yes", "y"}:
        return True
    partition = _s(row.get("正式推薦分區"))
    if _contains(partition, ["正式推薦", "a-準主推薦", "a-準主"]):
        return True
    return False


def score_trade_quality_row(row: dict[str, Any], experience: dict[str, Any] | None = None) -> dict[str, Any]:
    experience = experience or {}
    alpha_raw, _ = _first(row, ["AI Alpha品質分", "候選強度分", "股神實戰總分", "股神推薦優先分"], 50.0)
    cont, _ = _first(row, ["AI Continuation延續分", "主流主升優先分", "強勢動能分"], 50.0)
    mainrise, _ = _first(row, ["主流主升優先分", "主流資金分"], 50.0)
    alpha_score = _clip(alpha_raw * 0.62 + cont * 0.22 + mainrise * 0.16)

    entry, _ = _first(row, ["AI Timing時機分", "Entry進場買點分", "進場買點分"], 50.0)
    risk, _ = _first(row, ["AI Risk風控分", "Risk風控安全分", "風控安全分"], 50.0)
    op, _ = _first(row, ["可操作分", "實戰操作品質分", "進場可執行分"], 50.0)
    raw_prob, _ = _first(row, ["SuperAI隔日上漲機率%"], 50.0)
    calibrated_prob, cal_n, cal_pp = _probability_calibration(raw_prob, experience)

    rr, rr_source = canonical_execution_rr(row)
    rr_text, rr_score, rr_pass = _rr_governance(rr)
    chase_text, chase_score, pullback_only = _chase_governance(row)
    official_ok, evidence_score, evidence_text = _official_evidence(row)
    market_ok, market_score, market_text = _market_alignment(row)

    probability_edge = _clip(50.0 + (calibrated_prob - 50.0) * 2.0)
    trade_score = _clip(
        entry * 0.22 + risk * 0.22 + op * 0.13 + rr_score * 0.19 +
        chase_score * 0.08 + evidence_score * 0.08 + market_score * 0.04 + probability_edge * 0.04
    )

    # Hard caps make the grade semantics truthful.  A great stock can remain
    # Alpha S+ while Trade is D/B because the current price is unattractive.
    trade_cap = 100.0
    if rr < 1.0 or rr <= 0:
        trade_cap = min(trade_cap, 49.0)
    elif rr < 1.3:
        trade_cap = min(trade_cap, 59.0)
    if not official_ok:
        trade_cap = min(trade_cap, 59.0)
    if not market_ok:
        trade_cap = min(trade_cap, 69.0)  # max B+
    if pullback_only:
        trade_cap = min(trade_cap, 68.0)
    trade_score = min(trade_score, trade_cap)

    hard_text = "｜".join(_s(row.get(k)) for k in ["操作許可", "真禁買原因", "硬否決原因", "極端市場LOCKDOWN", "正式推薦排除原因"] if _s(row.get(k)))
    hard_block = _contains(hard_text, ["禁止新倉", "禁止買進", "lockdown", "低流動", "正式排除"])
    legacy_formal = _legacy_formal_allowed(row)
    formal_ok = bool(legacy_formal and rr_pass and official_ok and market_ok and not pullback_only and not hard_block and trade_score >= 72)

    reasons = [rr_text, chase_text, evidence_text, market_text]
    if hard_block:
        permission = "BLOCK｜沿用既有硬風控禁止新倉"
    elif not official_ok:
        permission = "WAIT-DATA｜個股資料證據未完成"
    elif not market_ok:
        permission = "WAIT-MARKET｜大盤未對齊，Trade最高B+"
    elif rr <= 0 or rr < 1.0:
        permission = "NO-TRADE｜RR不足，不建立正式新倉"
    elif rr < 1.3:
        permission = "RADAR｜RR僅供觀察，不得正式推薦"
    elif pullback_only:
        permission = "WAIT-PULLBACK｜禁止突破追價，只准回測守價"
    elif not legacy_formal:
        permission = "RADAR｜舊硬門檻未升格，V188不自行越權升格"
    elif trade_score >= 72:
        permission = "READY-COND｜V188交易品質通過，仍需盤中觸發"
    else:
        permission = "WAIT｜交易品質未達A級"

    combined = _clip(alpha_score * 0.42 + trade_score * 0.58)
    if permission.startswith(("BLOCK", "NO-TRADE", "WAIT-DATA")):
        combined = min(combined, 59.0)
    elif permission.startswith(("RADAR", "WAIT-MARKET", "WAIT-PULLBACK")):
        combined = min(combined, 69.0)

    alpha_grade = _grade(alpha_score)
    trade_grade = _grade(trade_score)
    final_grade = f"Alpha {alpha_grade} × Trade {trade_grade}"
    formal_text = "是｜舊硬門檻＋V188均通過" if formal_ok else "否｜V188為降級治理，不越權升格"
    return {
        "V188版本": V188_VERSION,
        "SuperAI Alpha分": round(alpha_score, 1), "SuperAI Alpha等級": alpha_grade,
        "SuperAI Trade分": round(trade_score, 1), "SuperAI Trade等級": trade_grade,
        "SuperAI最終作戰等級": final_grade,
        "SuperAI執行風報比": round(rr, 3) if rr > 0 else 0.0, "SuperAI風報比來源": rr_source or "缺值",
        "SuperAI校準後隔日上漲機率%": calibrated_prob, "SuperAI機率校準樣本數": cal_n, "SuperAI機率校準幅度pp": cal_pp,
        "V188股神作戰優先分": round(combined, 1), "V188交易許可": permission, "V188正式推薦資格": formal_text,
        "V188RR治理": rr_text, "V188T+1追價治理": chase_text, "V188個股資料證據": evidence_text, "V188市場對齊治理": market_text,
        "V188類股集中治理": "尚未套用跨股集中治理", "V188類股集中扣分": 0.0,
        "V188治理原因": "｜".join(reasons),
    }


def _sector_name(row: pd.Series) -> str:
    for col in ["類別", "產業", "族群名稱", "V134動態族群名稱"]:
        if col in row.index and _s(row.get(col)):
            return _s(row.get(col))
    return "未分類"


def apply_trade_quality_governance(data: Any, experience: dict[str, Any] | None = None):
    is_df = isinstance(data, pd.DataFrame)
    if data is None:
        return pd.DataFrame(columns=V188_COLUMNS) if is_df else []
    if is_df:
        out = data.copy()
        records = out.to_dict(orient="records")
    elif isinstance(data, list):
        records = [dict(x) for x in data if isinstance(x, dict)]
        out = None
    else:
        out = pd.DataFrame(data)
        records = out.to_dict(orient="records")
        is_df = True
    scored = [score_trade_quality_row(r, experience or {}) for r in records]
    if not is_df:
        merged = [{**r, **s} for r, s in zip(records, scored)]
        # Cross-row concentration logic is easier/consistent through DataFrame.
        tmp = apply_trade_quality_governance(pd.DataFrame(merged), experience or {})
        return tmp.to_dict(orient="records")

    score_df = pd.DataFrame(scored, index=out.index)
    out = out.drop(columns=[c for c in V188_COLUMNS if c in out.columns], errors="ignore")
    out = pd.concat([out, score_df.reindex(columns=V188_COLUMNS)], axis=1)

    # Sector concentration is a portfolio/ranking property, not a single-stock
    # signal.  First two names in a sector are untouched; later names get a soft
    # penalty only.  No candidate is deleted.
    if not out.empty:
        base_raw = out["V188股神作戰優先分"] if "V188股神作戰優先分" in out.columns else pd.Series([0.0] * len(out), index=out.index)
        legacy_raw = out["股神推薦優先分"] if "股神推薦優先分" in out.columns else pd.Series([0.0] * len(out), index=out.index)
        base_priority = pd.to_numeric(base_raw, errors="coerce").fillna(0.0)
        legacy_priority = pd.to_numeric(legacy_raw, errors="coerce").fillna(0.0)
        order = sorted(range(len(out)), key=lambda i: (float(base_priority.iloc[i]), float(legacy_priority.iloc[i])), reverse=True)
        counts: dict[str, int] = {}
        for pos in order:
            idx = out.index[pos]
            sector = _sector_name(out.loc[idx])
            counts[sector] = counts.get(sector, 0) + 1
            nth = counts[sector]
            penalty = 0.0 if sector == "未分類" or nth <= 2 else min(12.0, 3.0 * (nth - 2))
            if penalty > 0:
                out.at[idx, "V188類股集中扣分"] = penalty
                out.at[idx, "V188類股集中治理"] = f"SOFT｜{sector}第{nth}檔，同族群集中扣{penalty:.0f}分"
                out.at[idx, "V188股神作戰優先分"] = round(max(0.0, _f(out.at[idx, "V188股神作戰優先分"]) - penalty), 1)
            else:
                out.at[idx, "V188類股集中治理"] = f"PASS｜{sector}第{nth}檔"

    # Demotion-only: existing hard recommendation fields are tightened when V188
    # says the trade is not executable.  Never set legacy formal fields to true.
    if "V188交易許可" in out.columns:
        for idx in out.index:
            permission = _s(out.at[idx, "V188交易許可"])
            formal_ok = _s(out.at[idx, "V188正式推薦資格"]).startswith("是")
            if not formal_ok:
                if "是否正式推薦" in out.columns:
                    out.at[idx, "是否正式推薦"] = False
                if "正式推薦分區" in out.columns:
                    _old_bucket = _s(out.at[idx, "正式推薦分區"])
                    if _old_bucket in {"正式下週主推薦", "A-｜準主推薦小量試單", "正式推薦", "A-準主推薦"}:
                        out.at[idx, "正式推薦分區"] = "不可直接買觀察" if permission.startswith(("BLOCK", "NO-TRADE")) else "盤中雷達追蹤"
                if "正式推薦資格" in out.columns:
                    out.at[idx, "正式推薦資格"] = f"V188降級｜{permission}"
                if "下週是否可直接買" in out.columns:
                    out.at[idx, "下週是否可直接買"] = "不可｜V188未通過Trade治理"
                if "正式推薦動作" in out.columns:
                    out.at[idx, "正式推薦動作"] = "V188未通過交易品質治理；保留Alpha研究價值，但不得建立正式新倉。"
                if "操作許可" in out.columns and permission:
                    old = _s(out.at[idx, "操作許可"])
                    # Preserve hard block wording if it was already stricter.
                    if not _contains(old, ["禁止新倉", "禁止買進", "lockdown"]):
                        out.at[idx, "操作許可"] = permission
                if "SuperAI進場狀態" in out.columns:
                    if permission.startswith("WAIT-PULLBACK"):
                        out.at[idx, "SuperAI進場狀態"] = "WAIT-PULLBACK｜V188禁止追價，只准回測守價"
                    elif permission.startswith("WAIT-MARKET"):
                        out.at[idx, "SuperAI進場狀態"] = "WAIT-MARKET｜V188大盤未對齊"
                    elif permission.startswith("WAIT-DATA"):
                        out.at[idx, "SuperAI進場狀態"] = "WAIT-DATA｜V188個股資料證據不足"
                    elif permission.startswith(("NO-TRADE", "BLOCK")):
                        out.at[idx, "SuperAI進場狀態"] = "AVOID｜V188交易品質禁止新倉"
    return out


__all__ = [
    "V188_VERSION", "V188_COLUMNS", "canonical_execution_rr", "score_trade_quality_row", "apply_trade_quality_governance",
]
