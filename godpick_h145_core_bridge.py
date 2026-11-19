# -*- coding: utf-8 -*-
"""H145: restore only proven same-run market candidate input for manager export.

Pure, read-only guards. Does not certify official facts, grant trades, fetch
quotes, use historical ranks, or write durable state.
"""
from __future__ import annotations
from typing import Any, Mapping, Callable
import re
import pandas as pd

VERSION = "V191-H145.0-SAME-RUN-CANDIDATE-BRIDGE"


def _frame(value: Any) -> pd.DataFrame:
    return value if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _count(value: Any) -> int:
    try:
        v = int(value)
        return v if v >= 0 else 0
    except (TypeError, ValueError, OverflowError):
        return 0


def _codes(frame: pd.DataFrame) -> set[str]:
    if frame.empty or "股票代號" not in frame:
        return set()
    return {str(v).strip().removesuffix(".0") for v in frame["股票代號"].tolist()
            if re.fullmatch(r"[1-9][0-9]{3}", str(v).strip().removesuffix(".0"))}


def restore_export_pool(
    supplied: Any, cached_decision: Any, final_recommendations: Any,
    *, scan_report: Mapping[str, Any] | None, context_run_id: str,
    scan_run_id: str, decision_signature: str, active_signature: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Prefer supplied full scan; otherwise use only validated current-run cache.

    Never use a historical snapshot or final 8 rows to fabricate a 1665-row
    diagnosis. A matching scan signature AND matching run ID are mandatory to
    recover a partial supplied export input.
    """
    supplied = _frame(supplied)
    cache = _frame(cached_decision)
    final = _frame(final_recommendations)
    report = scan_report if isinstance(scan_report, Mapping) else {}
    expected = _count(report.get("candidate_diagnosis_count"))
    if not expected:
        expected = _count(report.get("analyzed_ok"))
    final_expected = _count(report.get("final_candidate_count")) or _count(report.get("final_count"))
    expected_final_codes = _codes(final)
    result = {
        "expected_diagnosis": expected, "expected_final": final_expected,
        "supplied_rows": len(supplied), "cache_rows": len(cache),
        "source": "SUPPLIED_PARTIAL", "recovered": False,
        "same_run_proven": False, "candidate_complete": False,
        "missing_final_codes": [], "status": "CANDIDATE_SCOPE_NOT_PROVEN",
    }
    # Do not claim full-scan coverage without a scanner denominator.
    run_ids_match = bool(context_run_id and scan_run_id and context_run_id == scan_run_id)
    supplied_complete = bool(expected > 0 and len(supplied) == expected)
    if supplied_complete:
        result.update(source="SUPPLIED_FULL_SCAN", same_run_proven=run_ids_match,
                      candidate_complete=run_ids_match,
                      status="SUPPLIED_FULL_SCAN_MATCHED" if run_ids_match else
                             "BLOCKED_FULL_SCAN_RUN_ID_UNVERIFIED")
        selected = supplied
    else:
        same_run = bool(run_ids_match and decision_signature and
                        decision_signature == active_signature)
        # Scanner's count is a stronger check than a rounded cache-size ratio.
        cache_ok = bool(same_run and expected > 0 and len(cache) == expected and
                        "股票代號" in cache and len(_codes(cache)) == expected)
        if cache_ok:
            result.update(source="VERIFIED_SAME_RUN_DECISION_CACHE", recovered=True,
                          same_run_proven=True, candidate_complete=True,
                          status="RECOVERED_FULL_SCAN_SAME_RUN")
            selected = cache
        else:
            selected = supplied
            result["status"] = "BLOCKED_PARTIAL_EXPORT_NO_TRUSTED_RECOVERY"
    missing = sorted(expected_final_codes - _codes(selected))
    result["missing_final_codes"] = missing
    if missing:
        was_complete = result["candidate_complete"]
        result["candidate_complete"] = False
        if was_complete or not result["status"].startswith("BLOCKED_"):
            result["status"] = "BLOCKED_FINAL_CODES_NOT_IN_CANDIDATES"
    if final_expected and len(expected_final_codes) < final_expected:
        # H170: the scanner's first pre-SuperAI shortlist and the final
        # post-governance selector are different decision STAGES. Count
        # differences are not necessarily missing Excel rows. However, only
        # an independent SAME-RUN stage contract and complete mother pool
        # can prove a legitimate reselection; otherwise fail closed.
        try:
            from godpick_h170_stage_reconcile import validate_stage_contract
            stage = validate_stage_contract(
                report.get("h170_stage_contract"), expected_pre=final_expected,
                actual_final=final,context_run_id=context_run_id,
                scan_run_id=scan_run_id,decision_signature=decision_signature,
                active_signature=active_signature,
            )
        except Exception as exc:
            stage = {"stage_verified":False,"result":"BLOCKED_STAGE_VALIDATION_EXCEPTION",
                     "exception_type":type(exc).__name__}
        result["h170_stage_audit"]=stage
        if stage.get("stage_verified") and result.get("candidate_complete") and not missing:
            result["status"]="RECONCILED_POST_GOVERNANCE_RESELECTION"
            # No candidate was added; H109/H126 is still the only trade gate.
            result["trade_authority_changed"]=False
        else:
            result["candidate_complete"] = False
            result["status"] = "BLOCKED_FINAL_RESULT_COUNT_GAP"
    # H169: distinguish a scanner count measured before de-duplication from
    # actual unique final codes. Without a same-run list of expected codes we
    # cannot reconstruct the missing name nor silently lower the denominator.
    raw_final_rows = len(final)
    actual_unique = len(expected_final_codes)
    result["final_reconciliation"] = {
        "scanner_expected_count": final_expected,
        "actual_final_rows": raw_final_rows,
        "actual_unique_4digit_codes": actual_unique,
        "duplicated_or_invalid_final_rows": max(0, raw_final_rows - actual_unique),
        "unaccounted_unique_gap": max(0, final_expected - actual_unique),
        "same_run_ids_match": run_ids_match,
        "candidate_codes_missing_in_diagnosis": list(missing),
        "exact_missing_code_known": False,
        "reason": (
            "最終原始列不少於掃描預期，但重複或無效代號造成唯一代號不足；須追查去重前資料"
            if final_expected > actual_unique and raw_final_rows >= final_expected
            else "正式候選結果在輸出前已減少；現有欄位無法推定遺失的股票代號"
            if final_expected > actual_unique
            else "正式候選唯一代號數量核對通過；仍須逐股官方證據與H126權威"
        ),
        "can_authorize_trade": False,
    }
    result["selected_rows"] = len(selected)
    return selected.copy(deep=False), result


def bounded_with_final_coverage(
    complete: Any, final: Any, *, selector: Callable[[pd.DataFrame], pd.DataFrame],
    limit: int = 240,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Keep existing H104 240-row scan strategy, but guarantee surviving final codes.

    Only uses original rows already in the full candidate frame. No additional
    stocks, no future quotes, no score changes, no trading permission.
    """
    source, final = _frame(complete), _frame(final)
    if source.empty or not 1 <= limit <= 500:
        return pd.DataFrame(), {"status": "EMPTY_INPUT", "missing": sorted(_codes(final))}
    selected = selector(source.copy(deep=False))
    selected = _frame(selected).head(limit).copy()
    desired = _codes(final)
    if not desired or "股票代號" not in source:
        return selected, {"status": "NO_FINAL_CODES", "missing": []}
    available = _codes(source)
    missing = sorted(desired - available)
    if missing:
        return selected, {"status": "FINAL_CODE_NOT_IN_FULL_POOL", "missing": missing}
    # Work with strings for ID comparison, never numeric stock IDs.
    selected["__h145_code"] = selected["股票代號"].astype(str).str.strip().str.removesuffix(".0")
    source_codes = source["股票代號"].astype(str).str.strip().str.removesuffix(".0")
    must = source.loc[source_codes.isin(desired)].copy()
    must["__h145_code"] = must["股票代號"].astype(str).str.strip().str.removesuffix(".0")
    must = must.drop_duplicates("__h145_code", keep="first")
    # Retain all final stocks at top so a capped Audit cannot silently hide them.
    rest = selected.loc[~selected["__h145_code"].isin(desired)].copy()
    combined = pd.concat([must, rest], ignore_index=True, sort=False)
    combined = combined.drop_duplicates("__h145_code").head(limit)
    combined = combined.drop(columns=["__h145_code"])
    return combined.reset_index(drop=True), {"status": "FINAL_CODES_PINNED", "missing": [],
                                               "pinned": len(must), "bounded_rows": len(combined)}


def audit_final_coverage(audit: Any, final: Any) -> dict[str, Any]:
    wanted = _codes(_frame(final))
    present = _codes(_frame(audit))
    missing = sorted(wanted-present)
    return {"wanted": len(wanted), "observed": len(wanted & present),
            "missing": missing, "complete": not missing}
