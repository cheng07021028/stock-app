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
        # A scanner says it selected eight but only one result row survived;
        # don't treat the remaining seven as implicitly in the spreadsheet.
        result["candidate_complete"] = False
        result["status"] = "BLOCKED_FINAL_RESULT_COUNT_GAP"
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
