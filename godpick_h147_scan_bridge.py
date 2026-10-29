# -*- coding: utf-8 -*-
"""H147 exact schema bridge: Chinese scan-quality contract -> H144/H145 keys.

The scan generator emits Chinese keys. Earlier scope/repair readers expected
English debug-summary keys. Translate only identical semantics; never invent
counts from Excel rows, assume a run ID, or authorize a trade.
"""
from __future__ import annotations
from typing import Any, Mapping
import math

VERSION = 'V191-H147.0-SCAN-SCHEMA-BRIDGE'
ALIASES = {
    'total_count':'預計掃描數',
    'candidate_diagnosis_count':'完整候選診斷數',
    'final_candidate_count':'最終作戰候選數',
    # This is a pre-selection subset; NOT effective-K coverage.
    'analyzed_ok':'通過推薦前置篩選數',
}


def _count(value: Any) -> int | None:
    if value is None or isinstance(value,bool): return None
    try:
        n=float(str(value).replace(',','').strip())
        return int(n) if math.isfinite(n) and 0<=n<1000000 and n.is_integer() else None
    except (TypeError,ValueError,OverflowError): return None


def normalize_scan_report(raw: Any) -> tuple[dict[str,Any],dict[str,Any]]:
    """Separate missing, valid zero and contradictory counts; never overwrite errors.

    If both formats disagree, the original values remain available for forensic
    review and the conflicting canonical count is dropped (fail closed).
    """
    report=dict(raw) if isinstance(raw,Mapping) else {}
    issues=[];translated=[];seen={}
    for english,chinese in ALIASES.items():
        a=_count(report.get(english))
        b=_count(report.get(chinese))
        if a is not None and b is not None and a!=b:
            issues.append(f'CONFLICT_{english}:英文={a}，中文={b}')
            report.pop(english,None)
            continue
        v=a if a is not None else b
        if v is None:
            issues.append(f'MISSING_{english}')
            continue
        report[english]=v
        seen[english]=v
        if a is None and b is not None: translated.append(english)
    scanned=seen.get('total_count')
    diagnosed=seen.get('candidate_diagnosis_count')
    final=seen.get('final_candidate_count')
    if scanned == 0 and (diagnosed or final):
        issues.append('ZERO_SCAN_WITH_DOWNSTREAM_ROWS')
    if scanned is not None and diagnosed is not None and diagnosed>scanned:
        issues.append('DIAGNOSIS_GT_SCANNED')
    if diagnosed is not None and final is not None and final>diagnosed:
        issues.append('FINAL_GT_DIAGNOSIS')
    # Canonical integer counts are safe to DISPLAY, not evidence of same-run provenance.
    diag={'version':VERSION,'translated_keys':translated,'values':seen,
          'issues':issues,'status':'SCHEMA_RECONCILED' if not issues else 'SCAN_SCHEMA_NEEDS_REVIEW',
          'run_id_proven':False,'trade_authority_changed':False}
    return report,diag


__all__=['VERSION','ALIASES','normalize_scan_report']
