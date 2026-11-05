# -*- coding: utf-8 -*-
"""H153.1 | explicit research risk states and read-only evidence-quality checks.

A research state never changes or overrides the independent H126 gate.
Data missing is UNKNOWN, not an affirmative H109 veto or an affirmative PASS.
"""
from __future__ import annotations
from collections import Counter
from datetime import date
from typing import Any, Mapping
from math import isfinite

VERSION = 'V191-H153.1-QUALITY-REPAIR'
PASS_STATES = frozenset({'ALLOW','PASS','NONE','RESEARCH_ELIGIBLE'})
UNKNOWN_STATES = frozenset({'','UNKNOWN','PENDING','PENDING_EVIDENCE','MISSING','NOT_AVAILABLE','UNVERIFIED','NULL','NA','N/A'})


def classify_h109(value: Any) -> str:
    raw = str(value if value is not None else '').strip().upper()
    if raw in PASS_STATES:
        return 'RESEARCH_ALLOWED'  # never means H126 trading approval
    if raw in UNKNOWN_STATES:
        return 'PENDING_EVIDENCE'
    if any(k in raw for k in ('CONFLICT','INCONSISTENT','DISAGREE')):
        return 'EVIDENCE_CONFLICT'
    if any(k in raw for k in ('VETO','QUARANTINE','BLOCK','DENY','REJECT','EXCLUDE','PROHIBIT','STOP')):
        return 'EXPLICIT_VETO'
    # Unrecognized future H109 values are NOT silently promoted or vetoed.
    return 'PENDING_EVIDENCE'


def _finite(x: Any) -> float | None:
    if x is None or isinstance(x,bool):return None
    try:
        v=float(str(x).strip().replace(',',''))
        return v if isfinite(v) else None
    except (ValueError, TypeError, OverflowError):return None


def liquidity_reasons(*, amount: Any, average: Any, lots: Any, ratio: Any,
                      min_amount: float=150., min_lots: float=500., min_ratio: float=.55) -> list[str]:
    """Independent, potentially overlapping reasons; missing != failed threshold."""
    issues=[]
    for label,val,minimum in (
        ('當日成交額百萬',amount,min_amount),('20日均成交額百萬',average,min_amount),
        ('成交量張',lots,min_lots),('量比',ratio,min_ratio)):
        n=_finite(val)
        if n is None:issues.append(label+'缺來源')
        elif n<minimum:issues.append(label+'未達研究門檻')
    return issues


def source_coverage(facts: Mapping[str, Any] | None) -> dict[str,int]:
    """Evidence availability only; does NOT claim full TWSE/TPEx market coverage."""
    stats=Counter()
    for data in (facts or {}).values():
        if not isinstance(data,Mapping):continue
        hist=data.get('H114月營收歷史')
        count=len({str(x.get('year_month')) for x in hist if isinstance(x,Mapping) and x.get('year_month')}) if isinstance(hist,list) else 0
        if count: stats['有至少1月已公告營收個股']+=1
        if count>=12:stats['有至少12月已公告營收個股']+=1
        if count>=18:stats['有至少18月已公告營收個股']+=1
        if count>=24:stats['有至少24月已公告營收個股']+=1
        if _finite(data.get('H114本益比實值')) is not None and _finite(data.get('H114近四季EPS實值')) is not None:
            stats['有官方原始PE_EPS個股']+=1
        if _finite(data.get('H114同業本益比中位數_官方')) is not None:
            stats['具足額同業比較個股']+=1
    return dict(stats)

__all__=['VERSION','classify_h109','liquidity_reasons','source_coverage']
