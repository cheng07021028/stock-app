# -*- coding: utf-8 -*-
"""H123 evidence-tier attribution. Read-only; never approves alpha or weights."""
from __future__ import annotations
from collections import defaultdict,Counter
from typing import Any,Mapping,Sequence
from statistics import mean,median
import hashlib
import json

VERSION='V191-H123.1-SOURCE_QUALITY_ATTRIBUTION'
HORIZONS=(1,3,5)


def source_check(manifest:Mapping[str,Any],details_bytes:bytes)->str:
    if manifest.get('h118_quality_policy')!='STRICT':return 'H118_NOT_STRICT'
    if manifest.get('h120_policy')!='MANUAL_RAW_PROOF_ENFORCED':return 'H120_PROOF_NOT_ENFORCED'
    if not manifest.get('h120_proof_accepted_days'):return 'H120_NO_ATTESTED_DAYS'
    if hashlib.sha256(details_bytes).hexdigest()!=manifest.get('h120_details_csv_sha256'):
        return 'DETAILS_HASH_MISMATCH'
    return 'STRICT_EVIDENCE_COMPLETE'


def review_quality(*,snapshots:Sequence[Mapping[str,Any]],details:Sequence[Mapping[str,Any]],
                   source_manifest:Mapping[str,Any],details_bytes:bytes)->dict[str,Any]:
    """A separate group analysis, not a hindsight reclassification of predictions.

    Only actually captured H116 diagnostics count. Cannot re-run H123 on
    historical rows to create a purported evidence-grade history.
    """
    status=source_check(source_manifest,details_bytes)
    result={'version':VERSION,'status':status,'no_auto_apply':True,
            'message':'實證來源分組僅供稽核，不能直接證明某策略具備超額報酬',
            'groups':[],'snapshots':len(snapshots),'horizons':list(HORIZONS)}
    if status!='STRICT_EVIDENCE_COMPLETE':return result
    from godpick_h116_daily_feedback import verify_snapshot
    # Repeated scans in a day never multiply performance: use first captured
    # snapshot for one stock+strategy+day, no artificial selection of winner.
    cohorts={}
    for snap in sorted(snapshots,key=lambda s:(str(s.get('decided_at')),str(s.get('run_id')))):
        if not verify_snapshot(snap):raise ValueError('H116推薦快照被修改')
        for e in snap.get('decisions',[]):
            key=(str(e.get('decision_date')),str(e.get('code')),str(e.get('strategy')))
            if key in cohorts:continue
            d=e.get('diagnostics',{}) or {}
            tag=str(d.get('H123法人實證狀態') or 'NOT_CAPTURED')
            asof=str(e.get('asof_status') or '')
            cohort=tag if asof=='VERIFIED_PER_ROW' else 'ASOF_UNVERIFIED'
            cohorts[key]=cohort
    by=defaultdict(list);days=defaultdict(set);seen=set();reason=Counter()
    for row in details:
        key=(str(row.get('decision_date')),str(row.get('code')),str(row.get('strategy')))
        try:h=int(row.get('horizon'))
        except Exception:continue
        if h not in HORIZONS:continue
        unique=(*key,h)
        if unique in seen:continue
        seen.add(unique)
        cohort=cohorts.get(key,'SNAPSHOT_MISSING')
        if row.get('data_status')!='OK' or row.get('return_basis') not in ('PRICE_ONLY_NO_ACTION','PRICE_ONLY'):
            reason[str(row.get('data_status') or 'UNKNOWN')]+=1;continue
        if cohort not in ('PROVEN_FLOW','VERIFIED_BUT_NOT_BUYING'):
            reason[cohort]+=1;continue
        try:net=float(row['net_return_pct']);alpha=float(row['alpha_pp'])
        except (TypeError,ValueError):reason['MISSING_MATCHED_BENCHMARK']+=1;continue
        if not all(abs(x)<1e4 for x in (net,alpha)):reason['INVALID_RETURN_RANGE']+=1;continue
        group=(key[2],cohort,h)
        by[group].append((key[0],net,alpha));days[group].add(key[0])
    rows=[]
    for (strategy,cohort,h),values in sorted(by.items()):
        dedup=defaultdict(list)
        for d,net,alpha in values:dedup[d].append((net,alpha))
        daily=[(mean(v[0] for v in vals),mean(v[1] for v in vals)) for vals in dedup.values()]
        rows.append({'strategy':strategy,'institution_quality':cohort,'horizon':h,
                     'stock_day_samples':len(values),'distinct_decision_days':len(days[(strategy,cohort,h)]),
                     'mean_daily_net_pct':round(mean(v[0] for v in daily),4),
                     'mean_daily_alpha_pp':round(mean(v[1] for v in daily),4),
                     'status':'REVIEWABLE_QUALITY_COHORT_NOT_CAUSAL' if len(days[(strategy,cohort,h)])>=20
                         else 'INSUFFICIENT_DISTINCT_DAYS',
                     'auto_apply':False})
    result.update(groups=rows,excluded=dict(reason),
                  status='EVIDENCE_COHORTS_RESEARCH_ONLY' if rows else 'NO_VERIFIED_MATCHED_PAIRS')
    return result
