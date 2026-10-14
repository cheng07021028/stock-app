# -*- coding: utf-8 -*-
"""H120 rolling-session paired strategy comparisons, exploratory only.

Never auto-updates production weights, H113 gates, settings or trades.
Only H118/H119 unadjusted, quality-qualified price windows (data_status OK)
with a matched benchmark are included.  Cash receivables and unsettled stock
rights are explicitly excluded from a price-index alpha comparison.
"""
from __future__ import annotations
from collections import Counter, defaultdict
from datetime import date
from statistics import mean,median
from typing import Any,Iterable,Mapping
import csv,hashlib,json,math,random

VERSION='V191-H120.1-ROLLING-PAIRWISE-HUMAN-ONLY'
WINDOWS=(60,120)
HORIZONS=(1,3,5)


def _num(x:Any)->float|None:
    try:v=float(x)
    except (TypeError,ValueError):return None
    return v if math.isfinite(v) else None


def _day(s:Any)->str:
    d=date.fromisoformat(str(s or ''))
    return d.isoformat()


def _series_rows(details:Iterable[Mapping[str,Any]])->list[dict[str,Any]]:
    rows=[];seen=set()
    for raw in details:
        if raw.get('data_status')!='OK':continue
        day=_day(raw.get('decision_date'))
        horizon=int(raw.get('horizon') or 0)
        if horizon not in HORIZONS:continue
        code=str(raw.get('code') or '').strip()
        strat=str(raw.get('strategy') or '').strip()
        tier=str(raw.get('tier') or '').strip()
        alpha=_num(raw.get('alpha_pp'));net=_num(raw.get('net_return_pct'))
        if not code or not strat or alpha is None or net is None:continue
        if raw.get('return_basis') not in ('PRICE_ONLY_NO_ACTION','PRICE_ONLY'):continue
        key=(day,code,strat,horizon)
        if key in seen:continue
        seen.add(key)
        rows.append({'decision_date':day,'code':code,'strategy':strat,'tier':tier,
                     'horizon':horizon,'alpha_pp':alpha,'net_return_pct':net})
    return rows


def _block_bootstrap(values:list[float],n:int=500,block:int=5)->tuple[float,float]|None:
    if len(values)<10:return None
    rnd=random.Random(0x120120)
    b=min(block,len(values));results=[]
    # Circular moving block bootstrap preserving short-range day dependence.
    for _ in range(n):
        sample=[]
        while len(sample)<len(values):
            start=rnd.randrange(len(values));sample.extend(values[(start+i)%len(values)] for i in range(b))
        results.append(mean(sample[:len(values)]))
    results.sort();return (round(results[int(.025*(n-1))],4),round(results[int(.975*(n-1))],4))


def rolling_comparison(details:Iterable[Mapping[str,Any]], *,market_sessions:Iterable[str],
                       baseline:str='H113_LEGACY', candidate_prefix:str='H114_',
                       min_paired_days:int=20, min_window_sessions:int=60)->dict[str,Any]:
    sessions=sorted(set(_day(x) for x in market_sessions))
    if not sessions:return {'version':VERSION,'status':'NO_VERIFIED_MARKET_CALENDAR','windows':[],
                            'no_auto_apply':True,'data_policy':'PRICE_ONLY;HUMAN_REVIEW'}
    rows=_series_rows(details)
    windows=[]
    for size in WINDOWS:
        days=sessions[-size:];lookup=set(days)
        window_rows=[r for r in rows if r['decision_date'] in lookup]
        for horizon in HORIZONS:
            sub=[r for r in window_rows if r['horizon']==horizon]
            daily=defaultdict(lambda:defaultdict(list))
            for r in sub:daily[r['strategy']][r['decision_date']].append(r)
            base_daily={d:mean(q['alpha_pp'] for q in x) for d,x in daily[baseline].items()}
            candidates=sorted(s for s in daily if s.startswith(candidate_prefix))
            if not candidates:
                windows.append(dict(window_sessions=size,horizon=horizon,baseline=baseline,
                     candidate='NONE',status='NO_CANDIDATE_EVIDENCE',
                     available_sessions=len(days),paired_days=0,auto_apply=False))
                continue
            for strategy in candidates:
                alt_daily={d:mean(q['alpha_pp'] for q in x) for d,x in daily[strategy].items()}
                paired=sorted(set(base_daily)&set(alt_daily))
                deltas=[alt_daily[d]-base_daily[d] for d in paired]
                n=len(paired)
                enough=(len(days)>=min_window_sessions and n>=min_paired_days)
                bounds=_block_bootstrap(deltas) if enough else None
                windows.append({'window_sessions':size,'available_sessions':len(days),
                  'window_start':days[0] if days else None,'window_end':days[-1] if days else None,
                  'horizon':horizon,'baseline':baseline,'candidate':strategy,
                  'baseline_distinct_days':len(base_daily),'candidate_distinct_days':len(alt_daily),
                  'paired_days':n,'paired_session_coverage_pct':round(100*n/len(days),2) if days else 0,
                  'paired_mean_alpha_diff_pp':round(mean(deltas),4) if deltas else None,
                  'paired_median_alpha_diff_pp':round(median(deltas),4) if deltas else None,
                  'paired_positive_rate_pct':round(100*sum(v>0 for v in deltas)/n,2) if n else None,
                  'exploratory_block_bootstrap_low_pp':bounds[0] if bounds else None,
                  'exploratory_block_bootstrap_high_pp':bounds[1] if bounds else None,
                  'status':'ELIGIBLE_FOR_EXPLORATORY_HUMAN_REVIEW_NOT_APPROVED' if enough else 'INSUFFICIENT_PAIRED_TRADING_DAYS',
                  'auto_apply':False})
    status='HUMAN_REVIEW_ONLY' if any(x['status']=='ELIGIBLE_FOR_EXPLORATORY_HUMAN_REVIEW_NOT_APPROVED' for x in windows) else 'INSUFFICIENT_VERIFIABLE_SAMPLES'
    return {'version':VERSION,'status':status,'available_price_rows':len(rows),
            'total_market_sessions':len(sessions),'windows':windows,'no_auto_apply':True,
            'limitations':'同日配對但非隨機實驗；重疊5日持有期使樣本相關；bootstrap區間僅探索性，非策略獲利證明；未含權息收益。'}
