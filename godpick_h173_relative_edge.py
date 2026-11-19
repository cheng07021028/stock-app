# -*- coding: utf-8 -*-
"""V191-H173: honest relative-index attribution of signed H172 historical outcomes.

This is an *ex-post audit*, never a ranking, re-training signal, forecast, or trade
permission. H172's close-to-close stock price-only performance is checked anew
against the matching H117 signed stock and official TAIEX price-index quotes.

A late/restated report, missing session, duplicate market bar or source mismatch
fails closed. Price returns exclude cash/stock dividends, splits, taxes and fees.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from hashlib import sha256
from math import isfinite, sqrt
from statistics import median
from typing import Any, Mapping
import json
import re

from godpick_h117_official_market_closure import verify_manifest
from godpick_h172_closed_loop import _moment, verify_research_report

VERSION = 'V191-H173-SIGNED-RELATIVE-EDGE-RESEARCH-ONLY'
SOURCES = ('TAIEX', 'TWSE', 'TPEX')
HORIZONS = (1, 5, 10)


def _sha(content: Mapping) -> str:
    return sha256(json.dumps(content, ensure_ascii=False, sort_keys=True,
                             separators=(',', ':'), allow_nan=False).encode('utf-8')).hexdigest()


def _positive(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        x = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return x if isfinite(x) and x > 0 else None


def _wilson_lower(wins: int, count: int) -> float | None:
    if not count:
        return None
    p = wins / count
    z = 1.959963984540054
    den = 1 + z * z / count
    low = (p + z*z/(2*count) - z * sqrt((p * (1-p) + z*z/(4*count))/count)) / den
    return round(max(0, low) * 100, 2)


def _market_evidence(daily: Mapping, cutoff: datetime, expected_hashes: list) -> tuple[list[str], dict, dict]:
    if not isinstance(daily, Mapping) or any(not isinstance(daily.get(src), Mapping) for src in SOURCES):
        raise ValueError('H173 all three H117 source containers required')
    manifests = {}
    digest_list = []
    for src in SOURCES:
        for day, m in daily[src].items():
            if not isinstance(day, str) or not re.fullmatch(r'20\d{2}-\d{2}-\d{2}', day):
                raise ValueError('H173 invalid market session label')
            if not isinstance(m, Mapping) or not verify_manifest(m) or m.get('source') != src or m.get('date') != day:
                raise ValueError(f'H173 H117 hash/source/date mismatch {src}/{day}')
            at = _moment(m.get('collected_at'))
            if at is None or at.date().isoformat() < day:
                raise ValueError('H173 invalid official first-observed time')
            if at > cutoff:
                continue
            manifests[(src,day)] = m
            digest_list.append(m['content_sha256'])
    if sorted(digest_list) != expected_hashes:
        raise ValueError('H173 signed H172/H117 manifest list differs; no mixed runs')
    sessions = sorted(day for src,day in manifests if src=='TAIEX')
    index_close = {}
    stock_close = {}
    for (src,day),m in manifests.items():
        rows = m.get('rows')
        if not isinstance(rows,list):
            raise ValueError('H173 H117 non-list rows')
        if src == 'TAIEX':
            if len(rows) != 1:
                raise ValueError('H173 benchmark must be unique for each official day')
            r = rows[0]
            if not isinstance(r,Mapping) or r.get('code') != 'BENCHMARK' or r.get('exchange') != 'TAIEX' or r.get('date') != day:
                raise ValueError('H173 benchmark origin mismatch')
            v=_positive(r.get('close'))
            if v is None:
                raise ValueError('H173 TAIEX index close must be positive')
            index_close[day]=v
        else:
            for r in rows:
                if not isinstance(r,Mapping):
                    continue
                code=str(r.get('code') or '')
                if not re.fullmatch(r'[1-9]\d{3}',code):
                    continue
                if r.get('exchange')!=src or r.get('date')!=day:
                    raise ValueError('H173 stock bar source/date mismatch')
                k=(code,day)
                if k in stock_close:
                    raise ValueError('H173 cross-exchange duplicate stock bar')
                v=_positive(r.get('close'))
                if v is None:
                    raise ValueError('H173 invalid stock close in signed market evidence')
                stock_close[k]=v
    return sessions,index_close,stock_close


def _stats(items: list[Mapping], n:int, *, min_events:int, min_days:int, min_codes:int) -> dict:
    """One observation per (code, day) to limit repeat-run inflation."""
    mature = []
    unique = {}
    for event in sorted(items,key=lambda e:(str(e.get('code')),str(e.get('signal_date')),str(e.get('event_id')))):
        part=event['relative'][f'T+{n}']
        if part['status']!='NOT_MATURED':
            mature.append(event)
        unique.setdefault((event['code'],event['signal_date']),event)
    valid=[e for e in unique.values() if e['relative'][f'T+{n}']['status']=='EVALUATED_RELATIVE_PRICE_ONLY']
    vals=[e['relative'][f'T+{n}'] for e in valid]
    m=len(vals)
    plus=sum(v['excess_return_pct']>0 for v in vals)
    negatives=[v for v in vals if v['index_fell']]
    separate_dates=len({e['signal_date'] for e in valid})
    separate_codes=len({e['code'] for e in valid})
    mature_unique=sum(e['relative'][f'T+{n}']['status']!='NOT_MATURED' for e in unique.values())
    coverage=round(100*m/mature_unique,2) if mature_unique else None
    reviewable=(m>=min_events and separate_dates>=min_days and separate_codes>=min_codes
                and coverage is not None and coverage>=80)
    return {
        'mature_distinct_code_days':mature_unique,
        'evaluated_distinct_code_days':m,
        'excluded_duplicate_events':len(items)-len(unique),
        'distinct_signal_days':separate_dates,
        'distinct_stock_codes':separate_codes,
        'evaluated_coverage_pct':coverage,
        'stock_win_rate_pct':round(100*sum(v['stock_return_pct']>0 for v in vals)/m,2) if m else None,
        'index_outperformance_rate_pct':round(100*plus/m,2) if m else None,
        'index_outperformance_wilson_lower_95_pct':_wilson_lower(plus,m),
        'mean_excess_pct_points':round(sum(v['excess_return_pct'] for v in vals)/m,4) if m else None,
        'median_excess_pct_points':round(median(v['excess_return_pct'] for v in vals),4) if m else None,
        'negative_index_windows':len(negatives),
        'positive_stock_on_negative_index_pct':round(100*sum(v['stock_held_flat_or_up'] for v in negatives)/len(negatives),2) if negatives else None,
        'outperformed_during_negative_index_pct':round(100*sum(v['beat_index'] for v in negatives)/len(negatives),2) if negatives else None,
        'evidence_status':'PRELIMINARY_REVIEW_ALLOWED_NO_AUTO_RANK' if reviewable else 'INSUFFICIENT_INDEPENDENT_EVIDENCE',
        'thresholds':{'min_unique_code_days':min_events,'min_signal_days':min_days,
                      'min_distinct_stock_codes':min_codes,'min_evaluated_coverage_pct':80},
    }


def build_relative_report(h172: Mapping, daily: Mapping, *, min_events:int=30,
                          min_days:int=15,min_codes:int=8) -> dict:
    if not verify_research_report(h172):
        raise ValueError('H173 H172 report SHA verification failed')
    if min_events<20 or min_days<10 or min_codes<5:
        raise ValueError('H173 cannot weaken minimum sample independence')
    cutoff=_moment(h172.get('as_of'))
    if cutoff is None:
        raise ValueError('H173 cutoff must be timezone-aware')
    expected=h172.get('h117_manifest_hashes')
    if not isinstance(expected,list) or not all(isinstance(x,str) for x in expected):
        raise ValueError('H173 H172 source manifest inventory missing')
    sessions,index,stocks=_market_evidence(daily,cutoff,expected)
    if len(sessions)!=h172.get('official_index_observed_sessions'):
        raise ValueError('H173 H172 session inventory mismatch')
    session_map={d:i for i,d in enumerate(sessions)}
    outcomes=[]
    for e in h172.get('results',[]):
        code=str(e.get('code') or '')
        day=str(e.get('signal_date') or '')
        if not re.fullmatch(r'[1-9]\d{3}',code) or day not in session_map:
            raise ValueError('H173 H172 event code/session invalid')
        returns=e.get('returns')
        if not isinstance(returns,Mapping):
            raise ValueError('H173 H172 returns container invalid')
        relative={}
        for n in HORIZONS:
            key=f'T+{n}'
            orig=returns.get(key)
            if not isinstance(orig,Mapping):
                raise ValueError('H173 missing original H172 horizon')
            target_idx=session_map[day]+n
            status=str(orig.get('status') or '')
            if target_idx>=len(sessions):
                if status=='EVALUATED_PRICE_ONLY':
                    raise ValueError('H173 un-matured stock result is marked evaluated')
                relative[key]={'status':'NOT_MATURED','stock_return_pct':None,'index_return_pct':None,
                               'excess_return_pct':None}
                continue
            target=sessions[target_idx]
            if status!='EVALUATED_PRICE_ONLY':
                relative[key]={'status':'STOCK_NOT_EVALUATED','stock_status':status,
                               'target_session':target,'stock_return_pct':None,
                               'index_return_pct':None,'excess_return_pct':None}
                continue
            if orig.get('target_session')!=target:
                raise ValueError(f'H173 H172 target session differs for {code}/{key}')
            s0=stocks.get((code,day));s1=stocks.get((code,target))
            if s0 is None or s1 is None:
                raise ValueError('H173 H172 evaluated stock lacks certified source close')
            audit_ret=round((s1/s0-1)*100,4)
            if not isinstance(orig.get('return_pct'),(int,float)) or abs(orig['return_pct']-audit_ret)>0.0002:
                raise ValueError('H173 H172 stock return conflicts with H117 signed close')
            b0=index.get(day);b1=index.get(target)
            if b0 is None or b1 is None:
                relative[key]={'status':'BENCHMARK_PRICE_MISSING','target_session':target,
                               'stock_return_pct':audit_ret,'index_return_pct':None,'excess_return_pct':None}
                continue
            index_ret=round((b1/b0-1)*100,4)
            excess=round(audit_ret-index_ret,4)
            relative[key]={'status':'EVALUATED_RELATIVE_PRICE_ONLY','target_session':target,
                           'stock_return_pct':audit_ret,'index_return_pct':index_ret,
                           'excess_return_pct':excess,'beat_index':excess>0,
                           'index_fell':index_ret<0,'stock_held_flat_or_up':index_ret<0 and audit_ret>=0,
                           'outperformed_during_index_decline':index_ret<0 and excess>0}
        outcomes.append({'event_id':e['event_id'],'code':code,'signal_date':day,
                         'strategy':str(e.get('strategy') or '')[:80],
                         'tier':str(e.get('tier') or '')[:80],
                         'source_table':str(e.get('source_table') or '')[:80],
                         'relative':relative})
    groups={}
    for dimension in ('strategy','tier','source_table'):
        by=defaultdict(list)
        for event in outcomes:
            by[event[dimension] or '未分類'].append(event)
        groups[dimension]={}
        for label,events in sorted(by.items()):
            groups[dimension][label]={f'T+{n}':_stats(events,n,min_events=min_events,
                             min_days=min_days,min_codes=min_codes) for n in HORIZONS}
    report={
        'version':VERSION,'status':'RESEARCH_RELATIVE_PRICE_ONLY_NOT_A_TRADE_BACKTEST',
        'as_of':h172['as_of'],'h172_report_sha256':h172['report_sha256'],
        'h117_manifest_hashes':sorted(expected),
        'independent_holiday_calendar_verified':False,
        'corporate_actions_adjusted':False,'trade_fees_dividends_taxes_included':False,
        'signal_origin':'H116_SIGNED_PIT_H172_VERIFIED',
        'observed_index_sessions':len(sessions),
        'research_events':len(outcomes),'excluded_h116_events':len(h172.get('excluded_events',[])),
        'overall':{f'T+{n}':_stats(outcomes,n,min_events=min_events,min_days=min_days,min_codes=min_codes)
                   for n in HORIZONS},
        'by_group':groups,'outcomes':outcomes,
        'ranking_change_proposed':False,'ranking_change_applied':False,
        'historical_record_written':False,'h109_h126_trade_authority_changed':False,
        'limitations':'同期加權股價指數，不是總報酬基準；官方市場假日/除權息尚未獨立認證。只比較截至as_of已觀察行情；同日重複股票按代號+推薦日去重。不得視為明日漲跌預測、買進訊號或策略勝率保證。',
    }
    report['report_sha256']=_sha(report)
    return report


def verify_relative_report(item: Mapping) -> bool:
    if not isinstance(item,Mapping) or item.get('version')!=VERSION:
        return False
    if item.get('ranking_change_applied') is not False or item.get('h109_h126_trade_authority_changed') is not False:
        return False
    if item.get('historical_record_written') is not False:
        return False
    body=dict(item);digest=body.pop('report_sha256',None)
    try:
        return isinstance(digest,str) and digest==_sha(body)
    except (TypeError,ValueError,OverflowError):
        return False


def load_relative_report(path: Any) -> dict | None:
    from pathlib import Path
    p=Path(path)
    if not p.is_file() or p.stat().st_size > 32_000_000:
        return None
    try:
        raw=json.loads(p.read_text('utf-8-sig'))
        return raw if verify_relative_report(raw) else None
    except (OSError,ValueError,TypeError,UnicodeError):
        return None

__all__=['VERSION','build_relative_report','verify_relative_report','load_relative_report']
