# -*- coding: utf-8 -*-
"""H153.4 | 三率三升 (gross/operating/net margin), announcement-time research evidence.

Only OFFICIAL individual-quarter ratios from H115 PIT observations are eligible.
Two consecutive quarters must improve on their corresponding year-ago quarters.
QoQ is independently shown, NEVER substituted for YoY.  Research, not H126.
"""
from __future__ import annotations
from datetime import date, datetime
from math import isfinite
from typing import Any, Mapping

VERSION = 'V191-H153.4-THREE-RISES-PIT'
KEYS = ('gross_margin_pct', 'operating_margin_pct', 'net_margin_pct')
MISSING = {'status': '待補官方單季財報', 'yoy_two': False, 'qoq': None,
           'breakout': False, 'quarter': '', 'gross': None, 'operating': None,
           'net': None, 'yoy_delta': '', 'available_at': '',
           'reason': '需本季／前季及對應去年兩季的已公告單季三率；突破另需近五季；累計不能冒充單季'}


def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool): return None
    try:
        x = float(str(value).strip().replace(',', '').replace('%',''))
        return x if isfinite(x) and -100 <= x <= 1000 else None
    except (TypeError, ValueError, OverflowError): return None


def _period(value: Any) -> tuple[int,int] | None:
    s = str(value or '').upper().strip().replace('-', '').replace('/', '')
    if 'Q' not in s:return None
    try:
        a,b=s.split('Q');y=int(a);q=int(b)
    except ValueError:return None
    if y < 1911:y+=1911
    return (y,q) if q in (1,2,3,4) and 1900 <= y <= 2200 else None


def _quarter_index(s: str) -> int | None:
    p=_period(s)
    return (p[0]*4+p[1]-1) if p else None


def evaluate_three_rises(history: Any, *, research_day: Any) -> dict[str, Any]:
    """No future data, no gaps, no fabricated recognition of three-rises.

    Date-only available_at is interpreted at 23:59:59 Asia/Taipei. A source
    observation must be marked by H115's canonical PIT reader, not arbitrary
    data in a scanned stock row. Return comparable percentage-point changes.
    """
    out=dict(MISSING)
    from godpick_h115_pit_evidence import _datetime, _source
    cutoff=_datetime(research_day, end_of_date=True)
    if cutoff is None or not isinstance(history, (list, tuple)):
        return out
    quarters={}
    for r in history:
        if not isinstance(r, Mapping):continue
        idx=_quarter_index(r.get('year_quarter'))
        t=_datetime(r.get('available_at'),end_of_date=True)
        if (idx is None or t is None or t>cutoff or
            _source(r.get('source')) not in {'TWSE','TPEX','MOPS'} or
            r.get('statement_basis')!='SINGLE_QUARTER'):
            continue
        vals=tuple(_num(r.get(k)) for k in KEYS)
        if any(v is None for v in vals):continue
        if idx not in quarters or t>quarters[idx][0]:quarters[idx]=(t,vals,r)
    if not quarters:return out
    end=max(quarters)
    latest_quarter=cutoff.year*4+(cutoff.month-1)//3
    if end>latest_quarter or latest_quarter-end>2:
        out['reason']='單季三率資料落後超過兩季或季別超過研究日，不認證近期改善'
        return out
    current_t,current,ref=quarters[end]
    out.update({'quarter':str(ref.get('year_quarter')),
                'gross':current[0],'operating':current[1],'net':current[2],
                'available_at':current_t.isoformat(timespec='seconds')})
    # To certify two YoY quarters we require current/end-1 and end-4/end-5.
    required=(end,end-1,end-4,end-5)
    if not all(k in quarters for k in required):
        out['reason']='三率有本季觀察，但缺連續兩季與各自去年同季的完整公告資料'
        return out
    now=quarters[end][1];prev=quarters[end-1][1]
    ly=quarters[end-4][1];lyprev=quarters[end-5][1]
    latest_delta=tuple(round(now[i]-ly[i],3) for i in range(3))
    out['yoy_delta']='毛利{:+.2f}pp／營益{:+.2f}pp／淨利{:+.2f}pp'.format(*latest_delta)
    out['qoq']=all(now[i]>prev[i] for i in range(3))
    rising=all(now[i]>ly[i] and prev[i]>lyprev[i] for i in range(3))
    out['yoy_two']=rising and now[1]>0 and now[2]>0
    out['status']=('連兩季三率年增｜獲利為正' if out['yoy_two'] else
                   '單季三率資料已核｜未達連兩季三率年增')
    # No claims about a 4-quarter record without the complete prior 4 quarters.
    if out['yoy_two'] and all(k in quarters for k in (end-1,end-2,end-3,end-4)):
        older=[quarters[k][1] for k in (end-1,end-2,end-3,end-4)]
        out['breakout']=all(now[i]>max(v[i] for v in older) for i in (0,1))
    out['reason']=('三率採單季%，兩季各對比去年同季；突破須毛利率與營益率均創近五季新高。'
                   '淨利率可能受業外影響，須再核獲利品質。')
    return out

__all__=['VERSION','evaluate_three_rises']
