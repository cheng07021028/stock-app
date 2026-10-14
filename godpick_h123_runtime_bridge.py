# -*- coding: utf-8 -*-
"""H123 conservative bridge: H117 verified calendar -> H123 evidence -> H113.

Mode OFF (default) preserves legacy; MONITOR audits only; STRICT may only veto.
No HTTP inside full-market stock scanning. Lazy context constructed once/scan.
"""
from __future__ import annotations
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any,Mapping
import os
from godpick_h123_flow_provenance import evidence_by_day,assess_flow,enforce_h113

VERSION='V191-H123.1-SAFE-RESEARCH-BRIDGE'
TZ=ZoneInfo('Asia/Taipei')


def config_mode()->str:
    m=os.getenv('GODPICK_H123_MODE','MONITOR').strip().upper()
    return m if m in ('OFF','MONITOR','STRICT') else 'OFF'


def load_context(base_dir:Path)->dict[str,Any]:
    """H117 three-source intersection: no synthetic market sessions."""
    ctx={'mode':config_mode(),'calendar':[],'evidence':{},'status':'UNAVAILABLE'}
    if ctx['mode']=='OFF':return ctx
    try:
        from godpick_h117_official_market_closure import verify_manifest
        from gzip import open as gzopen
        archive=Path(base_dir)/'data'/'h117'/'daily'
        sources=('TWSE','TPEX','TAIEX')
        filemap={src:{p.name[:-8]:p for p in (archive/src).glob('*.json.gz')}
                 for src in sources}
        # UNION, not intersection: missing any exchange archive is a genuine
        # session-integrity blocker, not a reason to skip that trading day.
        union=sorted(set().union(*(set(filemap[src]) for src in sources)))
        recent=union[-5:]
        if len(recent)<5 or any(day not in filemap[src] for day in recent for src in sources):
            ctx['status']='INCOMPLETE_OFFICIAL_DAY_CALENDAR'
            return ctx
        for day in recent:
            for src in sources:
                with gzopen(filemap[src][day],'rt',encoding='utf-8') as fh:
                    obj=__import__('json').load(fh)
                if not verify_manifest(obj) or obj.get('date')!=day or obj.get('source')!=src:
                    raise ValueError('official market archive failed integrity: '+src+'/'+day)
        ctx['calendar']=recent
        ctx['evidence']=evidence_by_day(base_dir)
        ctx['status']='OFFICIAL_SESSIONS_AND_ARCHIVE_LOADED'
    except Exception as e:
        ctx['status']='EVIDENCE_INPUT_INVALID:'+type(e).__name__
        ctx['evidence']={};ctx['calendar']=[]
    return ctx


def review(row:Mapping[str,Any],*,context:Mapping[str,Any],live_source:bool,
           info_asof:str,market_day:str)->dict[str,Any]:
    """Return H123 diagnostics and H113-safe official daily values if and only if verified.

    A market-day string alone cannot prove a clock-time of observation.
    LIVE mode means current scan now (not historical reconstruction).
    """
    if context.get('mode')=='OFF':return {'H123法人實證狀態':'DISABLED'}
    if not live_source or context.get('status')!='OFFICIAL_SESSIONS_AND_ARCHIVE_LOADED':
        return {'H123法人實證狀態':'PENDING_EVIDENCE','H123法人實證原因':
                '需當晚正式LIVE研究與三市場完整交易日；'+str(context.get('status'))}
    if not context.get('calendar') or context['calendar'][-1] != market_day:
        return {'H123法人實證狀態':'PENDING_EVIDENCE',
                'H123法人實證原因':'H117最後完整交易日與本輪行情日期不同，不可用舊法人資料替代：'+str(context.get('calendar',[])[-1:])}
    now=datetime.now(TZ)
    if now.hour<15:return {'H123法人實證狀態':'PENDING_EVIDENCE','H123法人實證原因':'市場尚未收盤'}
    decision_at=now.isoformat(timespec='seconds')
    result=assess_flow(code=str(row.get('股票代號') or ''),market=str(row.get('市場別') or ''),
               evidence_days=context.get('evidence',{}),info_cutoff=decision_at,
               official_sessions=context.get('calendar',[]),preverified=True)
    result['H123研究實際時間']=decision_at
    # Do not rewrite the user's K-line day or known H110 conflict.
    result['H123行情資料日']=market_day
    return result


def official_fields_if_proven(result:Mapping[str,Any])->dict[str,Any]:
    if result.get('H123法人實證狀態') not in ('PROVEN_FLOW','VERIFIED_BUT_NOT_BUYING'):
        return {}
    # Even a verified net-sell belongs in the daily diagnostic, not a buy signal.
    return {
       'H110法人方向驗證':'OFFICIAL_DATED',
       '外資逐日買賣超_官方':result.get('H123外資逐日股數',[]),
       '投信逐日買賣超_官方':result.get('H123投信逐日股數',[]),
       '外資近3日買賣超_官方':result.get('H123外資近3日買賣超_股'),
       '外資近5日買賣超_官方':result.get('H123外資近5日買賣超_股'),
       '投信近3日買賣超_官方':result.get('H123投信近3日買賣超_股'),
       '投信近5日買賣超_官方':result.get('H123投信近5日買賣超_股'),
       '三大法人近5日合計_官方':result.get('H123近5日法人合計股數'),
    }


def apply_strict_h113(row:dict[str,Any], result:Mapping[str,Any], *,mode:str)->dict[str,Any]:
    if mode!='STRICT':return row
    row.update(enforce_h113(result,row))
    return row
