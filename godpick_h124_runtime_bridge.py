# -*- coding: utf-8 -*-
"""H124 safe superseding adapter for the existing H123 bridge.

H113 authority remains unchanged. H124 STRICT only adds veto; MONITOR is audit.
Full-market scan never performs HTTP or revalidates bulky raw snapshots per stock.
"""
from __future__ import annotations
import os
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any,Mapping
from godpick_h123_runtime_bridge import (load_context as legacy_load,
    review as legacy_review, official_fields_if_proven as legacy_fields,
    apply_strict_h113 as legacy_strict)
from godpick_h124_ownership_flow import load_captures,assess_tdcc,assess_tpex


def load_context(base_dir:Path)->dict[str,Any]:
    ctx=legacy_load(base_dir)
    ctx['h124_tpex']={};ctx['h124_tdcc']={}
    ctx['h124_status']='PENDING_H124_OFFICIAL_ARCHIVES'
    if ctx['mode']=='OFF':return ctx
    issues=[]
    for source,key in [('TPEX','h124_tpex'),('TDCC','h124_tdcc')]:
        try:ctx[key]=load_captures(base_dir,source, max_periods=12)
        except (OSError,ValueError,KeyError,TypeError) as exc:
            issues.append(source+':'+str(exc))
    ctx['h124_status']='VERIFIED_H124_ARCHIVES' if not issues else 'ARCHIVE_INVALID:'+';'.join(issues)
    return ctx


def review(row:Mapping[str,Any], *,context:Mapping[str,Any],live_source:bool,
           info_asof:str,market_day:str)->dict[str,Any]:
    if context.get('mode')=='OFF':return {'H123法人實證狀態':'DISABLED','H124大戶實證狀態':'DISABLED'}
    result=legacy_review(row, context=context,live_source=live_source,
                         info_asof=info_asof,market_day=market_day)
    # One common decision cutoff for every source. No historical reconstruction
    # without original observed_at; scans must be truly live, not cached replay.
    if (live_source and context.get('status')=='OFFICIAL_SESSIONS_AND_ARCHIVE_LOADED'
            and context.get('calendar') and context['calendar'][-1]==market_day
            and datetime.now(ZoneInfo('Asia/Taipei')).hour>=15):
        cutoff=result.get('H123研究實際時間') or datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds')
        if str(row.get('市場別') or '').strip()=='上櫃':
            result.update(assess_tpex(code=str(row.get('股票代號') or ''),
                        evidence=context.get('h124_tpex',{}),sessions=context['calendar'],
                        cutoff=cutoff,preverified=True))
            result['H123研究實際時間']=cutoff;result['H123行情資料日']=market_day
        result.update(assess_tdcc(code=str(row.get('股票代號') or ''),
                     evidence=context.get('h124_tdcc',{}),cutoff=cutoff,preverified=True))
    else:
        result.update({'H124大戶實證狀態':'PENDING_EVIDENCE','H124大戶集中合格':'否',
                       'H124大戶原因':'缺完整H117行情日或本輪非真正LIVE研究'})
    result['H124研究身份']='OFFICIAL_EVIDENCE_REVIEW_NOT_FORMAL_BUY'
    result['H124版本']='V191-H124.1'
    return result


def official_fields_if_proven(result:Mapping[str,Any])->dict[str,Any]:
    fields=legacy_fields(result)
    if result.get('H124大戶實證狀態') in ('VERIFIED_RISING','VERIFIED_NOT_RISING'):
        fields['H124TDCC千張大戶週變化pp_官方']=result.get('H124大戶變化pp')
        fields['H124TDCC大戶官方來源']='TDCC_OPEN_DATA_1_5'
    return fields


def apply_strict_h113(row:dict, result:Mapping[str,Any], *,mode:str)->dict:
    legacy_strict(row,result,mode=mode)
    if mode!='STRICT':return row
    if result.get('H124大戶實證狀態')!='VERIFIED_RISING':
        if row.get('H113入榜資格')=='QUALIFIED':row['H113入榜資格']='WATCH_ONLY'
        row['H113大戶鎖碼合格']='否'
        row['H124主榜決策']='VETO_TDCC_WEEKLY_UNVERIFIED_OR_NOT_RISING'
        row['H113缺失原因']=str(row.get('H113缺失原因') or '').rstrip('；')+'；H124缺兩週經驗證TDCC大戶集中正變化'
    else:
        row['H124主榜決策']='TDCC_ONLY_CONFIRMED_OTHER_GATES_STILL_REQUIRED'
    return row
