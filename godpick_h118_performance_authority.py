# -*- coding: utf-8 -*-
"""V191-H118: corporate-action evidence and EOD performance truth gate.

This is NOT an adjusted-price engine. It quarantines trade windows touching
published corporate actions and refuses to certify windows without independently
verified, day-by-day corporate-action completeness. No trading actions.
"""
from __future__ import annotations
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse
import csv
import hashlib
import json
import math
import re

VERSION = 'V191-H118.1-CA-QUARANTINE-PIT-20261014'
ACTION_TYPES = frozenset({'CASH_DIVIDEND','STOCK_DIVIDEND','SPLIT','REVERSE_SPLIT',
                         'RIGHTS_ISSUE','OTHER_PRICE_ADJUSTMENT'})
MARKETS = frozenset({'TWSE','TPEX'})
ALLOWED_HOSTS = frozenset({'twse.com.tw','tpex.org.tw','mops.twse.com.tw','openapi.twse.com.tw'})
CSV_ACTION_COLUMNS = ('code','ex_date','action_type','source_url','verified_at','verification_status')
CSV_COVERAGE_COLUMNS = ('market','date','source_url','verified_at','status')


def _iso_day(x:Any)->str:
    s=str(x or '').strip()
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',s):
        raise ValueError(f'非法 ISO 日期：{s[:40]}')
    return date.fromisoformat(s).isoformat()


def _official_url(s:Any)->str:
    txt=str(s or '').strip()
    url=urlparse(txt)
    host=(url.hostname or '').lower()
    if url.scheme!='https' or not any(host==base or host.endswith('.'+base) for base in ALLOWED_HOSTS):
        raise ValueError('企業行動來源不是允許的官方 HTTPS 網址')
    if not url.path or url.username or url.password:
        raise ValueError('企業行動來源網址不完整或包含非預期資訊')
    return txt


def _evidence_time(s:Any, day:str, *, require_day_passed:bool=True)->str:
    raw=str(s or '').strip()
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})',raw):
        raise ValueError('缺少含時區的稽核時間')
    ts=datetime.fromisoformat(raw.replace('Z','+00:00'))
    if ts.astimezone(timezone.utc)>datetime.now(timezone.utc)+timedelta(minutes=5):
        raise ValueError('稽核時間晚於實際現在時間，不得使用未來證據')
    if require_day_passed and ts.date()<date.fromisoformat(day):
        raise ValueError('稽核時間早於交易日')
    return ts.isoformat(timespec='seconds')


def load_actions(rows:Iterable[Mapping[str,Any]])->list[dict[str,str]]:
    result=[];seen=set()
    for item in rows:
        code=str(item.get('code') or '').strip()
        if not re.fullmatch(r'[1-9]\d{3}',code):raise ValueError('公司行動股票代號不合法')
        ex_date=_iso_day(item.get('ex_date'))
        typ=str(item.get('action_type') or '').strip().upper()
        if typ not in ACTION_TYPES:raise ValueError('未知公司行動類別')
        if str(item.get('verification_status'))!='OFFICIAL_DOCUMENTED':
            raise ValueError('公司行動尚未完成官方來源核查')
        url=_official_url(item.get('source_url'))
        # An announced future ex-date can be documented before it occurs.
        stamp=_evidence_time(item.get('verified_at'),ex_date,require_day_passed=False)
        key=(code,ex_date,typ)
        if key in seen:raise ValueError('重複公司行動鍵，需人工合併')
        seen.add(key)
        result.append({'code':code,'ex_date':ex_date,'action_type':typ,'source_url':url,
                       'verified_at':stamp,'verification_status':'OFFICIAL_DOCUMENTED'})
    return sorted(result,key=lambda r:(r['code'],r['ex_date'],r['action_type']))


def load_coverage(rows:Iterable[Mapping[str,Any]])->list[dict[str,str]]:
    result=[];seen=set()
    for item in rows:
        market=str(item.get('market') or '').strip().upper()
        if market not in MARKETS:raise ValueError('企業行動稽核市場不合法')
        day=_iso_day(item.get('date'))
        if str(item.get('status'))!='OFFICIAL_COMPLETE':raise ValueError('核對紀錄未證明完整')
        url=_official_url(item.get('source_url'))
        stamp=_evidence_time(item.get('verified_at'),day)
        key=(market,day)
        if key in seen:raise ValueError('企業行動稽核日重複')
        seen.add(key)
        result.append({'market':market,'date':day,'source_url':url,
                       'verified_at':stamp,'status':'OFFICIAL_COMPLETE'})
    return sorted(result,key=lambda r:(r['date'],r['market']))


def read_csv(path:Path, columns:tuple[str,...])->list[dict[str,str]]:
    path=Path(path)
    if not path.exists():return []
    if path.stat().st_size>4_000_000:raise ValueError('稽核檔過大')
    with path.open('r',newline='',encoding='utf-8-sig') as f:
        reader=csv.DictReader(f)
        if not reader.fieldnames or not set(columns).issubset(reader.fieldnames):
            raise ValueError(f'{path}: 缺少欄位：{set(columns)-set(reader.fieldnames or [])}')
        return list(reader)


def assess_window(*,code:str,market:str,entry_day:str,exit_day:str,
                  market_days:Iterable[str], stock_bars:Iterable[Mapping[str,Any]],
                  previous_close:float|None, action_events:Iterable[Mapping[str,Any]],
                  coverage:Iterable[Mapping[str,Any]])->dict[str,Any]:
    """Never conclude action-free when source coverage is absent.

    An action *on entry day* is before the simulated next-session opening buy,
    so do not treat it as earned dividend; the entry gap must be reviewed if large.
    """
    entry=_iso_day(entry_day);exit_=_iso_day(exit_day)
    if exit_<entry:raise ValueError('出場日早於進場日')
    names=[_iso_day(x) for x in market_days if entry<=_iso_day(x)<=exit_]
    if not names or names[0]!=entry or names[-1]!=exit_:
        return {'status':'UNVERIFIED_MARKET_SESSIONS','reason':'缺少完整大盤交易日'}
    included=[e for e in action_events if str(e['code'])==str(code) and entry<e['ex_date']<=exit_]
    if included:
        types=','.join(sorted({e['action_type'] for e in included}))
        return {'status':'CORPORATE_ACTION_QUARANTINE','reason':f'{len(included)}筆事件於持有期間：{types}'}
    bars=list(stock_bars)
    # Market uses mostly 10% daily limits, but newer instruments/price-reference
    # changes exist. A >20% overnight gap is a *review trigger*, never a stock split inference.
    prev=previous_close
    for row in bars:
        opening=float(row['open']);closing=float(row['close'])
        if prev is not None and math.isfinite(prev) and prev>0:
            ratio=opening/prev
            if ratio<0.80 or ratio>1.20:
                return {'status':'UNEXPLAINED_PRICE_DISCONTINUITY',
                        'reason':f'隔夜開盤／前收偏離超過20%，{row["date"]}，需人工查核除權息或變更交易'}
        prev=closing
    if market not in MARKETS:
        return {'status':'MARKET_PROVENANCE_UNKNOWN','reason':'行情CSV未證明上市／上櫃市場'}
    covered={(e['market'],e['date']) for e in coverage}
    absent=[d for d in names if (market,d) not in covered]
    if absent:
        return {'status':'CORPORATE_ACTION_COVERAGE_UNVERIFIED',
                'reason':f'缺少{len(absent)}個交易日的官方企業行動完整核對（首日 {absent[0]}）'}
    return {'status':'CERTIFIED_UNADJUSTED_PRICE_WINDOW',
            'reason':'官方企業行動核對完整且本期未發現事件；仍為價格報酬（不含現金股利）'}


def manifest(actions:list[Mapping[str,Any]],coverage:list[Mapping[str,Any]])->dict[str,Any]:
    data={'version':VERSION,'corporate_actions':actions,'daily_market_coverage':coverage,
          'policy':'OFFICIAL_EVIDENCE_REQUIRED_NO_AUTO_ADJUSTMENT_NO_TRADE'}
    data['sha256']=hashlib.sha256(json.dumps(data,ensure_ascii=False,
                      sort_keys=True,separators=(',',':')).encode('utf-8')).hexdigest()
    return data
