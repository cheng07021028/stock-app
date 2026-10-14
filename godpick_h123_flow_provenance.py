# -*- coding: utf-8 -*-
"""H123 TWSE institutional trade-day provenance and H113 fail-closed authority.

All share volumes are SHARES, not lots. An observed data timestamp is NOT a
historical publication timestamp. No old recommendation is retro-certified.
For TPEx and TDCC, unavailable validated schemas remain explicitly pending.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse
import hashlib
import json
import math
import re

VERSION = 'V191-H123.1-DAILY-INSTITUTION-PROVENANCE'
TWSE_HOST = 'www.twse.com.tw'
REQUIRED_TWSE = ('證券代號','外陸資買賣超股數(不含外資自營商)','投信買賣超股數','三大法人買賣超股數')
SOURCES = {'TWSE': '上市', 'TPEX': '上櫃'}


def canonical(value: Any) -> bytes:
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',', ':'),allow_nan=False).encode('utf-8')


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def iso_date(v: Any) -> str:
    s=str(v or '').strip()
    if len(s)==8 and s.isdecimal():
        return datetime.strptime(s,'%Y%m%d').date().isoformat()
    return date.fromisoformat(s).isoformat()


def aware_stamp(v: Any) -> datetime:
    if not isinstance(v,str) or 'T' not in v:
        raise ValueError('須附帶完整時區時間，日期文字不能當成發布時間')
    d=datetime.fromisoformat(v.replace('Z','+00:00'))
    if d.tzinfo is None or d.utcoffset() is None:
        raise ValueError('缺少 UTC offset')
    return d


def shares(v: Any) -> int:
    if isinstance(v,bool):raise ValueError('股數不能是布林值')
    s=str(v).replace(',','').replace(' ','').strip()
    if not re.fullmatch(r'-?\d+', s): raise ValueError('法人股數不是整數(股)，拒絕不明單位')
    return int(s)


def code_id(v: Any) -> str:
    s=str(v or '').strip()
    if not re.fullmatch(r'[1-9]\d{3}',s):raise ValueError('只允許上市櫃非0開頭四碼個股；ETF/權證不得誤配')
    return s


def build_twse_evidence(raw: Mapping[str,Any], *, trade_day: str, collected_at: str,
                        source_url: str='https://www.twse.com.tw/fund/T86') -> dict[str,Any]:
    """Only accept actual TWSE T86 table schema; checksum captures raw content.

    Use only historical as-of when collected_at <= that historical decision time;
    a fresh download cannot back-certify old research or missing old timestamps.
    """
    day=iso_date(trade_day)
    observed=aware_stamp(collected_at)
    p=urlparse(source_url)
    if p.scheme!='https' or p.hostname not in (TWSE_HOST,'wwwc.twse.com.tw') or '/fund/T86' not in p.path:
        raise ValueError('非核定TWSE T86官方來源')
    if observed.date()<date.fromisoformat(day):raise ValueError('取用時間不能早於報表交易日')
    if not isinstance(raw,Mapping) or raw.get('stat') != 'OK':raise ValueError('TWSE未回傳已發布OK報表')
    actual=str(raw.get('date') or '').strip()
    if iso_date(actual)!=day:raise ValueError('官方報表交易日與請求日不同')
    fields=raw.get('fields');data=raw.get('data')
    if not isinstance(fields,list) or not isinstance(data,list) or not data:
        raise ValueError('TWSE資料欄位/筆數缺漏')
    if len(set(fields))!=len(fields) or any(c not in fields for c in REQUIRED_TWSE):
        raise ValueError('TWSE法人欄名缺失或重複，不允許猜測欄位')
    col={name:fields.index(name) for name in REQUIRED_TWSE}
    records={}
    for item in data:
        if not isinstance(item,list) or len(item)!=len(fields):raise ValueError('法人列長度不一致')
        code=str(item[col['證券代號']]).strip()
        if not re.fullmatch(r'[1-9]\d{3}',code):continue
        if code in records: raise ValueError('同交易日相同股票重複')
        records[code]={
          'foreign_shares':shares(item[col['外陸資買賣超股數(不含外資自營商)']]),
          'trust_shares':shares(item[col['投信買賣超股數']]),
          'total_shares':shares(item[col['三大法人買賣超股數']]),
        }
    if not records:raise ValueError('股票列為空')
    body={'version':VERSION,'source':'TWSE','market':'上市','trade_date':day,
          'first_observed_at':observed.isoformat(),'source_url':source_url,
          'capture_method':'OFFICIAL_HTTP_RAW_T86','raw_sha256':digest(raw),
          'raw_response':raw,
          'raw_row_count':len(data),'stocks':records,
          'authority':'SOURCE_OBSERVED_NOT_PUBLISH_TIME_ATTESTED'}
    body['evidence_sha256']=digest(body)
    return body


def verify_evidence(obj: Mapping[str,Any]) -> bool:
    if not isinstance(obj,Mapping):return False
    p=dict(obj);sha=p.pop('evidence_sha256',None)
    if not (isinstance(sha,str) and len(sha)==64 and digest(p)==sha and obj.get('version')==VERSION):
        return False
    raw=obj.get('raw_response')
    if not isinstance(raw,Mapping) or digest(raw)!=obj.get('raw_sha256'):
        return False
    try:
        rebuilt=build_twse_evidence(raw,trade_day=obj['trade_date'],
                collected_at=obj['first_observed_at'],source_url=obj['source_url'])
    except (TypeError,ValueError,KeyError):
        return False
    return rebuilt==obj


def save_evidence(root: Path, evidence: Mapping[str,Any]) -> Path:
    """Content-addressed immutable captures: a corrected exchange report is a NEW file."""
    if not verify_evidence(evidence):raise ValueError('原始法人證據SHA無效')
    d=iso_date(evidence.get('trade_date'));source=str(evidence.get('source'))
    if source!='TWSE':raise ValueError('目前僅支援證交所T86已解析來源')
    directory=Path(root)/'data'/'h123'/'twse_t86'/d
    directory.mkdir(parents=True,exist_ok=True)
    # Identical official bytes on a repeated collector run are not a revision.
    for previous in directory.glob('*.json'):
        current=json.loads(previous.read_text(encoding='utf-8'))
        if not verify_evidence(current):raise ValueError('既存證據毀損，不得重新覆寫')
        if current.get('raw_sha256')==evidence['raw_sha256']:
            return previous
    path=directory/(evidence['evidence_sha256']+'.json')
    payload=json.dumps(evidence,ensure_ascii=False,indent=2,sort_keys=True)+'\n'
    if path.exists():
        if path.read_text(encoding='utf-8')!=payload:raise ValueError('不可變證據檔內容衝突')
        return path
    # exclusive create prevents silent overwrites even with two collectors
    with path.open('x',encoding='utf-8') as fh:fh.write(payload)
    return path


def evidence_by_day(root: Path, *, market: str='TWSE', max_dates: int=10) -> dict[str,dict[str,Any]]:
    base=Path(root)/'data'/'h123'/'twse_t86'
    if market!='TWSE':return {}
    out={}
    all_files=sorted(base.glob('*/*.json'))
    days=sorted({p.parent.name for p in all_files})
    recent=set(days[-max_dates:]) if max_dates>0 else set(days)
    for path in all_files:
        if path.parent.name not in recent:continue
        obj=json.loads(path.read_text(encoding='utf-8'))
        if not verify_evidence(obj):raise ValueError('法人證據雜湊錯誤：'+str(path))
        day=iso_date(obj['trade_date'])
        # Distinct revisions are not silently picked; explicit review required.
        if day in out and out[day]['evidence_sha256']!=obj['evidence_sha256']:
            raise ValueError('同日期存在不同版本T86報表；需人工決定使用版本：'+day)
        out[day]=obj
    return out


def _bounded_series(evidence_days: Mapping[str,Mapping[str,Any]], *, code: str, market: str,
                    info_cutoff: str, session_days: Sequence[str], preverified: bool=False) -> tuple[list[dict[str,Any]],str]:
    try:cutoff=aware_stamp(info_cutoff)
    except Exception:return [],'缺少事前研究截止完整時間'
    if market!='上市':return [],'上櫃法人逐日來源仍需完成官方解析/驗證；不可借用TWSE'
    try:sessions=sorted({iso_date(x) for x in session_days})
    except Exception:return [],'無效的官方交易日曆'
    latest=[x for x in sessions if x<=cutoff.date().isoformat()]
    if len(latest)<5:return [],'官方交易日不足5日'
    # As-of session date is NOT necessarily the newest *published* institutional report.
    # We require exact day matches; missed current session means PENDING.
    window=latest[-5:]
    # H117 gives verified CAPTURED sessions, not an independent official holiday
    # calendar.  Never silently skip a weekday absent from all archives.
    # Holiday gaps need a separately verified holiday calendar in a later phase;
    # until then hold rather than mislabel a five-day buying streak.
    for left,right in zip(window,window[1:]):
        expected=date.fromisoformat(left)+timedelta(days=1)
        while expected.weekday()>=5:expected+=timedelta(days=1)
        if expected.isoformat()!=right:
            return [],'官方交易日曆未核對的缺口/休市：'+left+'→'+right
    series=[]
    for day in window:
        obj=evidence_days.get(day)
        if not obj or (not preverified and not verify_evidence(obj)):
            return [],'缺少或損毀逐日官方法人原始證據：'+day
        if obj.get('source')!='TWSE' or obj.get('market')!='上市' or obj.get('trade_date')!=day:
            return [],'官方來源/市場/日期不一致：'+day
        try:observed=aware_stamp(obj['first_observed_at'])
        except Exception:return [],'官方第一次取用時間不合法：'+day
        if observed>cutoff:return [],'此日官方資料晚於推薦時間才取得，不得回填歷史：'+day
        stock=obj.get('stocks',{}).get(code)
        if not isinstance(stock,dict):return [],'官方該交易日無此個股法人紀錄：'+day
        try:
            f=shares(stock['foreign_shares']);t=shares(stock['trust_shares']);combo=shares(stock['total_shares'])
        except Exception:return [],'官方法人單位不一致：'+day
        series.append({'date':day,'foreign':f,'trust':t,'total':combo})
    return series,''


def _streak(vals:Sequence[int]) -> int:
    count=0
    for x in reversed(vals):
        if x<=0:break
        count+=1
    return count


def assess_flow(*, code: str, market: str, evidence_days: Mapping[str,Mapping[str,Any]],
                info_cutoff: str, official_sessions: Sequence[str],
                preverified: bool=False) -> dict[str,Any]:
    """Fail-closed 5-session proof: consecutive days determined by OFFICIAL calendar.

    No 3D/5D summary or research proxy score is accepted without the daily
    records for each institution, and no unknown TDCC status is upgraded.
    """
    try:code=code_id(code)
    except ValueError:code=''
    rows,issue=_bounded_series(evidence_days,code=code,market=market,info_cutoff=info_cutoff,
                                 session_days=official_sessions,preverified=preverified)
    out={'H123版本':VERSION,'H123法人實證狀態':'PENDING_EVIDENCE',
         'H123法人實證原因':issue,'H123法人逐日有效天數':len(rows),'H123法人連買實證':'否',
         'H123不自動交易':'是','H123不回填舊推薦':'是',
         'H123外資連買天數':None,'H123投信連買天數':None,
         'H123近5日法人合計股數':None,'H123來源雜湊':None}
    if issue:return out
    f=[x['foreign'] for x in rows];t=[x['trust'] for x in rows];tot=[x['total'] for x in rows]
    f3,f5=sum(f[-3:]),sum(f);t3,t5=sum(t[-3:]),sum(t);combo=sum(tot)
    streakf=_streak(f);streakt=_streak(t)
    passed=combo>0 and ((streakf>=2 and f3>0 and f5>0) or (streakt>=2 and t3>0 and t5>0))
    out.update({'H123法人實證狀態':'PROVEN_FLOW' if passed else 'VERIFIED_BUT_NOT_BUYING',
         'H123法人實證原因':'逐日證據完整，符合法人連買' if passed else '逐日證據完整但連買及3/5日淨買未全通過',
         'H123法人逐日有效天數':5,'H123法人連買實證':'是' if passed else '否',
         'H123外資連買天數':streakf,'H123投信連買天數':streakt,
         'H123近5日法人合計股數':combo,
         'H123外資逐日股數':[{'date':x['date'],'net':x['foreign']} for x in rows],
         'H123投信逐日股數':[{'date':x['date'],'net':x['trust']} for x in rows],
         'H123來源雜湊':digest([evidence_days[x['date']]['evidence_sha256'] for x in rows]),
         'H123法人來源':'TWSE_T86_5_CONSECUTIVE_OFFICIAL_SESSIONS',
         'H123外資近3日買賣超_股':f3,'H123外資近5日買賣超_股':f5,
         'H123投信近3日買賣超_股':t3,'H123投信近5日買賣超_股':t5})
    return out


def enforce_h113(row: Mapping[str,Any], gate: Mapping[str,Any]) -> dict[str,Any]:
    """Never promote; optional H123 strict launch mode only vetoes legacy Research.

    Distinguishes 'PENDING source' from a verified institution net-sell.
    H113 TDCC/price/technical rules remain exclusively H113's authority.
    """
    result=dict(gate)
    if row.get('H123法人連買實證')!='是':
        if result.get('H113入榜資格')=='QUALIFIED':result['H113入榜資格']='WATCH_ONLY'
        result['H113法人連買合格']='否'
        result['H113缺失原因']=str(result.get('H113缺失原因') or '').rstrip('；')+'；H123缺逐日五交易日實證或法人連買不成立'
        result['H123最終主榜准入']='REJECTED'
    else:
        result['H123最終主榜准入']='H113_AND_H109_STILL_REQUIRED'
    return result
