# -*- coding: utf-8 -*-
"""H221: bounded official evidence recovery; no trade authority or historical signal writes."""
from datetime import datetime,time
from pathlib import Path
import godpick_h190_market_archive as ar
import godpick_h123_flow_provenance as flow
import godpick_h124_ownership_flow as own
import godpick_h192_financial_growth as fin
from godpick_h194_performance import Market
VERSION='V191-H221-OFFICIAL-EVIDENCE-CLOSURE'
ROOT=Path(__file__).resolve().parent
TWSE='https://www.twse.com.tw/rwd/zh/fund/T86'
TPEX='https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading'
MIN_ROWS={'TWSE':500,'TPEX':350}

def recent_sessions(root,now):
    m=Market(root,now)
    if m.errors:raise ValueError('H190 archive errors: '+str(m.errors)[:180])
    # Do not skip a missing session and call the remaining sessions consecutive.
    expected=[d for d in m.days if d<now.date().isoformat() or m.today_confirmed(d)][-5:]
    if len(expected)!=5 or not all(m.quotes(k,d) for d in expected for k in ar.URLS):raise ValueError('H190 latest five confirmed paired sessions unavailable')
    return expected

def evidence(root,market):
    return flow.evidence_by_day(root,max_dates=10) if market=='TWSE' else own.load_captures(root,'TPEX',max_periods=10)

def get_json(http,url,params):
    r=http(url,params=params,timeout=(5,20),allow_redirects=False,headers={'Accept':'application/json','User-Agent':'StockApp-H221/1.0'})
    if r.status_code!=200:raise ValueError('HTTP_'+str(r.status_code))
    if r.url.split('?')[0]!=url:raise ValueError('unexpected response URL')
    raw=r.json()
    return raw

def collect(root=ROOT,*,http=None,now=None,sessions=None,min_rows=None):
    """T86 dated history, TPEx current dated OpenAPI only. Never invent old reports.
    Explicit now/sessions/min_rows are injectable for tests, CLI uses real time.
    """
    fixed=now is not None;now=now or datetime.now(ar.TZ)
    if now.tzinfo is None:raise ValueError('timezone required')
    now=now.astimezone(ar.TZ);root=Path(root);limits=min_rows or MIN_ROWS
    if http is None:
        import requests
        http=requests.get
    records=[];health={'version':VERSION,'asof':now.isoformat(),'sources':records,'trade_authority_changed':False}
    try:
        days=list(sessions if sessions is not None else recent_sessions(root,now))
        if len(days)!=5 or days!=sorted(set(days)) or days[-1]>now.date().isoformat():raise ValueError('invalid five-session window')
        if days[-1]==now.date().isoformat() and now.time()<time(18,15):raise ValueError('before confirmed capture window')
    except Exception as exc:
        health.update(status='CALENDAR_PENDING',error=str(exc)[:240],sessions=[])
        ar._write(root/'data/h221/health.json',ar._seal(health));return health
    health['sessions']=days
    for market in ('TWSE','TPEX'):
        diag={'market':market,'attempts':[]};records.append(diag)
        try:old=evidence(root,market)
        except Exception as exc:
            diag.update(status='ARCHIVE_ERROR',error=str(exc)[:240]);continue
        missing=[d for d in days if d not in old]
        targets=list(reversed(missing)) if market=='TWSE' else ([days[-1]] if days[-1] in missing else [])
        for day in targets:
            attempt={'day':day};diag['attempts'].append(attempt)
            try:
                url=TWSE if market=='TWSE' else TPEX
                raw=get_json(http,url,{'response':'json','date':day.replace('-',''),'selectType':'ALLBUT0999'} if market=='TWSE' else {})
                attempt['raw_rows']=len(raw.get('data',[])) if isinstance(raw,dict) else len(raw) if isinstance(raw,list) else None
                attempt['fields']=list(raw.get('fields',[]))[:40] if isinstance(raw,dict) else list(raw[0])[:40] if isinstance(raw,list) and raw and isinstance(raw[0],dict) else []
                observed=now if fixed else datetime.now(ar.TZ)
                if market=='TWSE':ev=flow.build_twse_evidence(raw,trade_day=day,collected_at=observed.isoformat(),source_url=url)
                else:
                    ev=own.build_tpex(raw,trade_day=day,collected_at=observed.isoformat(),source_url=url)
                    if ev['schema_map']['foreign']==own.TPEX_FIELDS['foreign'][-1]:raise ValueError('foreign subtotal includes dealer; exclusive field required')
                if len(ev['stocks'])<limits[market]:raise ValueError('LOW_COVERAGE_'+str(len(ev['stocks'])))
                if market=='TWSE':flow.save_evidence(root,ev)
                else:own.save_capture(root,ev)
                attempt.update(status='VERIFIED_SAVED',stocks=len(ev['stocks']),observed_at=observed.isoformat())
            except Exception as exc:attempt.update(status='REJECTED',error=str(exc)[:240])
        try:
            current=evidence(root,market)
            visible={d for d,v in current.items() if flow.aware_stamp(v['first_observed_at'])<=(now if fixed else datetime.now(ar.TZ)) and len(v['stocks'])>=limits[market] and (market!='TPEX' or v.get('schema_map',{}).get('foreign') in own.TPEX_FIELDS['foreign'][:-1])}
            diag['verified_days']=[d for d in days if d in visible];diag['missing_days']=[d for d in days if d not in visible]
            diag['status']='VERIFIED_5' if not diag['missing_days'] else 'PENDING'
        except Exception as exc:diag.update(status='ARCHIVE_ERROR',error=str(exc)[:240])
    health['status']='VERIFIED_5_BOTH' if all(x['status']=='VERIFIED_5' for x in records) else 'EVIDENCE_PENDING'
    ar._write(root/'data/h221/health.json',ar._seal(health));return health

def inspect(root=ROOT,now=None):
    """Read-only evidence ledger; no inference from missing data."""
    now=now or datetime.now(ar.TZ);root=Path(root);errors=[]
    try:days=recent_sessions(root,now)
    except Exception as exc:days=[];errors.append(str(exc)[:240])
    rows=[]
    for market in ('TWSE','TPEX'):
        try:ev=evidence(root,market)
        except Exception as exc:ev={};errors.append(str(exc)[:240])
        for d in days:
            v=ev.get(d);ok=bool(v and flow.aware_stamp(v['first_observed_at'])<=now and len(v['stocks'])>=MIN_ROWS[market] and (market!='TPEX' or v.get('schema_map',{}).get('foreign') in own.TPEX_FIELDS['foreign'][:-1]))
            rows.append({'市場':market,'交易日':d,'狀態':'已核實' if ok else '待補證','股票數':len(v['stocks']) if ok else None,'首次觀察':v['first_observed_at'] if ok else None})
    data,err=fin.load_inputs(root);errors+=err;ledger,rejected,conflicts=fin.build_ledgers(data,now)
    fm,fq=fin.expected_periods(now);facts=[]
    for market in ('TWSE','TPEX'):
        for kind in fin.METRICS:
            target=[v for (k,m,c),v in ledger.items() if k==kind and m==market]
            required={'revenue':set(range(fm-12,fm+1)),'margin':{fq,fq-1,fq-4,fq-5},'ytd':set(range(fq-5,fq+1)),'pe':{days[-1]} if days else set()}[kind]
            facts.append({'市場':market,'來源':kind,'已有股票':len(target),'完整必要期數股票':sum(bool(required) and required.issubset(v) for v in target),'必要期數':len(required),'完整期數定義':'僅期間覆蓋，不代表值及科目可比'})
    try:health=ar._read(root/'data/h192/health.json')
    except (OSError,ValueError):health={}
    try:chip_health=ar._read(root/'data/h221/health.json')
    except (OSError,ValueError):chip_health={}
    return {'version':VERSION,'asof':now.isoformat(),'sessions':days,'flows':rows,'financial':facts,'financial_health':health,'chip_health':chip_health,'errors':errors,'rejected':rejected,'conflicts':conflicts,'trade_authority_changed':False}
