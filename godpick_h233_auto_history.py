# -*- coding: utf-8 -*-
"""H233 bounded public financial supplementation, no trade authority.
Priority and fair rotation are collection order only, never buy rankings.
One request per issuer per run. No new schedule; no historical PIT claims.
"""
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter
import time
import godpick_h190_market_archive as ar
import godpick_h192_financial_growth as fin
import godpick_h231_company_history as company
import godpick_h222_financial_history as history

VERSION='V191-H233-AUTO-HISTORY'
REL='data/h222/h233/health.json'
COMPANY_REL='data/h222/h231/health.json'

def health(root,rel=REL):
    p=Path(root)/rel
    return ar._read(p) if p.exists() else {}

def plan(root,now,*,quarters=8,stocks=None,previous=None,attempts=None):
    """At most one fair slot + one growth slot. Fair slot alternates markets.
    Current-master backfill only; skipped/cooldown/invalid files never overwritten.
    All priority values are already visible financial evidence at `now`.
    """
    if now.tzinfo is None or not 8<=quarters<=12:raise ValueError('invalid H233 clock or quarters')
    root=Path(root); master=company.identity_map(root)
    if not master:raise ValueError('missing current stock master')
    previous=health(root) if previous is None else previous
    if previous and (not fin.moment(previous.get('asof')) or fin.moment(previous['asof'])>now):raise ValueError('H233 health time invalid')
    attempts=health(root,COMPANY_REL).get('attempts',{}) if attempts is None else attempts
    if not isinstance(attempts,dict):raise ValueError('invalid company attempts')
    if stocks is None:stocks=history.inspect(root,now)['stocks']
    profiles={r['股票代號']:r for r in stocks if r.get('股票代號') in master and r.get('市場')==master[r['股票代號']]['市場']}
    _,q=fin.expected_periods(now); stats=Counter(); eligible=[]
    for code,identity in sorted(master.items()):
        market=identity['市場'];r=profiles.get(code,{})
        if market not in ('TWSE','TPEX'):continue
        seen=[];ready=[];present=0;blocked=0
        for p in range(q,q-quarters,-1):
            key=f'{market}:{code}:{p}'; prior=attempts.get(key)
            at=fin.moment(prior.get('at')) if isinstance(prior,dict) else None
            if prior is not None and (at is None or at>now):raise ValueError('invalid/future company attempt: '+key)
            if at:seen.append(at)
            if company.path_for(root,market,code,p).exists():present+=1;continue
            if at and now-at<timedelta(hours=12):blocked+=1;continue
            ready.append(p)
        stats['母體']+=1;stats['封存檔案存在期數']+=present;stats['冷卻期數']+=blocked;stats['可請求期數']+=len(ready)
        if not ready:
            stats['無可請求股票']+=1;continue
        # A current release with >=10% YoY is the existing H192 growth gate.
        yoy=fin.num(r.get('營收年增%'))
        fresh=r.get('營收新鮮') is True
        multi=fresh and (r.get('連3月成長') is True or r.get('近13月新高') is True)
        growth=multi or (fresh and yoy is not None and yoy>=10)
        eligible.append({'市場':market,'股票代號':code,'股票名稱':identity.get('股票名稱',code),'下個可補期間':fin.qstr(ready[0]),'可請求期數':len(ready),'已有封存檔案期數':present,'最近嘗試':max(seen).isoformat() if seen else '', '成長線索':growth,'多月成長線索':multi,'營收年增%':yoy if fresh else None})
    stats['可請求股票']=len(eligible);stats['具成長線索股票']=sum(r['成長線索'] for r in eligible)
    # Fairness independent of growth: oldest attempt first, never rank by future returns.
    target='TPEX' if previous.get('fair_market')=='TWSE' else 'TWSE'
    fair_key=lambda r:(r['最近嘗試'],r['股票代號'])
    fair=min([r for r in eligible if r['市場']==target] or eligible,key=fair_key,default=None)
    chosen=[]
    if fair:chosen.append({**fair,'佇列':'全市場公平輪替'})
    rest=[r for r in eligible if not fair or r['股票代號']!=fair['股票代號']]
    growth=[r for r in rest if r['成長線索']]
    if growth:
        # Complete partially collected issuers first, prefer multi-month evidence,
        # then oldest attempt; never sort raw YoY (tiny-base outlier bias).
        pick=min(growth,key=lambda r:(-r['已有封存檔案期數'],not r['多月成長線索'],r['最近嘗試'],r['股票代號']))
        chosen.append({**pick,'佇列':'成長證據優先補證'})
    elif rest:chosen.append({**min(rest,key=fair_key),'佇列':'無成長線索／公平補證'})
    return {'queue':chosen,'counts':dict(stats),'fair_market':fair['市場'] if fair else previous.get('fair_market',''),'trade_authority_changed':False}

def run(root,*,months=24,quarters=8,budget=8,now=None,http=None):
    """Preserve six bulk requests and at most two individual requests per run.
    A single overall budget, no automatic extra requests after failures.
    Manual original bulk/company modes remain available in CLI.
    """
    if not 3<=budget<=8:raise ValueError('auto budget must be 3..8')
    fixed=now is not None;start=now or datetime.now(ar.TZ);root=Path(root)
    if start.tzinfo is None:raise ValueError('timezone required')
    prev=health(root)
    if prev and (not fin.moment(prev.get('asof')) or fin.moment(prev['asof'])>start):raise ValueError('H233 health time invalid')
    bulk=history.collect(root,months=months,quarters=quarters,budget=budget-2,now=now,http=http)
    t=now or datetime.now(ar.TZ);errors=[];results=[];individual=0
    try:selection=plan(root,t,quarters=quarters,previous=prev)
    except (ValueError,OSError,TypeError,KeyError) as e:selection={'queue':[],'counts':{},'fair_market':prev.get('fair_market','')};errors.append(str(e)[:200])
    for row in selection['queue']:
        if not fixed:time.sleep(2)
        try:
            r=company.collect(root,codes=[row['股票代號']],quarters=quarters,budget=1,now=now,http=http)
            individual+=r['requests']
            outcome=next((x for x in r['scope'] if x['期間']==row['下個可補期間']),{})
            results.append({**row,'狀態':outcome.get('狀態','PENDING'),'原因':outcome.get('原因',''),'請求數':r['requests']})
        except (ValueError,OSError,TypeError,KeyError) as e:
            errors.append(str(e)[:200]);break # fail closed; never retry an unknown request count
    t=now or datetime.now(ar.TZ)
    try:
        checked=history.inspect(root,t);summary=checked['summary'];n=len(checked['candidates']);strict=len(checked.get('h232_candidates',[]))
    except (ValueError,OSError,TypeError,KeyError) as e:summary=[];n=strict=None;errors.append(str(e)[:200])
    body={'version':VERSION,'asof':t.isoformat(),'status':'EVIDENCE_PENDING' if errors or bulk['status']!='VERIFIED_SCOPE' or selection['queue'] else 'BULK_VERIFIED_NO_ELIGIBLE_COMPANY_REQUEST', 'bulk_status':bulk['status'],'bulk_requests':bulk['requests'],'company_requests':individual,'max_requests':budget,'queue':results,'counts':selection['counts'],'fair_market':selection['fair_market'],'financial_summary':summary,'growth_candidates':n,'h232_candidates':strict,'errors':errors,'trade_authority_changed':False}
    ar._write(root/REL,ar._seal(body));return body
