# -*- coding: utf-8 -*-
"""H211 independent research triage. Never mutates H126, H194 or H200 ranking.
All thresholds are hypotheses, not calibrated probabilities. Only as-of-visible
prospective partitions are compared; no reconstructed historical forecasts.
"""
from collections import Counter, defaultdict
from datetime import time, date
from pathlib import Path
import math, re
import godpick_h190_market_archive as ar
import godpick_h194_performance as perf
import godpick_h198_stage_review as stage

VERSION='V191-H211-COUNTEREVIDENCE'
REL=Path('data/h211/daily')
LOOKBACK=10
FIELDS=('H211研究序','H211狀態','H211反證','H211新證據','H211前次證據日','H211近10日已見次數','H211歷史覆蓋','H211複核條件')
CONTEXT=('收盤','MA20','MA20前5日','5日漲跌%','20日漲跌%','60日區間位置%','量比','流動性通過','產業覆蓋率%','產業有效檔數','產業成交額活躍比','產業上漲家數比%','產業主流成立')
STATES=('反證排除前段','資料待補證','重複無新增證據','歷史待補證','有新增證據','近窗新出現')
ORDER={s:i for i,s in enumerate(('有新增證據','近窗新出現','歷史待補證','重複無新增證據','資料待補證','反證排除前段'))}

def finite(x):return isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x)
def identity(r):return (r['市場'],r['股票代號'])
def pools(r):return set(filter(None,str(r.get('技術研究池') or '').split('、')))
def evidence(r):
    keys=('市場','股票代號','研究分級','技術研究池','財報研究池','技術日','H198階段','H198反證','H198資料日期','H198突破基準日','H198箱頂參考','H198失效參考','營收年月','營收年增%','三率季度','財報缺口','產業','H211技術證據','H211營收證據')
    return {k:r.get(k) for k in keys}
def ctx(r):return r.get('H211技術證據') if isinstance(r.get('H211技術證據'),dict) else {}
def valid_key(r):return r.get('市場') in ar.URLS and isinstance(r.get('股票代號'),str) and bool(re.fullmatch('[1-9][0-9]{3}',r['股票代號']))
def tech_ok(r,day):
    c=ctx(r);return bool(r.get('技術日')==day and r.get('H198資料日期')==day and r.get('H198階段') not in (None,'','待補證') and c.get('流動性通過') is True and all(finite(c.get(k)) for k in ('收盤','MA20','MA20前5日','5日漲跌%','20日漲跌%','量比')) and min(c['收盤'],c['MA20'],c['MA20前5日'],c['量比'])>0)
def sector_ok(c):return c.get('全市場技術覆蓋') is True and finite(c.get('產業覆蓋率%')) and c['產業覆蓋率%']>=70 and finite(c.get('產業有效檔數')) and c['產業有效檔數']>=5 and finite(c.get('產業成交額活躍比')) and finite(c.get('產業上漲家數比%')) and isinstance(c.get('產業主流成立'),bool)

def read_history(root,days,now):
    history={};errors=[]
    for day in days:
        p=Path(root)/REL/(day+'.json.gz')
        if not p.exists():continue
        try:
            b=ar._read(p);ts=perf.stamp(b['asof'])
            if b['version']!=VERSION or b['day']!=day or b['trade_authority_changed'] is not False or ts>now or ts.date().isoformat()!=day or ts.time()<time(18,15):raise ValueError('invalid prospective partition')
            rows=b['rows'];d={identity(r):r for r in rows if valid_key(r)}
            if len(d)!=len(rows) or any(not tech_ok(r,day) for r in rows):raise ValueError('invalid or duplicate evidence')
            history[day]=d
        except Exception as e:errors.append(day+': '+type(e).__name__+': '+str(e)[:120])
    return history,errors

def evaluate(r,day,history,days,legacy,now=None):
    k=identity(r);c=ctx(r);hits=[(d,history[d][k]) for d in days if d in history and k in history[d]]
    prior_day,prior=hits[-1] if hits else ('',{});p=ctx(prior)
    seen={d for d,_ in hits}|{d for d in days if (d,*k) in legacy}
    risks=[];new=[];missing=[]
    if r.get('H198資料日期')==day and r.get('H198階段')==stage.POOLS[3]:risks.append('H198：'+str(r.get('H198反證') or '突破反證'))
    if tech_ok(r,day):
        price=c['收盤'];ma=c['MA20'];pos=c.get('60日區間位置%')
        if (c['5日漲跌%']>15 or c['20日漲跌%']>30 or price/ma>1.15):risks.append('短期漲幅或均線乖離過大')
        if price<ma and ma<c['MA20前5日'] and c['5日漲跌%']<0:risks.append('跌破MA20且均線轉弱')
        if finite(pos) and pos>=90 and c['20日漲跌%']>=20 and c['5日漲跌%']<=0:risks.append('高位階漲勢衰退')
        if prior and '主流強勢' in pools(prior) and r.get('產業') and r.get('產業')==prior.get('產業') and sector_ok(c) and sector_ok(p) and p['產業主流成立'] and not c['產業主流成立'] and c['產業成交額活躍比']<1 and c['產業上漲家數比%']<50:risks.append('前次主流產業成交額活躍與廣度同步衰退')
        if '主流強勢' in pools(r) and (not sector_ok(c) or not c.get('產業主流成立')):missing.append('主流產業證據待核')
        if prior:
            added=pools(r)-pools(prior)
            if added:new.append('新增技術研究池：'+'、'.join(sorted(added)))
            anchor=r.get('H198突破基準日')
            if isinstance(anchor,str) and prior_day<anchor<=day and r.get('H198階段') in stage.POOLS[1:3]:new.append('新突破基準日：'+anchor)
            if r.get('H198階段')==stage.POOLS[2] and prior.get('H198階段')==stage.POOLS[1] and anchor and anchor==prior.get('H198突破基準日'):new.append('同一突破由待確認轉為回測守穩')
            rev=r.get('營收年月');old=prior.get('營收年月')
            def revenue_month(v):
                if not isinstance(v,str) or not re.fullmatch(r'\d{4}(?:-)?(?:0[1-9]|1[0-2])',v):return ''
                return v.replace('-','')
            rev,old=revenue_month(rev),revenue_month(old)
            q=r.get('H211營收證據') or {}
            try:known=bool(now is not None and q.get('新鮮') is True and q.get('來源') and perf.stamp(q['可得時間'])<=now)
            except (TypeError,ValueError,KeyError):known=False
            if known and old and old<rev<=day[:7].replace('-','') and finite(r.get('營收年增%')) and r['營收年增%']>0 and r.get('財報研究池'):new.append('新營收期間且年增為正：'+rev)
    else:missing.append('同日技術／流動性證據不足；財報研究仍保留')
    complete=len(days)==LOOKBACK and len(history)==LOOKBACK
    if risks:status=STATES[0]
    elif missing:status=STATES[1]
    elif new:status=STATES[4]
    elif prior:status=STATES[2] if prior_day==days[-1] else STATES[3]
    elif seen or not complete:status=STATES[3]
    else:status=STATES[5]
    action={STATES[0]:'等待反證解除並重新核實原條件',STATES[1]:'補齊同日技術及產業證據',STATES[2]:'保留原研究；等待新突破、研究池或已驗證新財報證據',STATES[3]:'補齊前次證據；不可宣稱新興股票或無新證據',STATES[4]:'核對新增證據與失效條件',STATES[5]:'近10個合格封存窗未入封存技術研究名單；可能曾未覆蓋，不代表首次推薦'}[status]
    return dict(zip(FIELDS,(None,status,'；'.join(risks+missing) or '未觸發本規則（非無風險）','；'.join(new) or '未核實新增證據',prior_day or (max(seen) if seen else ''),len(seen) if history or legacy else None,f'{len(history)}/{LOOKBACK}合格技術封存窗；已見次數為下限',action)))

def enrich(body,root,market,legacy,now):
    day=body['expected_session'];days=[d for d in (market.days if market and not market.errors else []) if d<day][-LOOKBACK:]
    history,errors=read_history(root,days,now);groups=defaultdict(list)
    for r in body['rows']:
        r.update(evaluate(r,day,history,days,legacy,now));groups[r['研究分級']].append(r)
    for rs in groups.values():
        rs.sort(key=lambda r:(ORDER[r['H211狀態']],r.get('H200研究優先序') or 10**12,*identity(r)))
        for i,r in enumerate(rs,1):r['H211研究序']=i
    cov=body.get('health',{}).get('technical_coverage') or {}
    ready=bool(market and not market.errors and body.get('technical_current') and body.get('market_day')==day==now.date().isoformat() and market.today_confirmed(day) and body['health'].get('technical_status')=='CROSS_MARKET_RESEARCH_READY' and all((cov.get(m,{}).get('ratio') or 0)>=.95 for m in ar.URLS) and any(tech_ok(r,day) for r in body['rows']))
    body['h211']={'version':VERSION,'day':day,'status':'RESEARCH_READY' if ready else 'DATA_PENDING','history_days':len(history),'expected_history_days':LOOKBACK,'history_errors':errors,'counts':dict(Counter(r['H211狀態'] for r in body['rows'])),'trade_authority_changed':False,'scope':'H195獨立研究；未修改H126、H194或H200；成交額非資金淨流入；未還原價格'}
    return ready

def freeze(body,root):
    d=body['expected_session'];p=Path(root)/REL/(d+'.json.gz')
    if body.get('h211',{}).get('status')!='RESEARCH_READY' or p.exists():return False
    ts=perf.stamp(body['asof'])
    if ts.date().isoformat()!=d or ts.time()<time(18,15):return False
    rows=[evidence(r) for r in body['rows'] if tech_ok(r,d)]
    if not rows:return False # empty file cannot prove complete absence
    # Atomic exclusive create: same-day reruns never replace first evidence.
    import gzip,os
    payload=ar._seal({'version':VERSION,'day':d,'asof':body['asof'],'trade_authority_changed':False,'rows':rows})
    blob=ar._bytes(payload)
    if len(blob)>16_000_000:raise ValueError('H211 partition too large')
    p.parent.mkdir(parents=True,exist_ok=True)
    import tempfile
    fd,tmp=tempfile.mkstemp(dir=p.parent,prefix='.h211-')
    try:
        with os.fdopen(fd,'wb') as f:f.write(gzip.compress(blob,mtime=0));f.flush();os.fsync(f.fileno())
        try:os.link(tmp,p)
        except FileExistsError:return False
    finally:os.unlink(tmp)
    return True

def ordered(rows):return sorted(rows,key=lambda r:(r.get('H211研究序') or 10**12,r.get('H200研究優先序') or 10**12,*identity(r)))
