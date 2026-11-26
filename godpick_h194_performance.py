# -*- coding: utf-8 -*-
"""H194 prospective public-research ledger. Raw close-price labels, NOT P&L.
No brokerage, records, ranking or trading-authority writes. No historical forecasts.
"""
from __future__ import annotations
from collections import Counter, defaultdict
from datetime import datetime,date,time
from pathlib import Path
import math, re, statistics, hashlib, io
import godpick_h190_market_archive as ar
VERSION='V191-H194-PROSPECTIVE-PRICE-LEARNING'
ROOT=Path(__file__).resolve().parent
HORIZONS=(1,3,5,10,20)
CLASSES=('下跌','盤整','上漲')
BAND=0.005
TIERS=('雙證據研究','技術研究','財報研究','補證觀察')

def stamp(v):
    d=datetime.fromisoformat(str(v))
    if d.tzinfo is None: raise ValueError('timezone required')
    return d.astimezone(ar.TZ)
def num(v):
    try:
        x=float(v);return x if math.isfinite(x) and x>0 else None
    except (ValueError,TypeError):return None
def identity(r):return (r['market'],r['code'])
def sid(r):return (r['signal_day'],r['market'],r['code'])
def okey(r):return (*sid(r),r['horizon'])
def label(x):return CLASSES[0] if x < -BAND-1e-12 else CLASSES[2] if x > BAND+1e-12 else CLASSES[1]
def digest(v):return hashlib.sha256(ar._bytes(v)).hexdigest()

def read_ledger(root):
    p=Path(root)/'data/h194'; signals={}; outcomes={}
    for f in sorted((p/'signals').glob('*.json.gz')):
        v=ar._read(f)
        if v['version']!=VERSION or v['kind']!='signals' or f.name!=v['day']+'.json.gz':raise ValueError('signal schema/path mismatch')
        created=stamp(v['created_at']);day=v['day']
        if created.date().isoformat()!=day or created.time()<time(18,15):raise ValueError('not prospective EOD snapshot')
        for r in v['rows']:
            k=sid(r)
            if k in signals or r['signal_day']!=day or r['created_at']!=v['created_at'] or r['tier'] not in TIERS or r['market'] not in ar.URLS or not re.fullmatch('[1-9][0-9]{3}',r['code']) or not num(r['base_close']):raise ValueError('invalid/duplicate signal')
            mod=v['models'][r['market']+'|'+r['tier']]
            if r['model_hash']!=digest(mod) or r['forecast']!=mod['probabilities'] or r['authority']!='RESEARCH_ONLY':raise ValueError('frozen model mismatch')
            prob=r['forecast']
            if prob is not None and (len(prob)!=3 or any(not isinstance(x,(float,int)) or not math.isfinite(x) or not 0<=x<=1 for x in prob) or abs(sum(prob)-1)>1e-9):raise ValueError('invalid probability')
            signals[k]=r
    for f in sorted((p/'outcomes').glob('*.json.gz')):
        v=ar._read(f)
        if v['version']!=VERSION or v['kind']!='outcomes':raise ValueError('outcome schema mismatch')
        for r in v['rows']:
            k=okey(r); s=signals.get(sid(r))
            if k in outcomes or not s or r['signal_hash']!=digest(s) or r['horizon'] not in HORIZONS or stamp(r['resolved_at'])!=stamp(v['created_at']) or r['target_day']<=s['signal_day'] or stamp(r['resolved_at']).date().isoformat()<r['target_day'] or stamp(r['resolved_at'])<=stamp(s['created_at']):raise ValueError('invalid/duplicate outcome')
            if not num(r['target_close']) or abs(r['raw_return']-(r['target_close']/s['base_close']-1))>1e-10 or r['label']!=label(r['raw_return']):raise ValueError('outcome arithmetic mismatch')
            outcomes[k]=r
    return signals,outcomes

def immutable(path,body):
    path=Path(path)
    if path.exists():raise ValueError('existing immutable ledger partition: '+path.name)
    ar._write(path,ar._seal(body)); assert ar._read(path)==body

class Market:
    def __init__(self,root,now):
        self.root=Path(root);self.now=stamp(now);self.p=self.root/'data/h190'
        cal,markets,rows,errors=ar._index(self.p,asof=self.now)
        self.errors=errors;self.cal={};self.cal_observed={};self.market=markets;self.cache={};self.bench={}
        # Never use a calendar/daily partition observed after the calculation time.
        for month,days in cal.items():
            v=ar._read(self.p/'calendar'/(month+'.json'))
            if stamp(v['observed_at'])<=self.now:
                self.cal[month]=days;self.cal_observed[month]=stamp(v['observed_at'])
        self.days=sorted(d for ds in self.cal.values() for d in ds if d<=self.now.date().isoformat())
    def today_confirmed(self,day):
        obs=self.cal_observed.get(day[:7]);return day in self.days and obs is not None and obs.date().isoformat()==day and obs.time()>=time(18,15)
    def quotes(self,m,d):
        k=(m,d)
        if k not in self.cache:
            self.cache[k]={}
            if self.errors or d not in self.market.get(m,{}):return self.cache[k]
            self.cache[k]={c:r[4] for c,(r,t) in ar.quote_rows(self.p,m,d,self.now).items() if num(r[4])}
        return self.cache[k]
    def span(self,day,n):
        if day not in self.days:return []
        i=self.days.index(day); ds=self.days[i:i+n+1]
        if len(ds)!=n+1:return []
        if any(m not in self.cal for m in ar._months(date.fromisoformat(day),date.fromisoformat(ds[-1]))):return []
        return ds
    def index(self,d):
        # H117 official TAIEX price index, not a TPEX index or an estimated proxy.
        if d not in self.bench:
            self.bench[d]=None
            p=self.root/'data/h117/daily/TAIEX'/(d+'.json.gz')
            if p.is_file():
                try:
                    import gzip,json
                    from godpick_h117_official_market_closure import verify_manifest,MARKET_URLS,VERSION as HV
                    with gzip.open(p,'rt',encoding='utf-8') as f:v=json.loads(f.read(1000001))
                    if verify_manifest(v) and v['schema']==HV and v['source']=='TAIEX' and v['source_url']==MARKET_URLS['TAIEX'] and v['date']==d and stamp(v['collected_at'])<=self.now and v['row_count']==len(v['rows'])==1:
                        r=v['rows'][0]
                        if r['date']==d and r['code']=='BENCHMARK' and r['exchange']=='TAIEX':self.bench[d]=num(r['close'])
                except (ImportError,OSError,ValueError,KeyError,TypeError):pass
        return self.bench[d]

def settle(signals,outcomes,market,now):
    new=[]; gaps=Counter(); ts=now.isoformat()
    for k,s in signals.items():
        if stamp(s['created_at'])>=now:continue
        for n in HORIZONS:
            if (*k,n) in outcomes:continue
            ds=market.span(s['signal_day'],n)
            if not ds or ds[-1]>now.date().isoformat() or (ds[-1]==now.date().isoformat() and now.time()<time(18,15)):gaps['未成熟或交易日曆缺口']+=1;continue
            prices=[market.quotes(s['market'],d).get(s['code']) for d in ds]
            if any(not num(x) for x in prices):gaps['缺官方連續行情（不跳過停牌）']+=1;continue
            if prices[0]!=s['base_close']:gaps['基準價衝突']+=1;continue
            r=prices[-1]/prices[0]-1
            new.append({'signal_day':s['signal_day'],'market':s['market'],'code':s['code'],'horizon':n,'target_day':ds[-1],'target_close':prices[-1],'raw_return':r,'label':label(r),'resolved_at':ts,'signal_hash':digest(s)})
    return new,dict(gaps)

def distribution(rows):
    c=Counter(r['label'] for r in rows);return [(c[x]+1)/(len(rows)+3) for x in CLASSES]
def brier(p,y):return sum((x-(c==y))**2 for x,c in zip(p,CLASSES))
def balanced_loss(rows,p):
    by=defaultdict(list)
    for r in rows:by[r['signal_day']].append(brier(p,r['label']))
    return statistics.mean(statistics.mean(v) for v in by.values())

def models(signals,outcomes,now):
    # Entire training set and holdout are from immutable prospective cohorts.
    # Holdout is split by SIGNAL DATE (never by stock); train labels must already
    # have been observed at the first holdout decision, not merely target dates.
    rows=[]
    for k,o in outcomes.items():
        if o['horizon']==1 and stamp(o['resolved_at'])<=now:
            s=signals[sid(o)];rows.append({**o,'tier':s['tier'],'created_at':s['created_at']})
    result={}
    for m in ar.URLS:
        mr=[r for r in rows if r['market']==m]
        for tier in TIERS:
            rr=[r for r in mr if r['tier']==tier];last_mature=max((r['signal_day'] for r in mr),default='');eligible=[s for s in signals.values() if s['market']==m and s['tier']==tier and s['signal_day']<=last_mature and stamp(s['created_at'])<=now];days=sorted({s['signal_day'] for s in eligible});v={'status':'樣本不足','samples':len(rr),'days':len(days),'probabilities':None}
            result[m+'|'+tier]=v
            if len(days)<30:continue
            hd=set(days[-10:]);hold=[r for r in rr if r['signal_day'] in hd];cut=min(stamp(s['created_at']) for s in eligible if s['signal_day'] in hd)
            train=[r for r in rr if r['signal_day']<min(hd) and stamp(r['resolved_at'])<=cut]
            base=[r for r in mr if r['signal_day']<min(hd) and stamp(r['resolved_at'])<=cut]
            v.update(train_samples=len(train),train_days=len({r['signal_day'] for r in train}),holdout_samples=len(hold),holdout_days=len(hd),holdout_start=min(hd),train_cutoff=cut.isoformat())
            expected_train=sum(s['signal_day']<min(hd) for s in eligible);expected_hold=sum(s['signal_day'] in hd for s in eligible)
            v.update(train_coverage=len(train)/expected_train if expected_train else 0,holdout_coverage=len(hold)/expected_hold if expected_hold else 0)
            if v['train_coverage']<.95 or v['holdout_coverage']<.95:
                v['status']='成熟樣本覆蓋不足';continue
            if len(train)<100 or v['train_days']<20 or len(hold)<50 or min(Counter(r['label'] for r in train).get(c,0) for c in CLASSES)<10:continue
            p=distribution(train);q=distribution(base);loss=balanced_loss(hold,p);baseline=balanced_loss(hold,q)
            v.update(holdout_brier=loss,baseline_brier=baseline,validation_probabilities=p,holdout_frequencies=[sum(r['label']==c for r in hold)/len(hold) for c in CLASSES])
            # Freeze validated train-only probability; do not refit on holdout.
            if loss+0.01<baseline:v.update(status='研究機率通過時序留出驗證',probabilities=p)
            else:v['status']='未優於同市場研究母體基準'
    return result

def freeze(result,market,model,now):
    day=now.date().isoformat();ts=now.isoformat()
    if stamp(result['asof']).date()!=now.date() or stamp(result['asof'])>now or now.time()<time(18,15) or market.errors or not market.today_confirmed(day):return None,'當日聯合行情未核實／不回填預測'
    rows=[]; seen=set();gaps=Counter()
    for tier in TIERS:
        if tier in TIERS[:2] and (result['market_day']!=day or not result['same_session']):
            gaps['技術／財技研究交易日未一致']+=len(result['pools'][tier]);continue
        for x in result['pools'][tier]:
            m=x['市場'];c=x['股票代號'];k=(m,c)
            if k in seen:raise ValueError('duplicate H193 candidate')
            seen.add(k)
            if m not in ar.URLS or not re.fullmatch('[1-9][0-9]{3}',c):gaps['非一般股票']+=1;continue
            price=market.quotes(m,day).get(c)
            if not num(price):gaps['缺同日官方收盤']+=1;continue
            mod=model[m+'|'+tier]
            rows.append({'signal_day':day,'created_at':ts,'market':m,'code':c,'name':str(x['股票名稱']),'tier':tier,'tech_pools':x['技術研究池'],'financial_pools':x['財報研究池'],'base_close':price,'research_rank':x['研究順序'],'forecast':mod['probabilities'],'forecast_status':mod['status'],'model_hash':digest(mod),'authority':'RESEARCH_ONLY'})
    if not rows:return None,'沒有可封存的同日具名研究證據'
    # H217: immutable contemporaneous H193 diagnostics, not reconstructed later.
    audit=[]
    for x in result.get('audit',[]):
        m,c=x.get('市場'),x.get('股票代號')
        if m not in ar.URLS or not isinstance(c,str) or not re.fullmatch('[1-9][0-9]{3}',c):continue
        context=x.get('H211技術證據') or {}
        audit.append({'market':m,'code':c,'tier':str(x.get('研究分級') or ''),'tech_gap':str(x.get('技術缺口') or ''),'financial_gap':str(x.get('財報缺口') or ''),'liquid':context.get('流動性通過') if isinstance(context,dict) else None,'tech_pools':str(x.get('技術研究池') or ''),'financial_pools':str(x.get('財報研究池') or '')})
    body={'version':VERSION,'kind':'signals','day':day,'created_at':ts,'h193_hash':digest(result),'models':model,'gaps':dict(gaps),'rows':rows}
    if audit and len({(x['market'],x['code']) for x in audit})==len(audit):
        body['h217_audit']={'decision_day':day,'market_day':result.get('market_day'),'same_session':result.get('same_session') is True,'rows':audit}
    return body,'當日研究快照建立'

def report(signals,outcomes,market,now,model,gaps=None):
    # Report only as-of-visible observations; benchmarks may become available later.
    ss={k:s for k,s in signals.items() if stamp(s['created_at'])<=now}; oo=[o for o in outcomes.values() if sid(o) in ss and stamp(o['resolved_at'])<=now]
    detail=[];groups=defaultdict(list)
    for o in oo:
        s=ss[sid(o)]; a,b=(market.index(s['signal_day']),market.index(o['target_day'])) if s['market']=='TWSE' else (None,None)
        bm=b/a-1 if a and b else None
        r={**o,'tier':s['tier'],'tech_pools':s['tech_pools'],'financial_pools':s['financial_pools'],'benchmark':'TAIEX_PRICE_INDEX' if bm is not None else '待同市場官方指數','benchmark_return':bm,'excess_return':o['raw_return']-bm if bm is not None else None}
        detail.append(r)
        groups[(s['market'],'分級:'+s['tier'],o['horizon'])].append(r)
        for pool in sorted(set((s['tech_pools']+'、'+s['financial_pools']).split('、'))):
            if pool:groups[(s['market'],'策略:'+pool,o['horizon'])].append(r)
    metrics=[]
    for (m,t,n),rr in sorted(groups.items()):
        ex=[r['excess_return'] for r in rr if r['excess_return'] is not None]; vals=[r['raw_return'] for r in rr]
        metrics.append({'市場':m,'研究分組':t,'T+N':n,'成熟筆數':len(rr),'獨立研究日':len({r['signal_day'] for r in rr}),'原始價格上漲率':sum(x>BAND+1e-12 for x in vals)/len(vals),'下跌筆數':sum(r['label']=='下跌' for r in rr),'盤整筆數':sum(r['label']=='盤整' for r in rr),'上漲筆數':sum(r['label']=='上漲' for r in rr),'平均原始價格變動':statistics.mean(vals),'超額有效筆數':len(ex),'平均價格超額':statistics.mean(ex) if ex else None})
    scored=[(ss[sid(o)]['forecast'],o) for o in oo if o['horizon']==1 and ss[sid(o)]['forecast'] is not None]
    calibration=[]
    for i,c in enumerate(CLASSES):
        for lo in range(0,10):
            z=[(p,o) for p,o in scored if min(9,int(p[i]*10))==lo]
            if z:calibration.append({'class':c,'bin':f'{lo*10}-{(lo+1)*10}%','n':len(z),'mean_probability':statistics.mean(p[i] for p,o in z),'observed_frequency':sum(o['label']==c for p,o in z)/len(z)})
    pred={'mature_forecasts':len(scored),'brier':statistics.mean(brier(p,o['label']) for p,o in scored) if scored else None,'accuracy':sum(CLASSES[max(range(3),key=lambda i:p[i])]==o['label'] for p,o in scored)/len(scored) if scored else None}
    latest=max((k[0] for k in ss),default='')
    return {'version':VERSION,'asof':now.isoformat(),'basis':'未還原收盤價格變動，非可成交報酬／含息績效','signals':len(ss),'mature':len(oo),'pending':len(ss)*len(HORIZONS)-len(oo),'gaps':gaps or {},'models':model,'prediction_metrics':pred,'calibration':calibration,'metrics':metrics,'outcomes':detail,'latest_signals':[s for s in ss.values() if s['signal_day']==latest],'trade_authority_changed':False}

def run(root=ROOT,now=None):
    from godpick_h193_joint_decision import run as joint
    live=now is None;now=stamp(now or datetime.now(ar.TZ));root=Path(root);p=root/'data/h194'
    signals,outcomes=read_ledger(root);market=Market(root,now)
    new,gaps=settle(signals,outcomes,market,now)
    if new:
        for i in range(0,len(new),3000):
            immutable(p/'outcomes'/(now.strftime('%Y%m%dT%H%M%S%f')+f'-{i//3000:05d}.json.gz'),{'version':VERSION,'kind':'outcomes','created_at':now.isoformat(),'rows':new[i:i+3000]})
        outcomes.update({okey(o):o for o in new})
    model=models(signals,outcomes,now);day=now.date().isoformat();path=p/'signals'/(day+'.json.gz')
    status='當日已有不可覆寫快照'
    capture_gate={'day':day,'calendar_confirmed':market.today_confirmed(day),'archive_errors':list(market.errors),
                  'market_quote_counts':{m:len(market.quotes(m,day)) for m in ar.URLS},'reason':'ALREADY_CAPTURED'}
    if not path.exists():
        if now.time()<time(18,15):reason='BEFORE_CAPTURE_WINDOW'
        elif market.errors:reason='ARCHIVE_ERROR'
        elif not market.today_confirmed(day):reason='CALENDAR_NOT_CONFIRMED'
        elif not any(capture_gate['market_quote_counts'].values()):reason='NO_CURRENT_QUOTES'
        else:reason='EVALUATE'
        capture_gate['reason']=reason
        if reason!='EVALUATE':status='當日聯合行情未核實／不回填預測：'+reason
        else:
            r,_=joint(root,now=now)
            finished=stamp(datetime.now(ar.TZ)) if live else now
            if finished.date()!=now.date() or finished<now:
                body=None;status='當日聯合行情未核實／不回填預測：CAPTURE_WINDOW_EXPIRED';capture_gate['reason']='CAPTURE_WINDOW_EXPIRED'
            else:
                body,status=freeze(r,market,model,finished)
                capture_gate['reason']='CAPTURED' if body else 'NO_ELIGIBLE_CURRENT_RESEARCH'
                capture_gate['research_day']=r.get('market_day');capture_gate['same_session']=r.get('same_session')
                if body:
                    immutable(path,body);signals.update({sid(s):s for s in body['rows']});now=finished
    out=report(signals,outcomes,market,now,model,gaps);out['capture_status']=status;out['capture_gate']=capture_gate;out['archive_errors']=market.errors;out['new_outcomes']=len(new)
    # Health/report is replaceable; source signals and outcomes are not.
    ar._write(p/'latest.json.gz',ar._seal({k:v for k,v in out.items() if k not in ('outcomes','latest_signals')}))
    return out

def excel(r):
    import pandas as pd
    def safe(v):return "'"+v if isinstance(v,str) and v.lstrip().startswith(('=','+','-','@')) else v
    buf=io.BytesIO()
    with pd.ExcelWriter(buf,engine='openpyxl') as w:
        for name,rows in [('摘要',[{k:v for k,v in r.items() if k not in ('models','metrics','outcomes','latest_signals')}]),('策略績效',r['metrics']),('逐筆追蹤',r['outcomes']),('最近研究',r['latest_signals']),('預測驗證',[{'分組':k,**v} for k,v in r['models'].items()]),('機率校準',r['calibration'])]:
            df=pd.DataFrame(rows);df=df.map(safe) if hasattr(df,'map') else df.applymap(safe);df.to_excel(w,sheet_name=name,index=False)
    return buf.getvalue()
