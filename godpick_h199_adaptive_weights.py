# -*- coding: utf-8 -*-
"""H199: prospective research-weight proposals, never production weights or trade authority."""
from collections import defaultdict, Counter
from datetime import datetime, date, timedelta, time
from pathlib import Path
import math, re, statistics as st, io, json
import godpick_h197_strategy_review as review
p=review.p; ar=p.ar
VERSION='V191-H199-REGIME-ADAPTIVE-REVIEW'
STRATEGIES=review.STRATEGIES
REGIMES=('多頭廣度','空頭廣度','盤整／分歧')
MIN_TRAIN=20; MIN_HOLD=10; MIN_COVER=.95
ROOT=Path(__file__).resolve().parent

def expected_day(now):
    d=now.date() if now.time()>=time(18,15) else now.date()-timedelta(days=1)
    while d.weekday()>=5:d-=timedelta(days=1)
    return d.isoformat()

def neutral():return {s:1.0 for s in STRATEGIES}

def valid_num(x):return isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x)

class Evidence:
    def __init__(self,market):self.market=market;self.cache={}
    def quotes(self,m,d,cut):
        k=m,d
        if k not in self.cache:
            self.cache[k]=None
            if not self.market.errors and d in self.market.market.get(m,{}):
                self.cache[k]=ar.quote_rows(self.market.p,m,d,self.market.now)
        v=self.cache[k] or {}
        return {c:(float(r[4]),float(r[6])) for c,(r,obs) in v.items() if obs<=cut and re.fullmatch('[1-9][0-9]{3}',c) and p.num(r[4]) and p.num(r[6])}
    def window(self,m,d,cut,n):
        days=self.market.days
        if d not in days:return [],[]
        i=days.index(d);ds=days[max(0,i-n+1):i+1]
        if len(ds)!=n or any(x not in self.market.cal for x in ar._months(date.fromisoformat(ds[0]),date.fromisoformat(ds[-1]))):return [],[]
        return ds,[self.quotes(m,x,cut) for x in ds]
    def regime(self,m,d,cut):
        ds,qs=self.window(m,d,cut,6)
        out={'市場':m,'研究日':d,'狀態':'待補證','有效檔數':0,'覆蓋率':0.,'5日中位漲跌%':None,'上漲比例':None,'下跌比例':None}
        if not qs or any(not q for q in qs):return out
        common=set.intersection(*(set(q) for q in qs));cov=len(common)/max(map(len,qs))
        out.update({'有效檔數':len(common),'覆蓋率':cov})
        if len(common)<100 or cov<.90:return out
        rr=[(qs[-1][c][0]/qs[0][c][0]-1)*100 for c in common]
        med=st.median(rr);up=sum(x>0 for x in rr)/len(rr);down=sum(x<0 for x in rr)/len(rr)
        out.update({'狀態':REGIMES[0] if med>=1 and up>=.60 else REGIMES[1] if med<=-1 and down>=.60 else REGIMES[2], '5日中位漲跌%':med,'上漲比例':up,'下跌比例':down,'起始日':ds[0]})
        return out

def rotation(e,root,day,now):
    # Current master only: descriptive comparison, NOT historical training features.
    from godpick_h191_opportunity_engine import master_map,read_json
    master=master_map(read_json(Path(root)/'stock_master_cache.json',[]));out=[]
    for m in ar.URLS:
        ds,qs=e.window(m,day,now,11)
        if not qs or any(not q for q in qs):continue
        common=set.intersection(*(set(q) for q in qs)); groups=defaultdict(set)
        for c,v in master.items():
            if v['市場']==m and v['產業'] not in ('','其他','未分類','待分類'):groups[v['產業']].add(c)
        eligible={c for c,v in master.items() if v['市場']==m}; matched=common&eligible
        if len(eligible)<100 or len(matched)/len(eligible)<.80:continue
        def momentum(c,a,b):return (qs[b][c][0]/qs[a][c][0]-1)*100
        prior_market=st.median(momentum(c,0,5) for c in matched);recent_market=st.median(momentum(c,5,10) for c in matched)
        for sector,codes in groups.items():
            g=codes&matched
            if len(g)<5 or len(g)/len(codes)<.70:continue
            prev=st.median(momentum(c,0,5) for c in g)-prior_market;recent=st.median(momentum(c,5,10) for c in g)-recent_market
            prev_amount=sum(q[c][1] for q in qs[1:6] for c in g);recent_amount=sum(q[c][1] for q in qs[6:11] for c in g)
            out.append({'市場':m,'產業':sector,'資料日':day,'有效檔數':len(g),'覆蓋率':len(g)/len(codes),'前5日相對pp':prev,'近5日相對pp':recent,'相對強度變化pp':recent-prev,'兩期成交額比':recent_amount/prev_amount,'口徑':'相同成分股成交額活躍度，非淨流入；現行主檔分類'})
    return sorted(out,key=lambda x:(x['市場'],-x['相對強度變化pp'],x['產業']))

def independent(rows):
    out=[];end=''
    for r in sorted(rows,key=lambda x:x['day']):
        if r['day']>end:out.append(r);end=r['target']
    return out

def good(row,keys,cut=None):
    for k in keys:
        r=row['strategies'].get(k)
        if not r or not r['有效'] or not valid_num(r['相對對照pp']) or not valid_num(r['候選日均%']) or not r['結果最晚觀測']:return False
        if cut is not None and p.stamp(r['結果最晚觀測'])>cut:return False
    return True

def fit(rows,blocked=False):
    """Fixed rule; train-only eligibility and weights; untouched chronological holdout."""
    result={'status':'樣本不足／維持原權重','weights':neutral(),'trial_weights':neutral(),'train_windows':0,'holdout_windows':0,'train_coverage':0.,'holdout_coverage':0.,'holdout_improvement_pp':None,'holdout_win_rate':None,'train_improvement_pp':None,'strategies_tested':[],'holdout_start':None}
    rr=independent(rows)
    result['independent_windows']=len(rr)
    if blocked:result['status']='資料異常／維持原權重';return result
    if len(rr)<MIN_TRAIN+MIN_HOLD:return result
    train,hold=rr[:-MIN_HOLD],rr[-MIN_HOLD:];cut=min(p.stamp(x['decision']) for x in hold)
    result['holdout_start']=hold[0]['day'];result['train_cutoff']=cut.isoformat()
    # Select only from training, never holdout. Require repeated strategy presence.
    keys=[s for s in STRATEGIES if sum(good(x,[s],cut) for x in train)>=MIN_TRAIN and sum(good(x,[s],cut) for x in train)/len(train)>=MIN_COVER]
    result['strategies_tested']=keys
    if len(keys)<2:return result
    tr=[x for x in train if good(x,keys,cut)];ho=[x for x in hold if good(x,keys)]
    result.update(train_windows=len(tr),holdout_windows=len(ho),train_coverage=len(tr)/len(train),holdout_coverage=len(ho)/len(hold))
    if len(tr)<MIN_TRAIN or len(ho)<MIN_HOLD or min(result['train_coverage'],result['holdout_coverage'])<MIN_COVER:
        result['status']='留出或成熟覆蓋不足／維持原權重';return result
    trial=neutral()
    for s in keys:
        edges=[x['strategies'][s]['相對對照pp'] for x in tr];avg=st.mean(edges);wins=sum(z>0 for z in edges)/len(edges)
        if avg>=.5 and wins>=.60:trial[s]=1.1
        elif avg<=-.5 and wins<=.40:trial[s]=.9
    result['trial_weights']=trial
    def improvements(z):
        v=[z['strategies'][s]['候選日均%'] for s in keys];w=[trial[s] for s in keys]
        return sum(x*y for x,y in zip(v,w))/sum(w)-st.mean(v)
    a=[improvements(x) for x in tr];b=[improvements(x) for x in ho]
    result.update(train_improvement_pp=st.mean(a),holdout_improvement_pp=st.mean(b),holdout_win_rate=sum(x>1e-12 for x in b)/len(b))
    if any(trial[s]!=1 for s in keys) and st.mean(a)>=.10 and st.mean(b)>=.10 and result['holdout_win_rate']>=.60:
        result.update(status='通過留出／研究權重建議（未套用）',weights=trial)
    else:result['status']='未優於等權基準／維持原權重'
    return result

def repetition(signals,now):
    by=defaultdict(lambda:defaultdict(dict));out=[];summary=[]
    for s in signals.values():
        if p.stamp(s['created_at'])<=now and s['tier'] in p.TIERS[:3] and s['authority']=='RESEARCH_ONLY':by[s['market']][s['signal_day']][s['code']]=s
    for m,dates in sorted(by.items()):
        ds=sorted(dates)[-5:];latest=dates[ds[-1]];prior=dates[ds[-2]] if len(ds)>1 else {};union=set(latest)|set(prior)
        summary.append({'市場':m,'最近封存日':ds[-1],'封存日數':len(ds),'前次重疊率':len(set(latest)&set(prior))/len(union) if prior else None,'今日資料狀態':'對應最近預期交易日' if ds[-1]==expected_day(now) else '歷史封存／非今日'})
        for c,s in latest.items():
            hits=sum(c in dates[d] for d in ds);streak=0
            for d in reversed(ds):
                if c not in dates[d]:break
                streak+=1
            out.append({'市場':m,'股票代號':c,'股票名稱':s.get('name',''),'最近封存日':ds[-1],'近5封存日出現次數':hits,'連續封存出現次數':streak,'可比較封存日數':len(ds),'提示':'持續出現／複核新證據（不扣分）' if hits>=3 else '一般觀察'})
    return summary,sorted(out,key=lambda x:(-x['近5封存日出現次數'],x['市場'],x['股票代號']))

def run(root=ROOT,now=None,*,ledger=None,market=None):
    root=Path(root);now=p.stamp(now or datetime.now(ar.TZ));ss,oo=p.read_ledger(root) if ledger is None else ledger
    market=p.Market(root,now) if market is None else market
    e=Evidence(market)
    day=expected_day(now);current=[e.regime(m,day,now) for m in ar.URLS]
    # Only immutable H194 cohorts. All date-eligible windows counted before filtering outcomes.
    groups=defaultdict(list)
    for s in ss.values():
        if p.stamp(s['created_at'])<=now and s['tier'] in p.TIERS[:3] and s['authority']=='RESEARCH_ONLY':groups[(s['market'],s['signal_day'])].append(s)
    daily=review.evaluate(ss,oo,market,now)['daily'] if groups and not market.errors else []
    lookup=defaultdict(dict)
    for x in daily:
        if x['T+N']==5:lookup[(x['市場'],x['研究日'])][x['策略']]=x
    history=[];cohorts=defaultdict(list)
    for (m,d),rows in sorted(groups.items()):
        span=market.span(d,5)
        if not span:continue
        cut=min(p.stamp(s['created_at']) for s in rows);reg=e.regime(m,d,cut);history.append(reg)
        cohorts[m].append({'day':d,'target':span[-1],'decision':cut.isoformat(),'regime':reg['狀態'],'strategies':lookup[(m,d)]})
    models=[];recommend=[]
    for r in current:
        m=r['市場'];allrows=cohorts[m];coverage=sum(x['regime'] in REGIMES for x in allrows)/len(allrows) if allrows else 0.
        for reg in REGIMES:
            v=fit([x for x in allrows if x['regime']==reg],blocked=bool(market.errors) or bool(allrows and coverage<MIN_COVER));models.append({'市場':m,'市場狀態':reg,'歷史狀態覆蓋率':coverage,**v})
        model=next((x for x in models if x['市場']==m and x['市場狀態']==r['狀態']),None)
        for s in STRATEGIES:recommend.append({'市場':m,'市場狀態':r['狀態'],'策略':s,'研究權重建議':model['weights'][s] if model else 1.0,'狀態':model['status'] if model else '當前市場未核實／維持原權重','正式權重已修改':False})
    repeat_summary,repeat_rows=repetition(ss,now)
    return {'version':VERSION,'asof':now.isoformat(),'expected_session':day,'status':'ADVISORY_AVAILABLE' if any(x['研究權重建議']!=1 for x in recommend) else 'BASELINE_ONLY','basis':'H194前瞻未還原收盤；策略袖套可重疊，等權比較非可成交組合績效。僅研究建議、不修改權重或授權。','archive_errors':list(market.errors),'signal_count':sum(map(len,groups.values())),'current_regime':current,'models':models,'recommendations':recommend,'historical_regimes':history,'rotation':rotation(e,root,day,now) if not market.errors else [],'repetition_summary':repeat_summary,'repetition':repeat_rows,'weights_changed':False,'trade_authority_changed':False}

def excel(r):
    import pandas as pd
    b=io.BytesIO()
    def safe(v):
        if isinstance(v,(dict,list)):v=json.dumps(v,ensure_ascii=False,allow_nan=False)
        if isinstance(v,str):
            v=re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]',' ',v)[:32000]
            if v.lstrip().startswith(('=','+','-','@')):v="'"+v
        return v
    tables=[('摘要',[{k:v for k,v in r.items() if not isinstance(v,list)}]),('市場狀態',r['current_regime']),('權重建議',r['recommendations']),('留出驗證',r['models']),('歷史市場狀態',r['historical_regimes']),('產業輪動',r['rotation']),('重複封存摘要',r['repetition_summary']),('重複封存明細',r['repetition'])]
    with pd.ExcelWriter(b,engine='openpyxl') as w:
        for name,rows in tables:
            df=pd.DataFrame(rows);df=df.map(safe) if hasattr(df,'map') else df.applymap(safe);df.to_excel(w,sheet_name=name,index=False)
    return b.getvalue()
