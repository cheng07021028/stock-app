# -*- coding: utf-8 -*-
"""H197 read-only prospective strategy evaluation. No authority/weights/ledger writes."""
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import statistics as st, math, io
import godpick_h194_performance as p
VERSION='V191-H197-STRATEGY-REVIEW'
ROOT=Path(__file__).resolve().parent
FIN={'營收成長','營收近13月新高','連兩季三率三升','獲利突破','低PE高TTM EPS'}
STRATEGIES=('主流強勢','低位階蓄勢','技術突破','大盤抗跌','大戶持股增加','財報高成長與估值')
PRIMARY=5
MIN_COVER=.95
MIN_PER_DAY=3
MIN_TRAIN=20
MIN_HOLD=10

def memberships(s):
    t=set(s['tech_pools'].split('、')); f=set(s['financial_pools'].split('、'))
    return (t & set(STRATEGIES[:5])) | ({STRATEGIES[-1]} if f & FIN else set())

def mean(xs):return st.mean(xs) if xs else None

def independent(rows):
    # Greedy non-overlap, including shared endpoints. Choose by dates, not results.
    out=[];end=''
    for r in sorted(rows,key=lambda x:x['研究日']):
        if r['研究日']>end:out.append(r);end=r['目標日']
    return out

def stats(rows):
    return {'日數':len(rows),'候選成熟筆數':sum(r['候選成熟'] for r in rows),'對照成熟筆數':sum(r['對照成熟'] for r in rows),
            '候選日均價格變動%':mean([r['候選日均%'] for r in rows]),'對照日均價格變動%':mean([r['對照日均%'] for r in rows]),
            '日均相對對照pp':mean([r['相對對照pp'] for r in rows]),'優於對照日比例':mean([float(r['相對對照pp']>0) for r in rows]),
            '官方指數有效日':sum(r['相對官方指數pp'] is not None for r in rows),'日均相對官方指數pp':mean([r['相對官方指數pp'] for r in rows if r['相對官方指數pp'] is not None])}

def assess(rows,blocked=False):
    # All date-eligible cohorts must enter the deterministic split before coverage is checked.
    mature=independent(rows); train=[];hold=[];cut=None
    if len(mature)>=MIN_TRAIN+MIN_HOLD:
        hold=mature[-MIN_HOLD:];cut=min(p.stamp(r['決策時間']) for r in hold)
        train=[r for r in mature[:-MIN_HOLD] if r['有效'] and p.stamp(r['結果最晚觀測'])<=cut]
    good_hold=[r for r in hold if r['有效']]
    a,b=stats(train),stats(good_hold)
    tr_expected=max(0,len(mature)-len(hold)); tc=len(train)/tr_expected if tr_expected else 0;hc=len(good_hold)/len(hold) if hold else 0
    state='樣本不足／繼續觀察'
    if blocked:state='行情驗證異常／禁止評估'
    elif len(mature)>=MIN_TRAIN+MIN_HOLD:
        if tc<MIN_COVER or hc<MIN_COVER:state='成熟覆蓋不足／繼續補證'
        elif len(train)>=MIN_TRAIN and len(good_hold)>=MIN_HOLD:
            # Pre-declared descriptive screen, not statistical significance or auto allocation.
            if a['日均相對對照pp']>=.5 and b['日均相對對照pp']>=.5 and a['優於對照日比例']>=.6 and b['優於對照日比例']>=.6:state='研究優先複核候選'
            elif a['日均相對對照pp']<=-.5 and b['日均相對對照pp']<=-.5 and a['優於對照日比例']<=.4 and b['優於對照日比例']<=.4:state='建議暫緩提升研究權重／人工複核'
            else:state='優勢未確認／維持觀察'
    return {'評估狀態':state,'非重疊成熟窗':len(mature),'訓練覆蓋率':tc,'留出覆蓋率':hc,'留出開始日':hold[0]['研究日'] if hold else None,'訓練截止時間':cut.isoformat() if cut else None,**{'訓練_'+k:v for k,v in a.items()},**{'留出_'+k:v for k,v in b.items()}}

def evaluate(signals,outcomes,market,now):
    now=p.stamp(now); cohorts=defaultdict(list)
    for s in signals.values():
        if p.stamp(s['created_at'])<=now and s['tier'] in p.TIERS[:3] and s['authority']=='RESEARCH_ONLY':cohorts[(s['market'],s['signal_day'])].append(s)
    daily=[]; summary=[]; evaluations=[]
    for m in p.ar.URLS:
        for strategy in STRATEGIES:
            for n in p.HORIZONS:
                rr=[];pending=0;expected=0
                for (market_id,day),ss in sorted(cohorts.items()):
                    if market_id!=m:continue
                    aa=[s for s in ss if strategy in memberships(s)]; bb=[s for s in ss if strategy not in memberships(s)]
                    if not aa:continue
                    expected+=1;span=market.span(day,n)
                    if not span:pending+=1;continue
                    target=span[-1]
                    def checked(group):
                        out=[]
                        for s in group:
                            o=outcomes.get((*p.sid(s),n))
                            if not o or p.stamp(o['resolved_at'])>now or o['target_day']!=target or o['signal_hash']!=p.digest(s):continue
                            # Recheck frozen base and target against archive; missing data remains missing.
                            if market.quotes(m,day).get(s['code'])!=s['base_close'] or market.quotes(m,target).get(s['code'])!=o['target_close']:continue
                            v=o['raw_return']
                            if not math.isfinite(v) or abs(v-(o['target_close']/s['base_close']-1))>1e-10:continue
                            out.append(o)
                        return out
                    a,b=checked(aa),checked(bb)
                    ac=len(a)/len(aa);bc=len(b)/len(bb) if bb else 0
                    valid=not market.errors and len(a)>=MIN_PER_DAY and len(b)>=MIN_PER_DAY and ac>=MIN_COVER and bc>=MIN_COVER
                    av=mean([o['raw_return']*100 for o in a]);bv=mean([o['raw_return']*100 for o in b]);ix=None
                    if m=='TWSE' and av is not None:
                        i,j=market.index(day),market.index(target)
                        if p.num(i) and p.num(j):ix=av-(j/i-1)*100
                    r={'市場':m,'策略':strategy,'T+N':n,'研究日':day,'目標日':target,'決策時間':min(s['created_at'] for s in ss),'結果最晚觀測':max((o['resolved_at'] for o in a+b),default=None),'候選應有':len(aa),'候選成熟':len(a),'候選覆蓋':ac,'對照應有':len(bb),'對照成熟':len(b),'對照覆蓋':bc,'候選日均%':av,'對照日均%':bv,'相對對照pp':av-bv if av is not None and bv is not None else None,'相對官方指數pp':ix,'有效':valid,'狀態':'可比較' if valid else '樣本／覆蓋／行情不足'}
                    rr.append(r);daily.append(r)
                good=[r for r in rr if r['有效']];summary.append({'市場':m,'策略':strategy,'T+N':n,'研究日總數':expected,'尚未成熟或日曆缺口':pending,'成熟窗':len(rr),'不可比較日':len(rr)-len(good),**stats(good)})
                if n==PRIMARY:evaluations.append({'市場':m,'策略':strategy,'主要期間':'T+5',**assess(rr,bool(market.errors))})
    return {'version':VERSION,'asof':now.isoformat(),'basis':'未還原收盤價格變動；非實際交易或含息績效。對照為同市場同日其餘合格研究股，非全市場指數；策略可能重疊。','signals':sum(len(v) for v in cohorts.values()),'archive_errors':list(market.errors),'status':'DATA_PENDING' if not cohorts or not any(x['日數'] for x in summary) else 'RESEARCH_REVIEW_ONLY','summary':summary,'evaluation':evaluations,'daily':daily,'trade_authority_changed':False,'weights_changed':False}

def run(root=ROOT,now=None):
    now=p.stamp(now or datetime.now(p.ar.TZ));ss,oo=p.read_ledger(root);m=p.Market(root,now)
    return evaluate(ss,oo,m,now)

def excel(r):
    import pandas as pd
    def safe(v):return "'"+v if isinstance(v,str) and v.lstrip().startswith(('=','+','-','@')) else v
    b=io.BytesIO()
    with pd.ExcelWriter(b,engine='openpyxl') as w:
        for name,rows in [('摘要',[{k:v for k,v in r.items() if k not in ('summary','evaluation','daily')}]),('六策略比較',r['summary']),('T5策略複核',r['evaluation']),('逐日對照',r['daily'])]:
            df=pd.DataFrame(rows);df=df.map(safe) if hasattr(df,'map') else df.applymap(safe);df.to_excel(w,sheet_name=name,index=False)
    return b.getvalue()
