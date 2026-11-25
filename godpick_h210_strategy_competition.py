# -*- coding: utf-8 -*-
"""H210: prospective, date-matched strategy competition; no trading authority."""
from collections import defaultdict
from datetime import datetime, timedelta, time
from pathlib import Path
import io
import math
import statistics as st
import godpick_h209_rolling_backtest as h209
import godpick_h199_adaptive_weights as h199

VERSION='V191-H210-STRATEGY-COMPETITION'
ROOT=Path(__file__).resolve().parent
TRAIN=20
HOLD=10
MIN_CANDIDATES=3
EDGE=.005
WIN=.60
PRIMARY=5
REGIMES=h199.REGIMES
BASIS=('H209.1 現金股利應收／交易成本後模擬報酬；全市場共用非重疊研究窗，按當時可觀測的 H199 原始價格廣度分組。'
       '同日等策略權重參考（策略可能持有同股，非市場指數、非可交易組合）；固定門檻，非統計顯著性。'
       '僅研究複核建議，不寫入正式權重、推薦紀錄或買進授權。')
p=h209.perf
ar=h209.ar

def finite(x):
    return isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x)

def windows(detail,e,now):
    """Use a shared calendar per market/horizon BEFORE filtering by regime or success.
    Do not silently drop partially covered strategy-days or duplicate stocks.
    A valid strategy-day requires all positions and >=3 distinct stocks.
    """
    by=defaultdict(lambda:defaultdict(list))
    for r in detail:
        by[(r['市場'],r['期間'])][r['研究日']].append(r)
    out=defaultdict(list);regcache={}
    for (m,n),days in sorted(by.items()):
        end=''
        for d,rows in sorted(days.items()):
            targets={r['出場日'] for r in rows}
            if len(targets)!=1 or not d<next(iter(targets))<=now.date().isoformat():
                raise ValueError('H210 conflicting research window: '+d)
            target=next(iter(targets))
            if d<=end:continue
            end=target
            cut=min(p.stamp(r['決策時間']) for r in rows)
            if cut>now or cut.date().isoformat()!=d:raise ValueError('H210 invalid decision time: '+d)
            key=(m,d,cut)
            if key not in regcache:regcache[key]=e.regime(m,d,cut)
            reg=regcache[key]['狀態'];strategies=defaultdict(list)
            for r in rows:strategies[r['策略']].append(r)
            v={}
            for s,rr in sorted(strategies.items()):
                codes={r['股票代號'] for r in rr}
                ok=[r for r in rr if r['status']=='VALID_CASH_MODEL' and finite(r.get('net_return')) and r['net_return']>-1 and r.get('resolved_at') and p.stamp(r['resolved_at'])<=now and p.stamp(r['resolved_at'])>=p.stamp(r['決策時間']) and p.stamp(r['resolved_at']).date().isoformat()>=target]
                complete=len(ok)==len(rr)==len(codes) and len(codes)>=MIN_CANDIDATES
                v[s]={'return':st.mean(r['net_return'] for r in ok) if complete else None,
                      'observed':max((p.stamp(r['resolved_at']) for r in ok)).isoformat() if complete else None,
                      'candidates':len(rr),'valid':len(ok),'unique':len(codes)}
            out[m,n].append({'day':d,'end':target,'decision':cut.isoformat(),'regime':reg,'strategies':v})
    return out

def good(w,s,cut=None):
    r=w['strategies'].get(s,{})
    return finite(r.get('return')) and r.get('observed') and (cut is None or p.stamp(r['observed'])<cut)

def metrics(rows,keys,s):
    v=[w['strategies'][s]['return'] for w in rows]
    # Equal strategy portfolios; peer benchmark includes self, frozen key list.
    b=[st.mean(w['strategies'][k]['return'] for k in keys) for w in rows]
    edges=[x-y for x,y in zip(v,b)];eq=[1.]
    for x in v:eq.append(eq[-1]*(1+x))
    return {'mean':st.mean(v),'edge':st.mean(edges),'win':sum(x>1e-12 for x in edges)/len(edges),
            'return':eq[-1]-1,'mdd':h209.drawdown(eq)}

def compete(rows):
    """Fixed 20/10 rolling split. Membership/rank selected ONLY on training.
    All ten holdout windows must match every eligible strategy, no substitutions.
    """
    folds=[];scores=[]
    for i in range(TRAIN,len(rows)-HOLD+1,HOLD):
        train=rows[i-TRAIN:i];hold=rows[i:i+HOLD];cut=p.stamp(hold[0]['decision'])
        keys=sorted({s for w in train for s in w['strategies'] if all(good(x,s,cut) for x in train)})
        f={'fold':len(folds)+1,'train_start':train[0]['day'],'hold_start':hold[0]['day'],'hold_end':hold[-1]['end'],'cutoff':cut.isoformat(),
           'eligible':keys,'train_windows':TRAIN,'hold_windows':HOLD,'matched_hold':0,'champion':None,'challenger':None,'observed':None,'status':'訓練策略不足'}
        folds.append(f)
        if len(keys)<2:continue
        tm={s:metrics(train,keys,s) for s in keys}
        ranking=sorted(keys,key=lambda s:(-tm[s]['edge'],s));f['champion'],f['challenger']=ranking[:2]
        f['matched_hold']=sum(all(good(w,s) for s in keys) for w in hold)
        if f['matched_hold']!=HOLD:f['status']='留出覆蓋不足';continue
        f['observed']=max(p.stamp(w['strategies'][s]['observed']) for w in hold for s in keys).isoformat()
        f['status']='OOS_COMPLETE'
        for rank,s in enumerate(ranking,1):
            a,b=tm[s],metrics(hold,keys,s)
            positive=(a['edge']>=EDGE and a['mean']>0 and a['win']>=WIN and b['edge']>=EDGE and b['mean']>0 and b['win']>=WIN)
            negative=(b['edge']<=-EDGE and b['mean']<0 and b['win']<=1-WIN)
            scores.append({'fold':f['fold'],'strategy':s,'train_rank':rank,'role':'冠軍候選' if rank==1 else '挑戰者' if rank==2 else '同場策略',
                           **{'train_'+k:v for k,v in a.items()},**{'hold_'+k:v for k,v in b.items()},'positive':positive,'negative':negative})
    return folds,scores

def current_day(market,now):
    """Do not substitute a stale known trading day for a missing current calendar."""
    target=now.date() if now.time()>=time(18,15) else now.date()-timedelta(days=1)
    while target.weekday()>=5:target-=timedelta(days=1)
    d=target.isoformat();obs=market.cal_observed.get(d[:7])
    if market.errors or not obs or obs.date()<target:return None
    ds=[x for x in market.days if x<=d]
    if not ds:return None
    latest=ds[-1]
    if any(x not in market.cal for x in ar._months(datetime.fromisoformat(latest).date(),target)):return None
    if latest==now.date().isoformat() and not market.today_confirmed(latest):return None
    return latest

def assess(detail,market,e,now):
    all_windows=windows(detail,e,now) if not market.errors else {}
    day=current_day(market,now)
    current=[e.regime(m,day,now) if day else {'市場':m,'研究日':None,'狀態':'待補證'} for m in ar.URLS]
    regimes={r['市場']:r['狀態'] for r in current}
    models=[];foldrows=[];scorerows=[];recommend=[];audit=[]
    for (m,n),rr in sorted(all_windows.items()):
        known=sum(w['regime'] in REGIMES for w in rr);coverage=known/len(rr)
        for w in rr:
            for s,v in w['strategies'].items():
                audit.append({'市場':m,'期間':n,'市場環境':w['regime'],'研究日':w['day'],'出場日':w['end'],'策略':s,'決策時間':w['decision'],'候選':v['candidates'],'有效':v['valid'],'不重複股票':v['unique'],'淨報酬':v['return'],'觀測時間':v['observed']})
        for reg in REGIMES:
            r=[w for w in rr if w['regime']==reg];fs,ss=compete(r)
            prefix={'市場':m,'期間':n,'市場環境':reg}
            foldrows.extend({**prefix,**f,'eligible':'、'.join(f['eligible'])} for f in fs)
            scorerows.extend({**prefix,**s} for s in ss)
            last=fs[-1] if fs else None;prior=fs[-2] if len(fs)>1 else None
            # Recent validation must be for current regime, observed now, two separate
            # holdout blocks; earlier block resolved before next holdout began.
            consecutive=bool(last and prior and last['status']==prior['status']=='OOS_COMPLETE' and p.stamp(prior['observed'])<p.stamp(last['cutoff']) and last['eligible']==prior['eligible'])
            age=sum(last['hold_end']<d<=day for d in market.days) if last and day else None
            fresh=age is not None and age<=max(10,6*n)
            # No stale ranking through recent incomplete mature windows: any remaining
            # windows after last completed fold must have the same matched evidence.
            tail=r[TRAIN+len(fs)*HOLD:] if fs else []
            tail_ok=bool(last and all(all(good(w,s) for s in last['eligible']) for w in tail))
            active=bool(not market.errors and coverage>=.95 and reg==regimes[m] and consecutive and fresh and tail_ok and n==PRIMARY)
            reason=('研究複核可評估' if active else '非主要T+5／僅觀察' if n!=PRIMARY else '歷史市場環境覆蓋不足' if coverage<.95 else '非當前已核實環境' if reg!=regimes[m] else '不足兩輪連續完整留出／比較母體不同' if not consecutive else '留出證據已過期' if not fresh else '近期成熟窗缺證')
            models.append({**prefix,'非重疊窗':len(r),'歷史環境覆蓋率':coverage,'留出輪數':len(fs),'完整留出輪數':sum(f['status']=='OOS_COMPLETE' for f in fs),'最近留出距今交易日':age,'近期未滿輪窗':len(tail),'狀態':reason})
            keys=sorted({s for w in r for s in w['strategies']})
            a={s['strategy']:s for s in ss if last and s['fold']==last['fold']};b={s['strategy']:s for s in ss if prior and s['fold']==prior['fold']}
            for s in keys:
                x=a.get(s,{});y=b.get(s,{});state='維持觀察'
                tail_metrics=metrics(tail,last['eligible'],s) if tail_ok and len(tail)>=3 and s in last['eligible'] else None
                if active and x and y:
                    if tail_metrics and tail_metrics['edge']<=-EDGE and tail_metrics['mean']<0 and not (x['negative'] and y['negative']):state='近期衰退警示／不淘汰'
                    elif x['negative'] and y['negative']:state='建議暫緩研究優先級／人工複核'
                    elif x['positive'] and y['positive'] and x['train_rank']==y['train_rank']==1:state='冠軍研究候選／人工複核'
                    elif x['negative']:state='近期衰退警示／不淘汰'
                    elif x['positive']:state='挑戰者／持續驗證'
                recommend.append({**prefix,'策略':s,'狀態':state,'門檻狀態':reason,'最近訓練排名':x.get('train_rank'),'最近留出均值':x.get('hold_mean'),'最近留出相對差':x.get('hold_edge'),'最近留出勝窗率':x.get('hold_win'),'前輪留出相對差':y.get('hold_edge'),'近期未滿輪相對差':tail_metrics['edge'] if tail_metrics else None,'研究權重倍率':1.0,'正式權重已修改':False})
    actionable=sum(x['狀態'] in ('冠軍研究候選／人工複核','建議暫緩研究優先級／人工複核') for x in recommend)
    return {'version':VERSION,'asof':now.isoformat(),'basis':BASIS,'status':'REVIEW_CANDIDATES' if actionable else 'BASELINE_ONLY','trade_authority_changed':False,'production_weights_changed':False,'current_day':day,'current_regime':current,'archive_errors':market.errors,'models':models,'folds':foldrows,'scores':scorerows,'recommendations':recommend,'audit':audit,'review_count':actionable,'thresholds':{'train_windows':TRAIN,'holdout_windows':HOLD,'min_distinct_stocks':MIN_CANDIDATES,'min_relative_edge':EDGE,'min_relative_win':WIN,'min_regime_coverage':.95,'primary_horizon':PRIMARY}}

def run(root=ROOT,now=None,*,backtest=None):
    now=p.stamp(now or datetime.now(ar.TZ));root=Path(root)
    b=h209.run(root,now,update=False) if backtest is None else backtest
    if b['revision']!=h209.REVISION or p.stamp(b['asof'])!=now:raise ValueError('H210 requires same-asof H209.1 evidence')
    m=p.Market(root,now)
    if b['archive_errors'] and not m.errors:raise ValueError('H209 archive errors; cannot rank')
    out=assess(b['detail'],m,h199.Evidence(m),now)
    out['upstream']={k:b[k] for k in ('revision','status','signal_count','valid_rows','pending','reasons')}
    return out

def excel(r):
    from openpyxl import Workbook
    from openpyxl.styles import Font,PatternFill
    from openpyxl.utils import get_column_letter
    import json
    w=Workbook();w.remove(w.active)
    health=[{'項目':k,'內容':json.dumps(r[k],ensure_ascii=False,allow_nan=False) if isinstance(r[k],(dict,list)) else r[k]} for k in ('version','asof','status','basis','current_day','upstream','thresholds','trade_authority_changed','production_weights_changed','archive_errors')]
    for title,rows in [('主管摘要',health),('當前市場',r['current_regime']),('策略競爭',r['recommendations']),('環境驗證',r['models']),('滾動留出',r['folds']),('逐輪績效',r['scores']),('研究窗稽核',r['audit'])]:
        ws=w.create_sheet(title);cols=list(dict.fromkeys(k for x in rows for k in x)) if rows else ['狀態'];ws.append(cols)
        for row in rows or [{'狀態':'尚無有效資料'}]:
            ws.append([row.get(k) for k in cols])
            for c in ws[ws.max_row]:
                if isinstance(c.value,str):c.data_type='s'
        for i,k in enumerate(cols,1):
            cell=ws.cell(1,i);cell.fill=PatternFill('solid',fgColor='16344D');cell.font=Font(color='FFFFFF',bold=True)
            ws.column_dimensions[get_column_letter(i)].width=min(48,max(15,len(k)*2+3))
            if k in {'覆蓋率','歷史環境覆蓋率','上漲比例','下跌比例','淨報酬','最近留出均值','最近留出相對差','最近留出勝窗率','前輪留出相對差','近期未滿輪相對差'} or k.startswith(('train_','hold_')) and k.split('_',1)[1] in ('mean','edge','win','return','mdd'):
                for cells in ws.iter_cols(min_col=i,max_col=i,min_row=2):
                    for c in cells:c.number_format='0.00%;[Red]-0.00%'
        ws.freeze_panes='C2';ws.auto_filter.ref=ws.dimensions
    b=io.BytesIO();w.save(b);return b.getvalue()
