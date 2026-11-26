# -*- coding: utf-8 -*-
"""H229: read-only T+1 return attribution and identical-cost market comparison.
Current master retrospective benchmark; not a reconstructed historical portfolio.
No order, weight, signal or outcome writes. Fixed rules, not optimized for this batch.
"""
from collections import Counter, defaultdict
from datetime import datetime,time
from pathlib import Path
import io,math,statistics
import godpick_h190_market_archive as ar
import godpick_h194_performance as p
import godpick_h209_rolling_backtest as b
import godpick_h225_integrated_quality as integrated
import godpick_h226_live_acceptance as live
import godpick_h227_strategy_validation as strategy
import godpick_h228_market_competition as competition
from godpick_h191_opportunity_engine import master_map,read_json
from godpick_h219_first_review import table
VERSION='V191-H229-NET-EDGE'
ROOT=Path(__file__).resolve().parent
MIN_UNIVERSE=100;MIN_AMOUNT=20_000_000;MIN_GROUP=3;MAX_DAYS=80
GROUPS=('前三研究級',*p.TIERS[:3]);TRAIN=20;HOLD=10;EDGE=.005;WIN=.6

def mean(xs):return statistics.mean(xs) if xs else None

def metrics(v):
    wins=[x for x in v if x>0];loss=[-x for x in v if x<0]
    return {'平均淨報酬':mean(v),'淨勝率':len(wins)/len(v) if v else None,'平均獲利':mean(wins),'平均虧損':mean(loss),'報酬獲利因子':sum(wins)/sum(loss) if loss else None}

def valid_trade(t,target,now):
    return (t.get('status')=='VALID_CASH_MODEL' and live.finite(t.get('net_return')) and t['net_return']>-1 and t.get('resolved_at') and target<=p.stamp(t['resolved_at']).date().isoformat() and p.stamp(t['resolved_at'])<=now)

def attribution(s,r,days,e,now):
    """Actual frozen decision; independent H218 result cross-check. No raw ledger imputation."""
    out={'研究日':s['signal_day'],'市場':s['market'],'股票代號':s['code'],'股票名稱':s['name'],'研究分級':s['tier'],'出場日':days[-1] if days else None,'原價收盤報酬':None,'隔夜跳空':None,'日內原價報酬':None,'成本拖累':None,'淨報酬':None,'結果觀測時間':None,'歸因':'未成熟／日曆待核','原價狀態':r.get('原始績效狀態','待核'),'淨績效狀態':r.get('淨績效狀態','待核')}
    if not days:return out
    if r.get('出場日')!=days[-1] or not p.stamp(s['created_at'])<now:return {**out,'歸因':'封存或目標日不一致'}
    if r.get('原始績效狀態')=='VERIFIED_RAW' and live.finite(r.get('原始收盤績效')):out['原價收盤報酬']=r['原始收盤績效']
    if s['tier'] not in p.TIERS[:3]:return {**out,'歸因':'補證觀察／不納入淨績效'}
    t=b.trade(s,days,e)
    if not valid_trade(t,days[-1],now):return {**out,'歸因':str(t.get('status','待核'))}
    if r.get('淨績效狀態')!='VALID_CASH_MODEL' or not live.finite(r.get('淨報酬')) or not math.isclose(t['net_return'],r['淨報酬'],rel_tol=1e-9,abs_tol=1e-10) or not r.get('結果觀測時間') or p.stamp(r['結果觀測時間'])!=p.stamp(t['resolved_at']):return {**out,'歸因':'H218淨績效對帳待核'}
    if p.stamp(t['resolved_at'])<=p.stamp(s['created_at']):return {**out,'歸因':'觀測時間異常'}
    raw=t['raw_return'];net=t['net_return'];gap=t['entry_open']/s['base_close']-1
    # T+1 opens after ex-date; cash entitlement is zero. Gap is raw, may include ex-right effects.
    if out['原價收盤報酬'] is not None and not math.isclose((1+gap)*(1+raw)-1,out['原價收盤報酬'],rel_tol=1e-8,abs_tol=1e-9):return {**out,'歸因':'原價與日內分解衝突'}
    tag='淨報酬為正' if net>0 else '成本使日內正報酬轉非正' if raw>0 else '跳空上漲但日內未延續' if gap>0 else '日內未獲利'
    return {**out,'隔夜跳空':gap,'日內原價報酬':raw,'成本拖累':raw-net,'淨報酬':net,'結果觀測時間':t['resolved_at'],'歸因':tag,'淨績效狀態':'VERIFIED_NET'}

def market_net(master,market,day,target,anchor,e,now):
    """Eligibility known by earliest frozen decision. Same H209 fixed-lot costs for all.
    Current master is retrospective. Missing base rows count against market coverage.
    Target missing or locked bars count against eligible denominator, never assigned zero.
    """
    codes={c for c,r in master.items() if r['市場']==market};base=0;eligible=set();results={};observations=[];blocked=Counter()
    for c in sorted(codes):
        r,t=e.quote(market,day,c)
        if not r or not t or p.stamp(t)>anchor or any(not p.num(x) for x in r[1:7]):continue
        base+=1
        if float(r[6])<MIN_AMOUNT:continue
        eligible.add(c)
        s={'market':market,'code':c,'base_close':float(r[4]),'created_at':anchor.isoformat()}
        x=b.trade(s,[day,target],e)
        if valid_trade(x,target,now) and p.stamp(x['resolved_at'])>anchor:
            results[c]=x['net_return'];observations.append(p.stamp(x['resolved_at']))
        else:blocked[str(x.get('status','待核'))]+=1
    ready=bool(codes and base*100>=95*len(codes) and len(eligible)>=MIN_UNIVERSE and len(results)*100>=95*len(eligible))
    return {'universe':len(codes),'base':base,'eligible':eligible,'results':results,'ready':ready,'observed_at':max(observations).isoformat() if observations else None,'blocked':dict(blocked)}

def group_rows(day,market,ss,attrs,bench,target):
    """No survivor-only group metrics; market baseline allows explicitly disclosed <=5% gap."""
    out=[];a={r['股票代號']:r for r in attrs};n=len(bench['results']) if bench else 0
    for group in GROUPS:
        pool=[s for s in ss if s['tier'] in p.TIERS[:3] and (group==GROUPS[0] or s['tier']==group)]
        k=len(pool);ids={s['code'] for s in pool};valid=[a[c]['淨報酬'] for c in ids if c in a and a[c]['淨績效狀態']=='VERIFIED_NET'];full=k>0 and len(valid)==k
        eligible=ids & bench['eligible'] if bench else set();matched=ids & bench['results'].keys() if bench else set()
        same=bool(full and bench and bench['ready'] and k>=MIN_GROUP and len(matched)==k and all(math.isclose(a[c]['淨報酬'],bench['results'][c],rel_tol=1e-9,abs_tol=1e-10) for c in ids))
        # Disjoint reference removes the group itself. 95% of remaining eligible members required.
        rest=[v for c,v in bench['results'].items() if c not in ids] if bench else [];rest_den=len(bench['eligible']-ids) if bench else 0
        same=same and len(rest)>=MIN_UNIVERSE and len(rest)*100>=95*rest_den
        bm=mean(rest) if same else None;gm=mean(valid) if full else None
        obs=max([p.stamp(a[c]['結果觀測時間']) for c in ids]+[p.stamp(bench['observed_at'])]).isoformat() if same else None
        out.append({'研究日':day,'出場日':target,'市場':market,'研究群組':group,'決策時間':min((s['created_at'] for s in pool),default=None),'結果觀測時間':obs,'主檔母體':bench['universe'] if bench else None,'決策前行情有效':bench['base'] if bench else None,'市場流動性母體':len(bench['eligible']) if bench else None,'市場淨績效有效':n if bench else None,'研究封存':k,'研究淨有效':len(valid),'研究符合流動性':len(eligible),'研究同模型核對':len(matched),'比較母體':rest_den if bench else None,'比較有效':len(rest) if bench else None,'完整比較窗':bool(same and bench['base']==bench['universe'] and n==len(bench['eligible'])),**metrics(valid if full else []),'非本組市場淨均值':bm,'非本組市場淨勝率':mean([v>0 for v in rest]) if same else None,'相對淨差':gm-bm if same else None,'狀態':'DESCRIPTIVE_ONLY' if same else 'DATA_PENDING','口徑':'T+1開盤至收盤／固定千股同成本／當前主檔回顧／決策前成交額≥2000萬／市場95%／研究100%／排除本組；非實際成交'})
    return out

def validate(rows,now):
    """Predeclared groups, non-overlapping day windows, no look-ahead or auto weight."""
    out=[]
    for market in ar.URLS:
        for group in GROUPS:
            rs=sorted([r for r in rows if r['市場']==market and r['研究群組']==group and r['出場日'] and r['研究封存']>0],key=lambda r:r['研究日']);chosen=[];end=''
            for r in rs:
                if r['研究日']>end:chosen.append(r);end=r['出場日']
            a=chosen[-30:-10] if len(chosen)>=30 else [];h=chosen[-10:] if a else [];cut=p.stamp(h[0]['決策時間']) if h else None
            def good(r,limit):return r['狀態']=='DESCRIPTIVE_ONLY' and r.get('完整比較窗') is True and live.finite(r['相對淨差']) and r['結果觀測時間'] and p.stamp(r['決策時間'])<p.stamp(r['結果觀測時間'])<limit and p.stamp(r['結果觀測時間']).date().isoformat()>=r['出場日']
            aa=[r for r in a if good(r,cut)];hh=[r for r in h if good(r,now)]
            av=mean([r['相對淨差'] for r in aa]) if len(aa)==TRAIN else None;hv=mean([r['相對淨差'] for r in hh]) if len(hh)==HOLD else None;state='BASELINE_ONLY'
            if av is not None and hv is not None:
                state='NO_CONFIRMED_EDGE'
                if min(av,hv)>=EDGE and all(mean([r['相對淨差']>0 for r in x])>=WIN and mean([r['平均淨報酬'] for r in x])>0 and mean([r['淨勝率'] for r in x])>.5 for x in (aa,hh)):state='REVIEW_EDGE'
            out.append({'市場':market,'研究群組':group,'非重疊窗':len(chosen),'訓練有效':len(aa),'留出有效':len(hh),'訓練相對淨差':av,'留出相對淨差':hv,'狀態':state,'權重倍率':1.0,'用途':'當前主檔回顧的時序複核／市場100%窗／非真正前瞻母體或統計顯著性'})
    return out

def run(root=ROOT,now=None,*,base=None,acceptance=None,review=None,ranking=None):
    root=Path(root);now=p.stamp(now or datetime.now(ar.TZ));fp=integrated.fingerprint(root)
    r=integrated.run(root,now,include_detail=True) if base is None else base
    q=live.run(root,now,base=r) if acceptance is None else acceptance
    z=strategy.run(root,now,base=r,acceptance=q) if review is None else review
    x=competition.run(root,now,base=r,acceptance=q,review=z) if ranking is None else ranking
    if any(p.stamp(v['asof'])!=now for v in (r,q,z,x)):raise ValueError('H229 observation mismatch')
    m=p.Market(root,now);master=master_map(read_json(root/'stock_master_cache.json',[]));ss=[s for s in r['review_signals'] if s['source']=='H194'];ix={p.sid(s):s for s in ss}
    if len(ix)!=len(ss):raise ValueError('duplicate signal')
    di={};errors=list(x['errors'])
    for d in r['review_detail']:
        if d['訊號來源']!='H194' or d['期間']!=1:continue
        k=(d['研究日'],d['市場'],d['股票代號'])
        if k not in ix or k in di:raise ValueError('H229 detail identity mismatch')
        di[k]=d
    days=sorted({s['signal_day'] for s in ss});first=days[0] if days else None;scope=sorted(set(days[-MAX_DAYS:])|({first} if first else set()));rows=[];details=[];diagnostics=[];by=defaultdict(list)
    for s in ss:by[s['signal_day'],s['market']].append(s)
    for day in scope:
        e=b.Evidence(root,m,now);span=p.confirmed_span(m,day,1,now) if not m.errors else [];target=span[-1] if span else None
        for market in ar.URLS:
            pool=by[day,market];attrs=[];bench=None
            if not pool:continue
            for s in pool:attrs.append(attribution(s,di.get(p.sid(s),{}),span,e,now))
            if span and any(s['tier'] in p.TIERS[:3] for s in pool):
                try:
                    anchor=min(p.stamp(s['created_at']) for s in pool);bench=market_net(master,market,day,target,anchor,e,now)
                except (OSError,ValueError,KeyError,TypeError,IndexError) as ex:errors.append(f'{market} {day}: {type(ex).__name__}: {str(ex)[:160]}')
            rows.extend(group_rows(day,market,pool,attrs,bench,target));details.extend(attrs if day==first else []);diagnostics.extend({'研究日':day,'市場':market,'原因':k,'數量':v} for k,v in (bench['blocked'].items() if bench else []))
    # Current-master retrospective comparison; no claim of point-in-time universe.
    valid=validate(rows,now)
    causes=[{'市場':mkt,'研究分級':tier,'歸因':cause,'筆數':n} for (mkt,tier,cause),n in sorted(Counter((v['市場'],v['研究分級'],v['歸因']) for v in details).items())]
    if fp!=integrated.fingerprint(root):raise RuntimeError('H229 evidence changed; retry')
    return {'version':VERSION,'asof':r['asof'],'day':x['day'],'status':'DESCRIPTIVE_ONLY' if any(v['狀態']=='DESCRIPTIVE_ONLY' for v in rows) else 'DATA_PENDING','batch_status':x['batch_status'],'counts':{**x['counts'],'首批歸因':len(details),'有效淨優勢比較':sum(v['狀態']=='DESCRIPTIVE_ONLY' for v in rows),'首批淨績效核實':sum(v['淨績效狀態']=='VERIFIED_NET' for v in details),'檢討研究日':len(scope)},'batch':x['batch'],'net':rows,'detail':details,'causes':causes,'validation':valid,'benchmark_gaps':diagnostics,'shortlist':x['shortlist'],'competition':x['competition'],'tasks':x['tasks'],'errors':errors,'weight_multiplier':1.0,'trade_authority_changed':False}

def excel(r):
    from openpyxl import Workbook
    w=Workbook();w.remove(w.active)
    for name,rows in [('01_主管報酬驗收',[{'資料日':r['day'],'狀態':r['status'],'首批':r['batch_status'],**r['counts'],'權重倍率':1.0,'正式授權':'僅原H126'}]),('02_首批績效',r['batch']),('03_同成本市場競爭',r['net']),('04_首批逐股歸因',r['detail']),('05_歸因分布',r['causes']),('06_樣本外複核',r['validation']),('07_市場基準缺口',r['benchmark_gaps']),('08_優先複核',r['shortlist']),('09_原價排名競爭',r['competition']),('10_資料待辦',r['tasks']),('11_異常',[{'錯誤':x} for x in r['errors']])]:
        ws=table(w,name,rows)
        for col in ws.iter_cols():
            if col[0].value in ('原價收盤報酬','隔夜跳空','日內原價報酬','成本拖累','淨報酬','平均淨報酬','淨勝率','平均獲利','平均虧損','非本組市場淨均值','非本組市場淨勝率','相對淨差','訓練相對淨差','留出相對淨差','原價平均','淨報酬平均','原價上漲比例','方向準確率','市場原價平均','研究原價平均','相對原價差','平均排名百分位','前十分位命中率','市場前十分位基準','強勢榜命中率','市場強勢榜基準'):
                for c in col[1:]:c.number_format='0.00%'
    f=io.BytesIO();w.save(f);return f.getvalue()
