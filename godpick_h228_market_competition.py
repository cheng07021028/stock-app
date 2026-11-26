# -*- coding: utf-8 -*-
"""H228 retrospective raw-price benchmark of immutable prospective cohorts.
Never a historical universe reconstruction, trade authorization, or automatic tuning.
"""
from bisect import bisect_left,bisect_right
from collections import Counter
from datetime import datetime,time
from pathlib import Path
import io, math, statistics
import godpick_h190_market_archive as ar
import godpick_h194_performance as p
import godpick_h206_market_gap as gap
import godpick_h217_prospective_quality as q
import godpick_h225_integrated_quality as integrated
import godpick_h226_live_acceptance as live
import godpick_h227_strategy_validation as strategy
from godpick_h191_opportunity_engine import master_map,read_json
from godpick_h219_first_review import table
VERSION='V191-H228-MARKET-COMPETITION'
ROOT=Path(__file__).resolve().parent
MIN_UNIVERSE=100
LIMIT=5
FRONT=('財技籌碼共振','高成長起漲研究','技術優先複核')

def benchmark(master,market,a,z):
    """Quote dictionaries are H206 validated (close, amount); 15% discontinuity excluded."""
    codes={c for c,r in master.items() if r['市場']==market};paired=codes&a.keys()&z.keys()
    ret={c:z[c][0]/a[c][0]-1 for c in paired if p.num(a[c][0]) and p.num(z[c][0])}
    clean={c:x for c,x in ret.items() if abs(x)<=.15+1e-12}
    ranks=sorted(clean.values());n=len(ranks)
    # Inclusive boundary for ties; report actual baseline frequency, not a nominal 10%.
    threshold=ranks[n-math.ceil(n/10)] if n else None
    top={c for c,x in clean.items() if x>=threshold} if n else set()
    leaders,_=gap.leaders(master,z,a,market,'','')
    board={r['股票代號'] for r in leaders}
    ready=len(codes)>0 and n>=MIN_UNIVERSE and n*100>=95*len(codes) and board<=clean.keys()
    return {'universe':len(codes),'paired':len(ret),'jump':len(ret)-n,'returns':clean,'ranks':ranks,'top':top,'leaders':board,'ready':ready}

def competition(root,m,master,signals,day,target,now,reader=None):
    """Each group must be fully matched; no survivor-only performance. Rank is raw price."""
    reader=reader or gap.read_quotes;out=[];detail=[];errors=[]
    ix=q.source_index(signals)
    for market in ar.URLS:
        b=None;a={};z={}
        try:
            if m.errors or p.confirmed_span(m,day,1,now)!=[day,target]:raise ValueError('official T+1 calendar not confirmed')
            a,_=reader(root,market,day,now);z,_=reader(root,market,target,now)
            b=benchmark(master,market,a,z)
        except (OSError,ValueError,KeyError,TypeError,IndexError) as ex:errors.append(f'{market} {day}: {type(ex).__name__}: {str(ex)[:160]}')
        for src in q.SOURCES:
            pool=list(ix.get((src,day,market),{}).values());groups={'全研究':pool}
            if src=='H194':groups.update({'前三研究級':[s for s in pool if s['tier'] in p.TIERS[:3]],**{t:[s for s in pool if s['tier']==t] for t in p.TIERS}})
            for group,ss in groups.items():
                valid={};ranks=b['ranks'] if b else [];n=len(ranks)
                for s in ss:
                    c=s['code'];ts=p.stamp(s['created_at']);ok=bool(b and c in b['returns'] and strategy.price_equal(s['base_close'],a[c][0]) and ts.date().isoformat()==day and ts.time()>=time(18,15) and ts<now)
                    if ok:valid[c]=b['returns'][c]
                    if group=='全研究':
                        x=valid.get(c);rank=(bisect_left(ranks,x)+bisect_right(ranks,x))/(2*n) if ok and b['ready'] else None
                        detail.append({'研究日':day,'目標日':target,'市場':market,'來源':src,'股票代號':c,'股票名稱':s['name'],'研究分級':s.get('tier','H209獨立召回'),'原價報酬':x,'市場排名百分位':rank,'前十分位':c in b['top'] if ok and b['ready'] else None,'強勢榜':c in b['leaders'] if ok and b['ready'] else None,'狀態':'原價已核對／非結算' if ok else '待核'})
                full=bool(b and b['ready'] and ss and len(valid)==len(ss));xs=list(valid.values());k=len(ss)
                top_rate=len(valid.keys()&b['top'])/k if full else None;leader_rate=len(valid.keys()&b['leaders'])/k if full else None
                top_base=len(b['top'])/n if b and b['ready'] else None;leader_base=len(b['leaders'])/n if b and b['ready'] else None
                out.append({'研究日':day,'目標日':target,'市場':market,'來源':src,'研究群組':group,'主檔母體':b['universe'] if b else None,'雙日有效':b['paired'] if b else None,'跳動排除':b['jump'] if b else None,'排名母體':n if b else None,'封存研究':k,'研究可核':len(valid),'市場原價平均':statistics.mean(ranks) if b and b['ready'] else None,'研究原價平均':statistics.mean(xs) if full else None,'相對原價差':statistics.mean(xs)-statistics.mean(ranks) if full else None,'平均排名百分位':statistics.mean((bisect_left(ranks,x)+bisect_right(ranks,x))/(2*n) for x in xs) if full else None,'前十分位命中率':top_rate,'市場前十分位基準':top_base,'前十分位提升倍數':top_rate/top_base if full and top_base else None,'強勢榜命中率':leader_rate,'市場強勢榜基準':leader_base,'強勢榜提升倍數':leader_rate/leader_base if full and leader_base else None,'隨機等量預期強勢命中':k*leader_base if full and leader_base is not None else None,'狀態':'DESCRIPTIVE_ONLY' if full else 'DATA_PENDING','口徑':'當前主檔回顧／雙日原價／排除>15%跳動；排名母體≥100及95%；非歷史成分、含息、機率或正式授權'})
    return out,detail,errors

def narrow(rows):
    """Subset only of H225 already-verified shortlist. No new candidate or ranking score."""
    count=Counter();seen=set();out=[]
    for r in rows:
        k=(r['市場'],r['股票代號'])
        if k in seen:raise ValueError('duplicate H225 shortlist identity')
        seen.add(k)
        if k[0] not in ar.URLS or r['研究分層'] not in FRONT or r.get('反證／衝突') or not p.num(r.get('官方收盤')):continue
        if count[k[0]]>=LIMIT:continue
        count[k[0]]+=1
        out.append({**r,'H228複核序':count[k[0]],'用途':'精簡優先複核／非正式買進'})
    return out

def run(root=ROOT,now=None,*,base=None,acceptance=None,review=None):
    root=Path(root);now=p.stamp(now or datetime.now(ar.TZ));fp=integrated.fingerprint(root)
    r=integrated.run(root,now,include_detail=True) if base is None else base
    v=live.run(root,now,base=r) if acceptance is None else acceptance
    s=strategy.run(root,now,base=r,acceptance=v) if review is None else review
    if any(p.stamp(x['asof'])!=now for x in (r,v,s)):raise ValueError('H228 observation mismatch')
    m=p.Market(root,now);master=master_map(read_json(root/'stock_master_cache.json',[]));ss=r['review_signals'];board=r['review_board'];pairs=[]
    d,t=board.get('previous_day'),board.get('day')
    if d and t:pairs.append((d,t))
    first_day=min((x['signal_day'] for x in ss if x['source']=='H194'),default=None)
    if first_day:
        ds=p.confirmed_span(m,first_day,1,now)
        if ds and tuple(ds) not in pairs:pairs.append(tuple(ds))
    rows=[];detail=[];errors=list(s['errors'])
    for d,t in pairs:
        a,b,e=competition(root,m,master,ss,d,t,now);rows.extend(a);detail.extend(b);errors.extend(e)
    short=narrow(r['shortlist']);
    if fp!=integrated.fingerprint(root):raise RuntimeError('H228 evidence changed; retry')
    return {'version':VERSION,'asof':r['asof'],'day':s['day'],'status':'DESCRIPTIVE_ONLY' if any(x['狀態']=='DESCRIPTIVE_ONLY' for x in rows) else 'DATA_PENDING','batch_status':s['batch_status'],'counts':{**s['counts'],'有效排名比較':sum(x['狀態']=='DESCRIPTIVE_ONLY' for x in rows),'優先複核':len(short)},'batch':s['batch'],'competition':rows,'detail':detail,'shortlist':short,'precision':s['precision'],'pairs':s['pairs'],'validation':s['validation'],'tasks':s['tasks'],'errors':errors,'weight_multiplier':1.0,'trade_authority_changed':False}

def excel(r):
    from openpyxl import Workbook
    w=Workbook();w.remove(w.active)
    for name,rows in [('01_主管驗收',[{'資料日':r['day'],'狀態':r['status'],'首批':r['batch_status'],**r['counts'],'正式授權':'僅原H126','權重倍率':1.0}]),('02_首批績效',r['batch']),('03_全市場排名競爭',r['competition']),('04_研究逐股排名',r['detail']),('05_優先複核',r['shortlist']),('06_強勢榜命中',r['precision']),('07_策略比較',r['pairs']),('08_樣本外',r['validation']),('09_資料待辦',r['tasks']),('10_異常',[{'錯誤':x} for x in r['errors']])]:
        ws=table(w,name,rows)
        for col in ws.iter_cols():
            if col[0].value in ('市場原價平均','研究原價平均','相對原價差','平均排名百分位','前十分位命中率','市場前十分位基準','強勢榜命中率','市場強勢榜基準','市場排名百分位','原價報酬','榜單命中精度','榜單召回率','原價平均','淨報酬平均','淨勝率','原價上漲比例','方向準確率'):
                for c in col[1:]:c.number_format='0.00%'
    f=io.BytesIO();w.save(f);return f.getvalue()
