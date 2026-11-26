# -*- coding: utf-8 -*-
"""H227: prospective membership precision, counterevidence and predeclared tier pairs.
Read-only, no changes to H126 or weights. Current master is retrospective only.
"""
from collections import defaultdict, Counter
from datetime import datetime, time
from pathlib import Path
import io, math, statistics
import godpick_h190_market_archive as ar
import godpick_h194_performance as p
import godpick_h218_evidence as e
import godpick_h219_first_review as first
import godpick_h217_prospective_quality as q
import godpick_h225_integrated_quality as integrated
import godpick_h226_live_acceptance as live
from godpick_h191_opportunity_engine import master_map, read_json
VERSION='V191-H227-PROSPECTIVE-COUNTEREVIDENCE'
ROOT=Path(__file__).resolve().parent
PAIRS=(('雙證據對技術',p.TIERS[1],p.TIERS[0]),('財報對技術',p.TIERS[1],p.TIERS[2]))

def mean(xs):return statistics.mean(xs) if xs else None

def price_equal(a,b):return bool(p.num(a) and p.num(b) and math.isclose(float(a),float(b),rel_tol=1e-9,abs_tol=1e-8))

def leader_precision(board,signals,m,master,now):
    """Both recall and precision; no empty source, missing quote, or partial board as zero."""
    day,prev=board.get('day'),board.get('previous_day');out=[];sectors=[]
    if not day or not prev:return out,sectors
    ix=q.source_index(signals)
    for market in ar.URLS:
        cv=board.get('coverage',{}).get(market,{})
        n,v=cv.get('主檔一般股'),cv.get('雙日有價')
        univ={c for c,r in master.items() if r['市場']==market}
        a,z=m.quotes(market,prev),m.quotes(market,day)
        paired={c for c in univ if p.num(a.get(c)) and p.num(z.get(c))}
        leaders=[r for r in board['rows'] if r['市場']==market];ids={r['股票代號'] for r in leaders}
        # Top-list is descriptive (3%/20M/top20), not all market gainers or adjusted returns.
        ready=bool(not m.errors and not board.get('errors') and p.confirmed_span(m,prev,1,now)==[prev,day] and type(n) is int and n>0 and n==len(univ) and type(v) is int and 0<=v<=n and v==len(paired) and v*100>=95*n and len(ids)==len(leaders) and ids<=paired and all(price_equal(r['前收盤'],a.get(r['股票代號'])) and price_equal(r['收盤'],z.get(r['股票代號'])) for r in leaders))
        for src in q.SOURCES:
            pool=ix.get((src,prev,market),{})
            groups={'全研究':list(pool.values())}
            if src=='H194':groups.update({'前三研究級':[s for s in pool.values() if s['tier'] in p.TIERS[:3]],**{t:[s for s in pool.values() if s['tier']==t] for t in p.TIERS}})
            for group,ss in groups.items():
                valid={s['code'] for s in ss if s['code'] in paired and price_equal(s['base_close'],a.get(s['code'])) and abs(float(z[s['code']])/float(a[s['code']])-1)<=.15 and p.stamp(s['created_at']).date().isoformat()==prev and p.stamp(s['created_at']).time()>=time(18,15) and p.stamp(s['created_at'])<now}
                hit=len(ids&valid);complete=ready and bool(ss) and len(valid)==len(ss)
                precision=hit/len(ss) if complete else None;recall=hit/len(ids) if complete and ids else None
                out.append({'交易日':day,'研究日':prev,'市場':market,'訊號來源':src,'研究群組':group,'前日封存':len(ss),'可核對':len(valid),'強勢榜樣本':len(ids),'已核命中':hit if ready else None,'未核研究':len(ss)-len(valid),'榜單命中精度':precision,'榜單召回率':recall,'狀態':'DESCRIPTIVE_ONLY' if complete and ids else 'NO_LEADERS' if complete else 'DATA_PENDING','口徑':'當前主檔／原價漲幅≥3%且成交額≥2000萬／每市場最多20；非H126、非含息'})
                if group!='全研究':continue
                by=defaultdict(set)
                for r in leaders:by[r.get('產業') or '產業待核'].add(r['股票代號'])
                for industry,codes in sorted(by.items()):
                    h=len(codes&valid)
                    sectors.append({'交易日':day,'研究日':prev,'市場':market,'訊號來源':src,'當前產業':industry,'強勢榜樣本':len(codes),'已核命中':h if ready else None,'未入前日研究':len(codes)-h if complete else None,'榜單召回率':h/len(codes) if complete else None,'狀態':'DESCRIPTIVE_ONLY' if complete else 'DATA_PENDING','口徑':'當前產業分類之回顧；不代表當時產業主流或資金淨流入'})
    return out,sectors

def tier_pairs(detail,now):
    """Predeclared disjoint H194 tiers, entire T+1 cohort, no same-day pseudo-replication."""
    grouped=defaultdict(lambda:defaultdict(list));seen=set();out=[]
    for r in detail:
        if r['訊號來源']!='H194' or r['期間']!=1 or r['研究分級'] not in p.TIERS[:3]:continue
        k=(r['研究日'],r['市場'],r['股票代號'])
        if k in seen:raise ValueError('H227 duplicate tier member')
        seen.add(k);grouped[k[:2]][r['研究分級']].append(r)
    for (d,m),g in sorted(grouped.items()):
        allrows=[r for rr in g.values() for r in rr];targets={r['出場日'] for r in allrows}
        if targets=={None}:continue # immature, never enters training denominator
        if len(targets)!=1 or not next(iter(targets)):raise ValueError('H227 conflicting target dates')
        target=next(iter(targets))
        for name,left,right in PAIRS:
            a,b=g[left],g[right];rr=a+b
            def valid(r):
                return r['淨績效狀態']=='VALID_CASH_MODEL' and live.finite(r['淨報酬']) and r['淨報酬']>-1 and r.get('結果觀測時間') and p.stamp(r['決策時間'])<p.stamp(r['結果觀測時間'])<=now and p.stamp(r['結果觀測時間']).date().isoformat()>=target
            ok=len(a)>=e.MIN_GROUP and len(b)>=e.MIN_GROUP and all(valid(r) for r in rr)
            av=mean([r['淨報酬'] for r in a]) if ok else None;bv=mean([r['淨報酬'] for r in b]) if ok else None
            out.append({'比較':name,'市場':m,'期間':1,'研究日':d,'出場日':target,'決策時間':min((r['決策時間'] for r in rr),default=min(r['決策時間'] for r in allrows)),'結果觀測時間':max(r['結果觀測時間'] for r in rr) if ok else None,'基準組':left,'比較組':right,'基準封存':len(a),'比較封存':len(b),'基準有效':sum(bool(valid(r)) for r in a),'比較有效':sum(bool(valid(r)) for r in b),'基準組淨均值':av,'比較組淨均值':bv,'相對淨差':bv-av if ok else None,'狀態':'DESCRIPTIVE_ONLY' if ok else 'DATA_PENDING'})
    return out

def validation(pairs):
    """Reuse fixed H218 chronology, 20/10 non-overlap, no optimization of thresholds."""
    out=[]
    for name,base,challenger in PAIRS:
        rows=[{**r,'H209相對差':r['相對淨差']} for r in pairs if r['比較']==name]
        rr=e.review(rows)
        for m in ar.URLS:
            x=next((r for r in rr if r['市場']==m),{'市場':m,'期間':1,'非重疊窗':0,'訓練有效':0,'留出有效':0,'訓練差':None,'留出差':None,'狀態':'BASELINE_ONLY','權重倍率':1.0})
            out.append({**x,'比較':name,'基準組':base,'比較組':challenger,'狀態':{'REVIEW_H209_EDGE':'REVIEW_CHALLENGER_EDGE','REVIEW_H194_EDGE':'REVIEW_BASE_EDGE'}.get(x['狀態'],x['狀態']),'用途':'人工複核／不調權'})
    return out

def frozen_stage(root,signals,detail,now):
    """Only optional same-day frozen H198 evidence; old snapshots remain unknown."""
    dates=sorted({s['signal_day'] for s in signals if s['source']=='H194'});audit={};out=[];groups=defaultdict(list)
    for d in dates:audit[d]=q.prior_audit(root,d,now)[0]
    for r in detail:
        if r['訊號來源']!='H194' or r['期間']!=1:continue
        d=r['研究日'];a=audit[d].get((r['市場'],r['股票代號']),{});stage=a.get('stage') if a.get('stage_day')==d else None
        tag=stage or '未封存初期證據';groups[(d,r['市場'],tag)].append(r)
    for (d,m,tag),rs in sorted(groups.items()):
        due=[r for r in rs if r['出場日']];raw=[r['原始收盤績效'] for r in due if r['原始績效狀態']=='VERIFIED_RAW' and live.finite(r['原始收盤績效'])];eligible=[r for r in due if r['研究分級'] in p.TIERS[:3]];net=[r['淨報酬'] for r in eligible if r['淨績效狀態']=='VALID_CASH_MODEL' and live.finite(r['淨報酬'])]
        full=len(due)==len(rs) and len({r['出場日'] for r in due})==1
        out.append({'研究日':d,'市場':m,'當時H198階段':tag,'封存':len(rs),'到期':len(due),'原價有效':len(raw),'原價平均':mean(raw) if full and len(raw)==len(rs) else None,'淨績效應結算':len(eligible),'淨績效有效':len(net),'淨報酬平均':mean(net) if full and eligible and len(net)==len(eligible) else None,'狀態':'當時階段未知／不反推' if tag=='未封存初期證據' else '描述性／不調權'})
    return out

def run(root=ROOT,now=None,*,base=None,acceptance=None):
    root=Path(root);now=p.stamp(now or datetime.now(ar.TZ));fp=integrated.fingerprint(root)
    r=integrated.run(root,now,include_detail=True) if base is None else base
    if p.stamp(r['asof'])!=now:raise ValueError('H227 observation mismatch')
    v=live.run(root,now,base=r) if acceptance is None else acceptance
    if p.stamp(v['asof'])!=now:raise ValueError('H227 acceptance mismatch')
    m=p.Market(root,now);master=master_map(read_json(root/'stock_master_cache.json',[]));signals=r['review_signals'];detail=r['review_detail'];board=r['review_board']
    precision,sectors=leader_precision(board,signals,m,master,now);pairs=tier_pairs(detail,now);reviews=validation(pairs);stages=frozen_stage(root,signals,detail,now)
    # Retain first batch and cause evidence without imputing historical labels.
    if fp!=integrated.fingerprint(root):raise RuntimeError('H227 evidence changed; retry')
    return {'version':VERSION,'asof':r['asof'],'day':board.get('day') or '', 'status':'DESCRIPTIVE_ONLY' if any(x['狀態']=='DESCRIPTIVE_ONLY' for x in precision) else 'DATA_PENDING','batch_status':v['batch_status'],'counts':{**v['counts'],'榜單有效比較':sum(x['狀態']=='DESCRIPTIVE_ONLY' for x in precision),'策略完整日窗':sum(x['狀態']=='DESCRIPTIVE_ONLY' for x in pairs)},'batch':v['batch'],'precision':precision,'sectors':sectors,'miss':v['miss'],'pairs':pairs,'validation':reviews,'stages':stages,'tasks':v['tasks'],'errors':v['errors'],'weight_multiplier':1.0,'trade_authority_changed':False}

def excel(r):
    from openpyxl import Workbook
    w=Workbook();w.remove(w.active)
    for name,rows in [('01_主管反證',[{'資料日':r['day'],'狀態':r['status'],'首批狀態':r['batch_status'],**r['counts'],'權重倍率':1.0}]),('02_首批績效',r['batch']),('03_強勢命中精度',r['precision']),('04_產業漏選',r['sectors']),('05_逐股漏選證據',r['miss']),('06_策略同日比較',r['pairs']),('07_樣本外校正',r['validation']),('08_當時初期型態',r['stages']),('09_缺口待辦',r['tasks']),('10_異常',[{'錯誤':x} for x in r['errors']])]:
        ws=first.table(w,name,rows)
        for col in ws.iter_cols():
            if col[0].value in ('榜單命中精度','榜單召回率','基準組淨均值','比較組淨均值','相對淨差','訓練差','留出差','方向準確率'):
                for c in col[1:]:c.number_format='0.00%'
    f=io.BytesIO();w.save(f);return f.getvalue()
