# -*- coding: utf-8 -*-
"""H239 read-only repeat/rotation counterproof. No retroactive signals or weights.
Current industry master is retrospective; comparisons are descriptive, not causal.
"""
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import io, statistics
import godpick_h190_market_archive as ar
import godpick_h194_performance as p
import godpick_h206_market_gap as gap
import godpick_h209_rolling_backtest as back
import godpick_h225_integrated_quality as integrated
import godpick_h226_live_acceptance as live
import godpick_h228_market_competition as rank
import godpick_h229_net_edge as net
import godpick_h238_first_attribution as first
from godpick_h191_opportunity_engine import master_map,read_json
from godpick_h219_first_review import table
VERSION='V191-H239-ROTATION-COUNTERPROOF'
ROOT=Path(__file__).resolve().parent
LOOKBACK=5;MAX_DAYS=20;MIN_GROUP=3;MIN_SECTOR=5
REPEAT='近5日曾入前三研究';NEW='近5日未入前三研究';UNKNOWN='歷史待核'
EMERGING='新興強勢產業';PERSIST='延續強勢產業';FADE='強勢退潮產業';OTHER='其他產業';UNVERIFIED='產業證據待核'


def earlier(m,day,n):
    ds=[d for d in m.days if d<day][-n:]
    return ds if len(ds)==n and m.span(ds[0],n)==ds+[day] else []


def membership(s,index,m):
    day=s['signal_day'];mk=s['market'];code=s['code'];anchor=p.stamp(s['created_at'])
    # A known positive can be reported despite missing other days. Absence requires full window.
    prior=[d for d in m.days if d<day][-LOOKBACK:];seen=[];observed=0;watch=0
    for i,d in enumerate(prior):
        if m.span(d,len(prior)-i)!=prior[i:]+[day]:continue
        rows=index.get((d,mk),{})
        if not rows or any(p.stamp(v['created_at'])>=anchor for v in rows.values()):continue
        observed+=1
        v=rows.get(code)
        if v and v['tier'] in p.TIERS[:3]:seen.append(d)
        elif v:watch+=1
    cls=REPEAT if seen else NEW if earlier(m,day,LOOKBACK) and observed==LOOKBACK else UNKNOWN
    return cls,len(seen),observed,watch


def sector_stats(master,market,a,z):
    bm=rank.benchmark(master,market,a,z)
    if not bm['ready']:return {},False
    industry=defaultdict(set);rets=bm['returns'];median=statistics.median(rets.values())
    for c,r in master.items():
        name=r.get('產業','').strip()
        if r['市場']==market and name and name not in ('未知','未分類','其他','其他類'):industry[name].add(c)
    result={}
    for name,codes in industry.items():
        valid=codes&rets.keys();v=[rets[c] for c in valid]
        if len(codes)<MIN_SECTOR or len(valid)*100<95*len(codes):continue
        med=statistics.median(v);breadth=sum(x>0 for x in v)/len(v);rel=med-median
        result[name]={'n':len(codes),'valid':len(v),'median':med,'relative':rel,'breadth':breadth,'strong':rel>=.01 and breadth>=.6}
    return result,True


def rotation(previous,current):
    if not previous or not current:return UNVERIFIED
    return PERSIST if previous['strong'] and current['strong'] else EMERGING if current['strong'] else FADE if previous['strong'] else OTHER


def sector_windows(root,m,master,market,day,target,anchor,now,reader=None):
    reader=reader or gap.read_quotes;ds=earlier(m,day,2);errors=[];before={};current={};after={}
    def load(d,t,cut):
        a,at=reader(root,market,d,cut);z,zt=reader(root,market,t,cut)
        if p.stamp(at)>cut or p.stamp(zt)>cut:raise ValueError('quote observation later than cutoff')
        return sector_stats(master,market,a,z)[0]
    if ds and not m.errors:
        try:before=load(ds[0],ds[1],anchor);current=load(ds[1],day,anchor)
        except (OSError,ValueError,KeyError,TypeError,IndexError) as ex:errors.append(f'{market} {day} decision: {type(ex).__name__}: {str(ex)[:120]}')
    if target and not m.errors:
        try:after=load(day,target,now)
        except (OSError,ValueError,KeyError,TypeError,IndexError) as ex:errors.append(f'{market} {day} outcome: {type(ex).__name__}: {str(ex)[:120]}')
    rows=[];classes={}
    for name in sorted(set(before)|set(current)|set(after)):
        a,b,c=before.get(name),current.get(name),after.get(name);cls=rotation(a,b);classes[name]=cls
        rows.append({'研究日':day,'市場':market,'產業':name,'決策時間':anchor.isoformat(),'目標日':target,'決策前輪動':cls,'前窗相對中位數':a['relative'] if a else None,'當窗相對中位數':b['relative'] if b else None,'當窗上漲廣度':b['breadth'] if b else None,'產業母體':b['n'] if b else None,'產業有效':b['valid'] if b else None,'次日相對中位數':c['relative'] if c else None,'次日上漲廣度':c['breadth'] if c else None,'次日強勢':c['strong'] if c else None,'口徑':'當前產業主檔回顧／非歷史分類／決策前行情時點核實／非因果'})
    return rows,classes,errors


def paired(rows):
    """Compare only same date, market and original tier, complete cohorts, no imputed zeros."""
    by=defaultdict(list);out=[]
    for r in rows:by[r['研究日'],r['市場'],r['研究分級']].append(r)
    for (day,mk,tier),rr in sorted(by.items()):
        for dim,a,b in [('研究新鮮度',REPEAT,NEW),('決策前輪動',PERSIST,EMERGING)]:
            aa=[r for r in rr if r[dim]==a];bb=[r for r in rr if r[dim]==b]
            def ok(r):return r['原價時點重驗']=='VERIFIED' and r['淨績效狀態']=='VERIFIED_NET' and live.finite(r['淨報酬']) and r['淨報酬']>-1
            av=[r['淨報酬'] for r in aa if ok(r)];bv=[r['淨報酬'] for r in bb if ok(r)]
            targets={r['出場日'] for r in aa+bb};ready=len(aa)>=MIN_GROUP and len(bb)>=MIN_GROUP and len(av)==len(aa) and len(bv)==len(bb) and len(targets)==1 and None not in targets
            out.append({'研究日':day,'市場':mk,'研究分級':tier,'比較維度':dim,'基準組':a,'比較組':b,'基準樣本':len(aa),'比較樣本':len(bb),'基準有效':len(av),'比較有效':len(bv),'其他／待核':len(rr)-len(aa)-len(bb),'基準淨均值':net.mean(av) if ready else None,'比較淨均值':net.mean(bv) if ready else None,'相對淨差':net.mean(bv)-net.mean(av) if ready else None,'基準淨勝率':net.mean([v>0 for v in av]) if ready else None,'比較淨勝率':net.mean([v>0 for v in bv]) if ready else None,'狀態':'DESCRIPTIVE_ONLY' if ready else 'DATA_PENDING','用途':'同市場日及原分級／完整分母／非實際成交／非權重校正'})
    return out


def run(root=ROOT,now=None,*,base=None,first_review=None):
    root=Path(root);now=p.stamp(now or datetime.now(ar.TZ));fp=integrated.fingerprint(root)
    masterfile=root/'stock_master_cache.json';ms=masterfile.read_bytes() if masterfile.exists() else None
    r=integrated.run(root,now,include_detail=True) if base is None else base
    if p.stamp(r['asof'])!=now or first_review is not None and p.stamp(first_review['asof'])!=now:raise ValueError('H239 observation mismatch')
    ledger,outcomes=p.read_ledger(root);ss=[s for s in ledger.values() if p.stamp(s['created_at'])<=now]
    ix=defaultdict(dict)
    for s in ss:ix[s['signal_day'],s['market']][s['code']]=s
    master=master_map(read_json(masterfile,[]));m=p.Market(root,now);errors=list(m.errors);rows=[];sectors=[]
    di={}
    for d in r['review_detail']:
        if d['訊號來源']=='H194' and d['期間']==1:
            k=d['研究日'],d['市場'],d['股票代號']
            if k in di:raise ValueError('H239 duplicate reconciliation')
            di[k]=d
    days=sorted({s['signal_day'] for s in ss});scope=sorted(set(days[-MAX_DAYS:])|set(days[:1]))
    for day in scope:
        e=back.Evidence(root,m,now);span=p.confirmed_span(m,day,1,now) if not m.errors else [];target=span[-1] if span else None
        for mk in ar.URLS:
            pool=list(ix.get((day,mk),{}).values())
            if not pool:continue
            anchor=min(p.stamp(s['created_at']) for s in pool)
            sr,classes,err=sector_windows(root,m,master,mk,day,target,anchor,now);sectors.extend(sr);errors.extend(err)
            eligible=[s for s in pool if s['tier'] in p.TIERS[:3]]
            at=[net.attribution(s,di.get(p.sid(s),{}),span,e,now) for s in eligible]
            at=first.recheck_raw(eligible,at,outcomes,e,target,now)
            for s,v in zip(eligible,at):
                cls,n,observed,watch=membership(s,ix,m) if not m.errors else (UNKNOWN,0,0,0)
                info=master.get(s['code'],{});industry=info.get('產業','') if info.get('市場')==mk else ''
                rows.append({'股票代號':s['code'],'股票名稱':s['name'],**{k:x for k,x in v.items() if k not in ('股票代號','股票名稱')},'研究新鮮度':cls,'近5日前三命中日':n,'近5日封存觀測日':observed,'近5日觀察級日':watch,'產業':industry,'決策前輪動':classes.get(industry,UNVERIFIED),'決策時間':s['created_at'],'用途':'前三研究非H126買進；原價時點及含成本模型雙核實'})
    pairs=paired(rows)
    if fp!=integrated.fingerprint(root) or ms!=(masterfile.read_bytes() if masterfile.exists() else None):raise RuntimeError('H239 source changed; retry')
    return {'version':VERSION,'asof':now.isoformat(),'day':r['day'],'first_day':days[0] if days else None,'status':'DESCRIPTIVE_ONLY' if any(x['狀態']=='DESCRIPTIVE_ONLY' for x in pairs) else 'DATA_PENDING','counts':{'前瞻封存':len(ss),'檢討研究日':len(scope),'前三研究逐股':len(rows),'原價及淨績效雙核實':sum(x['原價時點重驗']=='VERIFIED' and x['淨績效狀態']=='VERIFIED_NET' for x in rows),'有效同級比較':sum(x['狀態']=='DESCRIPTIVE_ONLY' for x in pairs),'決策前產業輪動有效':sum(x['決策前輪動']!=UNVERIFIED for x in sectors),'歷史待核':sum(x['研究新鮮度']==UNKNOWN for x in rows)},'detail':rows,'pairs':pairs,'sectors':sectors,'first_miss':first_review['miss'] if first_review is not None else [],'errors':list(dict.fromkeys(errors)),'weight_multiplier':1.0,'trade_authority_changed':False}


def excel(r):
    from openpyxl import Workbook
    w=Workbook();w.remove(w.active)
    for name,rows in [('01_主管反證',[{'資料日':r['day'],'首批研究日':r['first_day'],'狀態':r['status'],**r['counts'],'權重倍率':1.0,'用途':'研究反證／不自動調權'}]),('02_同級重複輪動比較',r['pairs']),('03_逐股反證',r['detail']),('04_產業輪動',r['sectors']),('05_首批漏選',r['first_miss']),('06_異常',[{'錯誤':x} for x in r['errors']])]:
        ws=table(w,name,rows)
        for col in ws.iter_cols():
            if col[0].value in ('基準淨均值','比較淨均值','相對淨差','基準淨勝率','比較淨勝率','前窗相對中位數','當窗相對中位數','當窗上漲廣度','次日相對中位數','次日上漲廣度','原價收盤報酬','隔夜跳空','日內原價報酬','成本拖累','淨報酬'):
                for c in col[1:]:c.number_format='0.00%'
    f=io.BytesIO();w.save(f);return f.getvalue()
