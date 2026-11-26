# -*- coding: utf-8 -*-
"""Fixed first-cohort T+1 review; never a backdated selection or formal buy list."""
from collections import Counter
from datetime import datetime
from pathlib import Path
import io, math, statistics
import godpick_h190_market_archive as ar
import godpick_h194_performance as p
import godpick_h206_market_gap as gap
import godpick_h209_rolling_backtest as back
import godpick_h217_prospective_quality as prior
import godpick_h225_integrated_quality as integrated
import godpick_h226_live_acceptance as live
import godpick_h227_strategy_validation as strategy
import godpick_h228_market_competition as rank
import godpick_h229_net_edge as net
from godpick_h191_opportunity_engine import master_map,read_json
from godpick_h219_first_review import table
VERSION='V191-H238-FIRST-T1-ATTRIBUTION'
ROOT=Path(__file__).resolve().parent


def first_board(root,m,master,day,now,reader=None):
    """Reconstruct exact first T+1, not latest H206. Current master is retrospective."""
    b={'day':'','previous_day':day or '', 'rows':[],'coverage':{},'errors':[]}
    ds=p.confirmed_span(m,day,1,now) if day and not m.errors else []
    if not ds:return b
    b['day']=ds[-1];reader=reader or gap.read_quotes
    for market in ar.URLS:
        try:
            a,_=reader(root,market,day,now);z,_=reader(root,market,ds[-1],now)
            rr,cv=gap.leaders(master,z,a,market,ds[-1],day)
            bm=rank.benchmark(master,market,a,z)
            cv['排名母體有效']=bm['ready'];b['rows'].extend(rr);b['coverage'][market]=cv
        except (OSError,ValueError,KeyError,TypeError,IndexError) as ex:b['errors'].append(market+': '+type(ex).__name__)
    return b


def diagnose(board,signals,m,master,audit,audit_state,now):
    """Do not call a missing membership a cause; observation-only is not a miss."""
    if not board['day']:return [],[],[]
    precision,_=strategy.leader_precision(board,signals,m,master,now)
    for x in precision:
        if board['coverage'].get(x['市場'],{}).get('排名母體有效') is not True:
            x.update(狀態='DATA_PENDING',榜單命中精度=None,榜單召回率=None,已核命中=None)
    ix=prior.source_index(signals);rows=[];summ=[]
    for market in ar.URLS:
        pool=ix.get(('H194',board['previous_day'],market),{})
        full=next((r for r in precision if r['市場']==market and r['訊號來源']=='H194' and r['研究群組']=='全研究'),{})
        ready=full.get('狀態') in ('DESCRIPTIVE_ONLY','NO_LEADERS') and board['coverage'].get(market,{}).get('排名母體有效') is True
        for r in board['rows']:
            if r['市場']!=market:continue
            c=r['股票代號'];s=pool.get(c);a=audit.get((market,c),{});tier=s.get('tier','') if s else ''
            reason='';action='';state='待核'
            if not ready:reason='完整前日封存／市場覆蓋待核';action='先補行情或封存，不調權'
            elif s:
                state='前三研究命中' if tier in p.TIERS[:3] else '觀察級命中'
                reason='原始封存分級；非正式買進';action='既有績效複核' if tier in p.TIERS[:3] else '核對觀察級缺證，不直接升格'
            else:
                state='未入研究封存'
                if not a:reason='無當時逐股證據';action='檢查原始決策稽核範圍，不事後推定'
                elif a.get('tier') not in ('','未入池'):reason='當時入池但無封存';action='H194封存完整性核對'
                elif a.get('liquid') is False:reason='當時流動性未通過';action='保留原門檻，人工複核'
                elif a.get('tech_gap'):reason='當時技術缺口';action='H212行情補證'
                elif a.get('financial_gap'):reason='當時財報缺口';action='H222/H233財報補證'
                elif a.get('stage_counterevidence'):reason='當時有型態反證文字';action='H198原始反證複核'
                else:reason='當時未入池，具體規則待核';action='人工檢討，不推定原因'
            rows.append({'市場':market,'股票名稱':r['股票名稱'],'股票代號':c,'研究日':board['previous_day'],'目標日':board['day'],'市場強勢順位':r['市場強勢順位'],'原始收盤漲幅%':r['原始收盤漲幅%'],'前日封存分級':tier,'對帳':state,'原因線索':reason,'當時技術缺口':a.get('tech_gap'),'當時財報缺口':a.get('financial_gap'),'當時型態反證':a.get('stage_counterevidence'),'原因來源':audit_state if a else '無逐股證據','處理':action,'口徑':'當前主檔回顧／原因線索非因果／非H126漏選'})
        rr=[r for r in rows if r['市場']==market];ct=Counter(r['對帳'] for r in rr)
        summ.append({'市場':market,'研究日':board['previous_day'],'目標日':board['day'],'強勢榜樣本':len(rr) if ready else None,'前三研究命中':ct['前三研究命中'] if ready else None,'觀察級命中':ct['觀察級命中'] if ready else None,'未入研究封存':ct['未入研究封存'] if ready else None,'待核':ct['待核'],'狀態':'DESCRIPTIVE_ONLY' if ready else 'DATA_PENDING'})
    return precision,rows,summ


def recheck_raw(signals,detail,ledger,e,target,now):
    """Verify raw label using quotes visible at original decision and settlement."""
    idx={p.sid(s):s for s in signals};out=[]
    for r in detail:
        k=(r['研究日'],r['市場'],r['股票代號']);s=idx.get(k);o=ledger.get((*k,1));row=dict(r)
        if s is None:raise ValueError('H238 unexpected attribution identity')
        ok=False
        if row.get('原價狀態')=='VERIFIED_RAW':
            if target and o and o['target_day']==target:
                start=p.stamp(s['created_at']);end=p.stamp(o['resolved_at']);a,at=e.quote(s['market'],s['signal_day'],s['code']);z,zt=e.quote(s['market'],target,s['code'])
                ok=bool(a and z and at and zt and p.stamp(at)<=start<end<=now and p.stamp(zt)<=end and end.date().isoformat()>=target and (end.date().isoformat()!=target or (end.hour,end.minute)>=(18,15)) and strategy.price_equal(a[4],s['base_close']) and strategy.price_equal(z[4],o['target_close']) and live.finite(row.get('原價收盤報酬')) and math.isclose(row['原價收盤報酬'],o['raw_return'],rel_tol=1e-9,abs_tol=1e-10))
            if not ok:row.update(原價狀態='原始決策／結算時點待核',原價收盤報酬=None)
        row['原價時點重驗']='VERIFIED' if ok else 'PENDING';out.append(row)
    return out


def attribution_summary(signals,detail,target):
    """Full frozen denominator, separate raw and simulated net; missing never zero."""
    idx={p.sid(s):s for s in signals};di={}
    if len(idx)!=len(signals):raise ValueError('duplicate first signal')
    for r in detail:
        k=(r['研究日'],r['市場'],r['股票代號'])
        if k not in idx or k in di:raise ValueError('first attribution identity mismatch')
        di[k]=r
    out=[]
    for market in ar.URLS:
        for tier in ('全研究',*p.TIERS):
            ss=[s for s in signals if s['market']==market and (tier=='全研究' or s['tier']==tier)]
            if not ss:continue
            rr=[di.get(p.sid(s),{}) for s in ss];eligible=[di.get(p.sid(s),{}) for s in ss if s['tier'] in p.TIERS[:3]]
            def raw_ok(r):return bool(target and r.get('出場日')==target and r.get('原價狀態')=='VERIFIED_RAW' and live.finite(r.get('原價收盤報酬')))
            def net_ok(r):return bool(target and r.get('出場日')==target and r.get('淨績效狀態')=='VERIFIED_NET' and live.finite(r.get('淨報酬')) and r['淨報酬']>-1)
            raw=[r for r in rr if raw_ok(r)];nv=[r for r in eligible if net_ok(r)];full=len(raw)==len(ss);nf=bool(eligible and len(nv)==len(eligible));v=[r['淨報酬'] for r in nv] if nf else []
            out.append({'市場':market,'研究分級':tier,'目標日':target,'封存':len(ss),'原價有效':len(raw),'原價平均':statistics.mean(r['原價收盤報酬'] for r in raw) if full else None,'原價上漲比例':statistics.mean(r['原價收盤報酬']>0 for r in raw) if full else None,'淨績效應結算':len(eligible),'淨績效有效':len(nv),**net.metrics(v),'成本轉非正':sum(r.get('歸因')=='成本使日內正報酬轉非正' for r in nv) if nf else None,'跳空未延續':sum(r.get('歸因')=='跳空上漲但日內未延續' for r in nv) if nf else None,'狀態':'DESCRIPTIVE_ONLY' if full and (nf or not eligible) else 'DATA_PENDING'})
    return out


def growth_subset(rows,day):
    """Existing H228 priority order, no new scoring or retroactive weight."""
    result=[];seen=set()
    for r in rows:
        k=r['市場'],r['股票代號']
        if k in seen:raise ValueError('duplicate H228 identity')
        seen.add(k)
        if r.get('資料日')!=day or r.get('研究分層') not in ('財技籌碼共振','高成長起漲研究') or r.get('反證／衝突') or not p.num(r.get('官方收盤')):continue
        result.append({**r,'用途':'既有H228高成長子集／非正式買進，非績效調權'})
    return result


def run(root=ROOT,now=None,*,base=None,acceptance=None,review=None,ranking=None,edge=None):
    root=Path(root);now=p.stamp(now or datetime.now(ar.TZ));fp=integrated.fingerprint(root)
    r=integrated.run(root,now,include_detail=True) if base is None else base
    v=live.run(root,now,base=r) if acceptance is None else acceptance
    z=strategy.run(root,now,base=r,acceptance=v) if review is None else review
    x=rank.run(root,now,base=r,acceptance=v,review=z) if ranking is None else ranking
    y=net.run(root,now,base=r,acceptance=v,review=z,ranking=x) if edge is None else edge
    if any(p.stamp(b['asof'])!=now for b in (r,v,z,x,y)):raise ValueError('H238 observation mismatch')
    ss=r['first_signals'];d=min((s['signal_day'] for s in ss),default=None);m=p.Market(root,now);master=master_map(read_json(root/'stock_master_cache.json',[]));board=first_board(root,m,master,d,now)
    a,state=prior.prior_audit(root,d,now) if d else ({},'NO_PARTITION')
    precision,miss,summ=diagnose(board,r['review_signals'],m,master,a,state,now)
    _,ledger=p.read_ledger(root);detail=recheck_raw(ss,y['detail'],ledger,back.Evidence(root,m,now),board['day'],now)
    attr=attribution_summary(ss,detail,board['day']);short=growth_subset(x['shortlist'],r['day'])
    if fp!=integrated.fingerprint(root):raise RuntimeError('H238 source changed; retry')
    return {'version':VERSION,'asof':now.isoformat(),'day':r['day'],'first_day':d,'target':board['day'] or None,'status':'DESCRIPTIVE_ONLY' if attr and all(a['狀態']=='DESCRIPTIVE_ONLY' for a in attr) else 'DATA_PENDING','counts':{**v['counts'],'H238首批原價時點有效':sum(a.get('原價時點重驗')=='VERIFIED' for a in detail),'首批漏選有效市場':sum(a['狀態']=='DESCRIPTIVE_ONLY' for a in summ),'高成長優先子集':len(short)},'attribution':attr,'detail':detail,'miss_summary':summ,'miss':miss,'precision':precision,'market_net':[a for a in y['net'] if a['研究日']==d],'growth':short,'tasks':v['tasks'],'errors':list(dict.fromkeys([*r['errors'],*board['errors']])),'weight_multiplier':1.0,'trade_authority_changed':False}


def excel(r):
    from openpyxl import Workbook
    w=Workbook();w.remove(w.active)
    for name,rows in [('01_主管首批驗收',[{'狀態':r['status'],'首批研究日':r['first_day'],'首批目標日':r['target'],'現有資料日':r['day'],**r['counts'],'權重倍率':1.0}]),('02_首批分級歸因',r['attribution']),('03_逐股績效',r['detail']),('04_首批強勢對帳',r['miss_summary']),('05_逐股漏選修復',r['miss']),('06_首批命中精度',r['precision']),('07_同成本競爭',r['market_net']),('08_高成長優先複核',r['growth']),('09_原有資料待辦',r['tasks']),('10_異常',[{'錯誤':x} for x in r['errors']])]:
        # Stock names remain in B when present, with identity columns separate.
        if rows and '股票名稱' in rows[0]:rows=[{'市場':a.get('市場'),'股票名稱':a.get('股票名稱'),**a} for a in rows]
        ws=table(w,name,rows)
        for col in ws.iter_cols():
            if col[0].value in ('平均淨報酬','平均獲利','平均虧損','原價平均','原價上漲比例','淨勝率','榜單命中精度','榜單召回率','相對淨差','原價收盤報酬','日內原價報酬','隔夜跳空','成本拖累'):
                for c in col[1:]:c.number_format='0.00%'
    f=io.BytesIO();w.save(f);return f.getvalue()
