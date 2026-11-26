# -*- coding: utf-8 -*-
"""H226: one-pass H225 evidence, first-batch settlement and actionable gap queue.
Read-only; no formal authority, no weight writes, no historical decision reconstruction.
"""
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
import io, math, statistics
import godpick_h190_market_archive as ar
import godpick_h194_performance as p
import godpick_h225_integrated_quality as integrated
import godpick_h219_first_review as first
VERSION='V191-H226-LIVE-ACCEPTANCE'
ROOT=Path(__file__).resolve().parent

def finite(v):return isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v)

def first_batch(rows,signals,errors=()):
    """Same first frozen day, each identity once. No partial-cohort win rate.
    Class prediction accuracy is distinct from raw positive return and net win rate.
    """
    idx={}
    for s in signals:
        k=p.sid(s)
        if k in idx:raise ValueError('duplicate first signal')
        idx[k]=s
    groups=defaultdict(list);seen=set()
    for r in rows:
        if r['訊號來源']!='H194':continue
        k=(r['研究日'],r['市場'],r['股票代號']);n=r['期間']
        if k not in idx or (*k,n) in seen or n not in p.HORIZONS:raise ValueError('first batch identity mismatch')
        seen.add((*k,n));groups[(r['研究日'],r['市場'],n)].append((r,idx[k]))
    out=[]
    for (day,market,n),items in sorted(groups.items()):
        expected=sum(s['signal_day']==day and s['market']==market for s in signals)
        due=[(r,s) for r,s in items if r['出場日']];raw=[(r,s) for r,s in due if r['原始績效狀態']=='VERIFIED_RAW' and finite(r['原始收盤績效'])];eligible=[(r,s) for r,s in due if s['tier'] in p.TIERS[:3]];net=[(r,s) for r,s in eligible if r['淨績效狀態']=='VALID_CASH_MODEL' and finite(r['淨報酬'])];pred=[(r,s) for r,s in items if s.get('forecast') is not None] if n==1 else []
        validpred=[(r,s) for r,s in raw if s.get('forecast') is not None] if n==1 else []
        full=bool(expected and len(items)==len(due)==expected and len({r['出場日'] for r,s in due})==1 and not errors)
        rawfull=full and len(raw)==expected;netfull=full and bool(eligible) and len(net)==len(eligible);predfull=full and bool(pred) and len(pred)==len(validpred)
        rv=[r['原始收盤績效'] for r,s in raw] if rawfull else [];nv=[r['淨報酬'] for r,s in net] if netfull else []
        # Deterministic max class only if a unique maximum; ties remain unscored.
        unique=[(r,s) for r,s in validpred if s['forecast'].count(max(s['forecast']))==1]
        out.append({'研究日':day,'市場':market,'期間':n,'封存':expected,'到期':len(due),'原價有效':len(raw),'淨績效應結算':len(eligible),'淨績效有效':len(net),'預測機率封存':len(pred) if n==1 else None,'預測有效':len(validpred) if n==1 else None,'原價平均':statistics.mean(rv) if rv else None,'原價上漲比例':statistics.mean(x>0 for x in rv) if rv else None,'淨報酬平均':statistics.mean(nv) if nv else None,'淨勝率':statistics.mean(x>0 for x in nv) if nv else None,'方向準確率':statistics.mean(p.CLASSES[s['forecast'].index(max(s['forecast']))]==p.label(r['原始收盤績效']) for r,s in unique) if predfull and len(unique)==len(pred) else None,'Brier':statistics.mean(p.brier(s['forecast'],p.label(r['原始收盤績效'])) for r,s in validpred) if predfull else None,'狀態':'完整描述性結果' if rawfull and (netfull or not eligible) else '到期待補證' if due else '未到期／日曆待核','校正':'僅描述性／單批不可調權'})
    # A missing whole horizon is itself a reconciliation failure.
    if len(seen)!=len(idx)*len(p.HORIZONS):raise ValueError('first batch horizon count mismatch')
    return out

def queue(r):
    """Concrete evidence tasks, not invented repair successes. Original archives untouched."""
    out=[]
    def add(priority,domain,market,period,state,reason,workflow):
        out.append({'優先':priority,'類型':domain,'市場':market,'日期／期別':period,'狀態':state,'原因':reason,'既有工作流／處理':workflow})
    for x in r['scope']:
        if x['必要範圍'] and x['狀態']!='VERIFIED':add(1,'歷史財報',x['市場'],x['期間'],x['狀態'],x['原因'] or x['來源'],'H222 official historical financial backfill' if x['狀態']=='MISSING' else '原始證據人工複核／禁止覆寫')
    for x in r['flows']:
        if x['狀態']!='已核實':add(1,'法人',x['市場'],x['交易日'],'PENDING','逐日封存缺證','H221 official institutional evidence closure' if x['市場']=='TWSE' else 'H221 最新日累積／歷史來源待驗證')
    for x in r['action_rows']:
        if x['狀態']!='VERIFIED':add(1,'權息',x['市場'],x['交易日'],x['狀態'],x['原因'],'H209 prospective rolling backtest' if x['狀態']=='MISSING' else '原始證據人工複核／禁止覆寫')
    rec=r['recovery']
    for x in rec['market_missing']:add(1,'整日行情',x['市場'],x['交易日'],'MISSING','官方整日檔缺口','H190 official full-market historical archive')
    for x in rec['day_queue']:add(2,'個股缺日',x['市場'],x['交易日'],'PENDING',f"{x['缺檔數']}檔／未區分停牌與來源漏列",'H190 / H212 官方補抓及重驗')
    for x in r['sources']:
        if x['狀態']!='VERIFIED_SOURCE':add(3,'同日研究','雙市場',r['day'],x['狀態'],x['來源'],'依既有 '+x['來源']+' 工作流；禁止跨日回填')
    for x in r['gates']:
        if x['狀態']!='PASS':add(4,'品質閘門',x['市場'],r['day'],x['狀態'],x['項目'],x['下一步'])
    return sorted(out,key=lambda x:(x['優先'],x['類型'],x['市場'],x['日期／期別'] or ''))

def run(root=ROOT,now=None,*,base=None):
    now=p.stamp(now or datetime.now(ar.TZ));r=integrated.run(root,now,include_detail=True) if base is None else base
    if p.stamp(r['asof'])!=now:raise ValueError('H226 requires same observation time')
    batch=first_batch(r['first_detail'],r['first_signals'],r['first_errors']);tasks=queue(r)
    passed=sum(x['狀態']=='PASS' for x in r['gates']);t1=[x for x in batch if x['期間']==1];n=sum(x['封存'] for x in t1);due=sum(x['到期'] for x in t1);raw=sum(x['原價有效'] for x in t1);pred=sum(x['預測機率封存'] for x in t1)
    state='NO_FROZEN_BATCH' if not n else 'WAIT_SESSION_OR_CALENDAR' if not due else 'FIRST_BATCH_DESCRIPTIVE' if all(x['狀態']=='完整描述性結果' for x in t1) else 'MATURE_EVIDENCE_PENDING'
    return {'version':VERSION,'asof':r['asof'],'day':r['day'],'status':'DATA_GATES_PASS' if r['status']=='DATA_GATES_PASS' else 'EVIDENCE_PENDING','batch_status':state,'counts':{'首批研究':n,'T+1到期':due,'T+1原價有效':raw,'T+1機率封存':pred,'品質通過':passed,'品質應有':len(r['gates']),'待辦數':len(tasks)},'batch':batch,'gates':r['gates'],'tasks':tasks,'stock_gaps':r['recovery']['stock_gaps'],'gap_summary':r['recovery']['gap_summary'],'recall':r['recall'],'miss':r['miss'],'shortlist':r['shortlist'],'errors':r['errors'],'weight_multiplier':1.0,'trade_authority_changed':False}

def excel(r):
    from openpyxl import Workbook
    w=Workbook();w.remove(w.active)
    for name,rows in [('01_主管實戰驗收',[{'資料日':r['day'],'資料狀態':r['status'],'首批狀態':r['batch_status'],**r['counts'],'權重倍率':1.0}]),('02_首批績效校正',r['batch']),('03_品質閘門',r['gates']),('04_具體缺口待辦',r['tasks']),('05_個股缺日',r['stock_gaps']),('06_缺口分類',r['gap_summary']),('07_強勢召回',r['recall']),('08_漏選原因',r['miss']),('09_整合研究',r['shortlist']),('10_異常',[{'錯誤':x} for x in r['errors']])]:
        ws=first.table(w,name,rows)
        for col in ws.iter_cols():
            if col[0].value=='方向準確率':
                for c in col[1:]:c.number_format='0.00%'
    f=io.BytesIO();w.save(f);return f.getvalue()
