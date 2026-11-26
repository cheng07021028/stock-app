# -*- coding: utf-8 -*-
"""H219 prospective first-cohort review. Research only; no ranking/authority writes.
Benchmarks are raw price breadth of the prior official four-digit quote universe,
not a market index or investable portfolio. No hindsight membership reconstruction.
"""
from collections import Counter, defaultdict
from datetime import datetime, time
from pathlib import Path
import math, statistics, io, json, re
import godpick_h190_market_archive as ar
import godpick_h194_performance as p
import godpick_h209_rolling_backtest as b
import godpick_h218_evidence as e
import godpick_h217_prospective_quality as q
VERSION='V191-H219-FIRST-PROSPECTIVE-REVIEW'
ROOT=Path(__file__).resolve().parent
LIMIT=5
MIN_COVERAGE=.95
MIN_BREADTH=100

def finite(v):return isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v)
def avg(xs):return statistics.mean(xs) if xs else None

def breadth(m,market,day,target):
    """Fixed starting quote universe; missing or extreme raw transitions not hidden."""
    a=m.quotes(market,day);z=m.quotes(market,target)
    codes={c for c,v in a.items() if re.fullmatch('[1-9][0-9]{3}',c) and p.num(v)}
    values={c:z[c]/a[c]-1 for c in codes if p.num(z.get(c))}
    # Raw price breadth only: disclose extreme transitions, do not silently discard.
    n=len(codes);cov=len(values)/n if n else None
    ready=bool(not m.errors and n>=MIN_BREADTH and cov>=MIN_COVERAGE)
    med=statistics.median(values.values()) if ready else None
    neg=avg([float(v<0) for v in values.values()]) if ready else None
    return {'市場':market,'研究日':day,'出場日':target,'起始四碼有價母體':n,'雙日有價':len(values),'覆蓋率':cov,'原價異常跳動':sum(abs(v)>.15 for v in values.values()),'市場原價中位數':med,'下跌比例':neg,'市場狀態':'弱勢廣度' if ready and med<=-.01 and neg>=.6 else ('非弱勢廣度' if ready else 'DATA_PENDING'),'口徑':'起始官方四碼有價證券；非指數／非含息／非歷史普通股名冊'}

def cohorts(detail,m,signals):
    """Do not pool days, markets, horizons or raw/net metrics; missing stays denominator."""
    groups=defaultdict(list); bench={};out=[]
    lookup={(s['source'],*p.sid(s)):s for s in signals}
    for r in detail:
        s=lookup[(r['訊號來源'],r['研究日'],r['市場'],r['股票代號'])]
        # Phase evidence is only usable if frozen in source memberships, never inferred later.
        tags=s.get('strategies',[])
        phase='H198已封存初期' if any(t.startswith('H198｜') for t in tags) else '初期證據未封存'
        group=r['研究分級'] if r['訊號來源']=='H194' else '獨立召回'
        groups[(r['訊號來源'],r['研究日'],r['市場'],r['期間'],group)].append(r)
        groups[(r['訊號來源'],r['研究日'],r['市場'],r['期間'],'全批次')].append(r)
        if r['訊號來源']=='H209':groups[(r['訊號來源'],r['研究日'],r['市場'],r['期間'],phase)].append(r)
        k=(r['市場'],r['研究日'],r['出場日'])
        if r['出場日'] and k not in bench:bench[k]=breadth(m,*k)
    for (src,day,market,n,group),rr in sorted(groups.items()):
        due=[r for r in rr if r['出場日']];raw=[r for r in due if r['原始績效狀態']=='VERIFIED_RAW' and finite(r['原始收盤績效'])]
        eligible=[r for r in due if src!='H194' or r['研究分級'] in p.TIERS[:3]]
        net=[r for r in eligible if r['淨績效狀態']=='VALID_CASH_MODEL' and finite(r['淨報酬'])]
        rawfull=bool(src=='H194' and due and len(raw)==len(due) and not m.errors);netfull=bool(eligible and len(net)==len(eligible) and not m.errors)
        target=due[0]['出場日'] if due else None;bm=bench.get((market,day,target),{});med=bm.get('市場原價中位數');weak=bm.get('市場狀態')=='弱勢廣度'
        rv=[r['原始收盤績效'] for r in raw] if rawfull else [];nv=[r['淨報酬'] for r in net] if netfull else []
        out.append({'訊號來源':src,'研究日':day,'市場':market,'期間':n,'研究群組':group,'封存':len(rr),'到期':len(due),'原價有效':len(raw),'淨績效應結算':len(eligible),'淨績效有效':len(net),'原價平均':avg(rv),'原價上漲比例':avg([float(x>0) for x in rv]),'淨報酬平均':avg(nv),'淨勝率':avg([float(x>0) for x in nv]),'市場原價中位數':med,'原價相對差':avg(rv)-med if rv and med is not None else None,'弱市不跌比例':avg([float(x>=0) for x in rv]) if rv and weak else None,'弱市跑贏比例':avg([float(x>med) for x in rv]) if rv and weak else None,'狀態':'未到期／日曆待核' if not due else ('完整描述性結果' if rawfull or netfull else '到期待補證'),'權重倍率':1.0})
    return out,list(bench.values())

def manager_rows(body,day):
    """Compact existing evidence shortlist. No new score, no mixing ranks across tiers."""
    if not body or body.get('expected_session')!=day or body.get('market_day')!=day or body.get('technical_current') is not True or body.get('same_session') is not True or body.get('trade_authority_changed') is not False:return [],'同日技術證據未核實'
    rows=body.get('rows',[]);seen=set();groups=defaultdict(list)
    for r in rows:
        k=r.get('市場'),r.get('股票代號')
        if k in seen or k[0] not in ar.URLS or not isinstance(k[1],str) or not re.fullmatch('[1-9][0-9]{3}',k[1]):raise ValueError('H219 manager identity conflict')
        seen.add(k)
        if r.get('研究分級') not in p.TIERS[:2] or r.get('技術日')!=day or r.get('H198資料日期')!=day or not r.get('技術研究池'):continue
        if r.get('H211狀態') not in ('有新增證據','近窗新出現','歷史待補證','歷史證據待補','重複無新增證據'):continue
        if r.get('H211反證')!='未觸發本規則（非無風險）' or r.get('H198階段')=='突破反證與追高警示':continue
        from godpick_h211_counterevidence import tech_ok
        if not tech_ok(r,day):continue
        rank=r.get('H211研究序')
        if not finite(rank) or rank<1:continue
        groups[(k[0],r['研究分級'])].append(r)
    out=[]
    for (market,tier),rr in sorted(groups.items()):
        for r in sorted(rr,key=lambda x:(x['H211研究序'],x['股票代號']))[:LIMIT]:
            out.append({k:r.get(k) for k in ('市場','股票代號','股票名稱','研究分級','技術研究池','H198階段','H211研究序','H211狀態','H211新證據','H211複核條件','官方原始收盤價','收盤日') }|{'用途':'研究複核／未授權','資料日':day})
    return out,'僅同日技術／雙證據複核；每市場每級最多5檔；原完整清單不變' if out else '沒有通過精簡研究證據的股票；不等同無市場機會'

def recall_gaps(h):
    out=[]
    for market in ar.URLS:
        c=h.get('coverage',{}).get(market,{});n=c.get('主檔');v=c.get('有效60日');need=math.ceil(n*MIN_COVERAGE) if isinstance(n,int) and n>0 else None
        out.append({'市場':market,'主檔':n,'有效60日':v,'覆蓋率':v/n if need is not None and isinstance(v,int) else None,'95%所需':need,'距門檻有效股數':max(0,need-v) if need is not None and isinstance(v,int) else None,'召回候選':c.get('召回'),'缺口原因':h.get('reasons',{}).get(market,{}),'狀態':c.get('狀態','待核'),'全市場狀態':h.get('status'),'權限':'研究診斷／不降低門檻'})
    return out

def run(root=ROOT,now=None):
    root=Path(root);now=p.stamp(now or datetime.now(ar.TZ));fp=e.fingerprint(root);mf=ar._archive_manifest(root/'data/h190')[1];m=p.Market(root,now);ss,oo=p.read_ledger(root)
    signals=[{**s,'source':'H194'} for s in ss.values() if p.stamp(s['created_at'])<=now]+[s for s in b.read_signals(root,now,ledger=(ss,oo)) if s['source']=='H209']
    detail,_=e.reconcile(signals,oo,m,b.Evidence(root,m,now),now);co,bm=cohorts(detail,m,signals)
    first=min((s['signal_day'] for s in signals if s['source']=='H194'),default=None)
    first_rows=[r for r in co if r['訊號來源']=='H194' and r['研究日']==first];first_detail=[r for r in detail if r['訊號來源']=='H194' and r['研究日']==first]
    from godpick_h206_market_gap import build as gap
    from godpick_h208_recall_review import build as recall
    from godpick_h195_command_center import load
    current=gap(root,now);h=recall(root,current,now);a,state=q.prior_audit(root,current['previous_day'],now) if current.get('previous_day') else ({},'NO_SESSION');rec,miss=q.compare_leaders(current,q.source_index(signals),a,state,now)
    body,s,_=load(root,now);short,reason=manager_rows(body,current.get('day')) if s=='CURRENT' else ([], 'H195 '+s)
    if fp!=e.fingerprint(root) or mf!=ar._archive_manifest(root/'data/h190')[1]:raise RuntimeError('H219 evidence changed; retry')
    counts={'首批研究日':first,'首批訊號':sum(x['訊號來源']=='H194' and x['研究日']==first and x['期間']==1 for x in first_detail),'首批T+1到期':sum(x['期間']==1 and bool(x['出場日']) for x in first_detail),'首批T+1原價有效':sum(x['期間']==1 and x['原始績效狀態']=='VERIFIED_RAW' for x in first_detail),'首批T+1淨績效有效':sum(x['期間']==1 and x['淨績效狀態']=='VALID_CASH_MODEL' and finite(x['淨報酬']) for x in first_detail),'H209訊號':sum(s['source']=='H209' for s in signals),'精簡研究數':len(short)}
    return {'version':VERSION,'asof':now.isoformat(),'day':current.get('day'),'counts':counts,'first':first_rows,'first_detail':first_detail,'cohorts':co,'breadth':bm,'recall':rec,'miss':miss,'recall_gaps':recall_gaps(h),'shortlist':short,'shortlist_note':reason,'errors':list(m.errors)+list(current.get('errors',[]))+list(h.get('errors',[])),'status':'DESCRIPTIVE_ONLY' if counts['首批T+1原價有效'] and not m.errors else 'DATA_PENDING','weight_multiplier':1.0,'trade_authority_changed':False}

def table(w,name,rows):
    from openpyxl.styles import Font,PatternFill
    from openpyxl.utils import get_column_letter
    from godpick_h195_command_center import safe
    s=w.create_sheet(name);cols=list(dict.fromkeys(k for r in rows for k in r)) or ['狀態'];s.append(cols)
    for r in rows or [{'狀態':'待證據／非零勝率'}]:s.append([safe(r.get(k)) for k in cols])
    for i,k in enumerate(cols,1):
        s.cell(1,i).font=Font(bold=True,color='FFFFFF');s.cell(1,i).fill=PatternFill('solid',fgColor='185C70');s.column_dimensions[get_column_letter(i)].width=35 if any(t in k for t in ('原因','條件','證據','口徑','用途')) else 19
        if k in ('覆蓋率','市場原價中位數','下跌比例','原價平均','原價上漲比例','淨報酬平均','淨勝率','原價相對差','弱市不跌比例','弱市跑贏比例','研究召回率','雙日覆蓋率','原始收盤績效','淨報酬'):
            for cell in list(s.columns)[i-1][1:]:cell.number_format='0.00%'
    s.freeze_panes='C2';s.auto_filter.ref=s.dimensions;return s

def excel(r):
    from openpyxl import Workbook
    w=Workbook();w.remove(w.active)
    for name,rows in [('01_主管摘要',[{'資料日':r['day'],'狀態':r['status'],**r['counts'],'精簡說明':r['shortlist_note'],'權重倍率':1.0}]),('02_首批績效',r['first']),('03_強勢命中',r['recall']),('04_漏選證據',r['miss']),('05_抗跌廣度',r['breadth']),('06_分組績效',r['cohorts']),('07_召回缺口',r['recall_gaps']),('08_精簡研究',r['shortlist']),('09_首批逐筆',r['first_detail'])]:table(w,name,rows)
    f=io.BytesIO();w.save(f);return f.getvalue()

def append_manager(wb,root,research_day,now=None):
    from godpick_h195_command_center import load
    from datetime import date
    now=p.stamp(now or datetime.now(ar.TZ));body,state,_=load(root,now)
    try:day=date.fromisoformat(str(research_day)[:10]).isoformat()
    except (ValueError,TypeError):day=''
    rows,note=manager_rows(body,day) if state=='CURRENT' and day else ([], 'H195或本輪日期待核')
    return table(wb,'H219_精簡研究',rows or [{'狀態':note,'用途':'非正式買進；請查閱原H126及H195'}])
