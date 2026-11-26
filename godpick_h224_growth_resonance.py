# -*- coding: utf-8 -*-
"""H224 independent growth evidence; no trade authority or automatic tuning."""
from collections import Counter, defaultdict
from datetime import datetime, time
from pathlib import Path
import io, statistics
import godpick_h190_market_archive as ar
import godpick_h192_financial_growth as fin
import godpick_h194_performance as perf
import godpick_h220_growth_rotation as rotation
import godpick_h222_financial_history as hist
import godpick_h223_growth_scenarios as prev
VERSION='V191-H224-GROWTH-RESONANCE'
ROOT=Path(__file__).resolve().parent
REL='data/h224/daily'
ACCEL=5.0 # percentage points; research threshold, not trained


def acceleration(rev,now,expected):
    # All six同比 ratios use actual, positive observations, no forecasts.
    _,reason=prev.scenarios(rev,now,expected)
    if reason not in ('SCENARIO_AVAILABLE','NO_FUTURE_MONTH'):
        return {'加速度狀態':'待證','加速度原因':reason,'近3月年增中位%':None,'前3月年增中位%':None,'營收加速度pp':None,'18月實績雜湊':None}
    end=max(rev);periods=range(end-17,end+1)
    vals={i:rev[i]['revenue_twd'] for i in periods}
    yoy=[(vals[i]/vals[i-12]-1)*100 for i in range(end-5,end+1)]
    old,new=statistics.median(yoy[:3]),statistics.median(yoy[3:]);delta=new-old
    state='加速成長' if new>0 and delta>=ACCEL else '成長降速' if new>0 and delta<=-ACCEL else '正成長未加速' if new>0 else '未達正成長'
    return {'加速度狀態':state,'加速度原因':'','近3月年增中位%':round(new,4),'前3月年增中位%':round(old,4),'營收加速度pp':round(delta,4),'18月實績雜湊':perf.digest([{'period':i,'value':vals[i],'observed':rev[i]['available_at'],'source':rev[i].get('source_url')} for i in periods])}


def classify(r,a,e,chip):
    # A same-day H220 snapshot is necessary but not enough: overlapping facts
    # must still match the current financial evidence and independent chip check.
    same=bool(e and r['營收年月']==e.get('營收年月') and r['營收年增%'] is not None and r['營收年增%']==e.get('營收年增%') and r['TTM EPS重建']==e.get('TTM EPS重建') and r['連兩季三率年增'] is e.get('連兩季三率三升') and r['獲利突破'] is e.get('獲利突破'))
    both=bool(same and e.get('籌碼分組')==rotation.FULL and chip.get('籌碼分組')==rotation.FULL and all(chip.get(k) and chip[k]==e.get(k) for k in ('法人來源雜湊','TDCC來源雜湊')))
    if both and a['加速度狀態']=='加速成長':level='財技籌碼加速共振'
    elif both:level='財技籌碼共振／加速度另核'
    elif same:level='財技同日交集／籌碼未共振或待核'
    else:level='財報成長複核／技術待核'
    return {**r,**a,**chip,'H224分層':level,'H232同分母起漲複核':bool(r.get('H232同分母成長複核') and same),'H232同分母籌碼共振':bool(r.get('H232同分母成長複核') and both),'H220財報交叉一致':same,'H220原始收盤':e.get('原始收盤') if same else None,'H198階段':e.get('H198階段') if same else None,'用途':'獨立研究／非正式買進'}

ORDER=['財技籌碼加速共振','財技籌碼共振／加速度另核','財技同日交集／籌碼未共振或待核','財報成長複核／技術待核']
def rank(rows):
    rows.sort(key=lambda r:(r['市場'],ORDER.index(r['H224分層']),not r['低PE高EPS複核'],r['加速度狀態']!='加速成長',r['連兩季三率年增'] is not True,r['獲利突破'] is not True,r['股票代號']))
    counts=Counter()
    for r in rows:counts[r['市場']]+=1;r['H224研究順位']=counts[r['市場']]
    return rows


def history(root,now):
    out=[]
    for p in sorted((Path(root)/REL).glob('*.json.gz')):
        v=ar._read(p);ts=perf.stamp(v['created_at']);d=v['day']
        if v['version']!=VERSION or v.get('trade_authority_changed') is not False or p.name!=d+'.json.gz' or ts.date().isoformat()!=d or ts.time()<time(18,15):raise ValueError('H224 invalid snapshot')
        if ts>now:continue
        rr=rotation.indexed(v['rows'])
        for k,r in rr.items():
            if r['用途']!='獨立研究／非正式買進' or r['H224分層'] not in ORDER or r['資料日']!=d:raise ValueError('H224 invalid research row')
            if r['H224分層'] in ORDER[:2] and (not r['H220財報交叉一致'] or r['籌碼分組']!=rotation.FULL or not perf.num(r['H220原始收盤'])):raise ValueError('H224 invalid convergence')
        out.append(v)
    return out


def run(root=ROOT,now=None,*,capture=False):
    root=Path(root);live=now is None;now=perf.stamp(now or datetime.now(ar.TZ));sig=prev.signature(root);chip_sig=rotation.source_fingerprint(root);mf=ar._archive_manifest(root/'data/h190')[1]
    old=prev.run(root,now);market=perf.Market(root,now);day=old['day'];data,errors=fin.load_inputs(root);ledger,_,_=fin.build_ledgers(data,now);expected,_=fin.expected_periods(now)
    early={};sources,chip_errors=rotation.chip_evidence(root);sessions=[d for d in market.days if d<=day][-5:];history_errors=[]
    try:early={rotation.key(r):r for v in rotation.read_history(root,now) if v['day']==day for r in v['rows']}
    except Exception as e:history_errors.append(str(e)[:160])
    accel=[];lookup={};rows=[]
    for r in old['audit']:
        k=rotation.key(r);a=acceleration(ledger['revenue',*k],now,expected)
        if r['狀態']=='H222_ARCHIVE_INVALID':a={'加速度狀態':'待證','加速度原因':'H222_ARCHIVE_INVALID','近3月年增中位%':None,'前3月年增中位%':None,'營收加速度pp':None,'18月實績雜湊':None}
        lookup[k]=a;accel.append({**{x:r[x] for x in ('市場','股票代號','股票名稱')},**a})
    for r in old['ranked']:
        k=rotation.key(r);ch=rotation.chips(*k,sources,sessions,now) if len(sessions)==5 and not market.errors and day else {'籌碼分組':rotation.UNKNOWN}
        rows.append({**classify(r,lookup[k],early.get(k,{}),ch),'資料日':day})
    rank(rows)
    if sig!=prev.signature(root) or chip_sig!=rotation.source_fingerprint(root) or mf!=ar._archive_manifest(root/'data/h190')[1]:raise RuntimeError('H224 source changed; retry')
    archived=history(root,now);cap='READ_ONLY'
    if capture:
        finish=perf.stamp(datetime.now(ar.TZ)) if live else now;path=root/REL/(now.date().isoformat()+'.json.gz')
        if path.exists():cap='ALREADY_CAPTURED'
        elif not rows or history_errors or market.errors or day!=now.date().isoformat() or not market.today_confirmed(day) or finish.date()!=now.date() or finish<now or finish.time()<time(18,15):cap='CAPTURE_PENDING'
        else:
            body={'version':VERSION,'day':day,'created_at':finish.isoformat(),'rows':rows,'trade_authority_changed':False}
            try:hist._persist(path,body);cap='CAPTURED';archived.append(body)
            except FileExistsError:cap='ALREADY_CAPTURED'
    # Existing mature evidence is not re-labelled as H224 performance.
    return {'version':VERSION,'asof':now.isoformat(),'day':day,'status':'RESEARCH_AVAILABLE' if rows else 'EVIDENCE_PENDING','capture':cap,'counts':{'母體':len(accel),'有效加速度':sum(r['加速度狀態']!='待證' for r in accel),'加速成長':sum(r['加速度狀態']=='加速成長' for r in accel),'財報候選':len(rows),'加速共振':sum(r['H224分層']==ORDER[0] for r in rows),'封存日':len(archived),**{k:old['counts'][k] for k in ('H222有效期','H222必要期')}},'rows':rows,'acceleration':accel,'scope':old['scope'],'forecast_feedback':old['feedback'],'errors':old['errors']+errors+chip_errors+history_errors,'trade_authority_changed':False,'weight_multiplier':1.0}


def excel(r):
    from openpyxl import Workbook
    from godpick_h219_first_review import table
    w=Workbook();w.remove(w.active)
    for name,rs in [('01_主管摘要',[{'資料日':r['day'],'狀態':r['status'],'封存':r['capture'],**r['counts'],'權重倍率':1.0}]),('02_成長共振',r['rows']),('03_營收加速度',r['acceleration']),('04_回補驗收',r['scope']),('05_營收情境誤差',r['forecast_feedback']),('06_來源異常',[{'錯誤':x} for x in r['errors']])]:table(w,name,rs)
    f=io.BytesIO();w.save(f);return f.getvalue()


def append_manager(w,root,research_day,now=None):
    from godpick_h219_first_review import table
    now=perf.stamp(now or datetime.now(ar.TZ));m=perf.Market(root,now);day=max((d for d in m.days if d<now.date().isoformat() or (d==now.date().isoformat() and m.today_confirmed(d))),default='') if not m.errors else ''
    rows=next((v['rows'] for v in history(root,now) if v['day']==day==str(research_day)[:10]),[])
    counts=Counter();out=[]
    for r in rows:
        counts[r['市場']]+=1
        if counts[r['市場']]<=10:out.append(r)
    return table(w,'H224_成長共振',out or [{'狀態':'同日H224未封存／無合格研究','用途':'獨立研究／保留H126原授權'}])
