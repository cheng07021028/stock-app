# -*- coding: utf-8 -*-
"""H223 independent growth research. Scenario ranges are not confidence intervals.
No official recommendation or weight mutation; no synthetic historical decisions.
"""
from collections import Counter, defaultdict
from datetime import datetime, time
from pathlib import Path
import io, math, statistics
import godpick_h190_market_archive as ar
import godpick_h192_financial_growth as fin
import godpick_h222_financial_history as hist
import godpick_h194_performance as perf

ROOT=Path(__file__).resolve().parent
VERSION='V191-H223-GROWTH-SCENARIOS'
REL='data/h223/daily'
WINDOW=18
METHOD='SEASONAL_LAST_YEAR_X_LAST6_YOY_MIN_MEDIAN_MAX'

def identity(r):return r['市場'],r['股票代號']
def good(x):return isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x) and x>0

def scenarios(rev,now,expected):
    """18 consecutive actual reports; no inferred comparator-only observations."""
    end=max(rev,default=-1)
    if end<expected or end<0:return [],'STALE_OR_MISSING'
    if fin.period_end('revenue',end)>=now.date():return [],'UNCLOSED_ACTUAL'
    periods=range(end-WINDOW+1,end+1)
    vals={i:rev.get(i,{}).get('revenue_twd') for i in periods}
    if not all(good(x) for x in vals.values()):return [],'ACTUAL_18_MONTHS_PENDING'
    for i in periods:
        r=rev[i];t=fin.moment(r.get('available_at'))
        if not t or t>now or fin.period_end('revenue',i)>=t.date():return [],'OBSERVATION_TIME_PENDING'
        for j,k in ((i-1,'previous_month_twd'),(i-12,'prior_year_month_twd')):
            if j in vals and r.get(k) is not None and r[k]!=vals[j]:return [],'COMPARATIVE_CONFLICT'
    ratios=[vals[i]/vals[i-12] for i in range(end-5,end+1)]
    lo,mid,hi=min(ratios),statistics.median(ratios),max(ratios)
    if not all(good(x) for x in (lo,mid,hi)):return [],'RATIO_INVALID'
    high=max(vals[i] for i in range(end-11,end+1));out=[]
    evidence_digest=perf.digest([{'month':fin.monthstr(i),'actual':vals[i],'available_at':rev[i]['available_at'],'source':rev[i].get('source_url')} for i in periods])
    for i in range(end+1,end+4):
        # Do not forecast a month that already ended even if its release is late.
        if fin.period_end('revenue',i)<=now.date():continue
        b=vals[i-12];pred=[round(b*x) for x in (lo,mid,hi)]
        if not all(good(x) and x<1e15 for x in pred):return [],'RATIO_OUT_OF_RANGE'
        out.append({'目標年月':fin.monthstr(i),'最新實際年月':fin.monthstr(end),'去年同期實際元':b,'過去12月實際高點元':high,'保守推估元':pred[0],'基準推估元':pred[1],'樂觀推估元':pred[2],'基準年增%':round((mid-1)*100,4),'基準超過既有12月高點':pred[1]>high,'六期年增中位%':round((mid-1)*100,4),'方法':METHOD,'18月證據雜湊':evidence_digest,'用途':'情境推估／非公告財報、非機率區間'})
    return out,'SCENARIO_AVAILABLE' if out else 'NO_FUTURE_MONTH'

def audit(root,now,months=24,quarters=8):
    """Independently reparse stored raw HTML, not just the collector status."""
    out=[]
    required=list(hist.targets(now,months,quarters));extra=[];aliases=[]
    for k in ('revenue','ytd'):
        for m in hist.MARKETS:
            for path in sorted((Path(root)/'data/h222'/k/m).glob('*.json.gz')):
                p=(fin.month if k=='revenue' else fin.quarter)(path.name.split('.')[0])
                if p is None or path!=hist.partition(root,k,m,p):aliases.append({'來源':k,'市場':m,'期間':path.name,'狀態':'INVALID','有效筆數':None,'原因':'noncanonical partition filename','必要範圍':False})
                elif (k,m,p) not in required:extra.append((k,m,p))
    for k,m,p in required+extra:
        path=hist.partition(root,k,m,p);row={'來源':k,'市場':m,'期間':fin.monthstr(p) if k=='revenue' else fin.qstr(p),'狀態':'MISSING','有效筆數':None,'原因':'','必要範圍':(k,m,p) in required}
        if path.exists():
            try:
                v=hist.read_partition(path,k,m,p);ts=fin.moment(v['observed_at'])
                if ts>now:row['狀態']='NOT_YET_OBSERVED'
                else:
                    parsed=hist.reparse_partition(v,k,m,p)
                    if parsed!=v['rows']:raise ValueError('raw/parsed rows mismatch')
                    row.update(狀態='VERIFIED',有效筆數=len(parsed))
            except Exception as e:row.update(狀態='INVALID',原因=str(e)[:180])
        out.append(row)
    return out+aliases

def signature(root):
    paths=[];root=Path(root)
    for rel in ('data/h115','data/h192','data/h222','data/h220/daily','data/h223/daily'):
        paths.extend(x for x in (root/rel).rglob('*') if x.is_file())
    if (root/'stock_master_cache.json').is_file():paths.append(root/'stock_master_cache.json')
    return tuple((str(x),s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_ino) for x in sorted(set(paths)) for s in [x.stat()])

def candidates(profiles,forecasts,early):
    grouped=defaultdict(list)
    for r in forecasts:grouped[identity(r)].append(r)
    out=[]
    for r in profiles:
        if not ((r['連3月成長'] is True or r['近13月新高'] is True) and (r['連兩季三率年增'] is True or r['獲利突破'] is True) and good(r['TTM EPS重建']) and r['TTM EPS重建']>=5 and r['獲利率為正'] and r['EPS季度']==r['三率季度']):continue
        k=identity(r);fs=grouped[k];e=early.get(k,{})
        value='低PE高TTM EPS' in r.get('研究池','').split('、') and r.get('PE同交易日') is True
        out.append({'市場':k[0],'股票代號':k[1],'股票名稱':r['股票名稱'],'產業':r['產業'],'研究分層':'H220同日成長起漲交集' if e else '財報成長複核／技籌待核','H198階段':e.get('H198階段'),'籌碼分組':e.get('籌碼分組'),'營收年月':r['營收年月'],'營收年增%':r['營收年增%'],'連3月成長':r['連3月成長'],'近13月新高':r['近13月新高'],'連兩季三率年增':r['連兩季三率年增'],'獲利突破':r['獲利突破'],'TTM EPS重建':r['TTM EPS重建'],'EPS可比性':'股本變動未獨立核實／暫定重建','官方PE':r['官方PE'] if r['PE同交易日'] else None,'低PE高EPS複核':value,'情境期數':len(fs),'基準可能超過既有12月高點':any(x['基準超過既有12月高點'] for x in fs) if fs else None,'用途':'獨立研究／非正式買進'})
    # Actual evidence precedes scenario; no forecast score or auto-weight.
    out.sort(key=lambda r:(r['市場'],r['研究分層']!='H220同日成長起漲交集',not r['低PE高EPS複核'],r['連兩季三率年增'] is not True,r['獲利突破'] is not True,r['近13月新高'] is not True,r['股票代號']))
    counts=Counter()
    for r in out:counts[r['市場']]+=1;r['分市場研究順位']=counts[r['市場']]
    return out

def read_history(root,now):
    out=[]
    for path in sorted((Path(root)/REL).glob('*.json.gz')):
        v=ar._read(path);ts=perf.stamp(v['created_at']);day=v['day']
        if v['version']!=VERSION or v.get('trade_authority_changed') is not False or path.name!=day+'.json.gz' or ts.date().isoformat()!=day or ts.time()<time(18,15):raise ValueError('H223 invalid snapshot identity')
        if ts>now:continue
        seen=set()
        for r in v['forecasts']:
            k=(*identity(r),r['目標年月']);p=fin.month(k[-1]);lo,mid,hi=(r[x] for x in ('保守推估元','基準推估元','樂觀推估元'))
            if k in seen or k[0] not in hist.MARKETS or not isinstance(k[1],str) or len(k[1])!=4 or not k[1].isascii() or not k[1].isdigit() or k[1][0]=='0' or p is None or fin.period_end('revenue',p)<=ts.date() or r['方法']!=METHOD or not isinstance(r.get('18月證據雜湊'),str) or len(r['18月證據雜湊'])!=64 or any(c not in '0123456789abcdef' for c in r['18月證據雜湊']) or not all(good(x) for x in (lo,mid,hi,r['去年同期實際元'])) or not lo<=mid<=hi:raise ValueError('H223 invalid frozen forecast')
            seen.add(k)
        ranks=set()
        for r in v['ranked']:
            k=identity(r)
            if k in ranks or k[0] not in hist.MARKETS or not isinstance(k[1],str) or len(k[1])!=4 or not k[1].isascii() or not k[1].isdigit() or k[1][0]=='0' or r.get('用途')!='獨立研究／非正式買進':raise ValueError('H223 invalid frozen ranking')
            ranks.add(k)
        out.append(v)
    return out

def outcomes(history,ledger,now):
    """First forecast per security/target only; latest visible actual, revisions possible."""
    detail=[];seen=set()
    for v in sorted(history,key=lambda x:perf.stamp(x['created_at'])):
        ts=perf.stamp(v['created_at'])
        if ts>now:continue
        for r in v['forecasts']:
            k=(*identity(r),r['目標年月'])
            if k in seen:continue
            seen.add(k);p=fin.month(k[-1]);actual=ledger.get(('revenue',k[0],k[1]),{}).get(p,{})
            t=fin.moment(actual.get('available_at'));value=actual.get('revenue_twd');visible=bool(t and ts<t<=now and fin.period_end('revenue',p)<t.date());ok=bool(visible and good(value));zero=bool(visible and isinstance(value,(int,float)) and not isinstance(value,bool) and value==0)
            status='VERIFIED_ACTUAL' if ok else ('ACTUAL_ZERO_PERCENT_ERROR_UNDEFINED' if zero else ('NOT_MATURE' if fin.period_end('revenue',p)>=now.date() else 'AWAITING_OFFICIAL_ACTUAL'))
            b=r['去年同期實際元'];base=r['基準推估元'];detail.append({'市場':k[0],'股票代號':k[1],'目標年月':k[2],'決策時間':v['created_at'],'狀態':status,'實際營收元':value if ok or zero else None,'實際觀測時間':t.isoformat() if ok or zero else None,'基準推估元':base,'去年同期基準元':b,'情境範圍命中':r['保守推估元']<=value<=r['樂觀推估元'] if ok else None,'模型絕對誤差率%':abs(base-value)/value*100 if ok else None,'去年同期基準誤差率%':abs(b-value)/value*100 if ok else None})
    summary=[]
    for m in hist.MARKETS:
        rr=[r for r in detail if r['市場']==m];ok=[r for r in rr if r['狀態']=='VERIFIED_ACTUAL'];due=[r for r in rr if r['狀態']!='NOT_MATURE']
        complete=bool(due and len(ok)==len(due));summary.append({'市場':m,'獨立預測數':len(rr),'已到期':len(due),'有效實際':len(ok),'未到期':len(rr)-len(due),'零營收誤差不可計':sum(r['狀態']=='ACTUAL_ZERO_PERCENT_ERROR_UNDEFINED' for r in due),'待實際證據':sum(r['狀態']=='AWAITING_OFFICIAL_ACTUAL' for r in due),'平均模型誤差率%':statistics.mean(r['模型絕對誤差率%'] for r in ok) if complete else None,'平均去年同期基準誤差率%':statistics.mean(r['去年同期基準誤差率%'] for r in ok) if complete else None,'範圍命中率':sum(r['情境範圍命中'] for r in ok)/len(ok) if complete else None,'狀態':'DESCRIPTIVE_ONLY' if complete else 'DATA_PENDING'})
    return summary,detail

def run(root=ROOT,now=None,*,capture=False):
    from godpick_h220_growth_rotation import read_history as h220_history
    root=Path(root);live=now is None;now=perf.stamp(now or datetime.now(ar.TZ));sig=signature(root);mf=ar._archive_manifest(root/'data/h190')[1];m=perf.Market(root,now)
    day=max((d for d in m.days if d<now.date().isoformat() or (d==now.date().isoformat() and m.today_confirmed(d))),default='') if not m.errors else ''
    scope=audit(root,now);data,errors=fin.load_inputs(root);ledger,rejected,conflicts=fin.build_ledgers(data,now);master=fin.read_json(root/'stock_master_cache.json',[],errors);f=fin.evaluate(master=master,data=data,now=now,market_day=day,errors=errors);expected,_=fin.expected_periods(now)
    early={};history_errors=[]
    try:early={identity(r):r for v in h220_history(root,now) if v['day']==day for r in v['rows']}
    except Exception as e:history_errors.append('H220: '+str(e)[:150])
    invalid=any(x['狀態']=='INVALID' for x in scope) or any(str(x).startswith('H222 ') for x in errors);forecast=[];audit_rows=[]
    for r in f['profiles']:
        k=identity(r);rr,reason=scenarios(ledger['revenue',*k],now,expected)
        if invalid:rr=[];reason='H222_ARCHIVE_INVALID'
        forecast.extend({**{key:r[key] for key in ('市場','股票代號','股票名稱')},**x} for x in rr)
        audit_rows.append({'市場':k[0],'股票代號':k[1],'股票名稱':r['股票名稱'],'實際月數':r['營收歷史月數'],'狀態':reason,'情境期數':len(rr)})
    ranked=candidates(f['profiles'],forecast,early) if not invalid else []
    if sig!=signature(root) or mf!=ar._archive_manifest(root/'data/h190')[1]:raise RuntimeError('H223 source changed; retry')
    cap='READ_ONLY';histories=read_history(root,now)
    if capture:
        finish=perf.stamp(datetime.now(ar.TZ)) if live else now;path=root/REL/(now.date().isoformat()+'.json.gz')
        if path.exists():cap='ALREADY_CAPTURED'
        elif not forecast or invalid or history_errors or m.errors or day!=now.date().isoformat() or not m.today_confirmed(day) or finish.date()!=now.date() or finish<now or finish.time()<time(18,15):cap='CAPTURE_PENDING'
        else:
            body={'version':VERSION,'day':day,'created_at':finish.isoformat(),'forecasts':forecast,'ranked':ranked,'trade_authority_changed':False}
            try:hist._persist(path,body);cap='CAPTURED';histories.append(body)
            except FileExistsError:cap='ALREADY_CAPTURED'
    summary,detail=outcomes(histories,ledger,now)
    return {'version':VERSION,'asof':now.isoformat(),'day':day,'status':'RESEARCH_AVAILABLE' if ranked or forecast else 'EVIDENCE_PENDING','capture':cap,'counts':{'母體':len(audit_rows),'有效情境股票':sum(x['情境期數']>0 for x in audit_rows),'情境數':len(forecast),'財報複核':len(ranked),'H220交集':sum(x['研究分層']=='H220同日成長起漲交集' for x in ranked),'H222有效期':sum(x['狀態']=='VERIFIED' and x['必要範圍'] for x in scope),'H222必要期':sum(x['必要範圍'] for x in scope)},'scope':scope,'ranked':ranked,'forecast':forecast,'audit':audit_rows,'outcomes':detail,'feedback':summary,'errors':errors+history_errors+list(m.errors),'rejected':rejected,'conflicts':conflicts,'trade_authority_changed':False,'weight_multiplier':1.0}

def excel(r):
    from openpyxl import Workbook
    from godpick_h219_first_review import table
    w=Workbook();w.remove(w.active)
    for name,rows in [('01_主管摘要',[{'資料日':r['day'],'狀態':r['status'],'封存':r['capture'],**r['counts'],'權重倍率':1.0}]),('02_歷史回補重驗',r['scope']),('03_成長優勢排序',r['ranked']),('04_營收情境',r['forecast']),('05_逐股缺口',r['audit']),('06_前瞻誤差',r['feedback']),('07_預測逐筆',r['outcomes']),('08_異常',[{'錯誤':x} for x in r['errors']]+[{'衝突類型':k,'件數':v} for k,v in r['conflicts'].items()])]:table(w,name,rows)
    f=io.BytesIO();w.save(f);return f.getvalue()

def append_manager(w,root,research_day,now=None):
    from godpick_h219_first_review import table
    now=perf.stamp(now or datetime.now(ar.TZ));m=perf.Market(root,now);day=max((d for d in m.days if d<now.date().isoformat() or (d==now.date().isoformat() and m.today_confirmed(d))),default='') if not m.errors else ''
    rows=next((v['ranked'] for v in read_history(root,now) if v['day']==day==str(research_day)[:10]),[]);counts=Counter();out=[]
    for r in rows:
        counts[r['市場']]+=1
        if counts[r['市場']]<=10:out.append(r)
    return table(w,'H223_成長優勢研究',out or [{'狀態':'同日H223研究未封存／無合格財報複核','用途':'獨立研究／保留H126原授權'}])
