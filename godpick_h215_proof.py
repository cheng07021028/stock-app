# -*- coding: utf-8 -*-
"""H215 proof-only release gate. Official status events are NOT a substitute for bars.
No changes to H126, research membership, or original quote/return archives.
"""
from datetime import datetime, timedelta, time
from pathlib import Path
from collections import Counter
import io, re
import godpick_h190_market_archive as ar
import godpick_h194_performance as perf
VERSION='V191-H215-PROOF'
ROOT=Path(__file__).resolve().parent
SOURCES={'TWSE':'https://openapi.twse.com.tw/v1/exchangeReport/TWTAWU','TPEX':'https://www.tpex.org.tw/openapi/v1/tpex_spendi_history'}
REL=Path('data/h215/status')

def parse_status(raw,market,now):
    """Positive dated announcements only; empty and absent rows prove nothing.
    Separate halt/resumption dates; no inference of full-day non-tradability.
    """
    now=perf.stamp(now)
    if market not in SOURCES or not isinstance(raw,list) or len(raw)>20000:raise ValueError('unexpected official status response')
    rows=[];seen=set()
    for row in raw:
        if not isinstance(row,dict):raise ValueError('status row not object')
        def field(names,required=True):
            vals={str(row[k]).strip() for k in names if k in row and row[k] is not None and str(row[k]).strip()}
            if len(vals)>1 or (required and not vals):raise ValueError('missing/conflicting status field: '+names[0])
            return next(iter(vals),'')
        code=field(('Code','SecuritiesCompanyCode','SecuritiesCode','StockCode','股票代號','證券代號'))
        # Reject unknown schema even when non-equity; do not infer from position.
        start=ar._day(field(('StartDate','SuspendDate','SuspensionDate','TradingHaltDate','暫停交易日期','停止交易日期','暫停買賣日期')))
        end_raw=field(('EndDate','ResumeDate','ResumptionDate','TradingResumeDate','恢復交易日期','恢復買賣日期'),False)
        end=ar._day(end_raw) if end_raw not in ('','--','-','尚未恢復') else ''
        if end and end<start:raise ValueError('reversed status interval')
        if start>now.date().isoformat():continue # announced future, not an effective halt
        if not re.fullmatch(r'[1-9]\d{3}',code):continue
        reason=field(('Reason','原因','暫停交易原因','說明'),False)
        k=(code,start,end,reason)
        if k in seen:continue
        seen.add(k);rows.append({'code':code,'halt_day':start,'resume_day':end,'reason':reason})
    return sorted(rows,key=lambda x:(x['code'],x['halt_day'],x['resume_day'],x['reason']))

def collect(root=ROOT,now=None,http=None):
    import requests
    live=now is None; now=perf.stamp(now or datetime.now(ar.TZ));http=http or requests.Session();result={}
    for m,url in SOURCES.items():
        try:
            resp=http.get(url,timeout=15);resp.raise_for_status();raw=resp.json();obs=perf.stamp(datetime.now(ar.TZ)) if live else now
            rows=parse_status(raw,m,obs)
            body={'version':VERSION,'market':m,'source':url,'observed_at':obs.isoformat(),'raw':raw,'rows':rows}
            p=Path(root)/REL/m/(obs.strftime('%Y%m%dT%H%M%S%f')+'.json.gz');perf.immutable(p,body)
            result[m]={'status':'POSITIVE_EVENTS_ONLY' if rows else 'EMPTY_UNPROVEN','rows':len(rows)}
        except Exception as e:result[m]={'status':'SOURCE_PENDING','error':type(e).__name__+': '+str(e)[:180]}
    return result

def load_status(root,now):
    out={};errors=[]
    for m,url in SOURCES.items():
        for p in sorted((Path(root)/REL/m).glob('*.json.gz'),reverse=True):
            try:
                v=ar._read(p);obs=perf.stamp(v['observed_at'])
                if obs>now:continue
                if now-obs>timedelta(days=7):break
                if v['version']!=VERSION or v['market']!=m or v['source']!=url or p.name!=obs.strftime('%Y%m%dT%H%M%S%f')+'.json.gz' or parse_status(v['raw'],m,obs)!=v['rows']:raise ValueError('status provenance mismatch')
                out[m]=v['rows'];break
            except Exception as e:errors.append(m+': '+str(e)[:150]);break
    return out,errors

def classify_gaps(gaps,events):
    result=[]
    for r in gaps:
        m,c=r['市場'],r['股票代號']
        for d in filter(None,r.get('缺日期','').split('、')):
            candidates=[x for x in events.get(m,[]) if x['code']==c and (d==x['halt_day'] or d==x['resume_day'] or (x['halt_day']<d and (not x['resume_day'] or d<x['resume_day'])))]
            state='NO_STATUS_EVIDENCE'
            if candidates:
                # An open-ended historical event is not proof that a stock remains halted.
                state='ANNOUNCEMENT_BOUNDARY' if any(d in (x['halt_day'],x['resume_day']) for x in candidates) else 'DATED_HALT_INTERVAL_REVIEW' if any(x['halt_day']<d<x['resume_day'] for x in candidates) else 'OPEN_END_REVIEW'
            result.append({'市場':m,'股票代號':c,'股票名稱':r.get('股票名稱',''),'缺日期':d,'狀態':state,'官方事件數':len(candidates),'原因':'；'.join(sorted({x['reason'] for x in candidates if x['reason']})),'處理':'保留原母體及缺日；不生成K線'})
    return result

def gate(q,u,now,market,errors=()):
    today=now.date().isoformat();obs=market.cal_observed.get(today[:7]);confirmed=bool(obs and obs.date().isoformat()==today and obs.time()>=time(18,15) and obs<=now)
    if now.time()<time(18,15):phase='BEFORE_EOD'
    elif confirmed and today not in market.days:phase='CONFIRMED_NON_SESSION'
    elif now.time()<time(23,30):phase='EOD_IN_PROGRESS'
    else:phase='EOD_DEADLINE'
    daily=[dict(x) for x in q['checks'] if x['環節']!='成熟含成本淨績效']
    # Mature evidence and adjusted research are distinct from a daily capture SLA.
    ready=bool(daily and all(x['狀態']=='PASS' for x in daily) and not errors and not q.get('errors') and q['session']==today and confirmed and today in market.days)
    state='PASS_DAILY' if ready else 'NON_SESSION' if phase=='CONFIRMED_NON_SESSION' else 'FAIL_DAILY' if phase=='EOD_DEADLINE' else 'PENDING_DAILY'
    return {'phase':phase,'daily_status':state,'daily_checks':daily,'daily_passed':sum(x['狀態']=='PASS' for x in daily),'daily_total':len(daily),'mature_status':'AVAILABLE' if q['counts']['valid_rows']>0 else 'PENDING','shadow_status':u['status'],'trade_authority_changed':False}

def run(root=ROOT,now=None):
    from godpick_h213_quality_gate import quality
    from godpick_h214_universe import run as universe
    from godpick_h212_data_recovery import gaps
    root=Path(root);now=perf.stamp(now or datetime.now(ar.TZ));q=quality(root,now);u=universe(root,now);g=gaps(root,now);m=perf.Market(root,now)
    events,errs=load_status(root,now);detail=classify_gaps(g['stock_gaps'],events)
    gts=gate(q,u,now,m,errs+g['archive_errors']);coverage=u['coverage']
    health={}
    for rel in ('h190/health.json','h209/collector.json','h194/latest.json.gz','h212/repair.json','h213/monthly_health.json'):
        try:
            v=ar._read(root/'data'/rel);ts=v.get('asof',v.get('observed_at'))
            if ts and perf.stamp(ts)<=now:health[rel]={k:v[k] for k in ('asof','observed_at','status','new_files','paired_files_present','new_valid_rows','saved','pending','attempted','errors','diagnostics','signals','mature','capture_status') if k in v}
        except (OSError,ValueError,KeyError,TypeError):pass
    return {'version':VERSION,'asof':now.isoformat(),'session':q['session'],'gate':gts,'coverage':coverage,'counts':q['counts'],'recall':q['recall'],'status_sources':{k:len(v) for k,v in events.items()},'status_summary':dict(Counter(r['狀態'] for r in detail)),'status_detail':detail,'errors':errs+q['errors']+u['errors'],'health':health,'trade_authority_changed':False}

def excel(r):
    import openpyxl
    from openpyxl.styles import Font,PatternFill
    from openpyxl.utils import get_column_letter
    import json
    w=openpyxl.Workbook();w.remove(w.active)
    def sheet(name,rows):
        s=w.create_sheet(name);keys=list(dict.fromkeys(k for r in rows for k in r)) or ['狀態'];s.append(keys)
        for r in rows:
            vals=[]
            for k in keys:
                x=r.get(k);x=json.dumps(x,ensure_ascii=False) if isinstance(x,(dict,list)) else x
                if isinstance(x,str):x=x[:32000];x="'"+x if x.startswith(('=','+','-','@')) else x
                vals.append(x)
            s.append(vals)
        for c in s[1]:c.font=Font(color='FFFFFF',bold=True);c.fill=PatternFill('solid',fgColor='174765')
        s.freeze_panes='A2';s.auto_filter.ref=s.dimensions
        for i,k in enumerate(keys,1):
            s.column_dimensions[get_column_letter(i)].width=min(45,max(15,len(k)*2+3))
            if '覆蓋率' in k or k=='研究榜召回率':
                for c in list(s.columns)[i-1][1:]:c.number_format='0.00%'
    sheet('01_強制驗收',[{'資料日':r['session'],'觀測時間':r['asof'],**{k:v for k,v in r['gate'].items() if k!='daily_checks'}}]);sheet('02_每日閘門',r['gate']['daily_checks']);sheet('03_母體覆蓋',r['coverage']);sheet('04_交易狀態',r['status_detail']);sheet('05_成熟召回',r['recall']);sheet('06_管線健康',[{'來源':k,**v} for k,v in r['health'].items()]);sheet('07_來源異常',[{'錯誤':x} for x in r['errors']]);b=io.BytesIO();w.save(b);return b.getvalue()
