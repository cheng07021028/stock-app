# -*- coding: utf-8 -*-
"""H195 read-only consolidated research publication. No trading authority writes.
Daily job computes once from H193; page 7 and Excel only read sealed snapshot.
"""
from datetime import datetime, date, time, timedelta
from pathlib import Path
from functools import lru_cache
from collections import Counter
import math, re, json, io
import godpick_h190_market_archive as ar
import godpick_h193_joint_decision as joint
import godpick_h194_performance as perf
VERSION='V191-H195-COMMAND-CENTER'
ROOT=Path(__file__).resolve().parent
REL=Path('data/h195/latest.json.gz')
TIERS=joint.TIERS
FIELDS=['研究分級','研究順序','市場','股票代號','股票名稱','產業','技術研究池','財報研究池','技術日','營收年月','三率季度','營收年增%','營收月增%','官方PE','TTM EPS重建','技術缺口','財報缺口','官方原始收盤價','收盤日','隔日預測日','上漲機率%','盤整機率%','下跌機率%','預測狀態','交易狀態']

# Append only; retain every original column and its position.
from godpick_h198_stage_review import FIELDS as STAGE_FIELDS
FIELDS.extend(STAGE_FIELDS)
import godpick_h200_integrated as h200
FIELDS.extend(h200.FIELDS)

def stamp(v): return perf.stamp(v)
def expected_day(now):
    d=now.date() if now.time()>=time(18,15) else now.date()-timedelta(days=1)
    while d.weekday()>=5:d-=timedelta(days=1)
    return d.isoformat() # conservative weekday cutoff; not official holiday proof

def clean(v):
    if v is None or isinstance(v,(str,bool,int)):return v
    if isinstance(v,float):return v if math.isfinite(v) else None
    if isinstance(v,dict):return {str(k):clean(x) for k,x in v.items()}
    if isinstance(v,(list,tuple)):return [clean(x) for x in v]
    if hasattr(v,'item'):return clean(v.item())
    return str(v)

def build(result,financial,root=ROOT,now=None,detail_limit=3,*,learn=False):
    root=Path(root);now=stamp(now or datetime.now(ar.TZ));day=expected_day(now)
    observed=stamp(result['asof'])
    if observed>now or observed.date()!=now.date():raise ValueError('H193 research timestamp not current')
    session=result.get('market_day',''); tech_current=bool(session==day and not result.get('archive_errors'))
    # H194 is independent: bad or missing ledger must not erase H193 research.
    errors=[];ss={};oo={};market=None;summary={}
    try:
        ss,oo=perf.read_ledger(root)
        ss={k:v for k,v in ss.items() if stamp(v['created_at'])<=now}
        oo={k:v for k,v in oo.items() if perf.sid(v) in ss and stamp(v['resolved_at'])<=now}
        market=perf.Market(root,now)
        rr=perf.report(ss,oo,market,now,perf.models(ss,oo,now))
        summary={k:rr[k] for k in ('signals','mature','pending','prediction_metrics','metrics','calibration','basis')}
        if market.errors:errors.append('H190 archive: '+str(market.errors[:3]))
    except Exception as e:
        errors.append('H194: '+type(e).__name__+': '+str(e)[:180]);ss={};oo={};market=None
    rows=[];seen=set();counts={p:0 for p in TIERS};radar=Counter();reviews=[]
    for tier in TIERS:
        for x in result['pools'][tier]:
            k=(x['市場'],x['股票代號'])
            if k in seen or k[0] not in ar.URLS or not re.fullmatch('[1-9][0-9]{3}',k[1]):raise ValueError('invalid or duplicate H193 identity')
            seen.add(k)
            if x['研究分級']!=tier:raise ValueError('H193 tier mismatch')
            # Do not carry stale technical qualification into current board.
            if tier in TIERS[:2] and not tech_current:continue
            r={field:x.get(field) for field in FIELDS};r.update({'交易狀態':'僅研究／未授權','官方原始收盤價':None,'收盤日':'','隔日預測日':'','上漲機率%':None,'盤整機率%':None,'下跌機率%':None,'預測狀態':'待同日封存／模型驗證'})
            if market is not None and not market.errors:
                price=market.quotes(k[0],day).get(k[1])
                if perf.num(price):r['官方原始收盤價']=price;r['收盤日']=day
                s=ss.get((day,*k))
                if s and s['tier']==tier and s['tech_pools']==x['技術研究池'] and s['financial_pools']==x['財報研究池'] and perf.num(price) and abs(price-s['base_close'])<1e-9:
                    r['隔日預測日']=day;r['預測狀態']=s['forecast_status']
                    if s['forecast'] is not None:
                        for i,c in enumerate(perf.CLASSES):r[c+'機率%']=round(s['forecast'][i]*100,4)
                elif s:r['預測狀態']='當日封存研究與目前證據不一致，不沿用機率'
            if not tech_current:
                for field in STAGE_FIELDS:r[field]=None
            rows.append(r);counts[tier]+=1
            for pool in set((str(x['技術研究池'])+'、'+str(x['財報研究池'])).split('、')):
                if pool:radar[pool]+=1
    status='RESEARCH_AVAILABLE' if rows else 'NO_MATCH_OR_EVIDENCE_PENDING'
    body={'version':VERSION,'asof':now.isoformat(),'research_asof':result['asof'],'expected_session':day,'market_day':session,'status':status,'technical_current':tech_current,'same_session':bool(tech_current and result.get('same_session')),'counts':counts,'universe':len(result['audit']),'rows':rows,'radar':dict(radar),'reviews':reviews,'performance':summary,'errors':errors,'health':{k:result.get(k) for k in ('technical_status','financial_status','technical_coverage','financial_coverage','financial_errors','financial_conflicts','archive_errors','duplicate_identities')},'trade_authority_changed':False}
    adaptive=None
    if learn and market is not None and not market.errors:
        try:
            from godpick_h199_adaptive_weights import run as learn_weights
            adaptive=learn_weights(root,now,ledger=(ss,oo),market=market)
        except Exception as e:
            body['errors'].append('H199: '+type(e).__name__+': '+str(e)[:180])
    h200.enrich(body,result,adaptive,now)
    # Review the actual new priority shortlist, not the old pre-weight top three.
    for tier in TIERS:
        for r in h200.ordered([x for x in rows if x['研究分級']==tier])[:max(0,detail_limit)]:
            k=(r['市場'],r['股票代號'])
            try:
                d=joint.detail(result,financial,*k,root=root,now=observed)
                reviews.append({'市場':k[0],'股票代號':k[1],'roles':[{'id':i,'status':o['status'],'conclusion':o['conclusion'],'gaps':o['gaps'],'provenance':o['provenance'],'metrics':o['metrics'], 'tables':o['tables'] if i in ('01','08') else {}} for i,o in d['roles'].items()]})
            except Exception as e:reviews.append({'市場':k[0],'股票代號':k[1],'error':type(e).__name__+': '+str(e)[:150],'roles':[]})
    return clean(body)

def publish(root=ROOT,now=None):
    root=Path(root);now=stamp(now or datetime.now(ar.TZ));r,f=joint.run(root,now=now);body=build(r,f,root,now,learn=True)
    p=root/REL
    if p.exists():
        old=ar._read(p)
        if stamp(old['asof'])>now:raise ValueError('refuse snapshot rollback')
    sealed=ar._seal(body)
    # Use the same bounds as archive reader; fail before replacing old snapshot.
    if len(ar._bytes(sealed))>16_000_000:raise ValueError('H195 snapshot exceeds verified reader size')
    ar._write(p,sealed);return body

@lru_cache(maxsize=2)
def _cached(path,mtime,size):return ar._read(path)

def load(root=ROOT,now=None):
    now=stamp(now or datetime.now(ar.TZ));p=Path(root)/REL
    try:
        stat=p.stat();b=_cached(str(p),stat.st_mtime_ns,stat.st_size)
        if b['version']!=VERSION or b['trade_authority_changed'] is not False:raise ValueError('H195 snapshot version/authority mismatch')
        ts=stamp(b['asof'])
        if ts>now:raise ValueError('future H195 snapshot')
        fresh=(now-ts<=timedelta(hours=36) and b['expected_session']==expected_day(now))
        return b,('CURRENT' if fresh else 'STALE'),str(b['asof'])
    except FileNotFoundError:return {},'MISSING','等待 H195 自動決策快照'
    except Exception as e:return {},'INVALID',type(e).__name__+': '+str(e)[:180]

def cache_token(root=ROOT,now=None):
    b,s,_=load(root,now)
    try:
        st=(Path(root)/REL).stat();sig=f'{st.st_mtime_ns}:{st.st_size}'
    except OSError:sig=''
    return VERSION+'|'+h200.VERSION+'|'+s+'|'+str(b.get('asof',''))+'|'+sig

def safe(v):
    if isinstance(v,(list,dict)):v=json.dumps(v,ensure_ascii=False,default=str)
    if isinstance(v,str):
        v=re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]',' ',v)[:32000]
        if v.lstrip().startswith(('=','+','-','@')):v="'"+v
    return v

def append_manager(wb,root=ROOT,research_day=None,now=None,*,formal=None):
    """Append separate sheet; never modify legacy sheets, H126 or their validation."""
    from openpyxl.styles import Font,PatternFill,Alignment
    from openpyxl.utils import get_column_letter
    b,s,msg=load(root,now);ws=wb.create_sheet('H195_聯合決策研究')
    # Never equate separately generated H195 snapshot with the legacy scan run.
    try:rd=date.fromisoformat(str(research_day)[:10]).isoformat()
    except (ValueError,TypeError):rd=''
    same=bool(rd and rd==b.get('expected_session'))
    ws.append(['H195 聯合決策｜獨立研究快照（非原掃描／非買進）',s])
    ws.append(['快照時間',safe(msg)])
    ws.append(['原推薦研究日',rd or '未確認','H195目標交易日',b.get('expected_session',''),'實際技術日',b.get('market_day','')])
    ws.append(['資料口徑','同日獨立研究，仍非同一 Run ID' if same else '不同日期／未核實：不得與本輪推薦合併'])
    ws.append(['交易授權','H126原表為唯一權威，本表一律未授權', '本輪H126授權', (formal or {}).get('h126_buyable'), 'H145',safe((formal or {}).get('h145_status','未確認')), '本輪診斷',safe((formal or {}).get('reason','獨立研究未取得本輪授權數')), 'Run ID',safe((formal or {}).get('run_id','未確認'))])
    ws.append(['研究筆數',sum(b.get('counts',{}).values()),'技術同日',str(b.get('technical_current',False)),'績效成熟筆數',b.get('performance',{}).get('mature'),'H200權重',safe(b.get('h200',{}).get('weight_status','尚未產生H200快照')),'H200缺口',safe(b.get('h200',{}).get('issues',[]))])
    ws.append(FIELDS)
    if s=='CURRENT' and same:
        for t in TIERS:
            for r in h200.ordered([x for x in b['rows'] if x['研究分級']==t]):ws.append([safe(r.get(k)) for k in FIELDS])
    else:ws.append(['待確認同日快照；歷史研究請至33頁'])
    for cell in ws[7]:cell.font=Font(color='FFFFFF',bold=True);cell.fill=PatternFill('solid',fgColor='185C70');cell.alignment=Alignment(wrap_text=True)
    ws.freeze_panes='D8';ws.auto_filter.ref=f'A7:{get_column_letter(len(FIELDS))}{ws.max_row}'
    for i,k in enumerate(FIELDS,1):ws.column_dimensions[get_column_letter(i)].width=36 if '缺口' in k or '研究池' in k else 18
    ws.sheet_properties.tabColor='185C70'
    return ws

def workbook(root=ROOT,now=None):
    from openpyxl import Workbook
    b,s,msg=load(root,now);w=Workbook();w.remove(w.active)
    append_manager(w,root,b.get('expected_session'),now)
    for name,rows in [('策略績效',b.get('performance',{}).get('metrics',[])),('研究雷達',[{'研究池':k,'檔數':v} for k,v in b.get('radar',{}).items()]),('資料健康',[{'欄位':k,'內容':v} for k,v in {**b.get('health',{}),'H200整合診斷':b.get('h200',{})}.items()]),('十二研究員',[{'市場':r['市場'],'股票代號':r['股票代號'],**o} for r in b.get('reviews',[]) for o in r['roles']])]:
        ws=w.create_sheet(name);ws.append(['快照時間',safe(msg),'狀態',s]);cols=list(dict.fromkeys(k for r in rows for k in r));ws.append(cols or ['無資料'])
        for r in rows:ws.append([safe(r.get(k)) for k in cols])
        ws.freeze_panes='A3'
    out=io.BytesIO();w.save(out);return out.getvalue()

def render_inline(root=ROOT):
    import streamlit as st
    import pandas as pd
    b,s,msg=load(root);st.subheader('H200｜每日研究優先序（H126正式授權獨立）')
    st.caption('獨立自動研究快照，不等同本輪掃描；正式授權仍以 H126 為準。')
    if s!='CURRENT':st.warning('H195 '+s+'｜'+msg+'；不以舊名單充當本日結果。');return
    st.caption('快照 '+msg+'｜目標交易日 '+b['expected_session']+'｜技術日 '+(b['market_day'] or '待補證'))
    for c,t in zip(st.columns(4),TIERS):c.metric(t,b['counts'][t])
    diag=b.get('h200',{})
    if diag:
        st.caption('H200｜'+diag.get('weight_status','')+'｜母體 '+str(diag.get('universe','未確認'))+'｜缺口 '+ '、'.join(diag.get('issues',[])))
        with st.expander('H200 資料缺口與市場狀態',expanded=not bool(b['rows'])):
            st.json(diag)
    else:st.info('目前為舊H195快照；等待自動工作流程產出H200。')
    rows=[r for t in TIERS for r in h200.ordered([x for x in b['rows'] if x['研究分級']==t])[:3]]
    if rows:
        frame=pd.DataFrame(rows); cols=['研究分級','研究順序','股票代號','股票名稱','技術研究池','財報研究池','上漲機率%','預測狀態']
        cols.extend(k for k in ('H198階段','H198反證','H200研究優先序','H200研究權重','H200風險複核') if k in frame.columns)
        st.dataframe(frame[cols],hide_index=True,use_container_width=True)
    else:st.warning('無具名研究；請檢查 H190／H192 來源健康與覆蓋率，非市場無機會。')
    if b['errors'] or not b['technical_current'] or b['health'].get('technical_status')!='CROSS_MARKET_RESEARCH_READY':st.caption('部分來源／技術資料待核實，詳細診斷見第33頁。')
