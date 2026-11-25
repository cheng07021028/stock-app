# -*- coding: utf-8 -*-
"""H214 shadow research, point-in-time evidence, no H126 or raw archive writes.
Current roster is not a historical constituent list. Cash-only reference-price
back-adjustment is a technical proxy, not total return or executable prices.
"""
from collections import Counter
from datetime import datetime, date, time, timedelta
from pathlib import Path
import io, re, math
import numpy as np
import pandas as pd
import godpick_h190_market_archive as ar
import godpick_h191_opportunity_engine as tech
import godpick_h194_performance as perf
import godpick_h198_stage_review as stage
import godpick_h209_rolling_backtest as actions
VERSION='V191-H214-UNIVERSE'
ROOT=Path(__file__).resolve().parent
URLS={'TWSE':'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2','TPEX':'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'}
REL=Path('data/h214/roster')

def parse_roster(html,market,now):
    now=perf.stamp(now)
    if market not in URLS or not isinstance(html,str) or len(html)>8_000_000:raise ValueError('roster input')
    result={};expected='上市' if market=='TWSE' else '上櫃'
    for t in pd.read_html(io.StringIO(html),header=None):
        # Official table includes section headings and a combined code/name column.
        for r in t.fillna('').astype(str).values.tolist():
            if len(r)<7:continue
            c=re.match(r'^([1-9]\d{3})[\s\u3000]+(.+)$',r[0].strip())
            if not c or r[3].strip()!=expected or not re.fullmatch(r'ES[A-Z0-9]{4}',r[5].strip()) or not re.fullmatch(r'[A-Z]{2}[A-Z0-9]{10}',r[1].strip()):continue
            d=date.fromisoformat(r[2].strip().replace('/','-')).isoformat()
            if d>now.date().isoformat() or c[1] in result:raise ValueError('future or duplicate roster row')
            result[c[1]]={'name':c[2].strip(),'listing_day':d,'isin':r[1].strip(),'cfi':r[5].strip()}
    if len(result)<ar.MIN_ROWS[market]:raise ValueError('partial official roster')
    return result

def collect(root=ROOT,now=None,http=None):
    import requests
    root=Path(root);live=now is None;now=perf.stamp(now or datetime.now(ar.TZ));http=http or requests.Session();out={}
    for m,url in URLS.items():
        try:
            res=http.get(url,timeout=15);res.raise_for_status();html=res.content.decode('cp950',errors='strict') if 'utf-8' not in res.headers.get('Content-Type','').lower() else res.content.decode('utf-8')
            obs=perf.stamp(datetime.now(ar.TZ)) if live else now
            rows=parse_roster(html,m,obs);p=root/REL/m/(obs.strftime('%Y%m%dT%H%M%S%f')+'.json.gz')
            perf.immutable(p,{'version':VERSION,'market':m,'source':url,'observed_at':obs.isoformat(),'raw':html,'rows':rows});out[m]={'status':'SAVED','count':len(rows)}
        except Exception as e:out[m]={'status':'PENDING','reason':str(e)[:180]}
    return out

def load_rosters(root,now):
    result={};errors=[]
    for m,url in URLS.items():
        for p in sorted((Path(root)/REL/m).glob('*.json.gz'),reverse=True):
            try:
                v=ar._read(p);obs=perf.stamp(v['observed_at'])
                if obs>now:continue
                if now-obs>timedelta(days=7):break
                if v['version']!=VERSION or v['market']!=m or v['source']!=url or p.stem.split('.')[0]!=obs.strftime('%Y%m%dT%H%M%S%f') or parse_roster(v['raw'],m,obs)!=v['rows']:raise ValueError('roster provenance mismatch')
                result[m]=v['rows'];break
            except Exception as e:errors.append(m+': '+str(e)[:120]);break
    return result,errors

def validated_bars(seq,expected):
    if not expected:return None
    s=sorted([r for r in seq if expected[0]<=r[0]<=expected[-1]],key=lambda r:r[0])
    if [r[0] for r in s]!=expected:return None
    try:a=np.asarray([r[1:7] for r in s],dtype=float)
    except (ValueError,TypeError):return None
    if a.shape!=(len(expected),6) or not np.isfinite(a).all() or (a<=0).any():return None
    o,h,l,c,v,amt=a.T
    if ((h<np.maximum.reduce([o,l,c]))|(l>np.minimum.reduce([o,h,c]))|(amt/v<l*.5)|(amt/v>h*2)|(v%1!=0)|(amt%1!=0)).any():return None
    return s

def adjust(seq,expected,events,code):
    """All transitions need explicit dated H209 coverage. Only cash, no complex events.
    Anchor at current raw close, preserve volume and turnover. No future adjustment.
    """
    s=validated_bars(seq,expected)
    if s is None:return None,'RAW_WINDOW_INVALID',0
    if any(d not in events for d in expected[1:]):return None,'ACTION_COVERAGE_PENDING',0
    factors=[1.]*len(s);factor=1.;count=0
    for i in range(len(s)-1,0,-1):
        x=events[expected[i]].get(code)
        if x is not None:
            prev=tech.num(x.get('previous_close'));ref=tech.num(x.get('reference'));cash=tech.num(x.get('cash'))
            if x.get('kind') not in ('息','除息') or prev is None or ref is None or cash is None or not 0<ref<=prev or cash<0 or not math.isclose(prev,s[i-1][4],abs_tol=1e-6,rel_tol=0) or abs(prev-ref-cash)>.011:return None,'COMPLEX_OR_CONFLICT_ACTION',count
            factor*=ref/prev;count+=1
        factors[i-1]=factor
    out=[(r[0],*(float(x)*f for x in r[1:5]),*r[5:]) for r,f in zip(s,factors)]
    # Residual discontinuities are blocked even after an event adjustment.
    if any(abs(out[i][1]/out[i-1][4]-1)>.12 or abs(out[i][4]/out[i-1][4]-1)>.15 for i in range(1,len(out))):return None,'RESIDUAL_GAP',count
    return out,'CASH_REFERENCE_ADJUSTED' if count else 'NO_REPORTED_ACTION',count

def features(seq,expected,events,code):
    adj,status,count=adjust(seq,expected,events,code)
    if adj is None:return None,None,status,count
    # H191 checks price/turnover consistency on original inputs and computes
    # indicators on adjusted price only; actual volume and turnover stay raw.
    f,why=tech.calc_features(seq,expected,adjusted_prices=[r[1:5] for r in adj])
    if f is None:return None,None,why,count
    return f,adj,status,count

def short_review(seq,expected,events,code):
    if len(expected)<21:return 'SHORT_LT21',None
    adj,status,_=adjust(seq,expected,events,code)
    if adj is None:return status,None
    c=np.array([r[4] for r in adj]);h=np.array([r[2] for r in adj]);v=np.array([r[5] for r in adj]);a=np.array([r[6] for r in adj]);r5=(c[-1]/c[-6]-1)*100;vol=v[-1]/v[-21:-1].mean();ma20=c[-20:].mean()
    ok=c[-1]>h[-21:-1].max() and vol>=1.5 and c[-1]>ma20 and 0<r5<=15 and c[-1]/ma20<=1.15 and min(a[-1],a[-20:].mean())>=20_000_000
    return 'SHORT_BREAKOUT_REVIEW' if ok else 'SHORT_NO_SETUP',{'5日漲跌%':float(r5),'量比':float(vol),'MA20':float(ma20)}

def analyze(master,expected,markets,rows,rosters,events,*,ready,raw_diagnostics=(),research=()):
    u=tech.master_map(master);rawdiag={(r['市場'],r['股票代號']):r for r in raw_diagnostics};reskeys={(r['市場'],r['股票代號']) for r in research};detail=[];short=[];changes=[];coverage=[]
    # Only current official listing dates with exact current-market identity and
    # contiguous post-listing raw bars can leave the 60-day denominator.
    for code,meta in u.items():
        m=meta['市場'];key=(m,code);seq=rows.get(key,[]);r=rawdiag.get(key,{});official=rosters.get(m,{}).get(code,{});listing=official.get('listing_day','');raw_f,raw_why=tech.calc_features(seq,expected) if ready else (None,'上游待補');fullraw=raw_f is not None
        row={**meta,'上市櫃生效日':listing,'原始60日有效':fullraw,'原缺口':raw_why,'60日母體':True,'有效母體原因':'保留待核','權息狀態':'UPSTREAM_PENDING','權息事件數':0,'還原60日有效':False,'原H198階段':r.get('H198階段',''),'還原H198階段':'','H195研究入列':key in reskeys if research else None}
        if ready and len(expected)==60 and all(d in markets.get(m,{}) for d in expected):
            sub=[d for d in expected if d>=listing] if listing else []
            if listing in expected[1:] and sub and not any(expected[0]<=x[0]<listing for x in seq) and validated_bars(seq,sub) is not None:
                row.update({'60日母體':False,'有效母體原因':'官方新上市櫃／完整短歷史','權息狀態':'SHORT_WINDOW','短歷史日數':len(sub)})
                state,metrics=short_review(seq,sub,events.get(m,{}),code);short.append({**meta,'上市櫃生效日':listing,'交易日數':len(sub),'狀態':state,**(metrics or {})});
            else:
                row['有效母體原因']='60日有效原價' if fullraw else '缺日／停牌／無成交等仍待證據'
                f,adj,st,n=features(seq,expected,events.get(m,{}),code);row.update({'權息狀態':st,'權息事件數':n,'還原60日有效':f is not None})
                if f is not None:
                    liq=min(f['當日成交額'],f['20日均成交額'])>=20_000_000
                    ph=stage.assess(adj,expected,f,liquid=liq);row['還原H198階段']=ph['H198階段'];row['還原H198反證']=ph['H198反證'];row['還原MA20']=f['MA20'];row['還原5日漲跌%']=f['5日漲跌%'];row['還原20日漲跌%']=f['20日漲跌%'];row['原MA20']=raw_f['MA20'] if fullraw else None
                    row['還原研究候選']=ph['H198階段'] in stage.POOLS[:3];row['還原箱頂']=ph['H198箱頂參考'];row['還原失效參考']=ph['H198失效參考']
                    if n or row['原H198階段']!=row['還原H198階段']:changes.append(dict(row))
        detail.append(row)
    for m in URLS:
        rr=[x for x in detail if x['市場']==m];eligible=[x for x in rr if x['60日母體']];raw=sum(x['原始60日有效'] for x in rr);adjusted=sum(x['還原60日有效'] for x in eligible);raw_eligible=sum(x['原始60日有效'] for x in eligible)
        coverage.append({'市場':m,'原始母體':len(rr),'原始有效':raw,'原始覆蓋率':raw/len(rr) if rr else None,'官方短歷史分流':len(rr)-len(eligible),'有效研究母體':len(eligible),'有效母體原價有效':raw_eligible,'有效母體原價覆蓋率':raw_eligible/len(eligible) if eligible else None,'還原有效':adjusted,'還原覆蓋率':adjusted/len(eligible) if eligible else None,'權息證據日數':sum(d in events.get(m,{}) for d in expected[1:])})
    return coverage,detail,short,changes

def run(root=ROOT,now=None,*,technical=None):
    root=Path(root);now=perf.stamp(now or datetime.now(ar.TZ));t=technical if technical is not None else tech.run(root,now=now,top=100);cal,markets,rows,errors=ar._index(root/'data/h190',asof=now);day=t['asof'];expected=sorted({d for ds in cal.values() for d in ds if d<=day})[-60:];rosters,re=load_rosters(root,now);events={m:{} for m in URLS};action_errors=[]
    for m in URLS:
        for d in expected[1:]:
            try:events[m][d]=actions.read_actions(root,m,d,now)[0]
            except FileNotFoundError:pass
            except (ValueError,OSError,KeyError,TypeError) as ex:action_errors.append(f'{m} {d}: {str(ex)[:100]}')
    from godpick_h195_command_center import load
    b,s,_=load(root,now);research=b.get('rows',[]) if s=='CURRENT' and b.get('market_day')==day and b.get('technical_current') else []
    ready=bool(t.get('calendar_fresh') and not errors and len(expected)==60 and all(d in markets[m] for m in URLS for d in expected))
    cov,detail,short,changes=analyze(tech.read_json(root/'stock_master_cache.json',[]),expected,markets,rows,rosters,events,ready=ready,raw_diagnostics=t['diagnostic'],research=research)
    # Roster differences are explicit; no silent enlargement or deletion of the master.
    master=tech.master_map(tech.read_json(root/'stock_master_cache.json',[]));reconcile=[]
    for m,official in rosters.items():
        current={c for c,x in master.items() if x['市場']==m}
        reconcile.extend({'市場':m,'股票代號':c,'差異':'官方名冊有／主檔無','名稱':official[c]['name']} for c in sorted(set(official)-current))
        reconcile.extend({'市場':m,'股票代號':c,'差異':'主檔有／官方名冊未核','名稱':master[c]['股票名稱']} for c in sorted(current-set(official)))
    # No authoritative same-run H126 data in this independent report.
    return {'version':VERSION,'asof':now.isoformat(),'session':day,'status':'SHADOW_READY' if ready and not (re or action_errors) and len(rosters)==2 and not reconcile and all((x['有效母體原價覆蓋率'] or 0)>=.95 and (x['還原覆蓋率'] or 0)>=.95 and x['權息證據日數']==59 for x in cov) else 'EVIDENCE_PENDING','coverage':cov,'detail':detail,'short':short,'changes':changes,'roster_markets':list(rosters),'roster_reconcile':reconcile,'errors':errors+re+action_errors,'research_snapshot_status':s,'formal_comparison':'缺同輪H126授權清單／不推算正式買進','trade_authority_changed':False,'basis':'目前主檔、官方短歷史分流；原始母體保留。現金除息參考價比例還原僅影子技術研究，非總報酬或成交價。停牌、無成交、減資未證不排除。'}

def excel(r):
    from openpyxl import Workbook
    from openpyxl.styles import Font,PatternFill
    from openpyxl.utils import get_column_letter
    from godpick_h195_command_center import safe
    w=Workbook();w.remove(w.active)
    for name,rs in [('01_主管摘要',[{k:v for k,v in r.items() if k not in ('detail','coverage','short','changes','roster_reconcile')}]),('02_雙母體覆蓋',r['coverage']),('03_逐股證據',r['detail']),('04_短歷史研究',r['short']),('05_權息技術差異',r['changes']),('06_主檔名冊差異',r['roster_reconcile'])]:
        s=w.create_sheet(name);keys=list(dict.fromkeys(k for x in rs for k in x));s.append(keys or ['無資料／待補證'])
        for x in rs:s.append([safe(x.get(k)) for k in keys])
        for c in s[1]:c.font=Font(bold=True,color='FFFFFF');c.fill=PatternFill('solid',fgColor='185C70')
        for i,k in enumerate(keys,1):
            s.column_dimensions[get_column_letter(i)].width=26
            if '覆蓋率' in k:
                for c in list(s.columns)[i-1][1:]:c.number_format='0.00%'
        s.freeze_panes='D2';s.auto_filter.ref=s.dimensions
    out=io.BytesIO();w.save(out);return out.getvalue()


def append_formal_audit(wb,root,*,research_day,parity,bridge,authority,trace,now=None):
    from godpick_h195_command_center import load,safe
    from godpick_h206_market_gap import compare_export
    from openpyxl.styles import Font,PatternFill
    from openpyxl.utils import get_column_letter
    b,state,_=load(root,now);day=b.get('market_day','')
    available=state=='CURRENT' and b.get('technical_current') is True and b.get('same_session') is True
    rs=[{k:r.get(k) for k in ('市場','股票代號','股票名稱','研究分級','H211狀態','H211反證')} for r in b.get('rows',[])] if available else []
    result=compare_export({'day':day if available else '', 'rows':rs},research_day=research_day,parity=parity,bridge=bridge,authority=authority,trace=trace)
    ws=wb.create_sheet('H214_正式研究差異');ws.append(['H214｜同日獨立H195研究 vs 原輪H126；不同Run，不授權'])
    ws.append(['獨立研究日期',day,'H195快照狀態',state,'本輪Run',safe(result['run_id']),'本輪身分核實',result['same_run_verified'],'原H126授權數',result['h126_buyable']]);ws.append(['說明','未核實不得推論漏選；本表不含影子還原研究，原H126為準'])
    keys=list(dict.fromkeys(k for r in result['rows'] for k in r));ws.append(keys or ['待同日證據'])
    for r in result['rows']:ws.append([safe(r.get(k)) for k in keys])
    for c in ws[4]:c.font=Font(bold=True,color='FFFFFF');c.fill=PatternFill('solid',fgColor='185C70')
    for i,k in enumerate(keys,1):ws.column_dimensions[get_column_letter(i)].width=34 if any(x in k for x in ('狀態','原因','缺失','反證')) else 20
    ws.freeze_panes='D5';ws.auto_filter.ref=f'A4:{get_column_letter(max(1,len(keys)))}{ws.max_row}'
    return result
