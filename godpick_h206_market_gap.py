# -*- coding: utf-8 -*-
"""H206 market leader reconciliation. Research audit only, not a trade gate.
Public daily job uses H190 and prospective H194; actual H126 is compared only
inside the originating export. No private records are published by this module.
"""
from datetime import datetime, date, time, timedelta
from pathlib import Path
from collections import Counter
import io, math, re
import godpick_h190_market_archive as ar
import godpick_h194_performance as perf
from godpick_h191_opportunity_engine import master_map, read_json
VERSION='V191-H206-MARKET-GAP'
ROOT=Path(__file__).resolve().parent
REL=Path('data/h206/latest.json.gz')
SHEET='H206_市場強勢差異'
TOP=20
MIN_GAIN=3.0
MIN_AMOUNT=20_000_000
BUY={'可以買進','允許買進','是','可買進'}

def text(v):
    s=str(v if v is not None else '').strip()
    return '' if s.lower() in ('nan','none','<na>','nat') else s

def code(v):
    s=text(v)
    if s.endswith('.0'):s=s[:-2]
    return s if re.fullmatch('[1-9][0-9]{3}',s) else ''

def exact_day(v):
    try:
        s=text(v); return date.fromisoformat(s).isoformat() if len(s)==10 else ''
    except ValueError:return ''

def positive(v):
    return perf.num(v) if not isinstance(v,bool) else None

def rows(v):
    if hasattr(v,'to_dict'):return v.to_dict('records')
    return v if isinstance(v,list) else []

def read_quotes(root,m,d,now):
    path=Path(root)/'data/h190/daily'/m/(d+'.json.gz')
    v=ar._read(path)
    if (v.get('version')!=ar.VERSION or v.get('day')!=d or v.get('market')!=m or v.get('source_url')!=ar.URLS[m] or v.get('basis')!='RAW_UNADJUSTED_FIRST_OBSERVED_BACKFILL' or perf.stamp(v['observed_at'])>now or v['row_count']!=len(v['rows']) or len(v['rows'])<ar.MIN_ROWS[m]):raise ValueError('H190 provenance or coverage mismatch')
    out={}
    for r in v['rows']:
        if len(r)!=8 or r[0] in out:raise ValueError('H190 duplicate or invalid row')
        o,h,l,c,vol,amt=map(positive,r[1:7])
        if not all((o,h,l,c,vol,amt)) or h<max(o,l,c) or l>min(o,h,c) or vol%1 or amt%1 or not l*.5<=amt/vol<=h*2:raise ValueError('H190 invalid OHLCV/amount')
        out[r[0]]=(c,amt)
    return out,v['observed_at']

def leaders(master,current,previous,m,d,prev):
    universe={c:r for c,r in master.items() if r['市場']==m}
    paired=set(universe)&set(current)&set(previous)
    missing=len(universe)-len(paired); candidates=[]; suspicious=0
    for c in paired:
        close,amt=current[c];base=previous[c][0];gain=(close/base-1)*100
        if abs(gain)>15: suspicious+=1;continue  # raw discontinuity; not proof of corporate action
        if gain+1e-10<MIN_GAIN or amt<MIN_AMOUNT:continue
        candidates.append({'市場':m,'股票代號':c,'股票名稱':universe[c]['股票名稱'],'產業':universe[c]['產業'],'交易日':d,'前交易日':prev,'前收盤':base,'收盤':close,'原始收盤漲幅%':round(gain,6),'成交額元':amt})
    candidates.sort(key=lambda r:(-r['原始收盤漲幅%'],-r['成交額元'],r['股票代號']))
    result=candidates[:TOP]
    for i,r in enumerate(result,1):r['市場強勢順位']=i
    return result,{'主檔一般股':len(universe),'雙日有價':len(paired),'缺雙日行情':missing,'雙日覆蓋率%':round(100*len(paired)/len(universe),2) if universe else None,'異常價格跳動排除':suspicious,'達研究條件':len(candidates),'展示':len(result)}

def build(root=ROOT,now=None):
    root=Path(root);now=perf.stamp(now or datetime.now(ar.TZ)); body={'version':VERSION,'asof':now.isoformat(),'day':'','previous_day':'','status':'DATA_PENDING','rows':[],'coverage':{},'errors':[],'prior_research':{},'trade_authority_changed':False,'basis':'原始收盤價變動；非除權息報酬。當前股票主檔；非歷史成分重建。'}
    market=perf.Market(root,now)
    if market.errors:body['errors'].extend(market.errors[:10]);return body
    cutoff=now.date().isoformat() if now.time()>=time(18,15) else (now.date()-timedelta(days=1)).isoformat()
    ds=[d for d in market.days if d<=cutoff]
    if len(ds)<2:body['errors'].append('缺官方連續交易日曆');return body
    prev,d=ds[-2:];body.update(day=d,previous_day=prev)
    if market.span(prev,1)!=[prev,d]:body['errors'].append('交易日曆跨月缺口');return body
    master=master_map(read_json(root/'stock_master_cache.json',[]))
    if not master:body['errors'].append('缺一般股主檔');return body
    for m in ar.URLS:
        try:
            current,obs=read_quotes(root,m,d,now);previous,pobs=read_quotes(root,m,prev,now)
            rr,cv=leaders(master,current,previous,m,d,prev);cv.update(當日觀測時間=obs,前日觀測時間=pobs)
            body['rows'].extend(rr);body['coverage'][m]=cv
        except (OSError,ValueError,KeyError,TypeError,IndexError) as e:body['errors'].append(m+': '+str(e)[:180])
    # H195 is a same-session published research board, not a historical forecast
    # or H126 authority. Preserve its independent snapshot timestamp.
    joint={};joint_ok=False
    body['joint_research']={'status':'MISSING_OR_DIFFERENT_SESSION','asof':None,'count':None}
    try:
        from godpick_h195_command_center import VERSION as JV, REL as JR
        j=ar._read(root/JR)
        if j['version']!=JV or j['trade_authority_changed'] is not False or perf.stamp(j['asof'])>now:raise ValueError('invalid H195 provenance')
        if j['expected_session']==d and j['market_day']==d and j['technical_current'] is True:
            for item in j['rows']:
                k=(item['市場'],code(item['股票代號']))
                if k[0] not in ar.URLS or not k[1] or k in joint:raise ValueError('invalid H195 identities')
                joint[k]=item
            if sum(j['counts'].values())!=len(joint):raise ValueError('H195 count mismatch')
            joint_ok=True;body['joint_research']={'status':'SAME_SESSION_RESEARCH_ONLY','asof':j['asof'],'count':len(joint)}
    except FileNotFoundError:pass
    except (OSError,ValueError,KeyError,TypeError) as e:body['joint_research']['status']='INVALID';body['joint_research']['reason']=str(e)[:150]
    for r in body['rows']:
        j=joint.get((r['市場'],r['股票代號'])) if joint_ok else None
        r['同日H195研究分級']=text(j.get('研究分級')) if j else ''
        r['同日H195研究對帳']='已入同日聯合研究（非預測）' if j else ('未入同日聯合研究快照（非正式漏選）' if joint_ok else '待同日聯合研究快照')
    # Do not label absent prospective records as a miss if no ledger exists.
    try:ss,_=perf.read_ledger(root)
    except Exception as e:ss={};body['errors'].append('H194: '+type(e).__name__+': '+str(e)[:120])
    for m in ar.URLS:
        prior={k[2]:s for k,s in ss.items() if k[0]==prev and k[1]==m and perf.stamp(s['created_at'])<=now}
        hits=0;known=0
        for r in body['rows']:
            if r['市場']!=m:continue
            s=prior.get(r['股票代號'])
            r['前日H194研究分級']=s['tier'] if s else ''
            if not prior:status='前日無可核對研究封存'
            elif s and not math.isclose(s['base_close'],r['前收盤'],rel_tol=1e-9):status='基準價衝突／待核'
            elif s:status='前日已入研究封存';hits+=1;known+=1
            else:status='未入前日已封存研究（非正式漏選）';known+=1
            r['前日研究對帳']=status
        body['prior_research'][m]={'前日封存研究股數':len(prior) if prior else None,'可比較強勢股':known,'前日研究命中':hits if known else None,'命中比例%':round(100*hits/known,2) if known else None}
    ready=len(body['coverage'])==2 and all((v['雙日覆蓋率%'] or 0)>=95 for v in body['coverage'].values())
    # Current weekday freshness is conservative; holiday exceptions must be checked, never invented.
    expected=date.fromisoformat(cutoff)
    while expected.weekday()>=5:expected-=timedelta(days=1)
    body['expected_day']=expected.isoformat()
    body['status']='MARKET_READY' if ready and d==body['expected_day'] and not body['errors'] else 'PARTIAL_OR_STALE'
    return body

def publish(root=ROOT,now=None):
    b=build(root,now);p=Path(root)/REL
    if p.exists():
        old=ar._read(p)
        if perf.stamp(old['asof'])>perf.stamp(b['asof']):raise ValueError('refuse H206 rollback')
        # Do not overwrite a useful earlier-day market report with an empty failure.
        if not b['coverage'] and old.get('coverage'):return b
    ar._write(p,ar._seal(b));return b

def load(root=ROOT,now=None):
    try:
        b=ar._read(Path(root)/REL);now=perf.stamp(now or datetime.now(ar.TZ));ts=perf.stamp(b['asof'])
        if b['version']!=VERSION or b['trade_authority_changed'] is not False or ts>now:raise ValueError('invalid H206 snapshot')
        return b
    except (OSError,ValueError,KeyError,TypeError):return {}

def compare_export(b,*,research_day,parity,bridge,authority,trace):
    """Only same-day actual H154/H126 export; no simulated current buy list."""
    day=exact_day(research_day);p=parity or {};g=bridge or {};out=[]
    matched=bool(day and day==b.get('day') and p.get('run_identity')=='MATCHED' and p.get('run_id') and p.get('run_id')==p.get('scan_run_id'))
    complete=bool(matched and g.get('same_run_proven') is True and g.get('candidate_complete') is True and (g.get('audit_final_coverage') or {}).get('complete') is True and not str(g.get('status','')).startswith('BLOCKED'))
    amap={};conf=set()
    for r in rows(authority):
        c=code(r.get('股票代號'));m={'上市':'TWSE','上櫃':'TPEX','TWSE':'TWSE','TPEX':'TPEX'}.get(text(r.get('市場別')));k=(m,c)
        if not c or not m:continue
        if k in amap:conf.add(k)
        amap[k]=r
    positives={k for k,r in amap.items() if text(r.get('H126能否買進')) in BUY}
    # Explicit positive count must match the actual H154 source, not a guessed 0.
    authority_rows=rows(authority)
    schema_ok=bool(authority_rows and all(isinstance(r,dict) and '股票代號' in r and 'H126能否買進' in r and text(r.get('H126能否買進')) for r in authority_rows))
    recognized=all((code(r.get('股票代號')) and {'上市':'TWSE','上櫃':'TPEX','TWSE':'TWSE','TPEX':'TPEX'}.get(text(r.get('市場別')))) or (not text(r.get('股票代號')) and text(r.get('H126權威層別'))=='無合格推薦' and text(r.get('H126能否買進'))=='禁止買進') for r in authority_rows)
    consistent=matched and schema_ok and recognized and not conf and type(p.get('h126_buyable')) is int and p['h126_buyable']==len(positives)
    tmap={};tconf=set()
    for r in rows(trace):
        c=code(r.get('股票代號'))
        if not c or exact_day(r.get('H148交易日'))!=day:continue
        if c in tmap:tconf.add(c)
        tmap[c]=r
    research=set(p.get('ranked_codes') or []);watch=set(p.get('watch_codes') or [])|set(p.get('h85_waiting_codes') or [])|set(p.get('h85_emerging_codes') or [])
    for r in b.get('rows',[]):
        x=dict(r);k=(r['市場'],r['股票代號']);c=k[1];a=amap.get(k,{});t=tmap.get(c,{}) if c not in tconf else {}
        if not consistent:status='本輪身分／日期／授權數未核實'
        elif k in positives:status='H126原表有授權（仍依原表）'
        elif c in research:status='已入A/B研究（非授權）'
        elif c in watch:status='已入觀察（非授權）'
        elif not complete:status='局部母體／不可判定漏選'
        elif t and t.get('H148漏網位置')=='不在H145診斷輸入／來源待查':status='未進本輪診斷輸入／待查'
        elif t:status='未入本輪研究／逐股複核'
        else:status='本輪未見逐股證據／待查'
        x.update({'本輪對帳':status,'H126原始狀態':text(a.get('H126能否買進')) if consistent else '', 'H148漏網位置':text(t.get('H148漏網位置')) if matched else '', 'H148量價狀態':text(t.get('H148量價狀態')) if matched else '', 'H109原風控':text(t.get('H148風控')) if matched else '', '待查原因':text(t.get('H148補證優先')) if matched else '', 'H126原缺失':text(a.get('H126缺失與處置')) if consistent else ''})
        out.append(x)
    return {'day':day,'market_day':b.get('day',''),'run_id':p.get('run_id',''),'same_run_verified':bool(consistent),'complete_scope':bool(complete and consistent),'h126_buyable':len(positives) if consistent else None,'status_counts':dict(Counter(r['本輪對帳'] for r in out)),'rows':out}

def append_sheet(wb,b,comparison=None):
    from openpyxl.styles import Font,PatternFill,Alignment
    from openpyxl.utils import get_column_letter
    from godpick_h195_command_center import safe
    ws=wb.create_sheet(SHEET);c=comparison or {};rr=c.get('rows',b.get('rows',[]))
    ws.append(['H206｜市場強勢差異分析（研究稽核，非買進）',b.get('status','MISSING')]);ws.append(['行情交易日',b.get('day',''),'前交易日',b.get('previous_day',''),'快照',b.get('asof','')]);ws.append(['口徑',b.get('basis','尚未建立快照'),'每市場前20、漲幅至少3%、成交額至少2000萬'])
    ws.append(['原推薦日',c.get('day',''),'Run ID',safe(c.get('run_id','')),'H126原授權數',c.get('h126_buyable'),'本輪身分核實',c.get('same_run_verified'),'母體完整',c.get('complete_scope')]);ws.append(['覆蓋率',safe(b.get('coverage',{})),'前日研究對帳',safe(b.get('prior_research',{})),'同日H195',safe(b.get('joint_research',{})),'缺口',safe(b.get('errors',[]))]);cols=list(dict.fromkeys(k for r in rr for k in r)) or ['資料狀態'];ws.append(cols)
    for r in rr:ws.append([safe(r.get(k)) for k in cols])
    if not rr:ws.append(['無有效市場強勢資料；不可推論無股票'])
    for cell in ws[6]:cell.font=Font(bold=True,color='FFFFFF');cell.fill=PatternFill('solid',fgColor='185C70');cell.alignment=Alignment(wrap_text=True)
    for i,k in enumerate(cols,1):ws.column_dimensions[get_column_letter(i)].width=34 if any(t in k for t in ('原因','狀態','對帳','漏網','缺失')) else 18
    ws.freeze_panes='D7';ws.auto_filter.ref=f'A6:{get_column_letter(len(cols))}{ws.max_row}';return ws

def workbook(b):
    from openpyxl import Workbook
    w=Workbook();w.remove(w.active);append_sheet(w,b);out=io.BytesIO();w.save(out);return out.getvalue()

def render(root=ROOT):
    import streamlit as st
    import pandas as pd
    b=load(root);st.subheader('H206｜市場強勢股與前日研究差異')
    if not b:st.info('等待 H206 自動快照；尚未建立可比較證據。');return
    st.caption(f"行情 {b['day']}｜前交易日 {b['previous_day']}｜封存 {b['asof']}｜{b['status']}。原始收盤價，非投資報酬；不等於正式推薦。")
    now=datetime.now(ar.TZ);cutoff=now.date() if now.time()>=time(18,15) else now.date()-timedelta(days=1)
    while cutoff.weekday()>=5:cutoff-=timedelta(days=1)
    if b['status']!='MARKET_READY' or b['day']!=cutoff.isoformat():st.warning('部分／歷史快照；不可外推全市場或當作最新推薦。')
    st.dataframe(pd.DataFrame(b['rows']),hide_index=True,use_container_width=True)
    with st.expander('來源覆蓋率及前日研究命中'):st.json({'coverage':b['coverage'],'prior_research':b['prior_research'],'joint_research':b.get('joint_research',{}),'errors':b['errors']})
    st.caption('本輪實際 H126 及漏選原因：請看第7頁當輪主管 Excel 的 H206 工作表。')
    st.download_button('下載 H206 市場差異 Excel',workbook(b),'H206_MarketGap.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
