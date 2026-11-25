# -*- coding: utf-8 -*-
"""H208 independent prospective recall and retrospective audit, never trade authority.
Uses only locally sealed raw official 60-session OHLCV. No future labels or
historical reconstruction of forecasts. Research thresholds are hypotheses.
"""
from collections import Counter
from datetime import datetime, date, time
from pathlib import Path
import godpick_h190_market_archive as ar
import godpick_h191_opportunity_engine as tech
import godpick_h198_stage_review as stage
import godpick_h194_performance as perf

VERSION='V191-H208-RECALL-REVIEW'
SHEET='H208_潛力召回'
AUDIT_SHEET='H208_漏選複核'
POOLS=('蓄勢／初突破／回測研究','接近突破待確認')
MIN_AMOUNT=20_000_000


def setup(seq,expected,f):
    """Independently check early-stage research, not use a future gainer list."""
    if min(f['當日成交額'],f['20日均成交額'])<MIN_AMOUNT:return None,'流動性未達研究門檻'
    s=stage.assess(seq,expected,f,liquid=True)
    if s.get('H198資料日期')!=expected[-1]:return None,'H198證據不足'
    if s.get('H198階段')==stage.POOLS[3]:return None,'H198反證／追高警示'
    phase=s.get('H198階段'); contraction=s.get('H198整理量縮比')
    if phase in stage.POOLS[:3]:pool=POOLS[0]
    elif (phase=='其他型態' and f['60日區間位置%'] is not None and 20<=f['60日區間位置%']<=80
          and -8<=f['距箱頂%']<=0 and f['箱體振幅%']<=22 and f['均線收斂%']<=4
          and f['MA20']>=f['MA20前5日'] and f['收盤']>=f['MA20']*.98
          and -10<=f['20日漲跌%']<=15 and .5<=f['量比']<=1.4
          and contraction is not None and contraction<=.95):pool=POOLS[1]
    else:return None,'未達獨立召回條件'
    fields=('收盤','距箱頂%','箱體振幅%','均線收斂%','60日區間位置%','20日漲跌%','量比','當日成交額','20日均成交額')
    result={k:round(float(f[k]),6) if f[k] is not None else None for k in fields}
    result.update({k:s.get(k) for k in stage.FIELDS})
    result.update({'召回池':pool,'量縮比':contraction,'研究用途':'僅研究／不提供買進授權','資料限制':'H190未還原；法人、TDCC、財報須另行驗證','召回理由':('H198有效初期階段；保留原確認／失效條件' if pool==POOLS[0] else '接近箱頂、均線收斂、量縮且未過熱；較寬研究雷達，待放量收盤突破')})
    if pool==POOLS[1]:result['H198確認條件']='收盤突破前20日箱頂0.5%且量比≥1.5，重新複核反證'
    return result,''


def evaluate(master,expected,markets,rows,day,allowed):
    counts={};out=[];reasons={}
    for m in ar.URLS:
        universe=[(c,x) for c,x in master.items() if x['市場']==m];why=Counter();valid=0
        if m not in allowed or len(expected)!=60 or expected[-1]!=day or any(d not in markets.get(m,{}) for d in expected):
            counts[m]={'主檔':len(universe),'有效60日':0,'召回':0,'狀態':'市場60日封存或時點待補證'};continue
        for c,meta in universe:
            seq=rows.get((m,c),[]);f,gap=tech.calc_features(seq,expected)
            if f is None:why[gap]+=1;continue
            valid+=1;r,gap=setup(seq,expected,f)
            if r is None:why[gap]+=1;continue
            out.append({**meta,**r,'研究交易日':day})
        counts[m]={'主檔':len(universe),'有效60日':valid,'覆蓋率%':round(valid/len(universe)*100,2) if universe else None,'召回':sum(r['市場']==m for r in out),'狀態':'已檢查個股'};reasons[m]=dict(why)
    # Sort within distinct market/pool only. Do not compare dissimilar strategies or use future gains.
    out.sort(key=lambda r:(r['市場'],POOLS.index(r['召回池']),abs(r['距箱頂%']),r['量縮比'],r['股票代號']))
    n=Counter()
    for r in out:
        k=r['市場'],r['召回池'];n[k]+=1;r['池內研究序']=n[k]
    return out,counts,reasons


def build(root,market_report,now):
    root=Path(root);now=perf.stamp(now);day=market_report.get('day','')
    b={'version':VERSION,'day':day,'asof':now.isoformat(),'status':'DATA_PENDING','rows':[],'coverage':{},'reasons':{},'errors':[],'trade_authority_changed':False}
    if not day or day!=market_report.get('expected_day') or not market_report.get('coverage'):
        b['errors'].append('H206 交易日／行情未核實');return b
    p=root/'data/h190';cal,markets,rows,errors=ar._index(p)
    if errors:b['errors']=list(errors[:10]);return b
    expected=sorted(set(d for ds in cal.values() for d in ds if d<=day))[-60:]
    if len(expected)!=60 or expected[-1]!=day:b['errors'].append('未滿60個官方交易日');return b
    try:
        for m in ar._months(date.fromisoformat(expected[0]),date.fromisoformat(day)):
            v=ar._read(p/'calendar'/(m+'.json'))
            if m not in cal or perf.stamp(v['observed_at'])>now:raise ValueError('calendar observation mismatch')
        # H206 already validates both latest daily partitions; confirm current
        # calendar checked after cutoff, not merely an old list of sessions.
        cv=ar._read(p/'calendar'/(day[:7]+'.json'));obs=perf.stamp(cv['observed_at'])
        if obs.date().isoformat()<day or (obs.date().isoformat()==day and obs.time()<time(18,15)):raise ValueError('calendar not finalized as of research day')
    except (OSError,ValueError,KeyError,TypeError) as e:b['errors'].append(str(e)[:180]);return b
    allowed=set()
    for m in ar.URLS:
        try:
            if (market_report['coverage'].get(m,{}).get('雙日覆蓋率%') or 0)<95:raise ValueError('雙日覆蓋不足95%')
            for d in expected:
                if d not in markets.get(m,{}):raise ValueError('缺市場交易日 '+d)
                v=ar._read(p/'daily'/m/(d+'.json.gz'))
                if perf.stamp(v['observed_at'])>now:raise ValueError('未來觀測 '+d)
            allowed.add(m)
        except (OSError,ValueError,KeyError,TypeError) as e:b['errors'].append(m+': '+str(e)[:150])
    master=tech.master_map(tech.read_json(root/'stock_master_cache.json',[]))
    rr,cv,why=evaluate(master,expected,markets,rows,day,allowed);b.update(rows=rr,coverage=cv,reasons=why)
    full=len(allowed)==2 and all((cv[m].get('覆蓋率%') or 0)>=95 for m in ar.URLS)
    b['status']='RESEARCH_READY' if full else ('PARTIAL_RESEARCH' if any(x['有效60日'] for x in cv.values()) else 'DATA_PENDING')
    # H195 membership is a separate, same-day source; absence never means formal exclusion.
    joint={};verified=False
    try:
        from godpick_h195_command_center import VERSION as JV,REL
        j=ar._read(root/REL)
        if j['version']==JV and j['trade_authority_changed'] is False and j['expected_session']==j['market_day']==day and j['technical_current'] is True and perf.stamp(j['asof'])<=now:
            for r in j['rows']:
                k=(r['市場'],r['股票代號'])
                if k in joint or k[0] not in ar.URLS or len(k[1])!=4 or not k[1].isdigit() or k[1][0]=='0':raise ValueError('H195 identity conflict')
                joint[k]=r['研究分級']
            verified=sum(j['counts'].values())==len(joint)
    except (OSError,ValueError,KeyError,TypeError):pass
    for r in rr:
        tier=joint.get((r['市場'],r['股票代號'])) if verified else None
        r['同日H195研究']=tier or ('未入同日快照／研究複核' if verified else '待同日快照核對')
    return b


def audit(comparison):
    """Use exact-export facts. Never conclude a veto was wrong from later price."""
    out=[]
    for r in comparison.get('rows',[]):
        x=dict(r);s=r.get('本輪對帳','');why=[]
        if not comparison.get('same_run_verified'):kind='身分待核';action='核對Run ID、交易日、H126原表'
        elif s.startswith(('H126原表','已入')):kind='已涵蓋';action='維持原研究／交易分類'
        elif not comparison.get('complete_scope'):kind='母體待核';action='補齊H145母體；不可判定漏選'
        else:
            loc=r.get('H148漏網位置','');vol=r.get('H148量價狀態','');risk=r.get('H109原風控','')
            if '未進H113' in loc or '不在H145' in loc:why.append('母體／稽核交接')
            if vol and vol!='同日完整OHLCV（內部標記）':why.append('量價待核')
            if risk and risk not in ('RESEARCH_ELIGIBLE','ALLOW','PASS','NONE'):
                why.append('風控複核'+('／族群退潮' if risk=='FADE_VETO' else '／市場衝擊隔離' if risk=='SHOCK_QUARANTINE' else ''))
            if '流動性' in r.get('待查原因',''):why.append('流動性／量比')
            if not why:why.append('策略或證據待逐股核對')
            kind='；'.join(why);action='依原H148證據逐項複核；不可因當日上漲解除風控'
        x.update({'H208原因分類':kind,'H208複核動作':action,'H208結論限制':'當日強勢不證明前日可預測；非買進建議'})
        out.append(x)
    return out


def valid(b):
    h=b.get('h208',{});return isinstance(h,dict) and h.get('version')==VERSION and h.get('day')==b.get('day') and h.get('asof')==b.get('asof') and h.get('trade_authority_changed') is False


def append(wb,b,comparison=None):
    from openpyxl.styles import Font,PatternFill,Alignment
    from openpyxl.utils import get_column_letter
    from godpick_h195_command_center import safe
    h=b.get('h208',{}) if valid(b) else {};c=comparison or {};rc=c.get('h208_comparison',{})
    rows=rc.get('rows',h.get('rows',[])) if h else []
    public=[{**r,'H208原因分類':'未取得本輪H126對帳','H208複核動作':'同日H195與前日H194僅供研究；第7頁匯出核實實際推薦','H208結論限制':'不可推論正式漏選'} for r in b.get('rows',[])]
    for name,rs,title in [(SHEET,rows,'獨立潛力召回；同日資料；不授權買進'),(AUDIT_SHEET,audit(c) if c else public,'H206當日強勢股本輪排除複核；非歷史預測')]:
        if name in wb.sheetnames:raise ValueError('duplicate H208 sheet')
        ws=wb.create_sheet(name);ws.append([title,h.get('status','DATA_PENDING'),'行情日',b.get('day',''),'觀測時間',b.get('asof','')]);ws.append(['覆蓋／缺口',safe(h.get('coverage',{})),safe(h.get('errors',[])),'Run ID',safe(c.get('run_id','')),'母體完整',c.get('complete_scope'),'召回對帳錯誤',safe(c.get('h208_comparison_error',''))]);cols=list(dict.fromkeys(k for r in rs for k in r)) or ['資料狀態'];ws.append(cols)
        for r in rs:ws.append([safe(r.get(k)) for k in cols])
        if not rs:ws.append(['尚無可核對結果；不代表沒有機會。' if name==SHEET else '需第7頁同輪主管Excel才能取得實際推薦對帳。'])
        for cell in ws[3]:cell.font=Font(bold=True,color='FFFFFF');cell.fill=PatternFill('solid',fgColor='185C70');cell.alignment=Alignment(wrap_text=True)
        for i,k in enumerate(cols,1):ws.column_dimensions[get_column_letter(i)].width=36 if any(x in k for x in ('原因','反證','條件','限制','複核','理由')) else 18
        ws.freeze_panes='D4';ws.auto_filter.ref=f'A3:{get_column_letter(len(cols))}{ws.max_row}'


def render(b):
    import streamlit as st
    import pandas as pd
    st.subheader('H208｜潛力召回與漏選複核')
    if not valid(b):st.info('等待新版 H206 自動排程建立 H208 研究快照。');return
    h=b['h208'];st.caption(f"{h['day']}｜{h['status']}｜僅研究；不是正式買進或已驗證的預測。")
    if h['status']!='RESEARCH_READY':st.warning('資料尚不完整；不能外推全市場。')
    st.dataframe(pd.DataFrame(h['rows']),hide_index=True,use_container_width=True)
    with st.expander('H208 研究覆蓋與缺口'):st.json({'coverage':h['coverage'],'reasons':h['reasons'],'errors':h['errors']})
    st.caption('實際漏選原因及本輪召回對帳：第7頁主管Excel H208兩張工作表。')
