# -*- coding: utf-8 -*-
"""H220: market-specific turnover activity, official flow/TDCC and growth/early-stage research.
Independent prospective cohorts; no H126, ranking or weight mutation. Turnover is not net inflow.
"""
from collections import Counter, defaultdict
from datetime import datetime, time
from pathlib import Path
import math, statistics, io, re, os, tempfile, gzip
import godpick_h190_market_archive as ar
import godpick_h191_opportunity_engine as tech
import godpick_h193_joint_decision as joint
import godpick_h194_performance as p
import godpick_h198_stage_review as stage
import godpick_h209_rolling_backtest as b
import godpick_h211_counterevidence as counter
import godpick_h218_evidence as e
import godpick_h219_first_review as prev
import godpick_h123_flow_provenance as flow
import godpick_h124_ownership_flow as own
VERSION='V191-H220-GROWTH-ROTATION'
ROOT=Path(__file__).resolve().parent
REL=Path('data/h220/daily')
FULL='籌碼雙證據成立'; CONTROL='籌碼已核未共振'; UNKNOWN='籌碼待補證'
MIN_COV=.95; MIN_GROUP=3

def finite(x):return isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x)
def key(r):return r.get('市場'),r.get('股票代號')
def valid_id(k):return k[0] in ar.URLS and isinstance(k[1],str) and bool(re.fullmatch('[1-9][0-9]{3}',k[1]))
def indexed(rows):
    out={}
    for r in rows:
        k=key(r)
        if not valid_id(k) or k in out:raise ValueError('H220 invalid/duplicate identity')
        out[k]=r
    return out

def valid_industry(s):return isinstance(s,str) and bool(s.strip()) and not s.isdigit() and s not in ('其他','未分類','待分類','櫃其他','櫃其他電子')

def sectors(t):
    """Fixed same-market constituents; compare today's turnover to the same firms' prior20 average."""
    idx=indexed(t.get('diagnostic',[]));den=Counter();members=defaultdict(list);valid_members=defaultdict(list);valid=defaultdict(list);mkt=defaultdict(list);cov={}
    for (m,c),r in idx.items():
        name=r.get('產業');f=r.get('H220產業量價') or {};g=(m,name)
        if valid_industry(name):den[g]+=1;members[g].append(c)
        if r.get('資料日期')!=t.get('asof') or not all(finite(f.get(k)) for k in ('當日成交額','前20日均成交額','5日漲跌%')) or min(f['當日成交額'],f['前20日均成交額'])<=0:continue
        mkt[m].append(f)
        if valid_industry(name):valid[g].append(f);valid_members[g].append(c)
    for m in ar.URLS:
        n=sum(k[0]==m for k in idx);v=len(mkt[m]);reported=t.get('coverage',{}).get(m,{})
        ready=bool(t.get('calendar_fresh') and not t.get('archive_errors') and t.get('status') in ('CROSS_MARKET_RESEARCH_READY','PARTIAL_RESEARCH') and n>=100 and v*100>=n*95 and reported.get('valid')==v and reported.get('total')==n)
        cov[m]={'主檔':n,'有效60日':v,'覆蓋率':v/n if n else None,'達標':ready}
    out=[]
    for (m,name),n in sorted(den.items()):
        rs=valid[m,name];v=len(rs);med=statistics.median(x['5日漲跌%'] for x in rs) if rs else None
        bm=statistics.median(x['5日漲跌%'] for x in mkt[m]) if mkt[m] else None
        active=sum(x['當日成交額'] for x in rs)/sum(x['前20日均成交額'] for x in rs) if rs else None
        breadth=sum(x['5日漲跌%']>0 for x in rs)/v if v else None
        ready=cov[m]['達標'] and v>=5 and v*100>=n*70
        status='主流活躍' if ready and med>0 and med>bm and active>=1.1 and breadth>=.6 else ('已核未達主流' if ready else '待補證')
        out.append({'市場':m,'產業':name,'母體':n,'有效':v,'覆蓋率':v/n,'5日中位漲跌%':med,'市場5日中位漲跌%':bm,'成交額活躍比':active,'上漲比例':breadth,'狀態':status,'母體指紋':p.digest(sorted(members[m,name])),'有效成分指紋':p.digest(sorted(valid_members[m,name])),'口徑':'同市場同產業同成分成交額；非資金淨流入'})
    return out,cov

def rotation(current,history,previous_day):
    """Compare only consecutive official sessions and identical eligible constituents."""
    old=next((v for v in history if v['day']==previous_day),None)
    prior={(r['市場'],r['產業']):r for r in old['sectors']} if old else {}
    for r in current:
        q=prior.get((r['市場'],r['產業']),{});r['前次比較日']=previous_day or None
        ok=bool(q and r['狀態']!='待補證' and q['狀態']!='待補證' and all(r.get(k) and r.get(k)==q.get(k) for k in ('母體指紋','有效成分指紋')))
        a=r['狀態']=='主流活躍';b_=q.get('狀態')=='主流活躍'
        r['輪動狀態']=('主流延續' if a and b_ else '新轉活躍' if a else '主流降溫' if b_ else '均未達主流') if ok else '前窗／成分待核'
        r['活躍比變化']=r['成交額活躍比']-q['成交額活躍比'] if ok else None
    return current

def source_fingerprint(root):
    """Guard mutable source archives and master as well as the market/return ledgers."""
    paths=[];root=Path(root)
    for rel in ('data/h115','data/h192','data/h222','data/h235','data/h123','data/h124','data/h211/daily','data/h117/daily/TAIEX','data/h220/daily','data/cache/tdcc_holder_truth'):
        paths.extend(x for x in (root/rel).rglob('*') if x.is_file())
    for rel in ('stock_master_cache.json','official_factors_cache.json','godpick_tdcc_holder_snapshots.json','data/h190/health.json'):
        if (root/rel).is_file():paths.append(root/rel)
    out=[]
    for x in sorted(set(paths)):
        st=x.stat();out.append((str(x),st.st_size,st.st_mtime_ns,st.st_ctime_ns,st.st_ino))
    return tuple(out)

def chip_evidence(root):
    sources={};errors=[]
    for name,fn in [('TWSE',lambda:flow.evidence_by_day(root)),('TPEX',lambda:own.load_captures(root,'TPEX',max_periods=10)),('TDCC',lambda:own.load_captures(root,'TDCC',max_periods=4))]:
        try:sources[name]=fn()
        except (OSError,ValueError,KeyError,TypeError) as exc:sources[name]={};errors.append(name+': '+str(exc)[:120])
    # Never promote a legacy TPEx subtotal that includes foreign dealers.
    sources['TPEX']={d:v for d,v in sources.get('TPEX',{}).items() if v.get('schema_map',{}).get('foreign') in own.TPEX_FIELDS['foreign'][:-1]}
    return sources,errors

def chips(m,c,sources,sessions,now):
    # Use only validated daily source archives, never stale aggregate factor cache.
    # Explicit latest session window avoids treating a missing newest report as zero.
    t=now.isoformat()
    if m=='TWSE':f=flow.assess_flow(code=c,market='上市',evidence_days=sources[m],info_cutoff=t,official_sessions=sessions,preverified=True)
    else:f=own.assess_tpex(code=c,evidence=sources[m],sessions=sessions,cutoff=t,preverified=True)
    d=own.assess_tdcc(code=c,evidence=sources['TDCC'],cutoff=t,min_delta_pp=.3,preverified=True)
    verified=f.get('H123法人實證狀態') in ('PROVEN_FLOW','VERIFIED_BUT_NOT_BUYING') and d.get('H124大戶實證狀態') in ('VERIFIED_RISING','VERIFIED_NOT_RISING')
    state=FULL if verified and f['H123法人實證狀態']=='PROVEN_FLOW' and d['H124大戶實證狀態']=='VERIFIED_RISING' else CONTROL if verified else UNKNOWN
    return {'籌碼分組':state,'法人狀態':f.get('H123法人實證狀態'),'法人原因':f.get('H123法人實證原因'),'外資連買天數':f.get('H123外資連買天數'),'外資5日買超股數':f.get('H123外資近5日買賣超_股'),'投信連買天數':f.get('H123投信連買天數'),'三大法人5日合計股數':f.get('H123近5日法人合計股數'),'法人來源雜湊':f.get('H123來源雜湊'),'TDCC狀態':d.get('H124大戶實證狀態'),'TDCC千張以上變化pp':d.get('H124大戶變化pp'),'TDCC證據週':d.get('H124大戶證據週'),'TDCC來源雜湊':d.get('H124大戶來源雜湊'),'TDCC原因':d.get('H124大戶原因')}

def screen(t,f,mkt,root,now):
    day=t.get('asof');sec,cov=sectors(t);sd={(r['市場'],r['產業']):r for r in sec};ti=indexed(t.get('diagnostic',[]));fi=indexed(f.get('profiles',[]));sources,errors=chip_evidence(root)
    sessions=[d for d in mkt.days if d<=day];source_summary=[]
    for label,src in sources.items():
        visible=[]
        for d,v in sorted(src.items()):
            if d<=day and p.stamp(v['first_observed_at'])<=now:visible.append(d)
        source_summary.append({'來源':label,'已核可見期數':len(visible),'最新可見日期':visible[-1] if visible else None,'需求':('連續兩期TDCC週報' if label=='TDCC' else '最近5個官方交易日逐日資料'),'狀態':'待補證' if (len(visible)<2 if label=='TDCC' else not set(sessions[-5:]).issubset(visible) or len(sessions)<5) else '期數初檢通過／逐股仍須核實'})
    history,history_errors=counter.read_history(root,sessions[:-1][-10:],now);old,_=p.read_ledger(root);legacy={k for k,s in old.items() if p.stamp(s['created_at'])<=now};rows=[];audit=[]
    for k,a in sorted(ti.items()):
        m,c=k;v=fi.get(k,{});ctx=a.get('H211技術證據') or {};sector=sd.get((m,a.get('產業')),{});tags=set(str(v.get('研究池') or '').split('、'));tp=str(a.get('研究池') or '')
        revenue=bool(v.get('營收新鮮') is True and tags.intersection(('營收成長','營收近13月新高')))
        profit=bool(tags.intersection(('連兩季三率三升','獲利突破')))
        valuation='低PE高TTM EPS' in tags and v.get('PE同交易日') is True
        r={**a,'技術研究池':tp,'技術日':day if tp else '', '財報研究池':v.get('研究池'),'營收年月':v.get('營收年月'),'營收年增%':v.get('營收年增%'),'H211營收證據':{'新鮮':v.get('營收新鮮') is True,'可得時間':v.get('營收可得時間'),'來源':v.get('營收來源')}}
        ce=counter.evaluate(r,day,history,sessions[:-1][-10:],legacy,now)
        reason=[]
        if not cov[m]['達標'] or f.get('market_day')!=day:reason.append('同市場60日覆蓋／財技日期待核')
        if not counter.tech_ok(r,day):reason.append('技術或流動性待核')
        if a.get('H198階段') not in stage.POOLS[:3]:reason.append('未符合H198初期型態')
        if ce['H211狀態'] in ('反證排除前段','資料待補證'):reason.append(ce['H211反證'])
        if sector.get('狀態')!='主流活躍':reason.append('同市場產業未達主流／待核')
        if not revenue:reason.append('實際連續營收成長或近13月新高未成立／待核')
        if not profit:reason.append('三率或獲利突破未成立／待核')
        px=mkt.quotes(m,day).get(c) if not reason else None
        if not reason and (not p.num(px) or not math.isclose(px,ctx['收盤'],rel_tol=1e-8)):reason.append('原始收盤衝突')
        audit.append({'市場':m,'股票代號':c,'股票名稱':a.get('股票名稱'),'產業':a.get('產業'),'H198階段':a.get('H198階段'),'營收成長成立':revenue,'三率或獲利成立':profit,'低PE高EPS成立':valuation,'營收年月':v.get('營收年月'),'營收年增%':v.get('營收年增%'),'營收歷史月數':v.get('營收歷史月數'),'三率季度數':v.get('三率季度數'),'YTD季度數':v.get('YTD季度數'),'財報缺口':v.get('缺口'),'產業狀態':sector.get('狀態'),'狀態':'成長起漲複核' if not reason else '未通過／待證','原因':'；'.join(reason)})
        if reason:continue
        rows.append({'市場':m,'股票代號':c,'股票名稱':a.get('股票名稱') or c,'產業':a.get('產業'),'資料日':day,'原始收盤':px,'H198階段':a['H198階段'],'H198失效參考':a.get('H198失效參考'),'H198確認條件':a.get('H198確認條件'),'H211狀態':ce['H211狀態'],'H211新證據':ce['H211新證據'],'營收年月':v.get('營收年月'),'營收年增%':v.get('營收年增%'),'連兩季三率三升':'連兩季三率三升' in tags,'獲利突破':'獲利突破' in tags,'低PE高EPS成立':valuation,'官方PE':v.get('官方PE') if v.get('PE同交易日') else None,'TTM EPS重建':v.get('TTM EPS重建'),'產業成交額活躍比':sector['成交額活躍比'],'產業上漲比例':sector['上漲比例'],**chips(m,c,sources,sessions,now),'用途':'獨立研究／未授權'})
    order={FULL:0,CONTROL:1,UNKNOWN:2};rows.sort(key=lambda r:(r['市場'],order[r['籌碼分組']],not r['低PE高EPS成立'],stage.POOLS.index(r['H198階段']),counter.ORDER.get(r['H211狀態'],99),r['股票代號']))
    return {'day':day,'coverage':cov,'sectors':sec,'rows':rows,'audit':audit,'source_summary':source_summary,'source_errors':errors+history_errors,'financial_errors':list(f.get('errors',[])),'financial_day':f.get('market_day'),'status':'RESEARCH_AVAILABLE' if rows else 'NO_MATCH_OR_EVIDENCE_PENDING'}

def read_history(root,now):
    out=[]
    for f in sorted((Path(root)/REL).glob('*.json.gz')):
        v=ar._read(f);ts=p.stamp(v['created_at']);d=v['day']
        if v['version']!=VERSION or v['trade_authority_changed'] is not False or f.name!=d+'.json.gz' or ts.date().isoformat()!=d or ts.time()<time(18,15):raise ValueError('H220 invalid prospective partition')
        if ts>now:continue
        rr=indexed(v['rows'])
        if any(r['資料日']!=d or r['籌碼分組'] not in (FULL,CONTROL,UNKNOWN) or not p.num(r['原始收盤']) or v['coverage'].get(k[0],{}).get('達標') is not True for k,r in rr.items()):raise ValueError('H220 invalid frozen evidence')
        out.append(v)
    return out

def feedback(history,m,ev,now):
    detail=[];pairs=[];groups=defaultdict(list)
    for v in history:
        for r in v['rows']:
            for n in p.HORIZONS:
                ds=e.due_span(m,v['day'],n,now);s={'signal_day':v['day'],'created_at':v['created_at'],'market':r['市場'],'code':r['股票代號'],'base_close':r['原始收盤']}
                t=b.trade(s,ds,ev) if ds and not m.errors else {'status':'未成熟／行情待核'};x=t.get('net_return');obs=t.get('resolved_at');ok=bool(t['status']=='VALID_CASH_MODEL' and finite(x) and x>-1 and obs and p.stamp(v['created_at'])<p.stamp(obs)<=now and p.stamp(obs).date().isoformat()>=ds[-1])
                line={'研究日':v['day'],'市場':r['市場'],'股票代號':r['股票代號'],'股票名稱':r['股票名稱'],'籌碼分組':r['籌碼分組'],'期間':n,'出場日':ds[-1] if ds else None,'決策時間':v['created_at'],'結果觀測時間':obs if ok else None,'淨報酬':x if ok else None,'狀態':'VALID_CASH_MODEL' if ok else ('淨績效證據異常' if t['status']=='VALID_CASH_MODEL' else t['status'])}
                detail.append(line)
                if ds:groups[(r['市場'],v['day'],n)].append(line)
    for (market,d,n),rr in sorted(groups.items()):
        a=[r for r in rr if r['籌碼分組']==CONTROL];z=[r for r in rr if r['籌碼分組']==FULL];ok=len(a)>=MIN_GROUP and len(z)>=MIN_GROUP and all(r['淨報酬'] is not None for r in a+z)
        av=statistics.mean(r['淨報酬'] for r in a) if ok else None;zv=statistics.mean(r['淨報酬'] for r in z) if ok else None
        pairs.append({'市場':market,'研究日':d,'期間':n,'出場日':rr[0]['出場日'],'決策時間':rr[0]['決策時間'],'結果觀測時間':max(r['結果觀測時間'] for r in a+z) if ok else None,'籌碼未共振':len(a),'籌碼雙證據':len(z),'籌碼待補':len(rr)-len(a)-len(z),'未共振均值':av,'雙證據均值':zv,'H209相對差':zv-av if ok else None,'狀態':'DESCRIPTIVE_ONLY' if ok else 'DATA_PENDING'})
    reviews=e.review(pairs)
    for r in reviews:r['狀態']={'REVIEW_H209_EDGE':'REVIEW_CHIP_EDGE','REVIEW_H194_EDGE':'REVIEW_CONTROL_EDGE'}.get(r['狀態'],r['狀態'])
    for r in pairs:r['籌碼雙證據相對差']=r.pop('H209相對差')
    return detail,pairs,reviews

def snapshot(root,now,current,m):
    day=now.date().isoformat();path=Path(root)/REL/(day+'.json.gz')
    if path.exists():return 'ALREADY_CAPTURED'
    if now.time()<time(18,15) or m.errors or not m.today_confirmed(day) or current['day']!=day or not any(x.get('達標') for x in current['coverage'].values()) or current.get('financial_day')!=day:return 'CAPTURE_PENDING'
    if not current['rows'] and current.get('financial_errors'):return 'CAPTURE_PENDING'
    body={'version':VERSION,'day':day,'created_at':now.isoformat(),'trade_authority_changed':False,'coverage':current['coverage'],'rows':current['rows'],'sectors':current['sectors'],'audit':current['audit'],'source_summary':current.get('source_summary',[]),'source_errors':current['source_errors'],'financial_errors':current.get('financial_errors',[])}
    # Atomic no-replace publication. A competing process can never overwrite an existing decision.
    path.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix='.h220-',dir=path.parent)
    try:
        with os.fdopen(fd,'wb') as fh:
            fh.write(gzip.compress(ar._bytes(ar._seal(body)),mtime=0));fh.flush();os.fsync(fh.fileno())
        try:os.link(tmp,path)
        except FileExistsError:return 'ALREADY_CAPTURED'
    finally:
        os.unlink(tmp)
    return 'CAPTURED' if body['rows'] else 'CAPTURED_NO_MATCH'

def run(root=ROOT,now=None,*,capture=False):
    root=Path(root);live=now is None;now=p.stamp(now or datetime.now(ar.TZ));manifest=ar._archive_manifest(root/'data/h190')[1];sig=e.fingerprint(root);sources_sig=source_fingerprint(root)
    m=p.Market(root,now);t=tech.run(root,now=now,top=100);_,f=joint.run(root,now,technical=t);current=screen(t,f,m,root,now);h=prev.run(root,now);history=read_history(root,now)
    prior=max((d for d in m.days if d<current['day']),default='');rotation(current['sectors'],history,prior)
    if sources_sig!=source_fingerprint(root) or manifest!=ar._archive_manifest(root/'data/h190')[1] or sig!=e.fingerprint(root):raise RuntimeError('H220 source changed; retry')
    status='READ_ONLY'
    if capture:
        finished=p.stamp(datetime.now(ar.TZ)) if live else now
        status=snapshot(root,finished,current,m) if finished.date()==now.date() and finished>=now else 'CAPTURE_WINDOW_EXPIRED'
    history=read_history(root,finished if capture and status in ('CAPTURED','CAPTURED_NO_MATCH','ALREADY_CAPTURED') else now);detail,pairs,reviews=feedback(history,m,b.Evidence(root,m,now),now)
    if manifest!=ar._archive_manifest(root/'data/h190')[1] or sig!=e.fingerprint(root):raise RuntimeError('H220 performance evidence changed; retry')
    return {'version':VERSION,'asof':now.isoformat(),'day':current['day'],'status':current['status'],'capture':status,'coverage':current['coverage'],'sectors':current['sectors'],'rows':current['rows'],'audit':current['audit'],'source_summary':current['source_summary'],'source_errors':current['source_errors'],'financial_errors':current['financial_errors'],'errors':h['errors'],'first':h['first'],'first_counts':h['counts'],'recall':h['recall'],'counts':{'候選':len(current['rows']),'籌碼雙證據':sum(r['籌碼分組']==FULL for r in current['rows']),'籌碼待補':sum(r['籌碼分組']==UNKNOWN for r in current['rows']),'封存日':len(history),'成熟淨績效':sum(r['淨報酬'] is not None for r in detail)},'detail':detail,'pairs':pairs,'review':reviews,'trade_authority_changed':False,'weight_multiplier':1.0}

def table(w,name,rows):
    s=prev.table(w,name,rows)
    for i,c in enumerate(s[1],1):
        if c.value in ('上漲比例','產業上漲比例','未共振均值','雙證據均值','籌碼雙證據相對差','訓練差','留出差'):
            for col in s.iter_cols(min_col=i,max_col=i,min_row=2):
                for cell in col:cell.number_format='0.00%'
    return s

def excel(r):
    from openpyxl import Workbook
    w=Workbook();w.remove(w.active)
    for name,rs in [('01_主管摘要',[{'資料日':r['day'],'狀態':r['status'],'封存':r['capture'],**r['counts'],**r['first_counts'],'權重倍率':1.0,'籌碼來源錯誤':r['source_errors'],'財報來源錯誤':r['financial_errors']}]),('02_成長起漲研究',r['rows']),('03_市場產業活躍',r['sectors']),('04_逐股診斷',r['audit']),('05_首批績效',r['first']),('06_強勢召回',r['recall']),('07_成熟逐筆',r['detail']),('08_籌碼績效對照',r['pairs']),('09_樣本外複核',r['review']),('10_籌碼來源',r['source_summary'])]:table(w,name,rs)
    out=io.BytesIO();w.save(out);return out.getvalue()

def append_manager(w,root,research_day,now=None):
    now=p.stamp(now or datetime.now(ar.TZ));m=p.Market(root,now);day=max((d for d in m.days if d<now.date().isoformat() or (d==now.date().isoformat() and now.time()>=time(18,15))),default='');history=read_history(root,now)
    rows=next((v['rows'] for v in history if v['day']==day==str(research_day)[:10]),[]) if not m.errors else []
    # No re-ranking; show at most 5 per market and group, unknown evidence explicit.
    counts=Counter();out=[]
    for r in rows:
        k=(r['市場'],r['籌碼分組']);counts[k]+=1
        if counts[k]<=5:out.append(r)
    return table(w,'H220_成長起漲研究',out or [{'狀態':'未取得同日H220封存；保留原H219及H126','用途':'獨立研究／未授權'}])
