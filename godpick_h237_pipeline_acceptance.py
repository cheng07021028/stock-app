# -*- coding: utf-8 -*-
"""H237 read-only operational acceptance, not another selection engine.
Separate collector claims, raw partitions, local/cloud metadata and mature evidence.
No automatic downloads, captures, weight changes or recommendation authority.
"""
from datetime import datetime,time
from pathlib import Path
import io, os
import godpick_h190_market_archive as ar
import godpick_h192_financial_growth as fin
import godpick_h194_performance as perf
import godpick_h222_financial_history as hist
import godpick_h223_growth_scenarios as scenario
import godpick_h235_official_batch as batch

VERSION='V191-H237-PIPELINE-ACCEPTANCE'
ROOT=Path(__file__).resolve().parent


def health(root,rel,clock,now):
    """Sealed health is a claim, not raw evidence. Future health is not visible."""
    path=Path(root)/rel
    if not path.is_file():return {},'MISSING'
    try:
        v=ar._read(path);t=fin.moment(v.get(clock))
        if t is None:return {},'INVALID_TIME'
        if t>now:return {},'NOT_YET_OBSERVED'
        return v,'SEALED_CLAIM'
    except (OSError,ValueError,TypeError,KeyError,OverflowError):return {},'INVALID'


def reconcile_periods(scope,claimed):
    index={};duplicates=set()
    raw=claimed.get('scope',[])
    for r in raw if isinstance(raw,list) else []:
        if not isinstance(r,dict):continue
        k=(r.get('kind'),r.get('market'),r.get('period'))
        if not all(isinstance(x,str) for x in k):continue
        if k in index:duplicates.add(k)
        index[k]=r
    out=[]
    for r in scope:
        k=(r['來源'],r['市場'],r['期間']);c=index.get(k,{})
        saved=c.get('status') in ('VERIFIED','VERIFIED_SAVED')
        valid=r['狀態']=='VERIFIED';n=r.get('有效筆數');cn=c.get('rows')
        if k in duplicates:state='DUPLICATE_CLAIM'
        elif saved and not valid:state='CLAIM_NOT_REVERIFIED'
        elif saved and valid:state='MATCH' if type(cn) is int and cn==n else 'COUNT_MISMATCH'
        elif valid:state='READER_VERIFIED_ONLY'
        else:state='PENDING'
        out.append({**r,'收集器狀態':c.get('status','NO_CLAIM'),'收集器筆數':cn if type(cn) is int else None,'對帳':state})
    return out


def cloud_audit(root,now,periods=None,store=None):
    """Only explicit opt-in does metadata network checks, never raw ZIP downloads."""
    _,end=fin.expected_periods(now)
    raw=periods if periods is not None else os.getenv('H236_RESTORE_QUARTERS','')
    labels=[x.strip() for x in raw.split(',')] if raw.strip() else []
    qs=[fin.quarter(x) for x in labels]
    if len(qs)>8 or None in qs or len(qs)!=len(set(qs)) or any(not end-11<=q<=end for q in qs):raise ValueError('invalid configured quarters')
    from godpick_h236_cloud_financial import GCS,cloud_status
    if not qs:return []
    store=store if store is not None else GCS();out=[]
    for q in qs:
        row={'季度':fin.qstr(q),'雲端狀態':'NOT_CHECKED','本機原始重驗':'MISSING','同雜湊及首次時間':None,'用途':'唯讀中繼資料核對，非雲端ZIP實際下載'}
        try:
            meta=cloud_status(root,q,store,now);row['雲端狀態']=meta['status']
            z,rp=batch.paths(root,q)
            if z.exists() or rp.exists():
                _,receipt,_,_=batch.read(root,q)
                t=fin.moment(receipt['observed_at'])
                if t is None or t>now:row['本機原始重驗']='NOT_YET_OBSERVED'
                else:
                    row['本機原始重驗']='VERIFIED'
                    if meta['status']=='CLOUD_MANIFEST_PRESENT':
                        row['同雜湊及首次時間']=bool(meta['archive_sha256']==receipt['archive_sha256'] and meta['observed_at']==receipt['observed_at'] and meta['archive_bytes']==receipt['archive_bytes'])
        except Exception as e:row['錯誤類型']=type(e).__name__;row['本機／雲端檢查']='ERROR'
        out.append(row)
    return out


def ledger_check(root,now,claim=None):
    s,o=perf.read_ledger(root)
    visible={k:v for k,v in s.items() if perf.stamp(v['created_at'])<=now}
    actual=[v for v in o.values() if perf.sid(v) in visible and perf.stamp(v['resolved_at'])<=now]
    first=min((v['signal_day'] for v in visible.values()),default=None)
    n=sum(v['signal_day']==first for v in visible.values())
    comparison='NO_CLAIM'
    if claim:
        t=fin.moment(claim.get('asof'))
        if t is not None and t<=now:
            claimed_signals={k for k,v in visible.items() if perf.stamp(v['created_at'])<=t}
            claimed_outcomes=sum(perf.sid(v) in claimed_signals and perf.stamp(v['resolved_at'])<=t for v in actual)
            expected=(len(claimed_signals),claimed_outcomes,len(claimed_signals)*len(perf.HORIZONS)-claimed_outcomes)
            reported=tuple(claim.get(k) for k in ('signals','mature','pending'))
            comparison='MATCH_AT_HEALTH_TIME' if all(type(x) is int for x in reported) and reported==expected else 'CLAIM_LEDGER_MISMATCH'
    return {'健康封存對帳':comparison,'封存訊號':len(visible),'原始結果封存':len(actual),'首批研究日':first,'首批研究':n,'待封存結果':len(visible)*len(perf.HORIZONS)-len(actual),'口徑':'封存完整性，不代表價格或淨績效已重驗'}


def run(root=ROOT,now=None,*,full=False,base=None,acceptance=None,net=None,cloud_check=False,periods=None,store=None):
    root=Path(root);now=perf.stamp(now or datetime.now(ar.TZ));errors=[];checks=[]
    def check(name,state,action,**more):checks.append({'項目':name,'狀態':state,'下一步':action,**more})
    def stage(name,fn,default):
        try:return fn()
        except Exception as e:errors.append({'階段':name,'錯誤類型':type(e).__name__});return default
    h222,hs=health(root,'data/h222/health.json','asof',now)
    h192,fs=health(root,'data/h192/health.json','checked_at',now)
    h190,ms=health(root,'data/h190/session_health.json','observed_at',now)
    h194,ps=health(root,'data/h194/latest.json.gz','asof',now)
    scope=stage('H223 raw reparse',lambda:scenario.audit(root,now),[])
    if hs=='SEALED_CLAIM' and not isinstance(h222.get('scope'),list):hs='INVALID_SCHEMA';h222={}
    scope=reconcile_periods(scope,h222)
    required=[r for r in scope if r['必要範圍']]
    verified=sum(r['狀態']=='VERIFIED' for r in required)
    claim=sum(r['收集器狀態'] in ('VERIFIED','VERIFIED_SAVED') for r in required)
    discrepancy=sum(r['對帳'] in ('CLAIM_NOT_REVERIFIED','COUNT_MISMATCH','DUPLICATE_CLAIM') for r in required)
    check('歷史彙總財報原始重驗','PASS' if len(required)==verified==64 and not discrepancy and not any(r['狀態']=='INVALID' for r in scope) else 'PENDING','H222 缺期回補；H223 原始重驗',有效=verified,應有=64,收集器宣稱=claim if hs=='SEALED_CLAIM' else None,對帳差異=discrepancy,健康時間=h222.get('asof'),健康驗證=hs)
    src=h192.get('sources',{});src=src if isinstance(src,dict) else {};fresh=fs=='SEALED_CLAIM' and (now-fin.moment(h192['checked_at'])).total_seconds()<=36*3600
    n=sum(fresh and isinstance(src.get(k+'_'+m),dict) and src[k+'_'+m].get('status')=='OK' for m in ar.URLS for k in ('revenue','margin','pe','ytd'))
    check('最新官方財報收集器健康','CLAIM_OK' if n==8 else 'PENDING','H192 收集器；不等逐股資料完整',有效=n,應有=8,健康時間=h192.get('checked_at'),健康驗證=fs)
    financial=stage('H222 financial research',lambda:hist.inspect(root,now),None)
    batch_scope=[];candidates=[];summary=[]
    if financial is not None:
        summary=financial['summary'];batch_scope=[{k:v for k,v in x.items() if k!='下載網址'} for x in financial['batch_scope']]
        for r in financial['candidates']:
            candidates.append({'市場':r['市場'],'股票名稱':r['股票名稱'],'股票代號':r['股票代號'],'研究用途':'H222初步財報複核／非買進',**{k:r.get(k) for k in ('營收年月','營收年增%','連3月成長','近13月新高','連兩季三率年增','獲利突破','TTM EPS重建','EPS可比性','H232同分母成長複核')}})
        for k in ('errors','rejected','conflicts'):
            if financial.get(k):check('H222 '+k,'REVIEW','原報表檢查拒絕／衝突',數量=len(financial[k]))
        check('具名高成長財報初步複核','RESEARCH_PRESENT' if candidates else 'PENDING','原H222研究條件；非同日技籌證據或正式買進',母體=sum(x['母體'] for x in summary),有效=len(candidates))
    else:check('具名高成長財報初步複核','ERROR','檢查財報解析及母體；不可當成零候選')
    _,end=fin.expected_periods(now);wanted={fin.qstr(q) for q in range(end-7,end+1)}
    local={r['季度']:r for r in batch_scope};local_rows=[{'季度':q,'狀態':'LOCAL_REVERIFIED' if q in local else 'NOT_RECONCILED' if financial is None else 'MISSING_OR_INVALID',**local.get(q,{})} for q in sorted(wanted)]
    check('H235官方批次本機原始重驗','ERROR' if financial is None else 'EVIDENCE_PRESENT' if local.keys()&wanted else 'PENDING','官方原ZIP需由H235直取；不可用H234手動ZIP升格',有效=len(local.keys()&wanted),應有=8)
    cloud=stage('H236 cloud metadata',lambda:cloud_audit(root,now,periods,store),None) if cloud_check else None
    check('H236雲端持久儲存','NOT_REQUESTED' if not cloud_check else 'ERROR' if cloud is None else 'METADATA_ONLY' if cloud else 'NOT_CONFIGURED','明確指定--cloud-check；實際原ZIP還原另用H236；中繼資料非完整驗收')
    ledger=stage('H194 sealed ledger',lambda:ledger_check(root,now,h194),None)
    check('H194前瞻封存','CLAIM_LEDGER_MISMATCH' if ledger and ledger['健康封存對帳']=='CLAIM_LEDGER_MISMATCH' else 'SEALED_LEDGER_PRESENT' if ledger and ledger['首批研究'] else 'ERROR' if ledger is None else 'PENDING','H194 正式封存與結算；封存不等成熟績效',健康時間=h194.get('asof'),健康驗證=ps,健康訊號=h194.get('signals'),健康成熟=h194.get('mature'),**(ledger or {}))
    market=stage('H190 session',lambda:perf.Market(root,now),None)
    session=h190.get('day');verified_markets=0
    if market is not None and not market.errors and session and session in market.days and h190.get('calendar_confirmed') is True and h190.get('status')=='SESSION_QUOTES_VERIFIED':
        verified_markets=stage('H190 session quotes',lambda:sum(bool(market.quotes(m,session)) for m in ar.URLS),0)
    state='LAST_SESSION_REVERIFIED' if verified_markets==2 else 'PENDING'
    if session==now.date().isoformat() and (now.time()<time(18,15) or market is None or not market.today_confirmed(session)):state='PENDING'
    check('H190盤後官方行情',state,'18:15後核實當日官方交易日曆與雙市場；不以日期猜測',資料日=session,健康時間=h190.get('observed_at'),健康驗證=ms,有效=verified_markets,應有=2)
    if full and base is None:
        from godpick_h225_integrated_quality import run as integrated
        base=stage('H225 integrated',lambda:integrated(root,now,include_detail=True),None)
    if (full or base is not None) and base is not None and acceptance is None:
        from godpick_h226_live_acceptance import run as accept
        acceptance=stage('H226 first batch',lambda:accept(root,now,base=base),None)
    if full and base is not None and acceptance is not None and net is None:
        from godpick_h229_net_edge import run as edge
        net=stage('H229 net edge',lambda:edge(root,now,base=base,acceptance=acceptance),None)
    if any(perf.stamp(v['asof'])!=now for v in (base,acceptance,net) if v is not None):raise ValueError('H237 requires one observation time')
    first=acceptance['batch'] if acceptance else [];gates=base['gates'] if base else [];short=net['shortlist'] if net else []
    check('首批成熟原價及含成本績效',acceptance['batch_status'] if acceptance else 'NOT_RECONCILED','H226 逐股完整結算；未完成不得計成零勝率')
    check('同成本市場優勢',net['status'] if net else 'NOT_RECONCILED','H229 時序複核；單批及原價排名不自動調權')
    status='REVIEW_AVAILABLE' if acceptance and acceptance['batch_status']=='FIRST_BATCH_DESCRIPTIVE' and net and net['status']=='DESCRIPTIVE_ONLY' and base and base['status']=='DATA_GATES_PASS' and not errors and not discrepancy and verified==64 and len(required)==64 and ledger and ledger['健康封存對帳']!='CLAIM_LEDGER_MISMATCH' and financial is not None and not financial['errors'] and not net.get('errors') else 'EVIDENCE_PENDING'
    return {'version':VERSION,'asof':now.isoformat(),'status':status,'checks':checks,'periods':scope,'local_batch':local_rows,'cloud':cloud or [],'summary':summary,'candidates':candidates,'shortlist':short,'first':first,'gates':gates,'errors':errors,'weight_multiplier':1.0,'trade_authority_changed':False}


def compact(r):
    """Artifact summary without individual stock rows, source URLs or credentials."""
    return {k:r[k] for k in ('version','asof','status','checks','summary','errors','weight_multiplier','trade_authority_changed')}|{'first_batch_groups':len(r['first']),'priority_research_count':len(r['shortlist']),'financial_candidate_count':len(r['candidates'])}


def excel(r):
    from openpyxl import Workbook
    from godpick_h219_first_review import table
    w=Workbook();w.remove(w.active)
    for name,rows in [('01_正式管線驗收',r['checks']),('02_分期原始對帳',r['periods']),('03_批次本機證據',r['local_batch']),('04_雲端中繼資料',r['cloud']),('05_財報母體',r['summary']),('06_具名財報複核',r['candidates']),('07_優先研究複核',r['shortlist']),('08_首批成熟績效',r['first']),('09_品質閘門',r['gates']),('10_異常階段',r['errors'])]:table(w,name,rows)
    f=io.BytesIO();w.save(f);return f.getvalue()
