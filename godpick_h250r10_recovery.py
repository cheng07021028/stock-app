# -*- coding: utf-8 -*-
"""R10 historical diagnostics and bounded, durable adjusted evidence repair.
No identity mutation, no new records, no trade authorization. Metadata is written
with the existing H241 exact-ID CAS, never to a parallel recommendation store.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

VERSION='H250R10_DIAGNOSTIC_V1'
ATTEMPT='H250R10_還原查詢履歷'
GAP_FIELDS=('H250R10_原價缺口','H250R10_N日缺口','H250R10_觸價缺口')
FIELDS=(*GAP_FIELDS,ATTEMPT)


def gap_days(a,ident,kind):
    import godpick_h250r7_tracking as r
    _,m,c,d=ident
    if d not in a.cal.dayset:return []
    if kind==r.TASKS[0]: days=[d,a.latest]
    elif kind==r.TASKS[1]: days=max((a.cal.span(d,n) or [] for n in r.PERIODS),key=len,default=[])
    else: days=[d,*[x for x in a.cal.days if d<x<=a.latest]]
    return [{'market':m,'code':c,'day':x,'reason':'STOCK_QUOTE_MISSING' if x in a.markets.get(m,{}) else 'MARKET_PARTITION_MISSING'}
            for x in sorted(set(days)) if x not in a.bars.get((m,c),{})]


def prior_attempt(row,ident,now):
    import godpick_h250r7_tracking as r
    p=r.parse(row.get(ATTEMPT))
    try:
        if p.get('version')!=VERSION or tuple(p['identity'])!=ident:return None
        t=datetime.fromisoformat(p['at'])
        if t.tzinfo is None or t>now:return None
        return t,p.get('status')
    except (KeyError,ValueError,TypeError):return None


def proof_reason(row,a,ident):
    import godpick_h250r7_tracking as r
    p=r.parse(row.get(r.ADJ_PROOF))
    if not p:return 'ADJUSTED_NO_PROOF'
    if not isinstance(p.get('identity'),list) or tuple(p['identity'])!=ident:return 'ADJUSTED_IDENTITY_CHANGED'
    if p.get('day')!=a.latest:return 'ADJUSTED_OLD_DAY'
    return 'ADJUSTED_INVALID_PROOF'


def plan(rows,a,kind,limit,cfg,fetch=None):
    import godpick_h250r7_tracking as r
    from godpick_h250r9_evidence import resolve, MAP, encode
    fetch=fetch or r.fetch_adjusted
    # Reuse the verified R9 raw calculations. R10 owns only adjusted query
    # scheduling and a separate diagnostic record. Do not call provider twice.
    changes,st,g=r.build_extended_plan(rows,a,kind,max(len(rows),limit),dict(cfg,adjusted_max_symbols=0))
    stats=Counter(st);gaps=Counter(g);ids=Counter(r.text(x.get('record_id')) for x in rows)
    diags={};metadata={};groups=defaultdict(list);eligible_keys=[];mappings={}
    idx=r.TASKS.index(kind);field=GAP_FIELDS[idx];now=a.cal.now
    if idx==0:
        for k in ('ADJUSTED_PENDING','ADJUSTED_PROVIDER_DEFERRED','ADJUSTED_SYMBOLS_ATTEMPTED','ADJUSTED_CORROBORATED','PAGE08_CURRENT_REUSED'):
            stats.pop(k,None)
    for row in rows:
        rid=r.text(row.get('record_id'));ident=r.identity(row,now)
        if not rid or ids[rid]!=1:continue  # no patch for ambiguous IDs
        diag={'version':VERSION,'state':'READY','reasons':[],'missing':[]};diags[rid]=(row,diag)
        if not ident:
            diag.update(state='IDENTITY_PENDING',reasons=r.identity_reasons(row,now,ids));continue
        eff,mapping,state=resolve(row,a);eid=r.identity(eff,now)
        mappings[rid]=mapping
        if state in ('AMBIGUOUS_OFFICIAL_MARKET','CLOSED_REFERENCE_PRICE_UNCORROBORATED','CLOSED_ANCHOR_QUOTE_PENDING','NOT_DUE'):
            diag.update(state='NOT_DUE' if state=='NOT_DUE' else 'QUOTE_PENDING',reasons=[state]);continue
        _,state,legacy=(r.current_delta,r.nday_delta,r.hits_delta)[idx](eff,a,eid)
        diag['state']=state
        if state in ('CALENDAR_GAP','QUOTE_PENDING'):
            diag['reasons']=[r.gap_reason(a,eid,kind)];diag['missing']=gap_days(a,eid,kind)
        elif state!='READY':diag['reasons']=[state]
        if idx or state!='READY':continue
        if r.adjusted_proof_valid(eff,a,eid):
            stats['ADJUSTED_CORROBORATED']+=1;diag['adjusted']='VERIFIED';continue
        reason=proof_reason(eff,a,eid);stats[reason]+=1
        if legacy:stats['PAGE08_CURRENT_REUSED']+=1 # legacy label is not a strict adjusted proof
        diag['adjusted']=reason
        prev=prior_attempt(row,eid,now)
        groups[eid[1:3]].append((rid,row,eff,eid,prev))
    # Fair across runs: unattempted first, then oldest eligible. A symbol with
    # any unattempted endpoint is eligible; do not requery solely because a
    # former batch failed 20 minutes ago. A failed attempt is not evidence.
    cooldown=timedelta(hours=6)
    for key,items in groups.items():
        due=[x for x in items if x[4] is None or now-x[4][0]>=cooldown]
        if due:
            rank=min((x[4][0].isoformat() if x[4] else '') for x in due)
            eligible_keys.append((rank,key,due))
        due_ids={x[0] for x in due}
        for x in items:
            if x[0] not in due_ids:
                stats['ADJUSTED_COOLDOWN']+=1;diags[x[0]][1]['adjusted']='COOLDOWN'
    cap=max(0,min(int(cfg.get('adjusted_max_symbols',64)),128))
    eligible_keys.sort(key=lambda x:(x[0],x[1]));chosen=eligible_keys[:cap];results={}
    if chosen:
        # Issue only one wave at a time. A rate-limit response stops new waves
        # and preserves the untouched symbols for a later scheduled execution.
        with ThreadPoolExecutor(max_workers=min(8,len(chosen))) as ex:
            for start in range(0,len(chosen),8):
                wave=chosen[start:start+8]
                fs={ex.submit(fetch,*key,min(x[3][3] for x in due),a.latest):key for _,key,due in wave}
                for f in as_completed(fs):
                    try:results[fs[f]]=(f.result(),'')
                    except Exception as exc:
                        s=str(exc);reason=s if isinstance(exc,ValueError) and re.fullmatch(r'[A-Z0-9_]{1,70}',s) else 'PROVIDER_ERROR'
                        results[fs[f]]=(None,reason);gaps['ADJUSTED_FETCH_'+reason]+=1
                if gaps.get('ADJUSTED_FETCH_HTTP_429',0):
                    stats['ADJUSTED_RATE_LIMITED']=1;break
    stats['ADJUSTED_SYMBOLS_ATTEMPTED']=len(results) if idx==0 else 0
    for _,key,due in eligible_keys:
        for rid,row,eff,eid,prev in due:
            diag=diags[rid][1]
            if key not in results:
                stats['ADJUSTED_PROVIDER_DEFERRED']+=1;diag['adjusted']='QUEUED';continue
            provider,reason=results[key];delta={}
            if provider is not None:delta,reason=r.adjusted_delta(eff,a,eid,provider)
            attempt=r.compact({'version':VERSION,'identity':eid,'day':a.latest,'at':now.isoformat(),'status':reason})
            metadata.setdefault(rid,{})[ATTEMPT]=attempt
            if delta:
                changes.setdefault(rid,{}).update(delta)
                if mappings[rid]:changes[rid][MAP]=encode(mappings[rid])
                elif r.text(row.get(MAP)):changes[rid][MAP]=''
                stats['ADJUSTED_CORROBORATED']+=1;diag['adjusted']='VERIFIED'
            else:
                stats['ADJUSTED_ATTEMPT_FAILED']+=1;diag['adjusted']=reason;gaps[reason]+=1
    if idx==0:
        stats['ADJUSTED_PENDING']=sum(1 for _,d in diags.values() if d.get('adjusted') not in (None,'VERIFIED'))
    # Stable diagnosis: no timestamp churn for unchanged historical gaps.
    for rid,(row,diag) in diags.items():
        value=r.compact(diag)
        if row.get(field)!=value:metadata.setdefault(rid,{})[field]=value
    metric_ids=set(changes);all_ids=list(changes)+[rid for rid in metadata if rid not in changes]
    selected=all_ids[:limit];patches={rid:dict(changes.get(rid,{}),**metadata.get(rid,{})) for rid in selected}
    stats['METRICS_PLANNED']=sum(rid in metric_ids for rid in selected)
    stats['DIAGNOSTICS_PLANNED']=sum(rid in metadata for rid in selected)
    stats['DEFERRED']=len(all_ids)-len(selected)
    return patches,dict(stats),dict(gaps)


def diagnostics_table(df):
    import pandas as pd
    import godpick_h250r7_tracking as r
    out=[]
    for field,label in zip(GAP_FIELDS,('推薦後績效','正式N日績效','隔日命中')):
        counts=Counter();adjusted=Counter()
        for value in df[field] if field in df else ['']*len(df):
            doc=r.parse(value)
            state=doc.get('state') if doc.get('version')==VERSION else None
            counts[state if isinstance(state,str) else 'NOT_AUDITED']+=1
            adj=doc.get('adjusted') if doc.get('version')==VERSION else None
            if field==GAP_FIELDS[0]:adjusted[adj if isinstance(adj,str) else 'NOT_APPLICABLE']+=1
        out.append({'任務':label,'已核實可計算':counts['READY'],'尚未到期':counts['NOT_DUE'],'原始身分待證':counts['IDENTITY_PENDING'],'行情／日曆待證':counts['QUOTE_PENDING']+counts['CALENDAR_GAP'],'其他待證':sum(n for k,n in counts.items() if k not in ('READY','NOT_DUE','IDENTITY_PENDING','QUOTE_PENDING','CALENDAR_GAP','NOT_AUDITED')),'未鑑識':counts['NOT_AUDITED'],'還原已核對':adjusted['VERIFIED'] if field==GAP_FIELDS[0] else None,'還原待證':sum(n for k,n in adjusted.items() if k not in ('VERIFIED','NOT_APPLICABLE')) if field==GAP_FIELDS[0] else None})
    return pd.DataFrame(out)


def queue_from_rows(rows):
    import godpick_h250r7_tracking as r
    out=set()
    for row in rows:
        for field in GAP_FIELDS:
            d=r.parse(row.get(field))
            if d.get('version')!=VERSION:continue
            for x in d.get('missing',[]) if isinstance(d.get('missing'),list) else []:
                if isinstance(x,dict) and x.get('market') in ('TWSE','TPEX') and re.fullmatch(r'\d{4,6}',str(x.get('code',''))) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',str(x.get('day',''))):
                    out.add((x['market'],x['day'],x['code'],x.get('reason','')))
    return sorted(out)


def diagnostic_details(df):
    """Existing history rows with saved gaps; no invented date or identity."""
    import pandas as pd
    import godpick_h250r7_tracking as r
    items=[]
    for row in df.to_dict('records'):
        for field,label in zip(GAP_FIELDS,('推薦後績效','正式N日績效','隔日命中')):
            doc=r.parse(row.get(field));state=doc.get('state');adj=doc.get('adjusted')
            if doc.get('version')!=VERSION or not isinstance(state,str):continue
            if state in ('READY','NOT_DUE') and adj in (None,'VERIFIED'):continue
            reasons=doc.get('reasons') if isinstance(doc.get('reasons'),list) else []
            items.append({'record_id':r.text(row.get('record_id')),'股票代號':r.text(row.get('股票代號')),'原始推薦日期':r.text(row.get('推薦日期')),'任務':label,'診斷':state,'原因':'、'.join(str(z) for z in reasons),'還原查詢狀態':adj if isinstance(adj,str) else ''})
    return pd.DataFrame(items,columns=['record_id','股票代號','原始推薦日期','任務','診斷','原因','還原查詢狀態'])
