# -*- coding: utf-8 -*-
"""H250-R7: three scheduled Page10 tasks track H241 history, not Page07 current list.
Read-only archive evidence; exact-ID, metrics-only CAS. Raw and adjusted returns stay separate.
"""
from __future__ import annotations
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
import copy, json, re, time
from godpick_h250r2_nday import (TZ, PERIODS, COLS, PROOF, text, number, positive,
    identity, base_price, load_calendar, months, compute, verified_value)
ROOT=Path(__file__).resolve().parent
VERSION='H250R7_HISTORY_TRACKING_V1'
TASKS=('recommend_list_performance','recommend_list_n_day','recommend_list_hits')
RAW_NOW='官方原價目前報酬%'
RAW_DATE='官方原價行情日期'
RAW_PRICE='官方原價收盤價'
RAW_PROOF='H250R7_原價即時證據'
NPROOF='H250R7_N日原價證據'
N_COLS=tuple('官方原價'+c for c in COLS)
HIT_PROOF='H250R7_觸價證據'
_ARCHIVE=None


def compact(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)

def parse(v):
    try:
        x=json.loads(text(v));return x if isinstance(x,dict) else {}
    except (ValueError,TypeError):return {}

def valid_bar(r):
    # H190 row = date, open, high, low, close, ...
    try:
        o,h,l,c=[positive(v) for v in r[1:5]]
        return bool(all(v is not None for v in (o,h,l,c)) and l<=min(o,c)<=max(o,c)<=h)
    except (TypeError,ValueError):return False

class Archive:
    def __init__(self,cal,index,markets):
        self.cal=cal; self.latest=cal.days[-1] if cal.days else '';self.markets=markets
        self.bars={}
        for k,seq in index.items():
            counts=Counter(r[0] for r in seq)
            self.bars[k]={r[0]:(r[0],*(positive(v) for v in r[1:5])) for r in seq if counts[r[0]]==1 and valid_bar(r)}
        self.quotes={k:{d:float(r[4]) for d,r in seq.items()} for k,seq in self.bars.items()}
    def base(self,r,ident):
        # No latest-price / update timestamp fallback for historical baseline.
        _,m,c,d=ident
        return base_price(r) or self.quotes.get((m,c),{}).get(d)
    def span(self,ident):
        _,m,c,d=ident
        if not self.latest:return None,'CALENDAR_GAP'
        if d not in self.cal.dayset:return None,self.cal.reason(d)
        if any(x not in self.cal.cal for x in months(d,self.latest)):return None,'CALENDAR_GAP'
        ds=[x for x in self.cal.days if d<x<=self.latest]
        if not ds:return [],'NOT_DUE'
        bars=self.bars.get((m,c),{})
        if any(x not in bars for x in [d,*ds]):return None,'QUOTE_PENDING'
        return [bars[x] for x in ds],'READY'


def load_archive(root=ROOT,now=None):
    """Share the verified H190 index within a Worker; detect any changed manifest."""
    global _ARCHIVE
    import godpick_h190_market_archive as ar
    root=Path(root).resolve();now=now or datetime.now(TZ);path=root/'data/h190'
    _,sig=ar._archive_manifest(path)
    cache_key=(str(root),now.date(),now.hour>=19,(now.hour,now.minute)>=(18,15),sig)
    if _ARCHIVE and _ARCHIVE[0]==cache_key and time.monotonic()-_ARCHIVE[1]<1800:
        return _ARCHIVE[2]
    cal,markets,index,errors=ar._index(path,asof=now)
    if errors:raise ValueError('H190_ARCHIVE_ERROR '+str(errors[:3]))
    c=load_calendar(root,now)
    if not c.days or {m:set(v) for m,v in cal.items()}!=c.cal:raise ValueError('H190_CALENDAR_PENDING')
    # Only report a completed no-work state if this month's calendar was actually observed recently.
    month=now.date().isoformat()[:7];obs=c.observed.get(month)
    if obs is None or obs.date()<now.date()-timedelta(days=1):raise ValueError('H190_CALENDAR_STALE')
    if now.weekday()<5 and now.hour>=19 and obs.date()!=now.date():raise ValueError('H190_TODAY_CALENDAR_NOT_CONFIRMED')
    if ar._archive_manifest(path)[1]!=sig:raise ValueError('H190_ARCHIVE_CHANGED')
    a=Archive(c,index,markets);_ARCHIVE=(cache_key,time.monotonic(),a);return a


def current_delta(r,a,ident):
    _,m,c,d=ident
    if d not in a.cal.dayset:return {},a.cal.reason(d),False
    if any(x not in a.cal.cal for x in months(d,a.latest)):return {},'CALENDAR_GAP',False
    q=a.quotes.get((m,c),{});base=a.base(r,ident);latest=q.get(a.latest)
    if not positive(base):return {},'BASE_PENDING',False
    if not positive(q.get(d)) or not positive(latest):return {},'QUOTE_PENDING',False
    # The adjusted candidate return belongs to Page08/v77. Verify its date and
    # evidence labels, do not relabel a raw-price return as an adjusted one.
    source=text(r.get('績效資料來源'));asof=text(r.get('績效行情日期'))[:10]
    existing=number(r.get('目前損益幅%'))
    reused=(asof==a.latest and existing is not None and text(r.get('績效更新狀態'))=='OK'
            and 'Yahoo adjusted OHLC' in source and positive(r.get('最新價')) is not None
            and abs(positive(r.get('最新價'))-latest)<=max(.011,latest*1e-5))
    value=round((latest/base-1)*100,2)
    proof=compact({'version':VERSION,'identity':ident,'base':base,'day':a.latest,'close':latest,'value':value,'basis':'H190_RAW_UNADJUSTED_PRICE_RETURN_NOT_NET'})
    delta={RAW_NOW:value,RAW_DATE:a.latest,RAW_PRICE:latest,RAW_PROOF:proof}
    return ({k:v for k,v in delta.items() if r.get(k)!=v},'READY',reused)


def nday_delta(r,a,ident):
    # Project the official-only metrics into R2's validated formula internally.
    # NEVER overwrite legacy adjusted returns or train AI with mixed price basis.
    src=dict(r)
    for raw,legacy in zip(N_COLS,COLS):src[legacy]=r.get(raw)
    src[PROOF]=r.get(NPROOF)
    mature=[n for n in PERIODS if a.cal.span(ident[3],n)]
    if not mature:return {},a.cal.reason(ident[3]),False
    if not a.base(r,ident):return {},'BASE_PENDING',False
    d,reason=compute(src,a.cal,a.quotes,force=True)
    out={raw:d[legacy] for raw,legacy in zip(N_COLS,COLS) if legacy in d}
    if PROOF in d:out[NPROOF]=d[PROOF]
    # All mature periods must have either previously verified or newly computed evidence.
    merged=dict(src,**d)
    q=a.quotes.get((ident[1],ident[2]),{})
    pending=any(verified_value(merged,n,a.cal.now) is None or any(x not in q for x in a.cal.span(ident[3],n)) for n in mature)
    calendar_gap=(a.cal.reason(ident[3])=='CALENDAR_GAP')
    if calendar_gap:reason='CALENDAR_GAP'
    elif pending:reason='QUOTE_PENDING'
    elif not mature:reason='NOT_DUE'
    else:reason='READY'
    return {k:v for k,v in out.items() if r.get(k)!=v},reason,pending or calendar_gap


_LEVEL_RE=re.compile(r'^\s*(?:約\s*)?(\d+(?:\.\d+)?)\s*(?:[-－—～~至]\s*(\d+(?:\.\d+)?))?\s*(?:元)?\s*$')
def level(r,keys):
    for key in keys:
        s=text(r.get(key)).replace(',','');m=_LEVEL_RE.fullmatch(s)
        if m:
            lo=positive(m[1]);hi=positive(m[2] or m[1]);
            if lo and hi:return min(lo,hi),max(lo,hi)
    return None


def hits_delta(r,a,ident):
    bars,reason=a.span(ident)
    if not bars:return {},reason,False
    base=a.base(r,ident)
    if not positive(base):return {},'BASE_PENDING',False
    levels={
        '進場點命中':level(r,['預估進場點','股神進場區間','回測承接價','近端支撐','主要支撐','推薦價格','推薦日價格']),
        '突破價命中':level(r,['突破確認價_隔日','突破確認價','近端壓力']),
        '停損價觸發':level(r,['停損價_隔日','停損價','停損參考','失效價位']),
        '第一壓力命中':level(r,['第一壓力價','賣出目標1','近端壓力'])}
    out={}; dates={}
    for key,bound in levels.items():
        if bound is None:
            out[key]='未設定';out[key+'日期']='';dates[key]='';continue
        lo,hi=bound
        def hit(b):
            if key=='停損價觸發':return b[3]<=lo
            if key in ('突破價命中','第一壓力命中'):return b[2]>=hi
            return b[3]<=hi and b[2]>=lo
        d=next((b[0] for b in bars if hit(b)),'');dates[key]=d
        out[key]='是' if d else '否';out[key+'日期']=d
    for n,lab in ((1,'隔日'),(3,'3日'),(5,'5日'),(10,'10日')):
        if len(bars)>=n:
            out[lab+'最高漲幅%']=round((max(b[2] for b in bars[:n])/base-1)*100,2)
            out[lab+'最低回撤%']=round((min(b[3] for b in bars[:n])/base-1)*100,2)
    out['作戰追蹤狀態']=next((s for k,s in [('停損價觸發','已觸發停損'),('第一壓力命中','第一壓力命中'),('突破價命中','突破價命中'),('進場點命中','進場點命中')] if dates[k]),'追蹤中' if any(levels.values()) else '缺計畫價位')
    out['作戰追蹤資料源']='H190官方原價OHLC｜觸價非成交'
    same_day=bool(dates['進場點命中'] and dates['進場點命中']==dates['停損價觸發'])
    out['作戰命中摘要']='｜'.join(k+':'+out[k] for k in levels)+('｜同日進場/停損順序不明' if same_day else '')+'｜非實際成交'
    out[HIT_PROOF]=compact({'version':VERSION,'identity':ident,'base':base,'first':bars[0][0],'last':bars[-1][0],'sessions':len(bars),'levels':levels,'dates':dates,'basis':'H190_RAW_TOUCH_NOT_FILL','same_day_ambiguous':same_day})
    delta={k:v for k,v in out.items() if r.get(k)!=v}
    if delta:delta['作戰追蹤更新時間']=a.cal.now.isoformat(timespec='seconds')
    return delta,'READY' if any(levels.values()) else 'PLAN_PENDING',False


def build_plan(rows,a,kind,limit):
    stats=Counter();patch={};ids=Counter(text(r.get('record_id')) for r in rows)
    funcs={'recommend_list_performance':current_delta,'recommend_list_n_day':nday_delta,'recommend_list_hits':hits_delta}
    for r in rows:
        rid=text(r.get('record_id'));ident=identity(r,a.cal.now)
        if not ident or ids[rid]!=1:stats['IDENTITY_PENDING']+=1;continue
        delta,state,extra=funcs[kind](r,a,ident);stats[state]+=1
        if kind==TASKS[0]:
            if extra:stats['PAGE08_CURRENT_REUSED']+=1
            elif state=='READY':stats['ADJUSTED_PENDING']+=1
        if delta:
            if len(patch)<limit:patch[rid]=delta
            else:stats['DEFERRED']+=1
    return patch,dict(stats)


def exact_commit(rows,patch,sig,h,p):
    if not patch:return None
    allowed={RAW_NOW,RAW_DATE,RAW_PRICE,RAW_PROOF,NPROOF,*N_COLS,HIT_PROOF,'作戰追蹤狀態','作戰追蹤資料源','作戰命中摘要','作戰追蹤更新時間'}
    for name in ('進場點命中','突破價命中','停損價觸發','第一壓力命中'):
        allowed.update((name,name+'日期'))
    for lab in ('隔日','3日','5日','10日'):
        allowed.update((lab+'最高漲幅%',lab+'最低回撤%'))
    if any(not isinstance(d,dict) or not set(d)<=allowed for d in patch.values()):raise ValueError('R7_ILLEGAL_PATCH_FIELD')
    with p._RECORDS_LOCAL_LOCK,p._RECORDS_GITHUB_UPLOAD_LOCK:
        base,head,sha=h._base(sig)
        # Full record comparison in addition to CAS signature; never modify a different generation.
        if base!=rows:raise ValueError('H241_BASE_CHANGED')
        by={text(r.get('record_id')):r for r in base}
        if len(by)!=len(base) or not set(patch)<=set(by):raise ValueError('H241_ID_CHANGED')
        final=[dict(r,**patch[text(r.get('record_id'))]) if text(r.get('record_id')) in patch else r for r in base]
        return h._commit(base,head,sha,final,set(),'H250R7 history-only verified performance metrics')


def task(kind,cfg=None,*,root=ROOT):
    """Return scheduler contract. No source list/snapshot writes, no Page07 rerun."""
    from godpick_auto_update_tasks import _report
    t=time.perf_counter();cfg=cfg or {};title={TASKS[0]:'推薦後績效',TASKS[1]:'正式N日績效',TASKS[2]:'隔日命中追蹤'}[kind]
    try:
        import godpick_h241_github_records as h
        import godpick_persistence_service as p
        rows,msgs,_=h.ensure()
        if p.records_authority_verification_h240().get('status')!='VERIFIED' or not isinstance(rows,list) or not rows:
            return _report(False,'R7 '+title+'：08歷史權威未核實或無歷史；禁止回寫',details={'outcome':'AUTHORITY_PENDING'},started=t)
        rows=copy.deepcopy(rows);sig=p.records_authority_signature()
        a=load_archive(root)
        configured=cfg.get('max_rows',300) if kind==TASKS[1] else cfg.get('max_records',300)
        # H190 local calculation avoids per-stock HTTP. Keep bounded write size;
        # hit max_rows=6 was the old network ceiling, not a new archive ceiling.
        if kind==TASKS[2]:configured=cfg.get('archive_max_rows',300)
        limit=max(1,min(int(configured or 300),300))
        patches,stats=build_plan(rows,a,kind,limit)
        pending=sum(stats.get(k,0) for k in ('IDENTITY_PENDING','CALENDAR_GAP','QUOTE_PENDING','BASE_PENDING','PLAN_PENDING','ADJUSTED_PENDING','DEFERRED'))
        report=exact_commit(rows,patches,sig,h,p)
        ok=not patches or bool(report and report.local_ok and report.github_ok and report.permanent_ok)
        confirmed=len(patches) if ok else 0
        outcome=('PARTIAL_SAVED' if pending else 'VERIFIED_SAVED') if ok and patches else 'NO_NEW_DUE' if ok and not pending else 'EVIDENCE_PENDING' if ok else 'REMOTE_PENDING'
        details={'outcome':outcome,'authority_count':len(rows),'latest_official_day':a.latest,'confirmed':confirmed,'diagnostics':stats,'sync_messages':report.messages() if report else []}
        msg=f'R7 {title}｜08權威 {len(rows)} 筆／官方至 {a.latest}／永久確認 {confirmed} 筆／'+('、'.join(f'{k} {v}' for k,v in stats.items()) or '無新資料')
        if not ok:msg+='｜遠端未確認，不計成功'
        elif not patches and not pending:msg+='｜無新到期工作，正常略過'
        elif pending:msg+='｜尚有證據缺口或待續，不冒充全數完成'
        result=_report(ok,msg,details=details,changed_files=['godpick_records.json'] if confirmed else [],started=t,warning=bool(ok and pending))
        result['retry_pending']=bool(ok and stats.get('DEFERRED',0))
        return result
    except Exception as exc:
        return _report(False,'R7 '+title+'：'+type(exc).__name__+'｜'+str(exc)[:220]+'；未確認不算成功',started=t)


def install_handlers(module):
    """Replace exactly three handlers in the existing central Worker process.
    No edits to the user's source files, scheduler settings or unrelated handlers.
    """
    mapping=module.TASK_HANDLERS
    if not all(callable(mapping.get(k)) for k in TASKS):raise RuntimeError('R7 central task contract mismatch')
    def bind(k):
        def call(cfg=None):return task(k,cfg)
        call.__name__='h250r7_'+k
        return call
    mapping.update({k:bind(k) for k in TASKS})
