# -*- coding: utf-8 -*-
"""H249 durable manual queue and pre-invocation journal. No pick logic.
GitHub Contents CAS, fail closed. Not distributed exactly-once business commits.
"""
from __future__ import annotations
import base64, copy, hashlib, json, os, re, uuid, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote
import requests

VERSION='V191-H249-DURABLE-QUEUE-1'
TZ=timezone(timedelta(hours=8))
ROOT='data/h249'
MODES=frozenset(('full_refresh_and_recommend','market_refresh_and_recommend','h248_retry_due','stock_master','macro_full','official_factors','super_ai_context','watchlist_runtime','godpick_recommendation','record_latest_price','record_performance','recommend_list_performance','recommend_list_n_day','recommend_list_hits','t1_truth','feedback_learning','durability_retry'))
ID_RE=re.compile(r'\d{8}-[0-9a-f]{32}\Z')
RUN_RE=re.compile(r'\d+\.\d+\Z')
ACTIVE={'QUEUED','CLAIMED','REVIEW_REQUIRED','EXECUTION_RECORDED'}
TERMINAL={'VERIFIED','INCOMPLETE','EXPIRED'}
MAX_DAY=100
MAX_BYTES=700_000
MARKER=Path('.h249_worker_receipt.json')

class GuardError(RuntimeError): pass
class Conflict(GuardError): pass

def clock():return datetime.now(TZ)
def stamp(now=None):return (now or clock()).astimezone(TZ).isoformat(timespec='seconds')
def parsed(s):
    try:
        t=datetime.fromisoformat(s);return t.astimezone(TZ) if t.tzinfo else None
    except (ValueError,TypeError):return None

def daytag(now):return now.astimezone(TZ).strftime('%Y%m%d')
def qpath(day):
    if not re.fullmatch(r'\d{8}',day):raise GuardError('invalid queue date')
    return f'{ROOT}/queue/{day}.json'
def reqpath(rid):
    if not isinstance(rid,str) or not ID_RE.fullmatch(rid):raise GuardError('invalid request ID')
    return qpath(rid[:8])
def batch():
    value=str(os.getenv('GITHUB_RUN_ID',''))+'.'+str(os.getenv('GITHUB_RUN_ATTEMPT','1'))
    if not RUN_RE.fullmatch(value):raise GuardError('missing GitHub run identity')
    return value

def encode(obj):return json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')

class Store:
    """Small JSON files only; exact SHA CAS, no blind overwrite after conflict."""
    def __init__(self,token,owner,repo,*,http=None):
        if not token or not all(re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',str(x)) for x in (owner,repo)):raise GuardError('H249 missing GitHub Contents credentials')
        self.base=f'https://api.github.com/repos/{owner}/{repo}';self.http=http or requests.Session()
        self.headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'}
    @classmethod
    def worker(cls):return cls(os.getenv('GITHUB_TOKEN',''),os.getenv('GITHUB_REPO_OWNER',''),os.getenv('GITHUB_REPO_NAME',''))
    def _request(self,method,url,**kw):
        try:return getattr(self.http,method)(url,headers=self.headers,timeout=(5,15),allow_redirects=False,**kw)
        except Exception as exc:raise GuardError('H249 transport '+type(exc).__name__) from None
    def read(self,path):
        r=self._request('get',self.base+'/contents/'+quote(path,safe='/'),params={'ref':'runtime-data','_':time.time_ns()})
        if r.status_code==404:
            # Distinguish missing file from inaccessible/missing branch.
            branch=self._request('get',self.base+'/git/ref/heads/runtime-data')
            if branch.status_code!=200:raise GuardError('H249 runtime-data unavailable')
            return None,None
        if r.status_code!=200:raise GuardError('H249 read HTTP '+str(r.status_code))
        try:
            v=r.json();sha=v['sha'];raw=base64.b64decode(''.join(v['content'].split()),validate=True)
            if not re.fullmatch(r'[0-9a-f]{40}',sha) or len(raw)>MAX_BYTES or hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()!=sha:raise ValueError()
            obj=json.loads(raw)
            if not isinstance(obj,dict):raise ValueError()
            return obj,sha
        except Exception:raise GuardError('H249 invalid remote JSON') from None
    def put(self,path,obj,sha):
        raw=encode(obj)
        if len(raw)>MAX_BYTES:raise GuardError('H249 receipt capacity exceeded')
        body={'branch':'runtime-data','message':'H249 durable control receipt','content':base64.b64encode(raw).decode()}
        if sha:body['sha']=sha
        r=self._request('put',self.base+'/contents/'+quote(path,safe='/'),json=body)
        if r.status_code in (409,422):raise Conflict('H249 CAS conflict')
        if r.status_code not in (200,201):raise GuardError('H249 write HTTP '+str(r.status_code))
        observed,_=self.read(path)
        if observed!=obj:raise Conflict('H249 write readback changed; no execution')
    def change(self,path,fn):
        for _ in range(4):
            obj,sha=self.read(path);new,value=fn(copy.deepcopy(obj))
            if new==obj:return value
            try:self.put(path,new,sha);return value
            except Conflict:continue
        raise GuardError('H249 concurrent change; retry next wake')
    def terminal(self,identity):
        if not RUN_RE.fullmatch(str(identity)):return False
        rid,attempt=identity.split('.')
        r=self._request('get',self.base+'/actions/runs/'+rid+'/attempts/'+attempt)
        if r.status_code!=200:return False
        try:
            d=r.json();return str(d['id'])==rid and int(d['run_attempt'])==int(attempt) and d['status']=='completed'
        except Exception:return False

def queue_doc(obj,day):
    if obj is None:return {'version':VERSION,'day':day,'requests':{}}
    if obj.get('version')!=VERSION or obj.get('day')!=day or not isinstance(obj.get('requests'),dict) or len(obj['requests'])>MAX_DAY:raise GuardError('H249 invalid queue schema')
    for rid,r in obj['requests'].items():
        if not ID_RE.fullmatch(rid) or rid[:8]!=day or not isinstance(r,dict) or r.get('id')!=rid or r.get('mode') not in MODES or r.get('state') not in ACTIVE|TERMINAL or not parsed(r.get('created_at')) or not isinstance(r.get('jobs'),dict) or type(r.get('claims')) is not int or not 0<=r['claims']<=3:raise GuardError('H249 invalid request schema')
        if r['state'] in {'CLAIMED','EXECUTION_RECORDED','VERIFIED','INCOMPLETE','REVIEW_REQUIRED'} and not RUN_RE.fullmatch(str(r.get('owner'))):raise GuardError('H249 invalid owner')
        if any(j not in MODES or not isinstance(v,dict) or v.get('state') not in ('STARTED','RETURNED') for j,v in r['jobs'].items()):raise GuardError('H249 invalid job journal')
    return obj

def enqueue(store,mode,now=None):
    if mode not in MODES:raise GuardError('H249 unknown manual mode')
    now=now or clock();day=daytag(now);rid=day+'-'+uuid.uuid4().hex
    # Deduplicate pending same-mode across midnight. Same-day CAS covers competing sessions.
    oldday=daytag(now-timedelta(days=1));old,_=store.read(qpath(oldday))
    for r in queue_doc(old,oldday)['requests'].values():
        if r['mode']==mode and r['state'] in ACTIVE and (now-parsed(r['created_at']))<timedelta(hours=24):return r,False
    def update(obj):
        d=queue_doc(obj,day)
        for r in d['requests'].values():
            if r['mode']==mode and r['state'] in ACTIVE:return d,(r,False)
        if len(d['requests'])>=MAX_DAY:raise GuardError('H249 daily queue capacity reached; no deletion')
        r={'id':rid,'mode':mode,'created_at':stamp(now),'updated_at':stamp(now),'state':'QUEUED','owner':'','jobs':{},'claims':0}
        d['requests'][rid]=r;return d,(r,True)
    return store.change(qpath(day),update)

def change_request(store,rid,fn):
    path=reqpath(rid)
    def update(obj):
        d=queue_doc(obj,rid[:8]);r=d['requests'].get(rid)
        if not r:raise GuardError('H249 request not found')
        value=fn(r);return d,value
    return store.change(path,update)

def claim(store,rid,mode,identity,now=None):
    now=now or clock()
    if not RUN_RE.fullmatch(identity):raise GuardError('H249 invalid claim identity')
    def update(r):
        if mode and mode!=r['mode']:raise GuardError('H249 dispatch mode mismatch')
        if r['state']=='CLAIMED' and r['owner']==identity and not r['jobs']:return r
        if r['state']=='CLAIMED':
            if not store.terminal(r['owner']):return None
            if r['jobs']:
                r.update(state='REVIEW_REQUIRED',updated_at=stamp(now),reason='PREVIOUS_RUN_STARTED_BUSINESS_WORK');return None
            r.update(state='QUEUED',owner='',updated_at=stamp(now))
        if r['state']=='EXECUTION_RECORDED' and store.terminal(r['owner']):
            r.update(state='REVIEW_REQUIRED',updated_at=stamp(now),reason='REMOTE_VERIFICATION_NOT_RECORDED');return None
        if r['state']!='QUEUED':return None
        age=now-parsed(r['created_at'])
        if age<timedelta(0) or age>=timedelta(hours=24) or int(r['claims'])>=3:
            r.update(state='EXPIRED',updated_at=stamp(now),reason='AGE_OR_CLAIM_LIMIT');return None
        r.update(state='CLAIMED',owner=identity,claims=int(r['claims'])+1,updated_at=stamp(now));return r
    return change_request(store,rid,update)

def queue_candidates(store,now=None):
    now=now or clock();out=[]
    for day in (daytag(now-timedelta(days=1)),daytag(now)):
        d,_=store.read(qpath(day));out.extend(queue_doc(d,day)['requests'].values())
    return sorted(out,key=lambda r:(r['created_at'],r['id']))

def note_job(store,rid,identity,job,state):
    if job not in MODES or state not in ('STARTED','RETURNED'):raise GuardError('H249 invalid job event')
    def update(r):
        if r['state']!='CLAIMED' or r['owner']!=identity:raise GuardError('H249 claim ownership lost')
        prev=r['jobs'].get(job)
        if state=='STARTED' and prev is not None:raise GuardError('H249 duplicate manual job')
        if state=='RETURNED' and (not prev or prev.get('state')!='STARTED'):raise GuardError('H249 missing start')
        r['jobs'][job]={'state':state,'at':stamp()};r['updated_at']=stamp();return True
    return change_request(store,rid,update)

def scope_verified(mode,identity,rows,status):
    jobs=[x.get('job') for x in rows]
    if not jobs or len(jobs)!=len(set(jobs)) or any(x.get('status')!='SUCCESS' or not x.get('run_key') for x in rows):return False
    if mode=='h248_retry_due':
        from godpick_h248_retry_policy import verify_retry_receipt
        receipt=status.get('last_h248_retry_batch') or {};rid,attempt=identity.split('.')
        return verify_retry_receipt(receipt,rid,attempt)[0] and receipt.get('planned_jobs')==jobs
    if mode in ('full_refresh_and_recommend','market_refresh_and_recommend'):
        receipt=status.get('last_manual_batch') or {}
        return receipt.get('batch_id')==identity and receipt.get('batch_mode')==mode and receipt.get('planned_jobs')==jobs and (receipt.get('validation') or {}).get('all_success') is True
    return jobs==[mode]


def verify_remote_execution(store,rid,identity,read_json):
    """Independent exact run/attempt and every job row; a green wake is insufficient."""
    d,_=store.read(reqpath(rid));r=queue_doc(d,rid[:8])['requests'].get(rid) or {}
    if r.get('owner')!=identity or r.get('state')!='EXECUTION_RECORDED':return False
    status,_=read_json('godpick_auto_scheduler_status.json',{})
    hist,_=read_json('godpick_auto_scheduler_history.json',{})
    if not isinstance(status,dict):return False
    if str(status.get('last_wakeup_run_id'))+'.'+str(status.get('last_wakeup_attempt') or '1')!=identity or status.get('last_wakeup_result')!='COMPLETED':return False
    rows=r.get('result') or [];summary=status.get('last_summary') or {}
    if summary.get('executed')!=len(rows) or summary.get('success')!=len(rows) or any(summary.get(k)!=0 for k in ('warning','failed','blocked')) or summary.get('in_progress') is not False:return False
    if not scope_verified(r.get('mode'),identity,rows,status):return False
    hr=hist.get('records',[]) if isinstance(hist,dict) else hist
    if not isinstance(hr,list):return False
    matching=[x for x in hr if isinstance(x,dict) and x.get('batch_id')==identity]
    actual={(x.get('job'),x.get('run_key'),x.get('status')) for x in matching}
    expected={(x.get('job'),x.get('run_key'),x.get('status')) for x in rows}
    return bool(expected and actual==expected and len(matching)==len(rows))


def record_execution(store,rid,identity,res):
    rows=res.get('executed') if isinstance(res,dict) else None
    rows=rows if isinstance(rows,list) else []
    compact=[{'job':r.get('job'),'status':r.get('status'),'run_key':r.get('run_key')} for r in rows if isinstance(r,dict) and r.get('job') in MODES]
    clean=bool(compact and len(compact)==len(rows) and len({r['job'] for r in compact})==len(compact) and all(r['status']=='SUCCESS' for r in compact) and res.get('ok') is True and res.get('batch_id')==identity)
    def update(r):
        if r['state']!='CLAIMED' or r['owner']!=identity:raise GuardError('H249 claim ownership lost')
        clean_jobs=all(r['jobs'].get(x['job'],{}).get('state')=='RETURNED' for x in compact)
        scope_ok=scope_verified(r['mode'],identity,compact,res.get('status') or {})
        r.update(state='EXECUTION_RECORDED',updated_at=stamp(),result=compact,execution_ok=clean and clean_jobs and scope_ok,verification='PENDING_EXISTING_REMOTE_VERIFIER');return True
    return change_request(store,rid,update)

def finalize(store,rid,identity,verified):
    def update(r):
        if r['owner']!=identity or r['state']!='EXECUTION_RECORDED':raise GuardError('H249 no matching execution receipt')
        r.update(state='VERIFIED' if verified and r.get('execution_ok') is True else 'INCOMPLETE',verification='REMOTE_VERIFIER_PASS' if verified else 'REMOTE_VERIFIER_NOT_CONFIRMED',updated_at=stamp());return r['state']
    return change_request(store,rid,update)

def dispatch_page17(*,token,owner,repo,branch='main',manual_job='',wakeup_source='page17_auto_catchup',receipt=None,**kw):
    """Keep original wake path for routine wakes. Manual: durable acceptance first."""
    from godpick_scheduler_wakeup_service import dispatch_scheduler_wakeup as direct
    if receipt is not None:receipt.clear()
    kwargs=dict(token=token,owner=owner,repo=repo,branch=branch,manual_job=manual_job,wakeup_source=wakeup_source,**kw)
    if not manual_job:
        if receipt is not None:kwargs['receipt']=receipt
        return direct(**kwargs)
    if branch!='main':return False,'H249 永久派工僅允許已部署 main，未送出。'
    try:r,new=enqueue(Store(token,owner,repo),manual_job)
    except Exception as exc:return False,'H249 永久派工未確認；未呼叫Worker：'+type(exc).__name__
    rid=r['id'];local={}
    if receipt is not None:receipt.update(request_id=rid)
    if not new:return True,'H249 既有永久派工 '+rid+'｜'+r['state']+'；不重複派送。'
    kwargs['wakeup_source']='h249_'+rid
    if receipt is not None:kwargs['receipt']=local
    try:ok,msg=direct(**kwargs)
    except Exception:ok=False;msg='即時喚醒未確認'
    if receipt is not None:receipt.update(local,request_id=rid)
    return True,'H249 永久派工已確認 '+rid+'｜'+('即時喚醒已受理' if ok else '即時喚醒未確認，保留佇列供例行Worker接手')+'；尚未完成工作。'

def reconcile_orphans(store,read_json,now=None):
    """Read-only business reconciliation before a new wake replaces scheduler status.
    Only the queue receipt changes; never repeat a handler or write business data.
    """
    recovered=[]
    for r in queue_candidates(store,now):
        if r['state']!='EXECUTION_RECORDED' or not store.terminal(r['owner']):continue
        # The prior exact checkpoint must still be available. A newer heartbeat
        # or missing history is NOT evidence that the previous job failed/succeeded.
        if verify_remote_execution(store,r['id'],r['owner'],read_json):
            if finalize(store,r['id'],r['owner'],True)=='VERIFIED':recovered.append(r['id'])
    return recovered


def worker_dispatch(manual_job,direct,*,store=None):
    """Routine due work first; drain at most one durable request on an idle wake."""
    source=os.getenv('GODPICK_WAKEUP_SOURCE','');rid=source[5:] if source.startswith('h249_') else ''
    if source.startswith('h249_') and (not ID_RE.fullmatch(rid) or manual_job not in MODES):return {'ok':False,'message':'H249 invalid durable dispatch identity','executed':[]}
    if not rid and manual_job:return direct(manual_job) # existing GitHub manual / external clients
    if not os.getenv('GITHUB_RUN_ID') and store is None:return direct(manual_job)
    # Before a normal wake updates the latest status, try recovering interrupted
    # verification with existing remote evidence. Unavailable data never blocks
    # scheduled work merely because the manual queue cannot be read.
    if store is None:
        try:
            store=Store.worker()
            from godpick_persistence_service import read_github_json
            reconcile_orphans(store,read_github_json)
        except Exception as exc:
            print('H249 orphan verification not confirmed: '+type(exc).__name__)
    if not rid:
        result=direct('')
        if not isinstance(result,dict) or result.get('executed') or result.get('ok') is not True or (result.get('skipped') and '未啟用' not in str(result.get('message')) and '週末' not in str(result.get('message'))):return result
    else:result={'ok':False,'message':'H249 request not executed','executed':[]}
    try:
        store=store or Store.worker();identity=batch()
        candidates=[{'id':rid,'mode':manual_job}] if rid else queue_candidates(store)
        for item in candidates:
            if not rid and item.get('state') not in ('QUEUED','CLAIMED','EXECUTION_RECORDED'):continue
            # A not-yet-claimed explicit request is accepted only by exact durable ID.
            req=claim(store,item['id'],item.get('mode'),identity)
            if not req:continue
            # Marker before business work, used by the original workflow verifier step.
            MARKER.write_text(json.dumps({'id':req['id'],'owner':identity}),encoding='utf-8')
            os.environ['GODPICK_H249_REQUEST']=req['id']
            try:
                res=direct(req['mode']);record_execution(store,req['id'],identity,res);return res
            finally:os.environ.pop('GODPICK_H249_REQUEST',None)
        return result
    except GuardError as exc:return {**result,'ok':False,'message':'H249 queue guard: '+str(exc),'executed':result.get('executed',[])}

def journal_path(job,slot,force,identity):
    if job not in MODES or not RUN_RE.fullmatch(identity):raise GuardError('H249 invalid journal identity')
    return (f'{ROOT}/manual/{identity.split(".")[0]}/{job}.json' if force else f'{ROOT}/slots/{slot.strftime("%Y%m%d")}/{job}_{slot.strftime("%H%M")}.json')

def journal_begin(store,path,identity,prior,*,force=False):
    def update(d):
        if d is None:d={'version':VERSION,'attempts':[]}
        if d.get('version')!=VERSION or not isinstance(d.get('attempts'),list):raise GuardError('H249 invalid journal')
        rows=d['attempts']
        if rows:
            last=rows[-1]
            if force or last.get('state')!='RETURNED':raise GuardError('H249 prior invocation uncertain or manual rerun; review')
            if last.get('status')=='SUCCESS':raise GuardError('H249 prior success already recorded; no repeat')
            if not isinstance(prior,dict) or prior.get('batch_id')!=last.get('owner') or prior.get('attempts')!=len(rows)+int(d.get('legacy_attempts',0)) or prior.get('code') not in ('TRANSIENT','CONTINUATION'):raise GuardError('H249 scheduler checkpoint mismatch; review')
            if len(rows)+int(d.get('legacy_attempts',0))>=3:raise GuardError('H249 durable slot attempt limit')
            at=parsed(last.get('finished_at'))
            if not at or clock()-at<timedelta(minutes=20):raise GuardError('H249 durable cooldown')
        elif isinstance(prior,dict):
            # Migration: preserve historical H248 attempt count, never reset it.
            n=prior.get('attempts',0)
            if type(n) is not int or not 0<=n<3:raise GuardError('H249 legacy attempt limit/invalid count')
            d['legacy_attempts']=n
        if len(rows)+int(d.get('legacy_attempts',0))>=3:raise GuardError('H249 durable slot attempt limit')
        rows.append({'owner':identity,'state':'STARTED','started_at':stamp()});return d,True
    return store.change(path,update)

def journal_finish(store,path,identity,result):
    def update(d):
        if not isinstance(d,dict) or d.get('version')!=VERSION or not d.get('attempts'):raise GuardError('H249 no journal start')
        last=d['attempts'][-1]
        if last.get('owner')!=identity or last.get('state')!='STARTED':raise GuardError('H249 journal ownership lost')
        status='WARNING' if result.get('ok') and result.get('warning') else 'SUCCESS' if result.get('ok') else 'BLOCKED' if result.get('blocked') else 'FAILED'
        last.update(state='RETURNED',status=status,finished_at=stamp());return d,True
    return store.change(path,update)

def guarded_execute(job,slot,force,identity,status,invoke):
    # Local interactive/simulation use original path; GitHub central Worker only.
    if os.getenv('GITHUB_ACTIONS')!='true':return invoke()
    rid=os.getenv('GODPICK_H249_REQUEST','')
    try:
        store=Store.worker();path=journal_path(job,slot,force,identity)
        prior=((status.get('jobs') or {}).get(job) or {}).get('h248_scheduled') if not force else None
        if prior and prior.get('slot')!=slot.strftime('%Y-%m-%d %H:%M'):prior=None
        journal_begin(store,path,identity,prior,force=force)
        if rid:note_job(store,rid,identity,job,'STARTED')
    except GuardError as exc:return {'ok':False,'blocked':True,'message':'H249 永久認領未通過；未執行工作：'+str(exc)}
    result=invoke() # Exceptions leave STARTED. No blind replay.
    try:
        journal_finish(store,path,identity,result)
        if rid:note_job(store,rid,identity,job,'RETURNED')
    except GuardError as exc:
        return {**result,'ok':False,'warning':False,'message':str(result.get('message',''))+'｜H249 執行後永久回執未確認；禁止盲目重跑：'+str(exc)}
    return result

def queue_view(token,owner,repo):
    try:
        rows=queue_candidates(Store(token,owner,repo));return [{'派工ID':r['id'],'模式':r['mode'],'狀態':r['state'],'認領Run':r['owner'],'建立':r['created_at'],'已返回工作':sum(x.get('state')=='RETURNED' for x in r['jobs'].values()),'已開始未回報':sum(x.get('state')=='STARTED' for x in r['jobs'].values()),'回執':r.get('verification',r.get('reason',''))} for r in reversed(rows)],''
    except Exception as exc:return [],'H249 遠端佇列未確認：'+type(exc).__name__

def finalize_cli():
    if not MARKER.exists():
        if os.getenv('GODPICK_WAKEUP_SOURCE','').startswith('h249_'):
            print('::warning::H249 explicit request has no claimed receipt');return 1
        return 0
    try:
        m=json.loads(MARKER.read_text('utf-8'));identity=batch()
        if m['owner']!=identity:raise GuardError('H249 local marker belongs to another run')
        from godpick_persistence_service import read_github_json
        store=Store.worker()
        verified=os.getenv('H249_VERIFY')=='success' and verify_remote_execution(store,m['id'],identity,read_github_json)
        state=finalize(store,m['id'],identity,verified);print('H249 durable queue receipt: '+state);return 0 if state=='VERIFIED' else 1
    except Exception as exc:print('::warning::H249 final receipt unconfirmed: '+type(exc).__name__);return 1

if __name__=='__main__':raise SystemExit(finalize_cli())
