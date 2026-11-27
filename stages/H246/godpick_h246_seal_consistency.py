# -*- coding: utf-8 -*-
"""H246: strict same-run remote evidence; repair ONLY exact full snapshot, never history.
Atomic git fast-forward publishes payload and state in one commit. No forced push.
"""
from __future__ import annotations
import hashlib, json, os, re, subprocess, tempfile, time
from datetime import datetime, time as clock_time
from pathlib import Path
from zoneinfo import ZoneInfo

TW=ZoneInfo('Asia/Taipei')
LATEST='godpick_latest_recommendations.json'
ANCHOR='godpick_latest_run_anchor.json'
LIST='godpick_recommend_list.json'
RECORDS_STATE='godpick_records_sync_state.json'
MAX=96*1024*1024

def digest(x):
    return hashlib.sha256(json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
def state_name(p): return p.removesuffix('.json')+'_sync_state.json'
def valid(p,s):
    return p is not None and isinstance(s,dict) and type(s.get('revision')) is int and s['revision']>0 and s.get('payload_hash')==digest(p)
def result(status,**kw): return {'status':status,'confirmed':status=='REMOTE_CONSISTENT','trading_authority':False,**kw}
def text(v):
    if v is None or isinstance(v,(dict,list,bool)): return ''
    s=str(v).strip(); return '' if s.lower() in {'nan','none','nat','null','<na>'} else s
def same_run(p,ctx):
    if not isinstance(p,dict): return False
    c=p.get('execution_context')
    if not isinstance(c,dict): return False
    rid=ctx.get('run_id'); day=ctx.get('run_date')
    if not rid or not day or any(c.get(k)!=ctx.get(k) for k in ('run_id','run_date','started_at','owner','trigger')):return False
    return (p.get('recommendation_date')==day and all(p.get(k,rid)==rid for k in ('run_id','scan_run_id')) and p.get('run_date',day)==day)
def rows(p):
    if not isinstance(p,dict) or not isinstance(p.get('recommendations'),list): return None
    a=p['recommendations']; n=p.get('recommendation_count')
    return a if type(n) is int and n==len(a) and all(isinstance(r,dict) for r in a) else None
def identities(a,day):
    if not isinstance(a,list):return None
    out={}
    for r in a:
        if not isinstance(r,dict):return None
        rid=text(r.get('record_id')); key=(text(r.get('股票代號')),text(r.get('市場別')),text(r.get('推薦日期')))
        if not rid or rid in out or not all(key) or key[2]!=day:return None
        out[rid]=key
    return out

def evaluate(read,ctx,history=None,local=None):
    """read(path) reads all values from one pinned remote commit. Pure evaluation."""
    anchor=read(ANCHOR); ast=read(state_name(ANCHOR)); latest=read(LATEST); ls=read(state_name(LATEST)); current=read(LIST); cs=read(state_name(LIST)); head=read(RECORDS_STATE)
    if not valid(anchor,ast) or not same_run(anchor,ctx) or rows(anchor) is None:
        return result('ANCHOR_NOT_CURRENT_OR_UNVERIFIED')
    want=anchor.get('full_snapshot_payload_hash')
    if not isinstance(want,str) or not re.fullmatch('[a-f0-9]{64}',want):return result('ANCHOR_HASH_MISSING')
    if not valid(current,cs) or not isinstance(current,list):return result('LIST_UNVERIFIED')
    a=identities(rows(anchor),ctx['run_date']); b=identities(current,ctx['run_date'])
    if a is None or b is None or a!=b:return result('LIST_IDENTITY_CONFLICT',anchor_count=len(rows(anchor)),list_count=len(current))
    if not isinstance(head,dict) or type(head.get('count')) is not int or head['count']<0 or head.get('storage_mode')!='h127-gzip-shards-v1' or not re.fullmatch('[a-f0-9]{64}',str(head.get('payload_hash',''))):return result('HISTORY_HEAD_UNVERIFIED')
    # Confirm actual H241 rows and current head, including zero-list case; never add records.
    if not isinstance(history,list) or len(history)!=head['count'] or digest(history)!=head['payload_hash']:return result('HISTORY_CONTENT_UNVERIFIED')
    hist={}; dup=set()
    for r in history:
        if not isinstance(r,dict):return result('HISTORY_CONTENT_UNVERIFIED')
        rid=text(r.get('record_id'))
        if rid in hist:dup.add(rid)
        hist[rid]=r
    if any(rid in dup or rid not in hist or identities([hist[rid]],ctx['run_date'])!={rid:key} for rid,key in a.items()):return result('HISTORY_IDENTITY_CONFLICT')
    # Current snapshot must be the exact payload sealed by current anchor.
    def matching(p):return same_run(p,ctx) and rows(p) is not None and identities(rows(p),ctx['run_date'])==a and digest(p)==want
    if valid(latest,ls) and matching(latest):return result('REMOTE_CONSISTENT',recommendation_count=len(a),history_count=len(history),full_hash=want)
    # Do not repair over unknown/unverified state, conflicting same-day run, or newer state.
    if not valid(latest,ls) or not isinstance(latest,dict):return result('LATEST_REMOTE_UNVERIFIED')
    if not isinstance(local,dict) or not matching(local):return result('EXACT_FULL_SNAPSHOT_UNAVAILABLE',recommendation_count=len(a),expected_hash=want)
    try:
        remote_day=latest['recommendation_date']; remote_saved=latest['saved_at']; local_saved=local['saved_at']
        # Require explicit valid timestamps. Old day or strictly earlier time; never overwrite same timestamp.
        for v in (remote_saved,local_saved):datetime.strptime(v,'%Y-%m-%d %H:%M:%S')
        if remote_day!=remote_saved[:10] or ctx['run_date']!=local_saved[:10] or remote_saved>=local_saved or ls['revision']>=ast['revision']:return result('LATEST_NEWER_OR_CONFLICT')
    except (KeyError,TypeError,ValueError):return result('LATEST_DATE_UNVERIFIED')
    return result('EXACT_REPAIR_AVAILABLE',recommendation_count=len(a),expected_hash=want)

class GitView:
    def __init__(self,root):self.root=Path(root)
    def git(self,*args,data=None,env=None,timeout=40):
        p=subprocess.run(['git',*args],cwd=self.root,input=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,timeout=timeout,check=True)
        return p.stdout
    def fetch(self,branch):
        if branch not in ('main','runtime-data'):raise ValueError('invalid branch')
        self.git('fetch','--no-tags','origin','refs/heads/'+branch,timeout=90)
        return self.git('rev-parse','FETCH_HEAD').decode().strip()
    def blob(self,head,path):
        try:
            spec=head+':'+path
            if int(self.git('cat-file','-s',spec))>MAX: return None
            return self.git('show',spec)
        except (subprocess.CalledProcessError,ValueError):return None
    def read(self,head,path):
        raw=self.blob(head,path)
        try:return json.loads(raw) if raw is not None else None
        except (ValueError,UnicodeError):return None
    def publish(self,parent,files):
        if set(files)!={LATEST,state_name(LATEST)}:raise ValueError('repair scope violation')
        with tempfile.TemporaryDirectory() as tmp:
            env={**os.environ,'GIT_INDEX_FILE':str(Path(tmp)/'index'),'GIT_AUTHOR_NAME':'github-actions[bot]','GIT_AUTHOR_EMAIL':'41898282+github-actions[bot]@users.noreply.github.com','GIT_COMMITTER_NAME':'github-actions[bot]','GIT_COMMITTER_EMAIL':'41898282+github-actions[bot]@users.noreply.github.com'}
            self.git('read-tree',parent,env=env)
            for name,value in files.items():
                raw=json.dumps(value,ensure_ascii=False,separators=(',',':'),default=str).encode()
                if len(raw)>MAX:raise ValueError('snapshot too large')
                blob=self.git('hash-object','-w','--stdin',data=raw,env=env).decode().strip()
                self.git('update-index','--add','--cacheinfo','100644,'+blob+','+name,env=env)
            tree=self.git('write-tree',env=env).decode().strip()
            commit=self.git('commit-tree',tree,'-p',parent,'-m','H246 exact current-run full snapshot repair',env=env).decode().strip()
            # No force, no rebase, no retry with new parent. Concurrent updates reject this commit.
            self.git('push','origin',commit+':refs/heads/runtime-data',timeout=120)
        return commit

def run_check(result07,root,*,view=None,history_loader=None,allow_write=None):
    if not isinstance(result07,dict) or not result07.get('ok') or result07.get('record_integrity_failure'):return result('SCAN_OR_HISTORY_FAILED')
    ctx=result07.get('execution_context')
    if not isinstance(ctx,dict) or ctx.get('owner')!='07_股神推薦' or ctx.get('trigger')!='V191中央自動排程' or result07.get('execution_owner')!='pages/7_股神推薦.py':return result('NOT_CANONICAL_RUN')
    try:
        if datetime.now(TW).date().isoformat()!=ctx.get('run_date'):return result('STALE_RUN')
        v=view or GitView(root)
        if view is None:
            repo=os.environ.get('GITHUB_REPOSITORY',''); url=v.git('remote','get-url','origin').decode().strip()
            if not re.fullmatch(r'[\w.-]+/[\w.-]+',repo) or url not in ('https://github.com/'+repo,'https://github.com/'+repo+'.git'):return result('WORKER_ORIGIN_UNVERIFIED')
        if history_loader is None:
            from godpick_persistence_service import ensure_records_local_authority_current, records_authority_verification_h240
            def history_loader():
                a,_,_=ensure_records_local_authority_current()
                return a if records_authority_verification_h240().get('status')=='VERIFIED' else None
        parent=v.fetch('runtime-data'); read=lambda p:v.read(parent,p)
        history=history_loader()
        try:
            path=Path(root)/LATEST
            local=json.loads(path.read_bytes()) if path.is_file() and path.stat().st_size<=MAX else None
        except (OSError,ValueError):local=None
        check=evaluate(read,ctx,history,local)
        if check['confirmed']:
            return check if v.fetch('runtime-data')==parent else result('REMOTE_CHANGED_DURING_VERIFY')
        if check['status']!='EXACT_REPAIR_AVAILABLE':return check
        permitted=(os.environ.get('GITHUB_ACTIONS','').lower()=='true') if allow_write is None else allow_write
        if not permitted:return result('EXACT_REPAIR_REQUIRES_CENTRAL_WORKER')
        if v.fetch('runtime-data')!=parent:return result('REMOTE_CHANGED_BEFORE_REPAIR')
        anchor_state=read(state_name(ANCHOR)); stamp=datetime.now(TW)
        state={'version':'named_json_durable_v2','path':LATEST,'payload_hash':digest(local),'revision':max(time.time_ns(),anchor_state['revision']+1),'updated_at':stamp.strftime('%Y-%m-%d %H:%M:%S'),'updated_at_utc':stamp.astimezone(ZoneInfo('UTC')).isoformat(),'updated_at_epoch':stamp.timestamp(),'h246_repair_run_id':ctx['run_id']}
        commit=v.publish(parent,{LATEST:local,state_name(LATEST):state})
        observed=v.fetch('runtime-data')
        if observed!=commit:return result('REMOTE_CHANGED_AFTER_REPAIR')
        verified=evaluate(lambda p:v.read(observed,p),ctx,history)
        return {**verified,'repaired':bool(verified['confirmed']),'commit':commit}
    except Exception as exc:return result('REMOTE_CHECK_OR_REPAIR_UNCONFIRMED',error_type=type(exc).__name__)

def check_eod(root,day,*,now,checker,view=None):
    """Keep H117/H138 evidence checks; read newly published official files from main only."""
    if now.tzinfo is None or now.astimezone(TW).date()!=day or day.weekday()>=5:return checker(root,day,now=now)
    if now.astimezone(TW).time()<clock_time(18,15):return {'status':'WAIT_FOR_H117_OFFICIAL_EOD_WINDOW','confirmed':False}
    original=checker(root,day,now=now)
    if original.get('status')=='EOD_THREE_MARKETS_INTERNAL_HASH_VERIFIED':return original
    if view is None and os.environ.get('GITHUB_ACTIONS','').lower()!='true':return original
    try:
        v=view or GitView(root); head=v.fetch('main')
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp)
            for source in ('TWSE','TPEX','TAIEX'):
                rel='data/h117/daily/'+source+'/'+day.isoformat()+'.json.gz'; raw=v.blob(head,rel)
                if raw is None or len(raw)>20_000_000:return {**original,'missing':source,'h246_source':'MAIN_NOT_READY'}
                p=base/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
            return {**checker(base,day,now=now),'h246_source':'PINNED_MAIN','main_commit':head}
    except Exception as exc:return {**original,'h246_source':'MAIN_READ_UNCONFIRMED','error_type':type(exc).__name__}
