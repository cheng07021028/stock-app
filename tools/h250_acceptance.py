# -*- coding: utf-8 -*-
"""H250 read-only release acceptance. No dispatch, repair, data mutation or secret output."""
from __future__ import annotations
import argparse, hashlib, json, os, re, statistics, subprocess, time, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree as ET
TZ=timezone(timedelta(hours=8))
JOBS=('stock_master','macro_full','official_factors','super_ai_context','watchlist_runtime','record_latest_price','record_performance','recommend_list_performance','recommend_list_n_day','recommend_list_hits','t1_truth','feedback_learning','durability_retry','godpick_recommendation')
REQUIRED={'godpick_auto_scheduler.py','pages/17_系統健康檢查.py','tools/run_godpick_auto_scheduler_v191.py','godpick_scheduler_wakeup_service.py','godpick_auto_update_tasks.py','godpick_h138_unattended_bridge.py','godpick_h124_ownership_flow.py','godpick_h222_financial_history.py','tools/h159_manual_verdict.py','.github/workflows/godpick_auto_scheduler_v191.yml','.github/workflows/h250_acceptance.yml','tools/h250_acceptance.py','tests/test_h250_acceptance.py',*(f'godpick_h{n}_{s}.py' for n,s in [(244,'dispatch_truth'),(245,'list_authority'),(246,'seal_consistency'),(247,'source_health'),(248,'retry_policy'),(249,'queue')])}
REQUIRED_TESTS={'test_h247_source_contract.py','test_h248_scheduler_contract.py','test_h249_queue.py','test_h250_acceptance.py'}

def check(name,ok,detail):return {'gate':name,'status':'PASS' if ok else 'PENDING','detail':detail}
def digest(raw):return hashlib.sha256(raw).hexdigest()
def blob(raw):return hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
def jsonhash(x):return digest(json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).encode())
def local_manifest(root):
    errors=[];count=0
    try:
        m=json.loads((root/'h250_release_manifest.json').read_text('utf-8'));files=m['files']
        if m.get('version')!='V191-H250' or not isinstance(files,dict) or not REQUIRED.issubset(files):raise ValueError('manifest missing required files')
        for p,v in files.items():
            f=root/p
            if not f.resolve().is_relative_to(root.resolve()) or f.is_symlink():errors.append('unsafe '+p);continue
            try:
                raw=f.read_bytes();variants=(raw,raw.replace(b'\r\n',b'\n'))
                if not any(digest(b)==v['sha256'] and blob(b)==v['git_blob'] for b in variants):errors.append('hash mismatch '+p);continue
                if p.endswith('.py'):compile(raw,p,'exec')
                count+=1
            except Exception as e:errors.append(p+': '+type(e).__name__)
    except Exception as e:errors.append(type(e).__name__+': '+str(e)[:150])
    return check('RELEASE_SOURCE',not errors,{'verified_files':count,'errors':errors[:50]})

def git(root,*args):return subprocess.check_output(['git','-C',str(root),*args],stderr=subprocess.DEVNULL,timeout=90)
def compile_project(root):
    errors=[];count=0
    try:
        paths=git(root,'ls-files','-z','--','*.py').decode('utf-8').split('\0')
        for p in paths:
            if not p:continue
            try:compile((root/p).read_bytes(),p,'exec');count+=1
            except Exception as e:errors.append({'file':p,'error':type(e).__name__,'line':getattr(e,'lineno',None)})
    except Exception as e:errors.append({'error':type(e).__name__,'detail':'requires Git checkout'})
    return check('PROJECT_SYNTAX',count>0 and not errors,{'compiled':count,'errors':errors[:50]})

def junit_gate(path):
    try:
        root=ET.parse(path).getroot();cases=list(root.iter('testcase'))
        # Pytest class names may be module.Class; search complete class path.
        present={n for n in REQUIRED_TESTS if any(n[:-3] in x.get('classname','').split('.') for x in cases)}
        bad=[x.get('name') for x in cases if any(x.find(tag) is not None for tag in ('failure','error','skipped'))]
        actual={x.get('name') for x in cases if 'ActualScheduler' in x.get('classname','') and all(x.find(t) is None for t in ('failure','error','skipped'))}
        return check('INSTALLED_INTEGRATION',bool(cases) and present==REQUIRED_TESTS and not bad and len(actual)>=6,{'cases':len(cases),'actual_scheduler_passed':len(actual),'missing_suites':sorted(REQUIRED_TESTS-present),'failed_or_skipped':bad[:40]})
    except Exception as e:return check('INSTALLED_INTEGRATION',False,{'error':type(e).__name__,'reason':'no valid installed-project JUnit'})

class Snapshot:
    def __init__(self,root,ref):
        self.root=root;self.commit=git(root,'rev-parse','--verify',ref+'^{commit}').decode().strip()
        if not re.fullmatch('[0-9a-f]{40}',self.commit):raise ValueError('invalid commit')
        self.paths=set(git(root,'ls-tree','-r','--name-only','-z',self.commit).decode('utf-8').split('\0'))
    def read(self,path,max_bytes=50_000_000):
        if path not in self.paths:return None
        spec=self.commit+':'+path
        if int(git(self.root,'cat-file','-s',spec))>max_bytes:raise ValueError('file exceeds read budget '+path)
        return json.loads(git(self.root,'show',spec))

def pair_gate(s):
    full='godpick_latest_recommendations.json';anchor='godpick_latest_run_anchor.json';lst='godpick_recommend_list.json'
    try:
        values={p:s.read(p) for p in (full,anchor,lst)};states={p:s.read(p[:-5]+'_sync_state.json') for p in values}
        verified=all(values[p] is not None and isinstance(states[p],dict) and states[p].get('payload_hash')==jsonhash(values[p]) for p in values)
        a=values[anchor];f=values[full];l=values[lst]
        same=isinstance(a,dict) and isinstance(f,dict) and bool(a.get('run_id')) and a.get('run_id')==f.get('run_id') and a.get('recommendation_date')==f.get('recommendation_date') and a.get('full_snapshot_payload_hash')==jsonhash(f)
        return check('RECOMMENDATION_SNAPSHOT',verified and same and isinstance(l,list),{'all_payload_hashes_match':verified,'same_run_anchor_and_full':same,'anchor_day':a.get('recommendation_date') if isinstance(a,dict) else None,'full_day':f.get('recommendation_date') if isinstance(f,dict) else None,'list_count':len(l) if isinstance(l,list) else None,'scope':'payload/anchor integrity only; Page08/H126 independent'})
    except Exception as e:return check('RECOMMENDATION_SNAPSHOT',False,{'error':type(e).__name__})

def exact_rows(hist,identity):return [r for r in hist if isinstance(r,dict) and r.get('batch_id')==identity]
def identity(r):return str(r.get('id',''))+'.'+str(r.get('run_attempt','1'))
def histkey(r):return (r.get('job'),r.get('run_key'),r.get('status'))
def runtime_gates(s,main,runs,now):
    status=s.read('godpick_auto_scheduler_status.json') or {};h=s.read('godpick_auto_scheduler_history.json') or {};hist=h.get('records',[]) if isinstance(h,dict) else h
    if not isinstance(hist,list):raise ValueError('history invalid')
    cfg=s.read('data/config/godpick_auto_scheduler_settings.json') or {};enabled={j for j in JOBS if (cfg.get('jobs') or {}).get(j,{}).get('enabled') is True}
    byid={identity(r):r for r in runs if isinstance(r,dict)};floor=(now.date()-timedelta(days=7)).strftime('%Y%m%d');today=now.strftime('%Y%m%d')
    queues=[];uncertain=[];manual=[];days={};returned=0;count=0
    for p in sorted(s.paths):
        if not p.startswith('data/h249/queue/') or not p.endswith('.json') or not floor<=p[-13:-5]<=today:continue
        d=s.read(p);rs=d.get('requests') if isinstance(d,dict) else None
        if not isinstance(rs,dict) or d.get('version')!='V191-H249-DURABLE-QUEUE-1':raise ValueError('invalid queue schema')
        for r in rs.values():
            if not isinstance(r,dict):raise ValueError('invalid request')
            queues.append(r);state=r.get('state');owner=r.get('owner');rr=r.get('result') or []
            if state in ('REVIEW_REQUIRED','EXECUTION_RECORDED','INCOMPLETE') or (state=='CLAIMED' and byid.get(owner,{}).get('status')=='completed'):uncertain.append({'request':r.get('id'),'state':state})
            actual=exact_rows(hist,owner)
            run=byid.get(owner,{})
            if state=='VERIFIED' and r.get('execution_ok') is True and r.get('verification')=='REMOTE_VERIFIER_PASS' and r.get('mode')=='full_refresh_and_recommend' and run.get('status')=='completed' and run.get('conclusion')=='success' and run.get('event')=='workflow_dispatch' and len(rr)==len(actual)==14 and {x.get('job') for x in rr}==set(JOBS) and all(x.get('status')=='SUCCESS' for x in rr) and {histkey(x) for x in rr}=={histkey(x) for x in actual} and all((r.get('jobs') or {}).get(j,{}).get('state')=='RETURNED' for j in JOBS):manual.append(r.get('id'))
    for p in sorted(s.paths):
        m=re.fullmatch(r'data/h249/slots/(\d{8})/([a-z0-9_]+)_(\d{4})\.json',p)
        if not m or not floor<=m[1]<=today:continue
        count+=1
        if count>500:raise ValueError('journal audit budget exceeded; narrow period')
        d=s.read(p);rows=d.get('attempts') if isinstance(d,dict) else None
        if not isinstance(rows,list) or not rows or d.get('version')!='V191-H249-DURABLE-QUEUE-1':raise ValueError('invalid journal schema')
        last=rows[-1];owner=last.get('owner');job=m[2];day=datetime.strptime(m[1],'%Y%m%d').date();slot=f'{day.isoformat()} {m[3][:2]}:{m[3][2:]}'
        if last.get('state')!='RETURNED':uncertain.append({'slot':slot,'job':job,'state':last.get('state')});continue
        returned+=1;run=byid.get(owner,{})
        exact=[r for r in exact_rows(hist,owner) if histkey(r)==(job,job+'|'+slot,'SUCCESS') and r.get('batch_mode')=='scheduled']
        if day.weekday()<5 and day<now.date() and last.get('status')=='SUCCESS' and run.get('event')=='schedule' and run.get('status')=='completed' and run.get('conclusion')=='success' and len(exact)==1:days.setdefault(day.isoformat(),set()).add(job)
    full_days=sorted(d for d,j in days.items() if enabled and enabled.issubset(j));queue_ok=any(r.get('state')=='VERIFIED' and r.get('execution_ok') is True for r in queues) and not uncertain
    cur=status.get('last_summary') or {};jobs=status.get('jobs') or {};durations={}
    for j in JOBS:
        vals=[float(r['duration_seconds']) for r in hist if isinstance(r,dict) and r.get('job')==j and isinstance(r.get('duration_seconds'),(int,float)) and 0<=r['duration_seconds']<=86400]
        if vals:durations[j]={'n':len(vals),'median_seconds':round(statistics.median(vals),2),'max_seconds':round(max(vals),2)}
    gates=[check('DURABLE_QUEUE',queue_ok,{'recent_requests':len(queues),'slot_journals':count,'returned':returned,'unresolved':uncertain[:50]}),check('MANUAL_FULL_14',bool(manual),{'verified_full_requests':manual,'scope':'exact queue + history + Actions run/attempt'}),check('UNATTENDED_TWO_DAYS',cfg.get('enabled') is True and len(full_days)>=2 and enabled==set(JOBS),{'enabled_jobs':len(enabled),'complete_prior_weekdays':full_days,'coverage_by_day':{d:len(j) for d,j in days.items()},'scope':'scheduled Actions only, exact slot journal + matching remote history; excludes today, zero-job wakes and manual runs'}),check('LATEST_JOB_HEALTH',all(jobs.get(j,{}).get('last_status')=='SUCCESS' for j in JOBS),{'last_wakeup':status.get('last_wakeup_run_id'),'latest_wake_summary':cur,'jobs':{j:jobs.get(j,{}).get('last_status','MISSING') for j in JOBS}}),pair_gate(s)]
    # Preserve source gates; do not invent new coverage or certify a claim as raw revalidation.
    for name,path,wanted in [('H221','data/h221/health.json','VERIFIED_5_BOTH'),('H222','data/h222/health.json','VERIFIED_SCOPE')]:
        d=main.read(path) or {};gates.append(check(name+'_SOURCE_HEALTH',d.get('status')==wanted,{'reported_status':d.get('status'),'asof':d.get('asof'),'scope':'collector health claim; not raw evidence revalidation'}))
    return gates,{'history_duration_samples':durations,'note':'historical job duration only; not UI latency, API overhead or benchmark improvement'}

def deep_gates(root,s,now):
    sys.path.insert(0,str(root));out=[]
    try:
        from godpick_h127_sharded_records import load_snapshot
        from godpick_h246_seal_consistency import evaluate,digest as h246_digest
        head=s.read('godpick_records_sync_state.json')
        rows,msg=load_snapshot(head,get_json=lambda p,default:(s.read(p) if p in s.paths else default,'pinned git object'),record_hash_func=h246_digest)
        a=s.read('godpick_latest_run_anchor.json') or {}
        ctx=a.get('execution_context') or {}
        r=evaluate(s.read,ctx,rows,None)
        out.append(check('H127_H246_HISTORY_AUTHORITY',r.get('confirmed') is True,{'status':r.get('status'),'history_count':len(rows) if isinstance(rows,list) else None,'scope':'original shard/hash/same-run verifier'}))
    except Exception as e:out.append(check('H127_H246_HISTORY_AUTHORITY',False,{'error_type':type(e).__name__}))
    try:
        from godpick_h237_pipeline_acceptance import run,compact
        r=run(root,now,full=True,cloud_check=False);c=compact(r)
        out.append(check('INDEPENDENT_SOURCE_REVALIDATION',r.get('status')=='REVIEW_AVAILABLE' and r.get('trade_authority_changed') is False and not r.get('errors'),{'status':r.get('status'),'checks':c.get('checks'),'errors':c.get('errors'),'scope':'original H237 full raw reparse + H225/H226/H229; descriptive review not trading authorization'}))
    except Exception as e:out.append(check('INDEPENDENT_SOURCE_REVALIDATION',False,{'error_type':type(e).__name__}))
    return out

def actions(root,snapshot=None):
    import requests
    token=os.environ.get('GITHUB_TOKEN','');repo=os.environ.get('GITHUB_REPOSITORY','')
    if not token or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repo):raise ValueError('missing read-only Actions credentials')
    out=[]
    for p in range(1,4):
        r=requests.get(f'https://api.github.com/repos/{repo}/actions/workflows/godpick_auto_scheduler_v191.yml/runs',params={'per_page':100,'page':p},headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'},timeout=(5,20),allow_redirects=False)
        if r.status_code!=200:raise ValueError('Actions HTTP '+str(r.status_code))
        rows=r.json().get('workflow_runs');
        if not isinstance(rows,list):raise ValueError('invalid Actions response')
        out.extend(rows)
        if len(rows)<100:break
    # Routine 10-minute wakes may exceed 300 results over a weekend. Query
    # missing exact journal/queue identities, bounded, rather than guessing.
    if snapshot is not None:
        known={identity(r) for r in out};want=set();floor=(datetime.now(TZ).date()-timedelta(days=7)).strftime('%Y%m%d')
        for path in sorted(snapshot.paths):
            m=re.fullmatch(r'data/h249/(?:queue/(\d{8})\.json|slots/(\d{8})/[a-z0-9_]+_\d{4}\.json)',path)
            if not m or (m[1] or m[2])<floor:continue
            d=snapshot.read(path)
            if not isinstance(d,dict):continue
            if m[1]:candidates=(r.get('owner') for r in (d.get('requests') or {}).values() if isinstance(r,dict) and r.get('state') in ('VERIFIED','CLAIMED'))
            else:candidates=(r.get('owner') for r in (d.get('attempts') or [])[-1:] if isinstance(r,dict) and r.get('status')=='SUCCESS')
            want.update(v for v in candidates if isinstance(v,str) and re.fullmatch(r'\d+\.\d+',v) and v not in known)
        for v in sorted(want,key=lambda x:tuple(map(int,x.split('.'))),reverse=True)[:60]:
            rid,attempt=v.split('.');r=requests.get(f'https://api.github.com/repos/{repo}/actions/runs/{rid}/attempts/{attempt}',headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'},timeout=(5,20),allow_redirects=False)
            if r.status_code==200:
                row=r.json()
                if identity(row)==v and str(row.get('path','')).split('@')[0]=='.github/workflows/godpick_auto_scheduler_v191.yml':out.append(row)
    return out

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--out',type=Path,required=True);p.add_argument('--junit',type=Path);p.add_argument('--runtime-ref');p.add_argument('--main-ref',default='HEAD');p.add_argument('--deep',action='store_true',help='invoke original H127/H246/H237 read-only verifiers');a=p.parse_args();start=time.monotonic();root=a.root.resolve();now=datetime.now(TZ)
    gates=[local_manifest(root),compile_project(root),junit_gate(a.junit) if a.junit else check('INSTALLED_INTEGRATION',False,{'reason':'JUnit not supplied'})];metrics={};pins={}
    if a.runtime_ref:
        try:
            s=Snapshot(root,a.runtime_ref);m=Snapshot(root,a.main_ref);pins={'runtime_data':s.commit,'main':m.commit};g,metrics=runtime_gates(s,m,actions(root,s),now);gates+=g
            if a.deep:gates+=deep_gates(root,s,now)
            else:gates.append(check('INDEPENDENT_SOURCE_REVALIDATION',False,{'reason':'--deep not requested'}))
        except Exception as e:gates.append(check('LIVE_EVIDENCE',False,{'error_type':type(e).__name__,'reason':'pinned remote evidence unavailable or invalid'}))
    else:gates.append(check('LIVE_EVIDENCE',False,{'reason':'not requested; no production acceptance'}))
    # Never claim full unattended acceptance solely from script health and static tests.
    r={'version':'V191-H250','observed_at':now.isoformat(),'pins':pins,'status':'ALL_GATES_PASS' if all(g['status']=='PASS' for g in gates) else 'ACCEPTANCE_PENDING','gates':gates,'metrics':metrics,'audit_seconds':round(time.monotonic()-start,2),'trade_authority_changed':False}
    a.out.mkdir(parents=True,exist_ok=True);(a.out/'H250_acceptance.json').write_text(json.dumps(r,ensure_ascii=False,indent=2),'utf-8');print(json.dumps(r,ensure_ascii=False,indent=2));return 0 if r['status']=='ALL_GATES_PASS' else 2
if __name__=='__main__':raise SystemExit(main())
