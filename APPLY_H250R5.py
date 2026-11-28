# -*- coding: utf-8 -*-
"""R5 audited source patch. Default: build overlay; --apply: backup + apply exact files.
No runtime-data, tokens or business records are edited. GitHub main is never pushed here.
"""
from pathlib import Path
import argparse, hashlib, json, os, tempfile, zipfile, sys, shutil, subprocess
from datetime import datetime
ROOT=Path(__file__).resolve().parent
SCHED='godpick_auto_scheduler.py'; PAGE='pages/17_系統健康檢查.py'; WORKER='tools/run_godpick_auto_scheduler_v191.py'; VERDICT='tools/h159_manual_verdict.py'; SERVICE='godpick_scheduler_wakeup_service.py'; WF='.github/workflows/godpick_auto_scheduler_v191.yml'; QUEUE='godpick_h249_queue.py'; HELPER='godpick_h250r5_manual.py'; TEST='tests/test_h250r5_manual.py'
BASE=json.loads((ROOT/'H250R5_BASELINE.json').read_text('utf-8'))
QUEUE_BLOB='bc72cb989947361e66fa1423a10b9442d036a4c5'

PATCHES={
SCHED:[
('H156_MANUAL_BATCH_MODES = ("market_refresh_and_recommend", "full_refresh_and_recommend")','H156_MANUAL_BATCH_MODES = ("market_refresh_and_recommend", "full_refresh_and_recommend", "force_enabled_jobs")'),
('    if mode == "market_refresh_and_recommend":\n        return list(H156_CORE_REFRESH_AND_RECOMMEND)', '''    if mode == "force_enabled_jobs":
        from godpick_h250r5_manual import plan_enabled
        return plan_enabled(normalize_settings(settings), JOB_LABELS)
    if mode == "market_refresh_and_recommend":
        return list(H156_CORE_REFRESH_AND_RECOMMEND)'''),
('    now=(now or now_tw()).astimezone(TZ); cfg=load_settings(); status=load_status(); history=load_history()','''    now=(now or now_tw()).astimezone(TZ); cfg=load_settings(); status=load_status(); history=load_history()
    _r5_manual=manual_batch_mode == "force_enabled_jobs"
    if _r5_manual:
        from godpick_h250r5_manual import plan_enabled, verify_restored_settings
        if not force_all_enabled or temporary_enable_jobs or selected_jobs is not None or execution_order_override:
            raise ValueError("R5 手動全啟用不接受其他工作範圍或臨時啟用覆寫")
        _r5_raw=_read_local(SETTINGS_FILE,{})
        if os.environ.get("GITHUB_ACTIONS")=="true":
            verify_restored_settings(_r5_raw,BASE_DIR)
        selected_jobs=plan_enabled(cfg,JOB_LABELS,raw=_r5_raw)
        if not selected_jobs:
            return {"ok":False,"skipped":True,"executed":[],"message":"R5 沒有已啟用工作；未執行任何項目"}
        execution_order_override=list(selected_jobs)
        strict_same_run=True'''),
('    repaired_count=_repair_failed_completed_keys(status)\n    repaired_future_count=_repair_future_completed_keys(status, now)','''    # R5 manual state is isolated from the regular schedule's completion keys and last-slot retry state.
    _r5_scheduled_jobs=copy.deepcopy(status["jobs"]) if _r5_manual else None
    _r5_scheduled_keys=list(status["completed_run_keys"]) if _r5_manual else None
    repaired_count=0 if _r5_manual else _repair_failed_completed_keys(status)
    repaired_future_count=0 if _r5_manual else _repair_future_completed_keys(status, now)'''),
('                key=_run_key(job,slot,force=force_all_enabled)','''                key=_run_key(job,slot,force=force_all_enabled)
                if _r5_manual: key += "|" + batch_id'''),
('                    if deps_ok and manual_batch_mode == "full_refresh_and_recommend":','''                    if deps_ok and _r5_manual and job_cfg.get("require_dependencies",False):
                        _r5_done={r.get("job"):r.get("status") for r in executed}
                        _r5_missing=[d for d in (job_cfg.get("dependencies") or []) if _r5_done.get(d)!="SUCCESS"]
                        if _r5_missing:
                            deps_ok=False
                            deps_msg="R5 本輪自訂前置尚未成功："+"、".join(JOB_LABELS.get(d,d) for d in _r5_missing)
                    if deps_ok and manual_batch_mode in ("full_refresh_and_recommend","force_enabled_jobs"):'''),
('                "validation":h156_manual_batch_summary(executed, planned_jobs=list(planned_order)),','''                "validation":h156_manual_batch_summary(executed, planned_jobs=list(planned_order)),'''),
('        status["last_wakeup_result"]="COMPLETED" if overall else "COMPLETED_WITH_FAILURE"','''        if _r5_manual:
            from godpick_h250r5_manual import validation
            _r5_batch=status["last_manual_batch"]
            _r5_batch["validation"]=validation(executed,list(planned_order),H155_REQUIRED_REFRESH_JOBS)
            overall=bool(_r5_batch["validation"]["all_success"])
            status["last_force_enabled_batch"]=copy.deepcopy(_r5_batch)
            status["jobs"]=_r5_scheduled_jobs
            status["completed_run_keys"]=_r5_scheduled_keys
        status["last_wakeup_result"]="COMPLETED" if overall else "COMPLETED_WITH_FAILURE"'''),
],
WORKER:[('    if manual_job in H156_MANUAL_BATCH_MODES:', '''    if manual_job == "force_enabled_jobs":
        # The scheduler re-reads the restored settings exactly once, inside this invocation.
        _prior=os.environ.get("GODPICK_PAGE17_FULL_REFRESH")
        os.environ["GODPICK_PAGE17_FULL_REFRESH"]="1"
        try:
            return run_due_jobs(force_all_enabled=True,strict_same_run=True,manual_batch_mode=manual_job)
        finally:
            if _prior is None: os.environ.pop("GODPICK_PAGE17_FULL_REFRESH",None)
            else: os.environ["GODPICK_PAGE17_FULL_REFRESH"]=_prior
    if manual_job in H156_MANUAL_BATCH_MODES:''')],
PAGE:[('''        _force_confirm = _b3.checkbox("允許本次強制跑全部已啟用工作", value=False, help="會包含第7頁股神推薦，可能需要數分鐘；只用於部署驗證。Hotfix2 使用獨立 FORCE 執行鍵，不會佔用今天尚未到時的正式排程時段。")
        if st.button("🧪 強制執行全部已啟用工作（部署驗證）", disabled=not _force_confirm, use_container_width=True):
            _h159_dispatch_central(
                manual_job="full_refresh_and_recommend",
                wake_source="page17_h159_force_all_worker",
            )
            st.caption("本鍵保留全部已啟用工作、固定推薦最後；由 GitHub Worker 執行並永久保存，不在 Streamlit UI 跑大型任務。")''','''        _b3.caption("手動全啟用工作請使用下方獨立按鈕")
        from godpick_h250r5_manual import render_button as _r5_render_button
        _r5_render_button(st,_auto_cfg,AUTO_JOB_LABELS,dispatch_auto_scheduler_wakeup)'''),
('            _next_rows = auto_next_run_rows(_live_cfg, _live_status) if callable(auto_next_run_rows) else []','''            from godpick_h250r5_manual import render_status as _r5_render_status
            _r5_render_status(st,_live_status,AUTO_JOB_LABELS)
            _next_rows = auto_next_run_rows(_live_cfg, _live_status) if callable(auto_next_run_rows) else []''')],
VERDICT:[('    if not manual_job:\n        return True, "routine wake: explicit-manual verdict not applicable"','''    if not manual_job:
        return True, "routine wake: explicit-manual verdict not applicable"
    if manual_job == "force_enabled_jobs":
        from godpick_h250r5_manual import verify_receipt
        return verify_receipt(status,run_id,str(os.environ.get("GITHUB_RUN_ATTEMPT") or ""))'''),
('    print(("H159 VERIFIED: " if ok else "::error::H159 MANUAL FAILED: ") + detail)','''    if ok and job == "force_enabled_jobs":
        # The old verifier may check only the last history key. R5 must read every row of this run.
        try:
            import sys
            sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
            from godpick_h250r5_manual import verify_receipt
            from godpick_persistence_service import read_github_json
            remote,_=read_github_json("godpick_auto_scheduler_status.json",{})
            hist,_=read_github_json("godpick_auto_scheduler_history.json",{})
            ok,detail=verify_receipt(remote,str(os.environ.get("H159_EXPECTED_RUN_ID") or ""),str(os.environ.get("GITHUB_RUN_ATTEMPT") or ""),history=hist if isinstance(hist,(dict,list)) else {})
        except Exception as exc:
            ok,detail=False,"R5 remote verification unavailable: "+type(exc).__name__
    print(("H159 VERIFIED: " if ok else "::error::H159 MANUAL FAILED: ") + detail)''')],
QUEUE:[("MODES=frozenset(('full_refresh_and_recommend'", "MODES=frozenset(('force_enabled_jobs','full_refresh_and_recommend'"),
("    if mode in ('full_refresh_and_recommend','market_refresh_and_recommend'):","    if mode in ('full_refresh_and_recommend','market_refresh_and_recommend','force_enabled_jobs'):")],
}
# Remove identity-only anchor from mutation list.
PATCHES[SCHED]=[(a,b) for a,b in PATCHES[SCHED] if a!=b]
# Manual verdict imports helper before its main() inserts sys.path; put root on path at module import.
PATCHES[VERDICT].insert(0,('from pathlib import Path\n','from pathlib import Path\nimport sys\nsys.path.insert(0,str(Path(__file__).resolve().parents[1]))\n'))

def blob(data):return hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
def sha(data):return hashlib.sha256(data).hexdigest()
def replace(s,a,b,p):
    if s.count(a)!=1:raise ValueError(f'{p}: 定位點不唯一或缺少 {a[:100]!r}')
    return s.replace(a,b,1)
def change(s,pairs,p):
    for a,b in pairs:s=replace(s,a,b,p)
    return s

def apply_known(s,n,pairs,p):return change(s if n==244 else s.lstrip('\ufeff'),pairs,p)
def recover(raw,p):
    """Accept exact original Git blob or exact known prefix of H244..H250R1."""
    entry=BASE[p];expected=entry['blob'];steps=entry['steps'];data=raw.replace(b'\r\n',b'\n');s=data.decode('utf-8')
    if blob(data)==expected:return s,0
    for count in range(len(steps),0,-1):
        try:
            prior=s
            for n,pairs in reversed(steps[:count]):prior=change(prior,[(b,a) for a,b in reversed(pairs)],p)
            for v in (prior,prior.lstrip('\ufeff'),'\ufeff'+prior.lstrip('\ufeff')):
                if blob(v.encode())!=expected:continue
                check=v
                for n,pairs in steps[:count]:check=apply_known(check,n,pairs,p)
                if check==s:return s,count
        except ValueError:pass
    raise ValueError(f'{p}: 與正式來源或已知升級版不符 ({blob(data)})；未覆蓋')

def read(root,p):
    f=root/p
    if not f.is_file() or f.is_symlink() or not f.resolve().is_relative_to(root):raise ValueError('缺少或不安全的來源 '+p)
    return f.read_bytes()

def prepare(root):
    root=root.resolve();payload={};original={};prefix={}
    for p in (SCHED,PAGE,WORKER,VERDICT,SERVICE,WF):
        raw=read(root,p);original[p]=raw
        s,k=recover(raw,p);prefix[p]=k
        # Apply H244's receipt-aware dispatch/serialized workflow only if absent.
        if p in (SERVICE,WF) and k==0:
            n,pairs=BASE[p]['steps'][0];assert n==244
            s=apply_known(s,n,pairs,p)
        if p in PATCHES:s=change(s,PATCHES[p],p)
        if p==WF:
            s=replace(s,'description: "Optional single central job to force-run (H71 manual authority)"','description: "Manual mode: force_enabled_jobs (saved enabled jobs), full_refresh_and_recommend, market_refresh_and_recommend, or single job; blank = due only"',p)
        if p.endswith('.py'):compile(s,p,'exec')
        payload[p]=s.encode()
    # H249 queue helper may be present without installed hooks. Update it when present,
    # but never enable a previously uninstalled queue or bypass an installed one.
    if (root/QUEUE).exists():
        raw=read(root,QUEUE);original[QUEUE]=raw;canon=raw.replace(b'\r\n',b'\n')
        if blob(canon)!=QUEUE_BLOB:raise ValueError('H249 queue source differs; stop rather than overwrite newer queue')
        s=change(canon.decode('utf-8'),PATCHES[QUEUE],QUEUE);compile(s,QUEUE,'exec');payload[QUEUE]=s.encode()
    elif 'worker_dispatch' in payload[WORKER].decode() and 'from godpick_h249_queue import worker_dispatch' in payload[WORKER].decode():raise ValueError('H249 integrated worker missing queue helper')
    for p in (HELPER,TEST):
        data=(ROOT/p).read_bytes();compile(data,p,'exec')
        if (root/p).exists():
            raw=read(root,p);original[p]=raw
            if raw.replace(b'\r\n',b'\n')!=data:raise ValueError('R5 existing file differs: '+p)
        payload[p]=data
    # Manifest is optional on older live release. Preserve all unrelated hashes.
    mpath='h250_release_manifest.json'
    if (root/mpath).is_file():
        raw=read(root,mpath);original[mpath]=raw;m=json.loads(raw)
        if m.get('version')!='V191-H250' or not isinstance(m.get('files'),dict):raise ValueError('Unknown H250 release manifest')
        for p,data in payload.items():
            if p in m['files']:
                v=m['files'][p];old=original.get(p,b'');variants=(old,old.replace(b'\r\n',b'\n'))
                if not any(sha(x)==v.get('sha256') and blob(x)==v.get('git_blob') for x in variants):raise ValueError('Manifest source mismatch '+p)
            m['files'][p]={'sha256':sha(data),'git_blob':blob(data)}
        m['revision']='R5';payload[mpath]=(json.dumps(m,ensure_ascii=False,sort_keys=True,indent=2)+'\n').encode()
    # Do not apply unchanged files (e.g. old H244 service already integrated).
    return {p:d for p,d in payload.items() if original.get(p)!=d},original,prefix

def integration_test(root,payload):
    # Run the actual transformed scheduler/worker against isolated mock handlers,
    # not the real project's business data, tokens, or scheduler status.
    with tempfile.TemporaryDirectory(prefix='h250r5-integration-') as td:
        t=Path(td)
        for p in (SCHED,WORKER,HELPER,TEST,QUEUE,'godpick_h248_retry_policy.py'):
            if p in payload:data=payload[p]
            elif (root/p).is_file():data=read(root,p)
            else:continue
            target=t/p;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
        env={k:v for k,v in os.environ.items() if not (k.startswith('GITHUB_') or k.startswith('GODPICK_') or 'TOKEN' in k.upper() or 'SECRET' in k.upper() or 'FIREBASE' in k.upper() or k=='PYTHONPATH')}
        env['R5_REQUIRE_ACTUAL']='1';env['PYTHONUTF8']='1'
        subprocess.run([sys.executable,'-m','unittest','discover','-s','tests','-p','test_h250r5_manual.py','-v'],cwd=t,env=env,check=True,timeout=120)


def build_zip(payload,out):
    if out.exists():raise FileExistsError('輸出已存在，請重新命名 '+str(out))
    out.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=out.parent,suffix='.zip',delete=False) as f:tmp=Path(f.name)
    try:
        with zipfile.ZipFile(tmp,'w',zipfile.ZIP_DEFLATED) as z:
            for p,b in sorted(payload.items()):z.writestr(p,b)
        with zipfile.ZipFile(tmp) as z:
            if z.testzip() or set(z.namelist())!=set(payload):raise ValueError('ZIP verification failed')
        tmp.replace(out)
    finally:tmp.unlink(missing_ok=True)

def atomic(f,data):
    f.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=f.parent,delete=False) as t:tmp=Path(t.name);t.write(data);t.flush();os.fsync(t.fileno())
    try:os.replace(tmp,f)
    finally:tmp.unlink(missing_ok=True)

def apply(root,payload,original,backup):
    if backup.exists():raise FileExistsError('Backup exists '+str(backup))
    backup.mkdir(parents=True)
    for p in payload:
        if p in original:atomic(backup/p,original[p])
    atomic(backup/'R5_backup_manifest.json',(json.dumps({'existing':[p for p in payload if p in original],'new':[p for p in payload if p not in original]},ensure_ascii=False,indent=2)+'\n').encode())
    # Confirm all sources unchanged since prepare before touching project.
    for p in payload:
        f=root/p
        if p in original:
            if read(root,p)!=original[p]:raise ValueError('Source changed during preparation '+p)
        elif f.exists():raise ValueError('Unexpected target appeared '+p)
    done=[]
    try:
        for p,b in payload.items():
            atomic(root/p,b);done.append(p)
            if read(root,p)!=b:raise IOError('read-back mismatch '+p)
    except Exception:
        for p in reversed(done):
            if p in original:atomic(root/p,original[p])
            else:(root/p).unlink(missing_ok=True)
        raise
    return done

def main():
    a=argparse.ArgumentParser();a.add_argument('project',type=Path);a.add_argument('--out',type=Path);a.add_argument('--apply',action='store_true');a.add_argument('--stage',action='store_true',help='Stage only changed files in a Git checkout, requires --apply');args=a.parse_args();root=args.project.resolve()
    if args.stage and not args.apply:raise ValueError('--stage requires --apply')
    payload,original,prefix=prepare(root)
    if not payload:raise ValueError('No files to modify')
    integration_test(root,payload)
    out=(args.out or ROOT/'StockApp_V191_H250R5_Overlay.zip').resolve();build_zip(payload,out)
    if args.apply:
        backup=root.parent/(root.name+'_H250R5_Backup_'+datetime.now().strftime('%Y%m%d_%H%M%S_%f'))
        apply(root,payload,original,backup);print('[BACKUP]',backup)
        if args.stage:subprocess.run(['git','-C',str(root),'add','--',*payload],check=True)
    print('[PASS] Exact source hashes, anchors, compile and ZIP integrity. '+('Applied locally.' if args.apply else 'Overlay generated; project unchanged.'))
    for p in payload:print('  '+p)
    print('[OUTPUT]',out)
if __name__=='__main__':
    try:main()
    except Exception as exc:print('[BLOCKED]',str(exc));sys.exit(1)
