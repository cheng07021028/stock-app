# -*- coding: utf-8 -*-
"""H248 exact-source overlay generator. No project or remote mutation."""
from __future__ import annotations
from pathlib import Path
import argparse, hashlib, json, zipfile, tempfile
ROOT=Path(__file__).resolve().parent
SCHED='godpick_auto_scheduler.py'
WORKER='tools/run_godpick_auto_scheduler_v191.py'
VERDICT='tools/h159_manual_verdict.py'
PAGE='pages/17_系統健康檢查.py'
H244='godpick_h244_dispatch_truth.py'
BASE={SCHED:'4e1e77b4dfe45c94b6aa6b845a511b45db1dbb81',WORKER:'2754449c19f59e90aadb6d3769bcee64e6e6cb0b',VERDICT:'73c9b96c7479048f56f60e60a22b074ac5834987',PAGE:'1ee0f4b64fab8460f56ddb3940772f025eaa03f8',H244:'5a2d2b6d0699b6ff2cdc54254eabd40b663190e9'}
DUE_OLD='''def _h243_due_allowed(status, job, slot, now):
    # Bound same-slot warning retries, while a new scheduled slot is always due.
    row = (status.get("jobs") or {}).get(job) or {}
    if not isinstance(row, dict) or not _h243_retry_warning(job, row):
        return True
    if not _h243_same_due_warning(row, slot.strftime("%Y-%m-%d %H:%M")):
        return True
    last = _parse_status_datetime(row.get("last_run_at"))
    return last is None or (now-last).total_seconds() >= 20*60'''
DUE_NEW='''def _h248_plan(status, job, slot, now, cfg=None):
    from godpick_h248_retry_policy import decision, slot_record
    row = (status.get("jobs") or {}).get(job) or {}
    prior = slot_record(row, slot)
    ready = False
    if prior and prior.get("code")=="WAIT_DEPENDENCY" and cfg is not None:
        jc = (cfg.get("jobs") or {}).get(job) or {}
        # Re-evaluate blocked dependencies against the same authoritative state.
        # The original execution gates still run immediately before each job.
        ready, _ = _dependency_check(jc, status, now)
        if ready:
            ready, _ = _recommendation_final_gate(cfg, status, now, job, recommendation_slot=slot)
    return decision(job, row, slot, now, dependency_ready=ready)


def _h243_due_allowed(status, job, slot, now, cfg=None):
    # Shared by actual Worker, Page17 wakeup preview and next-run table.
    return _h248_plan(status, job, slot, now, cfg)["allow"]


def _h248_retry_candidates(cfg, status, now):
    if not cfg.get("enabled") or (cfg.get("weekdays_only", True) and now.weekday() >= 5):
        return []
    out=[]
    for job in _ordered_job_keys(cfg):
        jc=(cfg.get("jobs") or {}).get(job) or {}
        if not jc.get("enabled"): continue
        for slot in _due_slots(jc, now, int(cfg.get("grace_minutes",35)), catch_up_missed_same_day=bool(cfg.get("catch_up_missed_same_day",True))):
            if not _already_done(status,_run_key(job,slot)) and _h248_plan(status,job,slot,now,cfg)["retry"]:
                out.append(job)
                break
    return out'''
PATCHES={SCHED:[
(DUE_OLD,DUE_NEW),
('    attempts=max(1,retry_count+1); delay=int(global_cfg.get("retry_delay_seconds",20) or 20)', '''    # H248: scheduled retries are spaced across wakes, not repeated expensive
    # calls inside a single wake. Keep manual retry settings as an upper bound.
    from godpick_h248_retry_policy import immediate_retry_allowed
    attempts=1 if global_cfg.get("_h248_single_attempt") else max(1,retry_count+1)
    delay=int(global_cfg.get("retry_delay_seconds",20) or 20)'''),
('        if last.get("ok"): break\n        if n<attempts:', '        if last.get("ok") or not immediate_retry_allowed(job,last): break\n        if n<attempts:'),
('manual_batch_mode:str="") -> dict[str,Any]:\n    now=(now or now_tw()).astimezone(TZ); cfg=load_settings(); status=load_status(); history=load_history()', '''manual_batch_mode:str="", retry_only:bool=False) -> dict[str,Any]:
    from godpick_h248_retry_policy import record_result
    if retry_only and (force_all_enabled or temporary_enable_jobs or strict_same_run or manual_batch_mode or selected_jobs is not None or execution_order_override):
        raise ValueError("H248 selective retry cannot combine with force or override")
    now=(now or now_tw()).astimezone(TZ); cfg=load_settings(); status=load_status(); history=load_history()'''),
('    enabled_order=[job for job in _ordered_job_keys(cfg) if bool(((cfg.get("jobs") or {}).get(job) or {}).get("enabled",False)) and (not selected_jobs or job in selected_jobs)]', '''    # H248: decide only after the existing lock. An empty selective plan must
    # never fall through to the original 'all enabled' behavior.
    if retry_only:
        selected_jobs=_h248_retry_candidates(cfg,status,now)
    enabled_order=[job for job in _ordered_job_keys(cfg) if bool(((cfg.get("jobs") or {}).get(job) or {}).get("enabled",False)) and (not (selected_jobs or retry_only) or job in (selected_jobs or []))]'''),
('_h243_due_allowed(status,_job,_slot,now)) for _slot in _slots)', '_h243_due_allowed(status,_job,_slot,now,cfg)) for _slot in _slots)'),
('            if selected_jobs and job not in selected_jobs: continue\n            if not bool(job_cfg.get("enabled",False)): continue', '            if (selected_jobs or retry_only) and job not in (selected_jobs or []): continue\n            if not bool(job_cfg.get("enabled",False)): continue'),
('_h243_due_allowed(status,job,slot,now)): continue', '_h243_due_allowed(status,job,slot,now,cfg)): continue'),
('                    result=_execute_one(job,job_cfg,cfg)', '                    result=_execute_one(job,job_cfg,{**cfg,"_h248_single_attempt":not force_all_enabled})'),
('                executed.append(row); history.append({k:v for k,v in row.items() if k != "details"})\n                js=status["jobs"].setdefault(job,{})', '''                js=status["jobs"].setdefault(job,{})
                record_result(job,js,result,row,slot,force=force_all_enabled,simulate=simulate)
                if not simulate:
                    row["h248_diagnostic"]=dict(js.get("h248_diagnostic") or {})
                executed.append(row); history.append({k:v for k,v in row.items() if k != "details"})'''),
('        status["last_wakeup_result"]="COMPLETED" if overall else "COMPLETED_WITH_FAILURE"', '''        if retry_only:
            from godpick_h248_retry_policy import retry_receipt
            status["last_h248_retry_batch"]=retry_receipt(batch_id,planned_order,executed,summary,now_text())
            overall=bool(status["last_h248_retry_batch"]["all_success"])
        status["last_wakeup_result"]="COMPLETED" if overall else "COMPLETED_WITH_FAILURE"'''),
('        if not simulate:\n            active=status.get("active_run") if isinstance(status.get("active_run"),dict) else {}\n            remaining=', '''        if retry_only and not executed:
            status["last_wakeup_message"]="H248：本輪沒有符合安全條件的到期補跑；未執行任何工作。"
        if not simulate:
            active=status.get("active_run") if isinstance(status.get("active_run"),dict) else {}
            remaining='''),
('_h243_due_allowed(status,job,slot,now) for slot in slots)', '_h243_due_allowed(status,job,slot,now,cfg) for slot in slots)'),
('_h243_due_allowed(status,job,latest_past,now) and (catch_up', '_h243_due_allowed(status,job,latest_past,now,cfg) and (catch_up'),
('        rows.append({\n            "設定順位": configured_order.index(job)+1,', '''        from godpick_h248_retry_policy import display
        _h248_due=bool(cfg.get("enabled") and jc.get("enabled") and trading_day_today and latest_past and not _already_done(status,_run_key(job,latest_past)) and (catch_up or now<=latest_past+timedelta(minutes=grace)))
        _h248_plan_row=_h248_plan(status,job,latest_past,now,cfg) if latest_past else {}
        _h248_cols=display(job,js,latest_past,now,dependency_ready=_h248_plan_row.get("reason")=="DEPENDENCY_READY",due=_h248_due)
        if _h248_due and not _h248_plan_row.get("allow") and not (live_active and job in active_planned):
            due_state="H248｜"+_h248_cols["H248補跑資格"]
        rows.append({
            "設定順位": configured_order.index(job)+1,'''),
('"最後狀態":js.get("last_status","尚未執行"),"最後成功":js.get("last_success_at",""),"最後訊息":js.get("last_message","")', '"最後狀態":js.get("last_status","尚未執行"),"最後成功":js.get("last_success_at",""),"最後訊息":js.get("last_message",""),**_h248_cols'),
],WORKER:[
('    if manual_job in H156_MANUAL_BATCH_MODES:', '''    if manual_job == "h248_retry_due":
        # Reuse actual configured slots, not force keys or temporary enables.
        return run_due_jobs(retry_only=True)
    if manual_job in H156_MANUAL_BATCH_MODES:'''),
],VERDICT:[
('    if manual_job in {"full_refresh_and_recommend", "market_refresh_and_recommend"}:', '''    if manual_job == "h248_retry_due":
        from godpick_h248_retry_policy import verify_retry_receipt
        return verify_retry_receipt(status.get("last_h248_retry_batch"),run_id,str(os.environ.get("GITHUB_RUN_ATTEMPT") or status.get("last_wakeup_attempt") or "1"))
    if manual_job in {"full_refresh_and_recommend", "market_refresh_and_recommend"}:'''),
],PAGE:[
('''            if _next_rows:
                st.dataframe(pd.DataFrame(_next_rows), use_container_width=True, hide_index=True, height=480)''','''            if _next_rows:
                st.dataframe(pd.DataFrame(_next_rows), use_container_width=True, hide_index=True, height=480)
            st.markdown("#### H248｜智慧診斷與選擇性補跑")
            _h248_eligible=[r.get("工作ID") for r in _next_rows if r.get("H248可選擇補跑")]
            _h248_busy=bool(not _h168_fragment_live.get("available") or _h168_fragment_live.get("running",0) or _h168_fragment_live.get("queued",0))
            _h248_wait=time.time()-float(st.session_state.get("_h248_dispatch_epoch",0) or 0)<120
            st.caption("目前安全可補跑："+str(len(_h248_eligible))+" 項｜每正式時段最多3次實際呼叫、間隔20分鐘；新正式時段照常執行。資料待補證及封存未確認不重跑股神。")
            if _h248_busy: st.caption("GitHub Worker 忙碌或狀態未知，暫不重送。")
            if st.button("H248｜僅補跑符合條件的到期失敗／待續工作",key="h248_retry_due_btn",disabled=not _h248_eligible or _h248_busy or _h248_wait,use_container_width=True):
                st.session_state["_h248_dispatch_epoch"]=time.time()
                _h159_dispatch_central(manual_job="h248_retry_due",wake_source="page17_h248_retry_due")
                st.caption("Worker 接手後會重新檢查正式時段、歷史結果及冷卻上限；空計畫不等於全模組成功。")
            _h248_batch=_live_status.get("last_h248_retry_batch") or {}
            if isinstance(_h248_batch,dict) and _h248_batch.get("batch_id"):
                st.caption("最近選擇性補跑（遠端狀態）："+str(_h248_batch.get("batch_id"))+"｜"+str(_h248_batch.get("status"))+"｜"+str(_h248_batch.get("summary")))'''),
('''                st.success("目前沒有待補跑工作；中央排程等待下一個正式時段。")''','''                st.info("目前沒有符合到期執行條件的工作；可能仍有待補證、冷卻或人工查核項目，請看 H248 診斷欄。")'''),
],H244:[
("'market_refresh_and_recommend': '核心6項', '': '僅到期工作'", "'market_refresh_and_recommend': '核心6項', 'h248_retry_due': 'H248選擇性補跑', '': '僅到期工作'"),
("    if mode in MANUAL_MODES and str(batch.get('batch_id')) == rid + '.' + attempt and batch.get('batch_mode') == mode:", """    if mode == 'h248_retry_due':
        batch = status.get('last_h248_retry_batch') or {}
        if str(batch.get('batch_id')) == rid + '.' + attempt:
            summary = batch.get('summary') or {}
            valid = {'all_success': batch.get('all_success') is True}
    elif mode in MANUAL_MODES and str(batch.get('batch_id')) == rid + '.' + attempt and batch.get('batch_mode') == mode:"""),
("    if mode in MANUAL_MODES and not valid.get('all_success'):","    if (mode in MANUAL_MODES or mode == 'h248_retry_due') and not valid.get('all_success'):"),
]}


def blob(raw):return hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
def replace_once(s,old,new,name):
    if s.count(old)!=1:raise ValueError(f'{name}: patch anchor missing/duplicated: {old[:100]!r}')
    return s.replace(old,new,1)

def source(root,name):
    raw=(root/name).read_bytes(); normalized=raw.replace(b'\r\n',b'\n');s=normalized.decode('utf-8-sig')
    if BASE[name] in (blob(raw),blob(normalized)):return s
    if name==PAGE:
        # Accept the exact H244 overlay by reversing only its audited edits;
        # keep the user's H244 code in the actual H248 output.
        pairs=json.loads((ROOT/'h244_page17_anchors.json').read_text('utf-8'))
        prior=s
        for old,new in reversed(pairs):prior=replace_once(prior,new,old,name)
        if BASE[name] in (blob(prior.encode()),blob(b'\xef\xbb\xbf'+prior.encode())):return s
    raise ValueError(f'{name}: differs from audited Git blob {BASE[name]}; no changes made')

def transform(name,s):
    for old,new in PATCHES[name]:s=replace_once(s,old,new,name)
    compile(s,name,'exec');return s

def create(root,out):
    for name in ('godpick_h245_list_authority.py','godpick_h246_seal_consistency.py','godpick_h247_source_health.py'):
        if not (root/name).is_file():raise ValueError('Install actual H245/H246/H247 overlays first: missing '+name)
    if 'run_check(result, BASE_DIR)' not in (root/'godpick_auto_update_tasks.py').read_text('utf-8-sig') or 'fair_scope(scope, attempts' not in (root/'godpick_h222_financial_history.py').read_text('utf-8-sig'):
        raise ValueError('H246/H247 actual runtime integration missing')
    if (root/'godpick_h248_retry_policy.py').exists():raise ValueError('H248 helper already exists; avoid overwriting a newer version')
    payload={name:transform(name,source(root,name)).encode('utf-8') for name in PATCHES}
    for name in ('godpick_h248_retry_policy.py','tests/test_h248_scheduler_contract.py','H248_README.txt','H248_TEST_RESULTS.txt'):
        payload[name]=(ROOT/name).read_bytes()
        if name.endswith('.py'):compile(payload[name],name,'exec')
    payload['H248_SHA256SUMS.txt']=''.join(f'{hashlib.sha256(v).hexdigest()}  {k}\n' for k,v in payload.items()).encode()
    if out.exists():raise ValueError('Output already exists; rename it before generating another package')
    out.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=out.parent,suffix='.zip',delete=False) as f:tmp=Path(f.name)
    try:
        with zipfile.ZipFile(tmp,'w',zipfile.ZIP_DEFLATED) as z:
            for name,raw in payload.items():z.writestr(name,raw)
        with zipfile.ZipFile(tmp) as z:
            if z.testzip() is not None:raise ValueError('ZIP verification failed')
        tmp.replace(out)
    finally:tmp.unlink(missing_ok=True)
    print(f'[PASS] {len(PATCHES)} audited sources, {sum(map(len,PATCHES.values()))} exact anchors, compile and ZIP');print(out)

def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('project',type=Path);a=ap.parse_args()
    try:create(a.project.resolve(),ROOT/'StockApp_V191_H248_Overlay.zip')
    except Exception as exc:raise SystemExit('[BLOCKED] '+str(exc))
if __name__=='__main__':main()
