# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import godpick_auto_scheduler as sched

TZ=ZoneInfo("Asia/Taipei")
NOW=datetime(2026,8,13,7,30,tzinfo=TZ)

def one_job_cfg():
    cfg=sched.normalize_settings(sched.DEFAULT_SETTINGS)
    cfg["enabled"]=True
    for job in cfg["jobs"].values():
        job["enabled"]=False
    cfg["jobs"]["stock_master"]["enabled"]=True
    cfg["jobs"]["stock_master"]["times"]=["07:30"]
    cfg["retry_count"]=0
    return cfg

orig=(sched.load_settings,sched.load_status,sched.load_history,sched._execute_one,sched._checkpoint_runtime,sched._acquire_lock,sched._release_lock)
try:
    cfg=one_job_cfg()
    status={"version":sched.VERSION,"jobs":{},"completed_run_keys":[]}
    history=[]
    sched.load_settings=lambda:cfg
    sched.load_status=lambda:status
    sched.load_history=lambda:history
    sched._checkpoint_runtime=lambda *a,**k:None
    sched._acquire_lock=lambda *a,**k:(True,"")
    sched._release_lock=lambda:None

    # Failure must NOT be marked complete, so the same due slot remains retryable.
    sched._execute_one=lambda *a,**k:{"ok":False,"message":"mock fail","finished_at":"2026-08-13 07:30:01"}
    first=sched.run_due_jobs(now=NOW)
    key="stock_master|2026-08-13 07:30"
    assert first["executed"][0]["status"]=="FAILED", first
    assert key not in status.get("completed_run_keys",[]), status

    # Next wakeup in the same grace window must retry and succeed.
    sched._execute_one=lambda *a,**k:{"ok":True,"message":"mock success","finished_at":"2026-08-13 07:31:00"}
    second=sched.run_due_jobs(now=datetime(2026,8,13,7,31,tzinfo=TZ))
    assert second["executed"][0]["status"]=="SUCCESS", second
    assert key in status.get("completed_run_keys",[]), status

    # Migrate a legacy V191 bad state where FAILED had already been stamped complete.
    status.clear(); status.update({"version":sched.VERSION,"jobs":{"stock_master":{"last_status":"FAILED","last_slot":"2026-08-13 07:30"}},"completed_run_keys":[key]})
    third=sched.run_due_jobs(now=datetime(2026,8,13,7,32,tzinfo=TZ))
    assert third.get("repaired_failed_run_keys")==1, third
    assert third["executed"] and third["executed"][0]["status"]=="SUCCESS", third

    # H10 correction: interrupted manual force-all remains diagnostic evidence,
    # but the next unattended wake executes the real production slot instead of
    # switching into FORCE retry-only mode.
    status.clear(); status.update({
        "version":sched.VERSION,"jobs":{},"completed_run_keys":[],
        "active_run":{"mode":"force_all","started_at":"2026-08-13 06:55:00","pending_jobs":["stock_master"],"completed_jobs":[],"failed_jobs":[],"blocked_jobs":[]},
    })
    resumed=sched.run_due_jobs(now=datetime(2026,8,13,7,33,tzinfo=TZ))
    assert resumed.get("resumed_force_batch") is False, resumed
    assert resumed["executed"] and resumed["executed"][0]["job"]=="stock_master", resumed
    assert "|FORCE|" not in resumed["executed"][0]["run_key"], resumed
    assert resumed.get("status",{}).get("last_force_pending_jobs")==["stock_master"], resumed

    print("PASS V191 scheduler resilience H10 | failed slot retryable | legacy bad key repaired | force diagnostics cannot hijack production")
finally:
    sched.load_settings,sched.load_status,sched.load_history,sched._execute_one,sched._checkpoint_runtime,sched._acquire_lock,sched._release_lock=orig
