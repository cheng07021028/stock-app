# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import sys, types
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import godpick_auto_scheduler as sched
import godpick_auto_update_tasks as tasks

TZ=ZoneInfo('Asia/Taipei')
NOW=datetime(2026,8,13,14,0,tzinfo=TZ)

# 1) Force-all FAILED job must remain pending and active_run must survive the batch.
orig_sched=(sched.load_settings,sched.load_status,sched.load_history,sched._execute_one,sched._checkpoint_runtime,sched._acquire_lock,sched._release_lock)
try:
    cfg=sched.normalize_settings(sched.DEFAULT_SETTINGS)
    cfg['enabled']=True; cfg['retry_count']=0
    for j in cfg['jobs'].values(): j['enabled']=False
    cfg['jobs']['stock_master']['enabled']=True
    status={'version':sched.VERSION,'jobs':{},'completed_run_keys':[]}; history=[]
    sched.load_settings=lambda:cfg; sched.load_status=lambda:status; sched.load_history=lambda:history
    sched._checkpoint_runtime=lambda *a,**k:None; sched._acquire_lock=lambda *a,**k:(True,''); sched._release_lock=lambda:None
    sched._execute_one=lambda *a,**k:{'ok':False,'message':'authority locked','finished_at':'2026-08-13 14:00:01'}
    out=sched.run_due_jobs(now=NOW,force_all_enabled=True)
    assert out['executed'][0]['status']=='FAILED',out
    assert status.get('active_run',{}).get('pending_jobs')==['stock_master'],status
    assert status.get('active_run',{}).get('mode')=='force_all',status

    # H10 correction: an unattended production wake must never be converted
    # into force-only retry mode.  The same job can still recover via its latest
    # missed production slot (same-day catch-up), while the failed force batch
    # remains diagnostic evidence only.
    sched._execute_one=lambda *a,**k:{'ok':True,'message':'recovered','finished_at':'2026-08-13 14:01:01'}
    resumed=sched.run_due_jobs(now=datetime(2026,8,13,14,1,tzinfo=TZ))
    assert resumed.get('resumed_force_batch') is False,resumed
    assert resumed['executed'][0]['status']=='SUCCESS',resumed
    assert '|FORCE|' not in resumed['executed'][0]['run_key'],resumed
    assert status.get('last_force_pending_jobs')==['stock_master'],status
    assert 'active_run' not in status,status
finally:
    sched.load_settings,sched.load_status,sched.load_history,sched._execute_one,sched._checkpoint_runtime,sched._acquire_lock,sched._release_lock=orig_sched

# 2) Page07 automation must authority-preflight before loading/running Page07.
orig_ensure=tasks._ensure_records_authority_safe
try:
    tasks._ensure_records_authority_safe=lambda:([{'record_id':'x'}]*1927,['recovered 1927'],True)
    fake=types.ModuleType('godpick_headless_page_loader')
    called={'load':0,'run':0}
    def load_page_namespace(*a,**k):
        called['load']+=1
        return {'_run_page07_automation_v191_h2':lambda cfg:(called.__setitem__('run',called['run']+1) or {'ok':True,'message':'ok','changed_files':[]})}
    fake.load_page_namespace=load_page_namespace
    old_mod=sys.modules.get('godpick_headless_page_loader')
    sys.modules['godpick_headless_page_loader']=fake
    out=tasks.task_auto_recommendation({})
    assert out.get('ok') is True,out
    assert called=={'load':1,'run':1},called
    assert out.get('details',{}).get('authority_preflight_count')==1927,out

    tasks._ensure_records_authority_safe=lambda:([],['still empty'],False)
    called={'load':0,'run':0}
    out2=tasks.task_auto_recommendation({})
    assert out2.get('ok') is False,out2
    assert called=={'load':0,'run':0},called
finally:
    tasks._ensure_records_authority_safe=orig_ensure
    if 'old_mod' in locals() and old_mod is not None: sys.modules['godpick_headless_page_loader']=old_mod
    else: sys.modules.pop('godpick_headless_page_loader',None)

print('PASS V191-H5/H10 | force failure preserved without hijacking production catch-up | Page07 authority preflight blocks unsafe scan and runs after recovery')
