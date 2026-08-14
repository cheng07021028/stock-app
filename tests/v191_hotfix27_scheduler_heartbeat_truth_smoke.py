# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import godpick_auto_scheduler as scheduler
import tools.verify_godpick_scheduler_remote_v191 as verifier

def _enabled_cfg():
    cfg=scheduler.normalize_settings(scheduler.DEFAULT_SETTINGS)
    cfg['enabled']=True
    cfg['weekdays_only']=True
    return cfg

def test_weekend_wakeup_persists_heartbeat(monkeypatch):
    calls=[]
    monkeypatch.setattr(scheduler,'load_settings',lambda:_enabled_cfg())
    monkeypatch.setattr(scheduler,'load_status',lambda:{})
    monkeypatch.setattr(scheduler,'load_history',lambda:[])
    monkeypatch.setattr(scheduler,'_persist_runtime',lambda p,x,r:(calls.append((p,dict(x),r)) or (True,'ok')))
    monkeypatch.setenv('GITHUB_RUN_ID','31849842924')
    monkeypatch.setenv('GITHUB_RUN_ATTEMPT','1')
    monkeypatch.setenv('GODPICK_WAKEUP_EVENT','schedule')
    res=scheduler.run_due_jobs(now=datetime(2026,8,15,7,17,tzinfo=scheduler.TZ))
    assert res['ok'] and res['skipped'] and '週末' in res['message']
    assert len(calls)==1
    payload=calls[0][1]
    assert payload['last_wakeup_at']=='2026-08-15 07:17:00'
    assert payload['last_wakeup_source']=='schedule'
    assert payload['last_wakeup_run_id']=='31849842924'
    assert payload['last_wakeup_result']=='SKIPPED_WEEKEND'

def test_verifier_strict_only_when_central_workflow_opts_in(monkeypatch):
    stale={'last_wakeup_at':'2026-08-14 23:26:59','updated_at':'2026-08-14 23:26:59','last_wakeup_run_id':'old'}
    monkeypatch.setenv('GODPICK_EXPECTED_WAKEUP_RUN_ID','new')
    monkeypatch.setattr(verifier,'_read_local',lambda rel,default:dict(stale))
    monkeypatch.setattr(verifier,'_read_remote',lambda rel,default:(dict(stale),'ok'))
    ok,msg=verifier.verify_status()
    assert not ok and 'not current workflow' in msg

def test_page17_weekend_truth_ui():
    src=(ROOT/'pages'/'17_系統健康檢查.py').read_text(encoding='utf-8')
    assert 'SKIPPED_WEEKEND' in src
    assert '這不是排程故障' in src
    assert 'last_wakeup_run_id' in src

def test_central_workflow_current_run_contract():
    src=(ROOT/'.github'/'workflows'/'godpick_auto_scheduler_v191.yml').read_text(encoding='utf-8')
    assert 'GODPICK_EXPECTED_WAKEUP_RUN_ID' in src
    assert 'github.run_id' in src
