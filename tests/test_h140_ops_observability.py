# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import Mock
import base64
import json

from godpick_h134_decision_vault import _json_bytes
from godpick_h140_observability import (SCHEMA, STAGES, _read, diagnose, read_remote_monitor)
from tools.h140_ops import main

DAY = date(2026,10,20)
VALID = {
    'H136': 'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY',
    'H137': 'INTERNAL_CHECK_PASS_NOT_PIT',
    'H138': 'READY_FOR_MANUAL_RESEARCH_NOT_TRADES',
    'H139_RECOVERY': 'ALREADY_READY_NO_RETRY',
    'H139_SETTLEMENT': 'RESEARCH_SETTLED_NO_NEW_MATURITY',
}


def populate(root: Path, *, day=DAY, overrides=None):
    overrides = overrides or {}
    for stage,(filename,schema,key,_) in STAGES.items():
        obj={'schema':schema, key:day.isoformat(),'status':overrides.get(stage,VALID[stage])}
        path=root/filename
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(_json_bytes(obj))


def test_everything_present_is_only_internal_ok(tmp_path):
    populate(tmp_path)
    r=diagnose(runtime_root=tmp_path,asof=DAY)
    assert r['pipeline_stages_ok']==5
    assert r['status']=='INTERNAL_PIPELINE_READY_NOT_LIVE_ATTESTED'
    assert r['pit_status']=='NOT_INDEPENDENTLY_ATTESTED'


def test_missing_all_is_blocked(tmp_path):
    r=diagnose(runtime_root=tmp_path,asof=DAY)
    assert r['status']=='BLOCKED_NEEDS_OPERATOR'
    assert len(r['findings'])==5
    assert all(v['state']=='MISSING' for v in r['stages'].values())


def test_missing_one_still_blocks(tmp_path):
    populate(tmp_path)
    (tmp_path/STAGES['H139_SETTLEMENT'][0]).unlink()
    r=diagnose(runtime_root=tmp_path,asof=DAY)
    assert r['pipeline_stages_ok']==4
    assert r['status']=='BLOCKED_NEEDS_OPERATOR'


def test_old_date_not_falsely_green(tmp_path):
    populate(tmp_path,day=DAY-timedelta(days=1))
    r=diagnose(runtime_root=tmp_path,asof=DAY)
    assert all(v['state']=='STALE' for v in r['stages'].values())


def test_future_date_quarantined(tmp_path):
    populate(tmp_path,day=DAY+timedelta(days=1))
    assert diagnose(runtime_root=tmp_path,asof=DAY)['pipeline_stages_ok']==0


def test_bad_stage_status_blocks(tmp_path):
    populate(tmp_path,overrides={'H137':'BLOCKED_REQUIRES_ATTENTION'})
    r=diagnose(runtime_root=tmp_path,asof=DAY)
    assert r['stages']['H137']['state']=='BLOCKED'
    assert not r['stages']['H137']['recommended_action']==''


def test_manual_closure_not_success(tmp_path):
    populate(tmp_path)
    r=diagnose(runtime_root=tmp_path,asof=DAY,market_closed=True)
    assert r['status']=='MARKET_CLOSED_MANUAL_OVERRIDE_NOT_ATTESTED'


def test_invalid_json_fail_closed(tmp_path):
    populate(tmp_path)
    (tmp_path/STAGES['H137'][0]).write_text('{bad',encoding='utf-8')
    assert diagnose(runtime_root=tmp_path,asof=DAY)['stages']['H137']['state']=='INVALID'


def test_noncanonical_json_rejected(tmp_path):
    populate(tmp_path)
    path=tmp_path/STAGES['H137'][0]
    value=json.loads(path.read_bytes())
    path.write_text(json.dumps(value,indent=3,ensure_ascii=False),encoding='utf-8')
    assert diagnose(runtime_root=tmp_path,asof=DAY)['stages']['H137']['state']=='NONCANONICAL'


def test_oversize_rejected(tmp_path):
    populate(tmp_path)
    (tmp_path/STAGES['H137'][0]).write_bytes(b'x'*101000)
    assert diagnose(runtime_root=tmp_path,asof=DAY)['stages']['H137']['state']=='UNSAFE_SIZE'


def test_symlink_never_followed(tmp_path):
    populate(tmp_path)
    path=tmp_path/STAGES['H137'][0];other=tmp_path/'elsewhere.json'
    other.write_bytes(path.read_bytes());path.unlink();path.symlink_to(other)
    assert diagnose(runtime_root=tmp_path,asof=DAY)['stages']['H137']['state']=='MISSING'


def test_does_not_echo_secrets_in_receipt(tmp_path):
    populate(tmp_path)
    report=diagnose(runtime_root=tmp_path,asof=DAY,workflow_run_id='1234',source_ref='main')
    assert report['workflow_run_id']=='1234'
    assert 'token' not in json.dumps(report).lower()


def test_bad_run_id_suppressed(tmp_path):
    r=diagnose(runtime_root=tmp_path,asof=DAY,workflow_run_id='abc:SECRET')
    assert r['workflow_run_id']==''


def test_cli_writes_only_monitor(tmp_path):
    populate(tmp_path)
    before=list(sorted(p for p in tmp_path.rglob('*') if p.is_file()))
    assert main(['--runtime-root',str(tmp_path),'--asof',DAY.isoformat()])==0
    after=list(sorted(p for p in tmp_path.rglob('*') if p.is_file()))
    assert set(after)-set(before)=={tmp_path/'data/h140/H140_monitor.json'}
    assert json.loads((tmp_path/'data/h140/H140_monitor.json').read_bytes())['schema']==SCHEMA


def test_remote_unconfigured():
    assert read_remote_monitor(config={},asof=DAY)['status']=='NOT_CONFIGURED'


def test_remote_non200_blocked():
    t=Mock();t.get.return_value.status_code=404
    cfg={'token':'dummy','owner':'foo','repo':'bar','branch':'runtime-data'}
    assert read_remote_monitor(config=cfg,asof=DAY,transport=t)['status']=='REMOTE_NOT_AVAILABLE'


def test_remote_read_from_runtime_branch(tmp_path):
    populate(tmp_path)
    item=diagnose(runtime_root=tmp_path,asof=DAY)
    payload={'encoding':'base64','content':base64.b64encode(_json_bytes(item)).decode()}
    t=Mock();t.get.return_value.status_code=200;t.get.return_value.json.return_value=payload
    cfg={'token':'dummy','owner':'foo','repo':'bar','branch':'runtime-data'}
    result=read_remote_monitor(config=cfg,asof=DAY,transport=t)
    assert result['status']=='INTERNAL_PIPELINE_READY_NOT_LIVE_ATTESTED'
    assert t.get.call_args.kwargs['params']['ref']=='runtime-data'
    assert t.get.call_args.kwargs['allow_redirects'] is False


def test_remote_previous_day_is_not_accepted(tmp_path):
    populate(tmp_path)
    item=diagnose(runtime_root=tmp_path,asof=DAY)
    item['asof_tw']=(DAY-timedelta(days=1)).isoformat()
    t=Mock();t.get.return_value.status_code=200
    t.get.return_value.json.return_value={'encoding':'base64','content':base64.b64encode(_json_bytes(item)).decode()}
    cfg={'token':'dummy','owner':'foo','repo':'bar','branch':'runtime-data'}
    assert read_remote_monitor(config=cfg,asof=DAY,transport=t)['status']=='OUTDATED_OR_FUTURE_MONITOR'


def test_remote_unsupported_status_blocked(tmp_path):
    populate(tmp_path)
    item=diagnose(runtime_root=tmp_path,asof=DAY)
    item['status']='DEPLOYED_SUCCESS'
    t=Mock();t.get.return_value.status_code=200
    t.get.return_value.json.return_value={'encoding':'base64','content':base64.b64encode(_json_bytes(item)).decode()}
    cfg={'token':'dummy','owner':'foo','repo':'bar','branch':'runtime-data'}
    assert read_remote_monitor(config=cfg,asof=DAY,transport=t)['status']=='REMOTE_UNKNOWN_STATUS'


def test_pure_no_runtime_data_change(tmp_path):
    populate(tmp_path)
    before={str(p.relative_to(tmp_path)):p.read_bytes() for p in tmp_path.rglob('*.json')}
    diagnose(runtime_root=tmp_path,asof=DAY)
    after={str(p.relative_to(tmp_path)):p.read_bytes() for p in tmp_path.rglob('*.json')}
    assert before==after
