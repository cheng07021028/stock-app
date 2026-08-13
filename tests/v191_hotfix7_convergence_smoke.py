# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import json
import sys
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

# 1) Page07 uses one formal/A-/R1 action partition.
from godpick_headless_page_loader import load_page_namespace
ns=load_page_namespace('pages/7_股神推薦.py',base_dir=ROOT)
st=ns['__headless_st__']; k=ns['_k']
st.session_state[k('scan_quality_report')]={'正式推薦可用':True}
ns['_phase93_single_source_decision_frame']=lambda a,b:a.copy()
ns['assess_individual_sample_quality']=lambda row:(True,'ok','高')
source=pd.DataFrame([
    {'股票代號':'2303','正式推薦分區':'盤中雷達追蹤','盤中雷達優先級':'R1-M','推薦總分':75},
    {'股票代號':'2472','正式推薦分區':'候選觀察','盤中雷達優先級':'','推薦總分':74},
])
action, formal_ok, notes=ns['_v191_actionable_tracking_frame'](source)
assert formal_ok and len(action)==1 and str(action.iloc[0]['股票代號'])=='2303',(action,notes)
print('PASS H7 Page07 single-source action partition keeps R1 exactly once')

# 2) Page10 current-list repair can recover only same-run Page08 rows.
import godpick_auto_update_tasks as tasks
import godpick_persistence_service as ps
orig_load_named=ps.load_named_json_permanent
orig_load_records=ps.load_records_permanent
orig_save_named=ps.save_named_json_permanent
saved={}
def fake_load_named(path, default, **kwargs):
    if path=='godpick_recommend_list.json': return [],['mock list empty']
    if path=='godpick_latest_recommendations.json':
        return {'saved_at':'2026-08-13 16:15:22','recommendation_date':'2026-08-13','recommendations':[]},['mock latest empty']
    return default,[]
def fake_load_records(*args,**kwargs):
    return [
        {'股票代號':'2303','推薦日期':'2026-08-13','推薦時間':'16:14:50','建立時間':'2026-08-13 16:14:50','紀錄來源':'07_股神推薦｜推薦完成自動記錄','紀錄層級':'R1-M核心雷達'},
        {'股票代號':'2472','推薦日期':'2026-08-13','推薦時間':'16:14:55','建立時間':'2026-08-13 16:14:55','推薦執行來源':'07_股神推薦','紀錄層級':'R1核心雷達'},
        {'股票代號':'9999','推薦日期':'2026-08-13','推薦時間':'14:00:00','建立時間':'2026-08-13 14:00:00','推薦執行來源':'07_股神推薦','紀錄層級':'R1核心雷達'},
    ],['mock records']
def fake_save_named(path,payload,**kwargs):
    saved[path]=payload
    return SimpleNamespace(permanent_ok=True)
ps.load_named_json_permanent=fake_load_named
ps.load_records_permanent=fake_load_records
ps.save_named_json_permanent=fake_save_named
try:
    df,msgs=tasks._load_recommend_list()
finally:
    ps.load_named_json_permanent=orig_load_named
    ps.load_records_permanent=orig_load_records
    ps.save_named_json_permanent=orig_save_named
assert set(df['股票代號'].astype(str))=={'2303','2472'},(df,msgs)
assert len(saved.get('godpick_recommend_list.json',[]))==2,saved
assert len((saved.get('godpick_latest_recommendations.json') or {}).get('recommendations',[]))==2,saved
print('PASS H7 Page10 repairs empty current list from same Page07 run only')

# 3) Legacy future completed keys must be removed without touching FORCE/past keys.
import godpick_auto_scheduler as sched
now=datetime(2026,8,13,16,20,tzinfo=sched.TZ)
status={'completed_run_keys':[
    'official_factors|2026-08-13 20:25',
    'godpick_recommendation|2026-08-13 20:55',
    'macro_full|2026-08-13 15:10',
    'stock_master|FORCE|2026-08-13 15:47',
]}
removed=sched._repair_future_completed_keys(status,now)
assert removed==2,status
assert 'macro_full|2026-08-13 15:10' in status['completed_run_keys']
assert 'stock_master|FORCE|2026-08-13 15:47' in status['completed_run_keys']
print('PASS H7 scheduler removes legacy future production slots but preserves FORCE/past')

# 4) Remote sync-state hash is durable proof even when process-local outbox is gone.
import godpick_durability_service as d
orig_registry=d.CORE_DURABLE_FILES
orig_read=ps.read_github_json
try:
    with TemporaryDirectory() as td:
        base=Path(td)
        payload={'x':1}
        (base/'market_snapshot.json').write_text(json.dumps(payload),encoding='utf-8')
        ph=d._hash(payload)
        state_name=d._state_file_for('market_snapshot.json')
        (base/state_name).write_text(json.dumps({'payload_hash':ph}),encoding='utf-8')
        d.CORE_DURABLE_FILES={'market_snapshot.json':{'critical':True,'purpose':'test'}}
        d._REMOTE_STATE_PROBE_CACHE.clear()
        ps.read_github_json=lambda path, default: ({'payload_hash':ph},'mock remote state') if path==state_name else (default,'mock')
        audit=d.audit_core_durability(base_dir=base,write_audit=False,verify_remote_states=True)
        assert audit['critical_remote_confirmed']==1,audit
        assert audit['rows'][0]['status']=='REMOTE_CONFIRMED',audit
finally:
    d.CORE_DURABLE_FILES=orig_registry
    ps.read_github_json=orig_read
print('PASS H7 durability recognizes remote same-hash state after outbox/restart loss')

# 5) Bounded convergence must persist each selected file and finish at remote-confirmed=0 remaining.
orig_registry=d.CORE_DURABLE_FILES
orig_read=ps.read_github_json
orig_save=ps.save_named_json_permanent
try:
    with TemporaryDirectory() as td:
        base=Path(td)
        files={'a.json':{'v':1},'b.json':{'v':2}}
        for name,payload in files.items():
            (base/name).write_text(json.dumps(payload),encoding='utf-8')
        d.CORE_DURABLE_FILES={name:{'critical':True,'purpose':'test'} for name in files}
        d._REMOTE_STATE_PROBE_CACHE.clear()
        remote_states={}
        def fake_read(path, default):
            return remote_states.get(path, default), 'mock remote state'
        def fake_save(path,payload,**kwargs):
            ph=d._hash(payload)
            remote_states[d._state_file_for(path)]={'payload_hash':ph}
            return SimpleNamespace(permanent_ok=True,local_message='local ok',github_message='gh ok',firestore_message='')
        ps.read_github_json=fake_read
        ps.save_named_json_permanent=fake_save
        result=d.sync_unconfirmed_critical_bounded(base_dir=base,max_files=2,time_budget_seconds=10)
        assert result['attempted']==2,result
        assert result['confirmed_this_run']==2,result
        assert result['failed_this_run']==0,result
        assert result['remaining']==0,result
        assert result['audit']['critical_remote_confirmed']==2,result
finally:
    d.CORE_DURABLE_FILES=orig_registry
    ps.read_github_json=orig_read
    ps.save_named_json_permanent=orig_save
print('PASS H7 bounded durability convergence checkpoints and reaches zero remaining')
