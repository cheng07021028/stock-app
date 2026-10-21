import pandas as pd
from godpick_h141_integrity_guard import record_snapshot_guard, manager_quality_gate
from godpick_h111_scan_settings_authority import _config

def test_rollback_old_date_blocked():
    ok, msg = record_snapshot_guard([{'推薦日期':'2026-07-09'}]*1810, {'count':2491,'latest_recommendation_date':'2026-10-17'})
    assert not ok and '回退' in msg

def test_rollback_old_count_blocked():
    ok, msg = record_snapshot_guard([{'推薦日期':'2026-10-17'}]*1810, {'count':2491,'latest_recommendation_date':'2026-10-17'})
    assert not ok and '縮水' in msg

def test_deletion_cannot_delete_newer_date():
    ok, _ = record_snapshot_guard([{'推薦日期':'2026-07-09'}],{'count':2491,'latest_recommendation_date':'2026-10-17'},destructive=True)
    assert not ok

def test_valid_destructive_can_reduce_count():
    ok,_= record_snapshot_guard([{'推薦日期':'2026-10-17'}],{'count':2491,'latest_recommendation_date':'2026-10-17'},destructive=True)
    assert ok

def test_manager_legacy_price_redaction():
    audit=pd.DataFrame([{'H109決策權威':'FADE_VETO','H123法人實證狀態':'PENDING_EVIDENCE','H124大戶實證狀態':'PENDING_EVIDENCE'}]*120)
    health, legacy=manager_quality_gate(pd.DataFrame(),audit,pd.DataFrame(),pd.DataFrame([{'H99主進場':36,'H99防守停損':35,'H99第一目標':42,'H99成本後RR':2,'股票代號':'2324'}]))
    assert 'STOP' in health.iloc[1,1]
    assert all(pd.isna(legacy.iloc[0][x]) for x in ['H99主進場','H99防守停損','H99第一目標','H99成本後RR'])
    assert legacy.at[0,'股票代號']=='2324'

def test_h111_prefers_runtime_branch():
    c=_config({'GITHUB_REPO_OWNER':'cheng07021028','GITHUB_REPO_BRANCH':'main','GITHUB_RUNTIME_DATA_BRANCH':'runtime-data','GITHUB_TOKEN':'test'})
    assert c['branch']=='runtime-data'

def test_live_mutation_blocks_1810_vs_2491(monkeypatch):
    import sys, types
    stub = types.ModuleType('streamlit')
    stub.secrets = {}
    stub.session_state = {}
    monkeypatch.setitem(sys.modules, 'streamlit', stub)
    import godpick_persistence_service as svc
    monkeypatch.setattr(svc,'github_config',lambda:{'token':'fake','branch':'runtime-data'})
    monkeypatch.setattr(svc,'firebase_configured',lambda:False)
    monkeypatch.setattr(svc,'read_github_json',lambda path, default: ({'count':2491,'payload_hash':'verified','latest_recommendation_date':'2026-10-17'},'remote verified'))
    touched=[]
    monkeypatch.setattr(svc,'write_local_json_atomic',lambda *args,**kwargs:(touched.append(args), (True,'ok'))[1])
    result=svc.save_records_mutation_fast([{'record_id':str(i),'推薦日期':'2026-07-09'} for i in range(1810)], reason='bad old batch')
    assert result.local_ok is False and result.permanent_ok is False
    assert '回退' in result.local_message and not touched

def test_no_token_blocks_all_new_mutations(monkeypatch):
    import sys, types
    stub = types.ModuleType('streamlit')
    stub.secrets = {}
    stub.session_state = {}
    monkeypatch.setitem(sys.modules, 'streamlit', stub)
    import godpick_persistence_service as svc
    monkeypatch.setattr(svc,'github_config',lambda:{'token':''})
    monkeypatch.setattr(svc,'firebase_configured',lambda:False)
    touched=[]
    monkeypatch.setattr(svc,'write_local_json_atomic',lambda *args,**kwargs:(touched.append(args), (True,'ok'))[1])
    result=svc.save_records_mutation_fast([{'record_id':'1','推薦日期':'2026-10-20'}],reason='test')
    assert not result.local_ok and not result.permanent_ok and 'GITHUB_TOKEN' in result.local_message
    assert not touched

def test_full_snapshot_cannot_overwrite_newer_remote(monkeypatch):
    import sys, types
    stub=types.ModuleType('streamlit'); stub.secrets={}; stub.session_state={}
    monkeypatch.setitem(sys.modules,'streamlit',stub)
    import godpick_persistence_service as svc
    monkeypatch.setattr(svc,'github_config',lambda:{'token':'fake','branch':'runtime-data'})
    monkeypatch.setattr(svc,'firebase_configured',lambda:False)
    monkeypatch.setattr(svc,'read_github_json',lambda path, default: ({'count':2491,'payload_hash':'verified','latest_recommendation_date':'2026-10-17'},'remote verified'))
    touched=[]
    monkeypatch.setattr(svc,'write_local_json_atomic',lambda *args,**kwargs:(touched.append(args),(True,'ok'))[1])
    r=svc.save_records_permanent([{'record_id':str(i),'推薦日期':'2026-07-09'} for i in range(1810)])
    assert not r.local_ok and not r.permanent_ok and not touched
