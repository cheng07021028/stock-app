# -*- coding: utf-8 -*-
"""H240 current sharded head and old-checkout recovery regressions (offline)."""
import sys, types
from pathlib import Path
import pytest
if 'streamlit' not in sys.modules:
    sys.modules['streamlit']=types.SimpleNamespace(secrets={})
import godpick_persistence_service as p
import godpick_h127_sharded_records as sh


def rows(n, newer=False):
    return [{'record_id': f'id{i}', '股票代號':str(1000+i), '推薦日期': '2026-11-25' if newer and i>=1796 else '2026-07-08', '推薦模式':'研究','私有欄位':f'保留{i}'} for i in range(n)]

@pytest.fixture
def setup(tmp_path,monkeypatch):
    monkeypatch.setattr(p,'BASE_DIR',tmp_path)
    monkeypatch.setattr(p,'github_config',lambda:{'token':'test','branch':'runtime-data'})
    monkeypatch.setattr(p,'firebase_configured',lambda:False)
    monkeypatch.setattr(sh,'PART_RAW_BYTES',5000)
    local=rows(1796); remote=rows(2623,True)
    state=p._new_state('fixture',remote,count=len(remote),latest_recommendation_date='2026-11-25')
    store={}; reads=[]
    def get(path,default):
        reads.append(path);return store.get(path,default),'mock official' if path in store else '404 not found'
    def put(path,body):store[path]=body;return True,'ok'
    ext,msg=sh.write_snapshot(remote,state,get_json=get,put_json=put,json_safe=p._json_safe_value,record_hash_func=p._json_hash)
    assert ext,msg
    state.update(ext);store[p.RECORDS_STATE_FILE]=state
    assert p.write_local_json_atomic(p.RECORDS_FILE,local)[0]
    monkeypatch.setattr(p,'read_github_json',get)
    monkeypatch.setattr(p,'recover_records_from_github_history',lambda *a,**k:pytest.fail('historical rescue before current head'))
    monkeypatch.setattr(p,'write_github_json',lambda *a,**k:pytest.fail('restore must not write remote'))
    reads.clear()
    return local,remote,state,store,reads


def test_exact_incident_1796_to_2623(setup):
    local,remote,state,store,reads=setup
    got,details,restored=p.ensure_records_local_authority_current()
    assert restored and got==remote
    assert p.read_local_json(p.RECORDS_FILE,[])[0]==remote
    assert p.read_local_json(p.RECORDS_STATE_FILE,{})[0]['revision']==state['revision']
    assert p.records_authority_verification_h240()['status']=='VERIFIED'
    assert list((p.BASE_DIR/'.cache/h240_pre_restore').glob('*.json'))
    assert p.RECORDS_FILE not in reads
    reads.clear(); got,_,restored=p.ensure_records_local_authority_current()
    assert got==remote and not restored
    assert not any('part-' in x for x in reads)


def test_missing_shard_does_not_restore_or_restamp(setup):
    local,remote,state,store,reads=setup
    key=next(k for k in store if 'part-' in k); del store[key]
    result,detail,ok=p.ensure_records_local_authority_current()
    assert not ok and result==local and p.records_authority_verification_h240()['status']=='BLOCKED'
    assert p.read_local_json(p.RECORDS_FILE,[])[0]==local
    assert not (p.BASE_DIR/p.RECORDS_STATE_FILE).exists()


def test_torn_local_metadata_does_not_hide_actual_old_date(setup):
    local,remote,state,store,reads=setup
    assert p.write_local_json_atomic(p.RECORDS_STATE_FILE,state)[0]
    result,detail,ok=p.ensure_records_local_authority_current()
    assert ok and result==remote


def test_unavailable_head_preserves_old_readonly_without_history(setup):
    local,remote,state,store,reads=setup
    store.clear()
    result,detail,ok=p.ensure_records_local_authority_current()
    assert not ok and result==local
    assert p.records_authority_verification_h240()['status']=='BLOCKED'
    assert not (p.BASE_DIR/p.RECORDS_STATE_FILE).exists()


def test_appending_today_cannot_mask_obsolete_base(setup):
    local,remote,state,store,reads=setup
    fake=local+[dict(r,推薦日期='2026-11-26') for r in remote[1796:]]
    ok,msg=p._h141_prevent_remote_rollback(fake)
    assert not ok and '舊基底' in msg
    assert p.read_local_json(p.RECORDS_FILE,[])[0]==local


def test_generic_restore_refuses_older_snapshot(setup):
    local,remote,state,store,reads=setup
    assert p.ensure_records_local_authority_current()[2]
    ok,msg=p.restore_records_snapshot(local,source='legacy')
    assert not ok and p.read_local_json(p.RECORDS_FILE,[])[0]==remote


def test_changed_head_during_read_not_committed(setup,monkeypatch):
    local,remote,state,store,reads=setup
    old=p.read_github_json
    def get(path,default):
        if path==p.RECORDS_STATE_FILE and reads.count(path)>=1:
            return dict(state,revision=state['revision']+1),'changed'
        return old(path,default)
    monkeypatch.setattr(p,'read_github_json',get)
    result,details,ok=p.ensure_records_local_authority_current()
    assert not ok and result==local


def test_remote_current_does_not_regress_more_complete_local(setup):
    local,remote,state,store,reads=setup
    later=remote+[dict(record_id='later',股票代號='2330',推薦日期='2026-11-26')]
    assert p.write_local_json_atomic(p.RECORDS_FILE,later)[0]
    result,_,ok=p.ensure_records_local_authority_current()
    assert not ok and result==later


def test_newer_firestore_conflict_is_not_marked_verified(setup,monkeypatch):
    local,remote,state,store,reads=setup
    monkeypatch.setattr(p,'firebase_configured',lambda:True)
    fs=dict(state,revision=state['revision']+1,payload_hash='different',storage_mode='')
    monkeypatch.setattr(p,'_read_records_firestore_summary',lambda:(fs,'mock'))
    result,_,ok=p.ensure_records_local_authority_current()
    assert result==remote and not ok
    assert p.records_authority_verification_h240()['status']=='CONFLICT'
    # No remote write; a conflicting newer source stays blocked on mutation.
    assert not p._h141_prevent_remote_rollback(remote)[0]


def test_full_replace_same_count_drops_id_blocked(setup):
    local,remote,state,store,reads=setup
    changed=[dict(r) for r in local]; changed[-1]['record_id']='other'
    assert not p._guard_records_full_replace(changed)[0]


def test_remote_smaller_only_explicit_newer_delete_permitted(setup):
    local,remote,state,store,reads=setup
    assert p.ensure_records_local_authority_current()[2]
    smaller=remote[:-1]; newer=p._new_state('fixture',smaller,count=len(smaller),latest_recommendation_date='2026-11-25',mutation_kind='destructive',mutation_deleted_count=1)
    def get(path,default):return store.get(path,default),'ok' if path in store else '404'
    def put(path,body):store[path]=body;return True,'ok'
    ext,_=sh.write_snapshot(smaller,newer,get_json=get,put_json=put,json_safe=p._json_safe_value,record_hash_func=p._json_hash)
    newer.update(ext);store[p.RECORDS_STATE_FILE]=newer
    got,_,ok=p.ensure_records_local_authority_current()
    assert ok and got==smaller


def test_legacy_main_branch_is_not_record_runtime_branch(monkeypatch):
    cfg={'GITHUB_REPO_BRANCH':'main','GITHUB_REPO_OWNER':'cheng07021028','GITHUB_REPO_NAME':'stock-app'}
    monkeypatch.setattr(p,'_secret',lambda key,default='':cfg.get(key,default))
    assert p.github_config()['branch']=='runtime-data'
    cfg['GITHUB_RUNTIME_DATA_BRANCH']='custom-runtime'
    assert p.github_config()['branch']=='custom-runtime'


def test_newer_wallclock_cannot_publish_old_date(setup):
    local,remote,state,store,reads=setup
    bad=p._new_state('fixture',local,count=len(local),latest_recommendation_date='2026-07-08')
    assert p._records_remote_state_relation_v191_h8(bad)[0]=='newer'


def test_firestore_only_old_summary_cannot_mark_github_current(setup,monkeypatch):
    local,remote,state,store,reads=setup
    old=p._new_state('fixture',local,count=len(local),latest_recommendation_date='2026-07-08')
    p.write_local_json_atomic(p.RECORDS_STATE_FILE,old)
    store.clear()
    monkeypatch.setattr(p,'firebase_configured',lambda:True)
    monkeypatch.setattr(p,'_read_records_firestore_summary',lambda:(old,'mock FS'))
    result,_,ok=p.ensure_records_local_authority_current()
    assert result==local and not ok and p.records_authority_verification_h240()['status']=='PARTIAL'
    assert not p._h141_prevent_remote_rollback(local)[0]


def test_newer_firestore_missing_content_uses_current_github_readonly(setup,monkeypatch):
    local,remote,state,store,reads=setup
    monkeypatch.setattr(p,'firebase_configured',lambda:True)
    fs=dict(state,revision=state['revision']+1,count=2624,latest_recommendation_date='2026-11-26',payload_hash='other',storage_mode='')
    monkeypatch.setattr(p,'_read_records_firestore_summary',lambda:(fs,'mock'))
    from datetime import datetime
    monkeypatch.setattr(p,'_read_records_firestore_full',lambda **k:([],'quota',datetime.min,{}))
    result,_,ok=p.ensure_records_local_authority_current()
    assert result==remote and not ok
    assert p.records_authority_verification_h240()['status']=='CONFLICT'


def test_newer_explicit_firestore_delete_does_not_resurrect_from_github(setup,monkeypatch):
    local,remote,state,store,reads=setup
    deleted=remote[:1796]
    fs=p._new_state('fixture',deleted,count=len(deleted),latest_recommendation_date='2026-07-08',mutation_kind='destructive',mutation_deleted_count=827)
    monkeypatch.setattr(p,'firebase_configured',lambda:True)
    monkeypatch.setattr(p,'_read_records_firestore_summary',lambda:(fs,'mock'))
    from datetime import datetime
    monkeypatch.setattr(p,'_read_records_firestore_full',lambda **k:(deleted,'verified',datetime.min,fs))
    result,_,ok=p.ensure_records_local_authority_current()
    assert result==deleted and ok


def test_h240_ui_uses_actual_verification():
    text=(Path(__file__).resolve().parents[1]/'pages'/'8_股神推薦紀錄.py').read_text('utf-8')
    assert '本機參考檔（遠端待驗收）' in text and 'verified else "warning"' in text
