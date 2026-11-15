# -*- coding: utf-8 -*-
"""H163 offline regressions: billed reads, quota fuse, source authority and H161 safety."""
from __future__ import annotations
import importlib
import sys
import types
from pathlib import Path
from unittest import mock
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
if "streamlit" not in sys.modules:
    st = types.ModuleType("streamlit")
    st.secrets = {}
    sys.modules["streamlit"] = st

import godpick_persistence_service as p
from godpick_h163_firestore_quota_guard import FirestoreReadGuard, is_quota_exception


class Clock:
    def __init__(self): self.sec = 1000.0
    def __call__(self): return self.sec
    def move(self, delta): self.sec += delta


class QuotaError(Exception):
    def code(self): return "StatusCode.RESOURCE_EXHAUSTED"


class FakeDoc:
    def __init__(self, source, path): self.src, self.path = source, path
    def get(self):
        self.src.get_count += 1
        if self.src.read_fail:
            raise self.src.read_fail
        payload = self.src.documents.get(self.path)
        return types.SimpleNamespace(exists=payload is not None, to_dict=lambda: dict(payload or {}),id=self.path.split('/')[-1])
    def set(self,*a,**k):
        self.src.set_count += 1
    def delete(self):
        self.src.delete_count += 1


class FakeCollection:
    def __init__(self, src, name): self.src, self.name = src, name
    def document(self, key): return FakeDoc(self.src, self.name+'/'+str(key))
    def stream(self):
        self.src.stream_count += 1
        if self.src.read_fail:
            raise self.src.read_fail
        for key, payload in self.src.documents.items():
            if key.startswith(self.name+'/'):
                yield types.SimpleNamespace(id=key.split('/')[-1], to_dict=lambda d=payload: dict(d))
    def order_by(self,*a,**kw): return self
    def limit(self,*a,**kw): return self


class FakeDB:
    def __init__(self, docs=None):
        self.documents = dict(docs or {})
        self.read_fail = None
        self.get_count = self.stream_count = self.set_count = self.delete_count = 0
    def collection(self,name): return FakeCollection(self,name)


@pytest.fixture
def env(monkeypatch,tmp_path):
    monkeypatch.setattr(p,'BASE_DIR',tmp_path)
    monkeypatch.setattr(p,'firebase_configured', lambda: True)
    monkeypatch.setattr(p,'github_config',lambda:{'token':'fixture-token','owner':'demo','repo':'stock-app','branch':'runtime-data'})
    clock = Clock()
    guard = FirestoreReadGuard(clock=clock, cache_seconds=90, quota_seconds=1800, full_retry_seconds=180)
    monkeypatch.setattr(p,'_H163_FS_GUARD',guard)
    db=FakeDB()
    fake_fire = types.SimpleNamespace(client=lambda:db,SERVER_TIMESTAMP='NOW',Query=types.SimpleNamespace(DESCENDING='DESC'))
    monkeypatch.setattr(p,'firestore',fake_fire)
    monkeypatch.setattr(p,'_init_firebase_app',lambda: (_ for _ in ()).throw(RuntimeError(guard.quota_reason())) if guard.status()['quota_blocked'] else None)
    monkeypatch.setattr(p,'_records_history_gap_audit_needed',lambda *a:(False,'no audit needed'))
    return db,clock,guard,tmp_path


def rows(n):
    return [{'record_id':f'r{i}','股票代號':f'{1000+i:04d}','推薦日期':'2026-11-13',
             '推薦時間':'20:35','推薦模式':'研究','保留不可見欄位':f'original-{i}'} for i in range(n)]


def authority(data):
    data=p.records_as_rows_exact(data)
    return p._new_state('fixture',data,count=len(data),latest_recommendation_date=p._latest_record_recommendation_date(data))


def make_local(data, state=None):
    payload=p.records_as_rows_exact(data)
    state=state or authority(payload)
    assert p.write_local_json_atomic(p.RECORDS_FILE,payload)[0]
    assert p.write_local_json_atomic(p.RECORDS_STATE_FILE,state)[0]
    return payload,state


def set_fs(db,data,state=None):
    payload=p.records_as_rows_exact(data)
    state=state or authority(payload)
    db.documents={'system/godpick_records_summary':dict(state),**{f'godpick_records/{r["record_id"]}':dict(r) for r in payload}}
    return state


def test_summary_cache_avoids_rerun_billing(env):
    db,clock,guard,_=env
    set_fs(db,rows(3))
    a,m=p._read_records_firestore_summary()
    b,m2=p._read_records_firestore_summary()
    assert a['count']==3 and b['count']==3
    assert db.get_count==1 and db.stream_count==0
    assert '快取' in m2 and guard.status()['summary_cache_hits']==1
    clock.move(91)
    p._read_records_firestore_summary()
    assert db.get_count==2


def test_cached_summary_is_defensive_copy(env):
    db,clock,guard,_=env
    set_fs(db,rows(3))
    a,_=p._read_records_firestore_summary()
    a['count']=0
    b,_=p._read_records_firestore_summary()
    assert b['count']==3


def test_same_hash_never_reads_whole_collection(env,monkeypatch):
    db,clock,guard,_=env
    original,local_state=make_local(rows(3))
    fs_state=set_fs(db,original)
    # Force remote to be the freshness winner; identical hash still means no stream.
    db.documents['system/godpick_records_summary']['revision']=local_state['revision']+10**10
    db.documents['system/godpick_records_summary']['updated_at_epoch']=local_state['updated_at_epoch']+1000
    monkeypatch.setattr(p,'read_github_json',lambda name,default:({},'not available'))
    result,msg,restored=p.ensure_records_local_authority_current()
    assert len(result)==3 and not restored
    assert db.stream_count==0 and db.get_count==1
    assert any('同Hash' in s for s in msg)


def test_fs_github_same_hash_prefers_verified_github_snapshot(env,monkeypatch):
    db,clock,guard,_=env
    old,_=make_local(rows(2))
    new=rows(4)
    fs_state=set_fs(db,new)
    gh_state=authority(new)
    db.documents['system/godpick_records_summary']['revision']=gh_state['revision']+500000
    db.documents['system/godpick_records_summary']['updated_at_epoch']=gh_state['updated_at_epoch']+1000
    def fake_github(name,default):
        return (gh_state,'github state') if name==p.RECORDS_STATE_FILE else (p.records_as_rows_exact(new),'github h127 full')
    monkeypatch.setattr(p,'read_github_json',fake_github)
    result,msg,restored=p.ensure_records_local_authority_current()
    assert restored and len(result)==4
    assert db.stream_count==0
    assert any('GitHub完整資料' in s for s in msg)
    assert len(p.read_local_json(p.RECORDS_FILE,[])[0])==4


def test_firestore_genuinely_newer_still_loads_full_and_preserves_fields(env,monkeypatch):
    db,clock,guard,_=env
    old,_=make_local(rows(2))
    new=rows(4)
    fs_state=set_fs(db,new)
    monkeypatch.setattr(p,'read_github_json',lambda name,default:({},'unavailable'))
    result,msg,restored=p.ensure_records_local_authority_current()
    assert restored and len(result)==4
    assert db.stream_count==1
    assert result[-1]['保留不可見欄位']=='original-3'
    # Now local is full and matches FS hash, next refresh must not stream.
    result2,_,again=p.ensure_records_local_authority_current()
    assert len(result2)==4 and not again and db.stream_count==1


def test_quota_trips_breaker_and_stops_repeat_requests(env,monkeypatch):
    db,clock,guard,_=env
    db.read_fail=QuotaError('429 Quota exceeded on Firestore reads')
    a,msg=p._read_records_firestore_summary()
    assert not a and '429' in msg
    count=db.get_count
    a2,msg2=p._read_records_firestore_summary()
    assert not a2 and '配額保護' in msg2
    assert db.get_count==count
    assert guard.status()['quota_blocked']
    clock.move(1801)
    db.read_fail=None
    set_fs(db,rows(2))
    a3,msg3=p._read_records_firestore_summary()
    assert a3['count']==2 and db.get_count==count+1


def test_full_read_429_then_repeat_suppressed_even_if_manual(env):
    db,clock,guard,_=env
    db.read_fail=QuotaError('RESOURCE_EXHAUSTED')
    a,msg,_,_=p._read_records_firestore_full(snapshot_key='s1')
    assert not a and db.stream_count==1
    a2,msg2,_,_=p._read_records_firestore_full(snapshot_key='s1')
    assert not a2 and db.stream_count==1 and '配額保護' in msg2
    assert guard.status()['quota_blocked']


def test_nonquota_full_failure_backoff_per_snapshot(env):
    db,clock,guard,_=env
    db.read_fail=ValueError('temporary schema decoding error')
    a,msg,_,_=p._read_records_firestore_full(snapshot_key='v1')
    assert db.stream_count==1 and not a
    a2,msg2,_,_=p._read_records_firestore_full(snapshot_key='v1')
    assert db.stream_count==1 and '冷卻' in msg2
    clock.move(181)
    a3,msg3,_,_=p._read_records_firestore_full(snapshot_key='v1')
    assert db.stream_count==2


def test_different_remote_hash_429_preserves_local_and_does_not_fake_success(env,monkeypatch):
    db,clock,guard,_=env
    old,_=make_local(rows(2))
    new=rows(4)
    fs_state=set_fs(db,new)
    db.read_fail=QuotaError('429 Quota exceeded')
    monkeypatch.setattr(p,'read_github_json',lambda name,default:({},'github unavailable'))
    result,msg,restored=p.ensure_records_local_authority_current()
    assert len(result)==2 and not restored
    assert len(p.read_local_json(p.RECORDS_FILE,[])[0])==2
    assert any('429' in s for s in msg)
    count=db.get_count
    result2,msg2,restored2=p.ensure_records_local_authority_current()
    assert not restored2 and len(result2)==2 and db.get_count==count


def test_summary_seed_and_invalidation_after_write(env):
    db,clock,guard,_=env
    set_fs(db,rows(2))
    p._read_records_firestore_summary()
    assert db.get_count==1
    guard.note_summary({'count':1,'payload_hash':'newhash'})
    a,_=p._read_records_firestore_summary()
    assert a['count']==1
    guard.invalidate_summary()
    b,_=p._read_records_firestore_summary()
    assert b['count']==2 and db.get_count==2


def test_compat_fallback_skips_full_when_same_hash(env,monkeypatch):
    db,clock,guard,_=env
    original,state=make_local(rows(2))
    set_fs(db,original)
    monkeypatch.setattr(p,'ensure_records_local_authority_current',lambda:(1/0))
    monkeypatch.setattr(p,'read_github_json',lambda name,default:({},'not available'))
    output,msg=p.load_records_permanent()
    assert len(output)==2 and db.stream_count==0
    assert any('H163' in x for x in msg)


def test_compat_fallback_recovers_remote_if_needed(env,monkeypatch):
    db,clock,guard,_=env
    original,state=make_local(rows(2))
    set_fs(db,rows(3))
    monkeypatch.setattr(p,'ensure_records_local_authority_current',lambda:(1/0))
    monkeypatch.setattr(p,'read_github_json',lambda name,default:({},'not available'))
    output,msg=p.load_records_permanent()
    assert len(output)==3 and db.stream_count==1


def test_code_branch_runtime_and_h161_protection_are_unchanged():
    source=(ROOT/'godpick_persistence_service.py').read_text('utf-8')
    assert '"branch": runtime_branch' in source
    assert 'H161精確刪除預檢成功' in source
    assert 'H161遠端刪除未獲確認，未更動本機權威' in source
    assert 'H126' not in (ROOT/'godpick_h163_firestore_quota_guard.py').read_text('utf-8')


def test_quota_detection_covers_grpc_and_429():
    assert is_quota_exception(QuotaError('oops'))
    assert is_quota_exception(RuntimeError('429 Quota exceeded'))
    assert not is_quota_exception(RuntimeError('network timeout'))
    assert not is_quota_exception(RuntimeError('permission denied'))


def test_repeated_reruns_100_times_only_one_summary_get(env):
    db,clock,guard,_=env
    original,st=make_local(rows(3))
    set_fs(db,original)
    for _ in range(100):
        data,msg=p._read_records_firestore_summary()
        assert data['count']==3
        clock.move(0.5)
    assert db.get_count==1 and db.stream_count==0
    assert guard.status()['summary_cache_hits']==99


def test_full_save_identical_remote_hash_skips_all_2632_firestore_documents(env,monkeypatch):
    db,clock,guard,_=env
    original=p.normalize_records(rows(8))
    make_local(original)
    set_fs(db,original)
    monkeypatch.setattr(p,'_h141_prevent_remote_rollback',lambda *args,**kwargs:(True,'verified'))
    monkeypatch.setattr(p,'_sync_records_github_snapshot_v191_h8',lambda *args,**kwargs:(True,'github verified'))
    monkeypatch.setattr(p,'write_records_firestore',lambda *a,**k: (_ for _ in ()).throw(AssertionError('full rewrite forbidden')))
    outcome=p.save_records_permanent(original,reason='full save unchanged')
    assert outcome.local_ok and outcome.firestore_ok and outcome.permanent_ok
    assert '同Hash' in outcome.firestore_message
    assert db.stream_count==0 and db.get_count==1


def test_firestore_write_429_fuse_does_not_mark_durable_success(env,monkeypatch):
    db,clock,guard,_=env
    original,_=make_local(rows(3))
    db.read_fail=QuotaError('429 Quota exceeded')
    state=authority(original)
    ok,msg=p.write_records_firestore_mutation(original,state,deleted_ids=['r0'],expected_previous_count=3)
    assert not ok and '429' in msg
    assert guard.status()['quota_blocked']
    count=db.get_count
    ok2,msg2=p.write_records_firestore_mutation(original,state,deleted_ids=['r0'],expected_previous_count=3)
    assert not ok2 and db.get_count==count
    assert '配額保護' in msg2


def test_github_shard_unreadable_firestorm_uses_fs_once_for_verified_twin(env,monkeypatch):
    db,clock,guard,_=env
    make_local(rows(2))
    new=rows(4)
    state=set_fs(db,new)
    def gh(name,default):
        if name == p.RECORDS_STATE_FILE:
            return dict(state),'same hash remote GitHub state'
        return [],'H127 manifest unavailable'
    monkeypatch.setattr(p,'read_github_json',gh)
    result,detail,restored=p.ensure_records_local_authority_current()
    assert restored and len(result)==4
    assert db.stream_count==1
    assert any('Firestore安全備援' in s for s in detail)


def test_quota_during_exception_fallback_keeps_existing_valid_state_version(env,monkeypatch):
    db,clock,guard,_=env
    old,state=make_local(rows(2))
    state_before=p.read_local_json(p.RECORDS_STATE_FILE,{})[0]
    db.read_fail=QuotaError('429 Quota exceeded')
    monkeypatch.setattr(p,'read_github_json',lambda name,default:({},'no data'))
    monkeypatch.setattr(p,'ensure_records_local_authority_current',lambda:(1/0))
    result,details=p.load_records_permanent()
    assert len(result)==2 and guard.status()['quota_blocked']
    assert p.read_local_json(p.RECORDS_STATE_FILE,{})[0]==state_before


def test_quota_during_empty_exception_fallback_never_creates_empty_authority(env,monkeypatch):
    db,clock,guard,root=env
    # Empty local plus no verified GitHub and quota exhausted MUST NOT produce
    # fresh authoritative 0-row metadata that might mask the remote 2,632 rows.
    assert not (root/p.RECORDS_STATE_FILE).exists()
    db.read_fail=QuotaError('429 Quota exceeded')
    monkeypatch.setattr(p,'read_github_json',lambda name,default:({},'no data'))
    monkeypatch.setattr(p,'ensure_records_local_authority_current',lambda:(1/0))
    result,details=p.load_records_permanent()
    assert result==[] and any('保留本機原貌' in t for t in details)
    assert not (root/p.RECORDS_STATE_FILE).exists()
