# -*- coding: utf-8 -*-
"""H176: 2617 vs 2560 repair path, no blind delete, hash/authority checks."""
from __future__ import annotations
import importlib
import sys
import types
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
if "streamlit" not in sys.modules:
    fake=types.ModuleType("streamlit");fake.secrets={};sys.modules["streamlit"]=fake
p=importlib.import_module("godpick_persistence_service")
from godpick_h176_delete_reconcile import plan_missing_record_repair as plan


def rows(n=2617):
    return [{"record_id":f"rec-{i:04d}","股票代號":"1477", "推薦日期":"2026-11-19", "推薦模式":"研究", "隱藏欄位":f"完整-{i}"} for i in range(n)]


def test_2617_2560_only_57_missing_ids_no_rewrite():
    local=rows(); remote=[{**r,"updated_at":"2026-11-19 00:00:00"} for r in local[:2560]]
    z=plan(local,remote,remote_doc_ids=[r["record_id"] for r in remote])
    assert z["allowed"] and len(z["missing_rows"])==57
    assert z["missing_ids"][0]=="rec-2560" and z["missing_ids"][-1]=="rec-2616"
    assert z["existing_count"]==2560 and z["expected_count"]==2617


def test_stale_summary_full_collection_needs_only_summary():
    local=rows(3)
    z=plan(local,local,remote_doc_ids=[r["record_id"] for r in local])
    assert z["allowed"] and z["missing_ids"]==[]


def test_remote_extra_id_is_never_erased():
    local=rows(3)
    remote=local+[dict(rows(4)[-1])]
    z=plan(local,remote,remote_doc_ids=[r["record_id"] for r in remote])
    assert not z["allowed"] and z["extra_ids"]==["rec-0003"]


def test_same_id_different_hidden_field_is_conflict():
    local=rows(3); remote=[dict(x) for x in local]
    remote[1]["隱藏欄位"]="other person's edit"
    z=plan(local,remote,remote_doc_ids=[r["record_id"] for r in remote])
    assert not z["allowed"] and z["conflict_ids"]==["rec-0001"]


def test_more_than_repair_limit_must_block():
    local=rows(401)
    assert not plan(local,local[:100],remote_doc_ids=[r["record_id"] for r in local[:100]])["allowed"]


def test_duplicate_and_wrong_id_block():
    local=rows(3)
    assert not plan(local,[local[0],local[0]],remote_doc_ids=["rec-0000","rec-0000"])["allowed"]
    assert not plan(local,[local[0]],remote_doc_ids=["rec-0002"])["allowed"]


@pytest.fixture
def state_fixture(tmp_path,monkeypatch):
    base=rows(4)
    state=p._new_state("test",base,count=len(base),latest_recommendation_date="2026-11-19")
    monkeypatch.setattr(p,"BASE_DIR",tmp_path)
    monkeypatch.setattr(p,"firebase_configured",lambda:True)
    monkeypatch.setattr(p,"github_config",lambda:{"token":"not-a-real-token", "branch":"runtime-data"})
    monkeypatch.setattr(p,"read_github_json",lambda name,default:(state,"verified metadata") if name==p.RECORDS_STATE_FILE else (base,"verified shards"))
    monkeypatch.setattr(p,"_read_records_firestore_summary",lambda:({**state,"count":3,"payload_hash":"older"},"stale FS"))
    return base,state


def test_guard_uses_verified_repair_and_preserves_old_rows(state_fixture,monkeypatch):
    before, state=state_fixture
    calls=[]
    def fake_repair(before_rows,*,expected_local_hash):
        calls.append((len(before_rows),expected_local_hash))
        return True,"已修復",state
    monkeypatch.setattr(p,"_h176_repair_firestore_gap_for_verified_delete",fake_repair)
    after=[x for x in before if x["record_id"]!="rec-0001"]
    allowed,msg=p._h141_prevent_remote_rollback(after,deleted_ids=["rec-0001"],previous_rows=before,previous_count=4)
    assert allowed and len(calls)==1
    assert calls[0][1]==p._json_hash(before)


def test_guard_repair_failure_does_not_delete(state_fixture,monkeypatch):
    before,_=state_fixture
    monkeypatch.setattr(p,"_h176_repair_firestore_gap_for_verified_delete",lambda *a,**k:(False,"H176不同內容",{}))
    after=[x for x in before if x["record_id"]!="rec-0001"]
    allowed,msg=p._h141_prevent_remote_rollback(after,deleted_ids=["rec-0001"],previous_rows=before,previous_count=4)
    assert not allowed and "H176不同內容" in msg


def test_guard_without_matching_github_does_not_enter_repair(state_fixture,monkeypatch):
    before,_=state_fixture
    monkeypatch.setattr(p,"read_github_json",lambda *a:({"payload_hash":"other","count":3},"other history"))
    monkeypatch.setattr(p,"_h176_repair_firestore_gap_for_verified_delete",lambda *a,**k:(_ for _ in ()).throw(AssertionError("not allowed")))
    allowed,msg=p._h141_prevent_remote_rollback(before[1:],deleted_ids=["rec-0000"],previous_rows=before,previous_count=4)
    assert not allowed and "H176" in msg


def test_guard_no_remote_authority_blocks(state_fixture,monkeypatch):
    before,_=state_fixture
    monkeypatch.setattr(p,"github_config",lambda:{"token":""})
    monkeypatch.setattr(p,"firebase_configured",lambda:False)
    allowed,msg=p._h141_prevent_remote_rollback(before[1:],deleted_ids=["rec-0000"],previous_rows=before,previous_count=4)
    assert not allowed and "GITHUB_TOKEN" in msg

class FakeDocument:
    def __init__(self, coll, rid):
        self.coll=coll; self.id=rid
    def get(self,transaction=None):
        return FakeSnapshot(self.id,self.coll.docs.get(self.id))

class FakeSnapshot:
    def __init__(self,rid,value):
        self.id=rid;self.exists=value is not None;self.value=value
    def to_dict(self):
        return dict(self.value) if self.value is not None else None

class FakeCollection:
    def __init__(self,docs):
        self.docs=docs
    def document(self,rid):
        return FakeDocument(self,rid)
    def stream(self):
        return [FakeSnapshot(k,v) for k,v in self.docs.items()]

class FakeTransaction:
    def set(self,ref,payload,merge=False):
        if merge and ref.id in ref.coll.docs:
            ref.coll.docs[ref.id].update(payload)
        else:
            ref.coll.docs[ref.id]=dict(payload)

class FakeFirestoreDB:
    def __init__(self,records,summary):
        self.recs=FakeCollection({r["record_id"]:dict(r,updated_at="timestamp") for r in records})
        self.sys=FakeCollection({"godpick_records_summary":dict(summary)})
    def collection(self,name):
        return self.sys if name=="system" else self.recs
    def transaction(self):
        return FakeTransaction()
    def get_all(self,refs):
        return [ref.get() for ref in refs]


def configure_fake_firestore(monkeypatch,records,summary):
    db=FakeFirestoreDB(records,summary)
    fs=types.SimpleNamespace(client=lambda:db,SERVER_TIMESTAMP="server-timestamp",transactional=lambda fn:fn)
    monkeypatch.setattr(p,"firestore",fs)
    monkeypatch.setattr(p,"_init_firebase_app",lambda:None)
    return db


def test_real_h176_helper_backfills_only_missing_and_checks_readback(state_fixture,monkeypatch):
    before,gh_state=state_fixture
    # Simulate 4 local/GitHub rows and 3 Firestore physical documents.
    summary=dict(gh_state,count=3,payload_hash=p._json_hash(before[:3]),revision=gh_state["revision"]-1000)
    db=configure_fake_firestore(monkeypatch,before[:3],summary)
    ok,msg,new=p._h176_repair_firestore_gap_for_verified_delete(before,expected_local_hash=p._json_hash(before))
    assert ok, msg
    assert len(db.recs.docs)==4
    assert db.recs.docs["rec-0000"]["隱藏欄位"]=="完整-0"  # older not rewritten
    assert db.recs.docs["rec-0003"]["隱藏欄位"]=="完整-3"  # only absent restored
    assert new["count"]==4 and new["payload_hash"]==p._json_hash(before)


def test_real_h176_helper_rejects_newer_firestore_without_writes(state_fixture,monkeypatch):
    before,gh_state=state_fixture
    summary=dict(gh_state,count=3,payload_hash=p._json_hash(before[:3]),revision=gh_state["revision"]+1000)
    db=configure_fake_firestore(monkeypatch,before[:3],summary)
    ok,msg,new=p._h176_repair_firestore_gap_for_verified_delete(before,expected_local_hash=p._json_hash(before))
    assert not ok and "Firestore版本比GitHub新" in msg
    assert len(db.recs.docs)==3 and db.sys.docs["godpick_records_summary"]["count"]==3


def test_real_h176_helper_rejects_same_id_content_conflict(state_fixture,monkeypatch):
    before,gh_state=state_fixture
    divergent=[dict(x) for x in before[:3]];divergent[0]["隱藏欄位"]="remote changed"
    summary=dict(gh_state,count=3,payload_hash=p._json_hash(divergent),revision=gh_state["revision"]-1000)
    db=configure_fake_firestore(monkeypatch,divergent,summary)
    ok,msg,new=p._h176_repair_firestore_gap_for_verified_delete(before,expected_local_hash=p._json_hash(before))
    assert not ok and "同ID但內容不同" in msg
    assert len(db.recs.docs)==3


def test_real_h176_helper_rejects_bad_github_shards(state_fixture,monkeypatch):
    before,gh_state=state_fixture
    monkeypatch.setattr(p,"read_github_json",lambda name,default:(gh_state,"state") if name==p.RECORDS_STATE_FILE else (before[:3],"incomplete H127"))
    db=configure_fake_firestore(monkeypatch,before[:3],dict(gh_state,count=3,revision=gh_state["revision"]-1000))
    ok,msg,new=p._h176_repair_firestore_gap_for_verified_delete(before,expected_local_hash=p._json_hash(before))
    assert not ok and "分片Hash" in msg
    assert len(db.recs.docs)==3


def test_h176_transaction_never_overwrites_concurrently_created_missing_id(state_fixture,monkeypatch):
    p._H163_FS_GUARD.note_full_success()  # isolate prior intentional conflict cooldown
    before,gh_state=state_fixture
    summary=dict(gh_state,count=3,payload_hash=p._json_hash(before[:3]),revision=gh_state["revision"]-1000)
    db=configure_fake_firestore(monkeypatch,before[:3],summary)
    original_get=FakeDocument.get
    def concurrent_create(self,transaction=None):
        if transaction is not None and self.id=="rec-0003":
            self.coll.docs[self.id]={"record_id":"rec-0003","隱藏欄位":"someone else wrote"}
        return original_get(self,transaction=transaction)
    monkeypatch.setattr(FakeDocument,"get",concurrent_create)
    ok,msg,state=p._h176_repair_firestore_gap_for_verified_delete(before,expected_local_hash=p._json_hash(before))
    assert not ok and "禁止覆蓋" in msg
    assert db.recs.docs["rec-0003"]["隱藏欄位"]=="someone else wrote"
    assert db.sys.docs["godpick_records_summary"]["count"]==3
