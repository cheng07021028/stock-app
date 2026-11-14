# -*- coding: utf-8 -*-
"""V191-H161 deletion authority / 403 / history-shrink regressions (offline)."""
from __future__ import annotations
import ast
import importlib
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if "streamlit" not in sys.modules:
    fake = types.ModuleType("streamlit")
    fake.secrets = {}
    sys.modules["streamlit"] = fake
p = importlib.import_module("godpick_persistence_service")


def rows():
    return [
        {"record_id": "rec-a", "股票代號": "2892", "推薦日期": "2026-11-12", "推薦模式": "研究", "不可見欄位": "永久保留a"},
        {"record_id": "rec-b", "股票代號": "1477", "推薦日期": "2026-11-13", "推薦模式": "研究", "不可見欄位": "永久保留b"},
        {"record_id": "rec-c", "股票代號": "2033", "推薦日期": "2026-11-12", "推薦模式": "研究", "不可見欄位": "永久保留c"},
    ]


def remote_state(data, **kwargs):
    exact = p.records_as_rows_exact(data)
    return p._new_state("fixture", exact, count=len(exact),
                        latest_recommendation_date=p._latest_record_recommendation_date(exact), **kwargs)


@pytest.fixture
def local(tmp_path, monkeypatch):
    monkeypatch.setattr(p, "BASE_DIR", tmp_path)
    monkeypatch.setattr(p, "firebase_configured", lambda: False)
    monkeypatch.setattr(p, "github_config", lambda: {"token": "TEST_ONLY", "owner": "demo", "repo": "stock-app", "branch": "runtime-data"})
    original = p.records_as_rows_exact(rows())
    remote = remote_state(original)
    assert p.write_local_json_atomic(p.RECORDS_FILE, original)[0]
    assert p.write_local_json_atomic(p.RECORDS_STATE_FILE, remote)[0]
    monkeypatch.setattr(p, "read_github_json", lambda name, default: (remote, "mock GitHub remote state") if name == p.RECORDS_STATE_FILE else (original, "mock GitHub data"))
    return original, remote, tmp_path


def test_delete_full_authority_preserves_all_unshown_fields(local, monkeypatch):
    before, state, root = local
    calls = []
    def verified_write(post_rows, post_state, reason):
        # Prove the canonical local file has not yet been mutated!
        current, _, _ = p.read_local_json(p.RECORDS_FILE, [])
        assert len(current) == 3
        assert any(r["record_id"] == "rec-b" for r in current)
        calls.append((post_rows, post_state))
        return True, "GitHub H127 state/hash confirmed"
    monkeypatch.setattr(p, "_sync_records_github_snapshot_v191_h8", verified_write)
    # Emulate the limited visible Streamlit table: its other two records do
    # not carry the full historical schema.
    ui_after = [{"record_id": r["record_id"], "股票代號":r["股票代號"], "推薦日期":r["推薦日期"]}
                for r in before if r["record_id"] != "rec-b"]
    report = p.save_records_mutation_fast(ui_after, deleted_ids=["rec-b"],
        previous_count=3, require_remote_confirm=True, reason="08 page delete")
    assert report.permanent_ok and report.github_ok and report.local_ok
    current, _, _ = p.read_local_json(p.RECORDS_FILE, [])
    assert {x["record_id"] for x in current} == {"rec-a", "rec-c"}
    assert [x["不可見欄位"] for x in current] == ["永久保留a", "永久保留c"]
    assert len(calls) == 1
    gh_status, _, _ = p.read_local_json(p.RECORDS_GITHUB_SYNC_STATUS_FILE, {})
    assert gh_status.get("status") == "success"
    assert gh_status.get("count") == 2
    persisted_state, _, _ = p.read_local_json(p.RECORDS_STATE_FILE, {})
    assert persisted_state.get("github_pending") is False


def test_delete_newest_day_is_legal_when_pre_delete_authority_verified(local, monkeypatch):
    before, _, _ = local
    monkeypatch.setattr(p, "_sync_records_github_snapshot_v191_h8", lambda *args: (True, "verified"))
    after = [r for r in before if r["record_id"] != "rec-b"]
    report = p.save_records_mutation_fast(after, deleted_ids=["rec-b"], previous_count=3,
                                          require_remote_confirm=True, reason="delete latest")
    assert report.permanent_ok
    now, _, _ = p.read_local_json(p.RECORDS_FILE, [])
    assert p._latest_record_recommendation_date(now) == "2026-11-12"


def test_http403_delete_never_changes_local_authority(local, monkeypatch):
    before, _, _ = local
    monkeypatch.setattr(p, "_sync_records_github_snapshot_v191_h8", lambda *args: (False, "GitHub HTTP 403 Contents permission denied"))
    after = [r for r in before if r["record_id"] != "rec-b"]
    report = p.save_records_mutation_fast(after, deleted_ids=["rec-b"], previous_count=3,
                                          require_remote_confirm=True, reason="delete")
    assert not report.local_ok and not report.permanent_ok
    assert "403" in report.github_message
    current, _, _ = p.read_local_json(p.RECORDS_FILE, [])
    assert current == before


def test_partial_2558_vs_2632_analogue_fails_closed(local, monkeypatch):
    before, _, _ = local
    extended = before + [{"record_id":"rec-d","股票代號":"2254","推薦日期":"2026-11-13","推薦模式":"研究"}]
    state4 = remote_state(extended)
    monkeypatch.setattr(p, "read_github_json", lambda name, default:(state4,"remote larger"))
    after = [r for r in before if r["record_id"] != "rec-b"]
    report = p.save_records_mutation_fast(after, deleted_ids=["rec-b"], previous_count=3,
                                          require_remote_confirm=True, reason="delete")
    assert not report.permanent_ok and "H161" in report.local_message
    current, _, _ = p.read_local_json(p.RECORDS_FILE, [])
    assert current == before


def test_same_count_but_different_content_hash_must_block_delete(local, monkeypatch):
    before, _, _ = local
    altered = [dict(x) for x in before]
    altered[0]["不可見欄位"] = "newer concurrent edit"
    state3 = remote_state(altered)
    monkeypatch.setattr(p,"read_github_json",lambda name,default:(state3,"different remote"))
    after = [r for r in before if r["record_id"] != "rec-b"]
    report = p.save_records_mutation_fast(after, deleted_ids=["rec-b"], previous_count=3,require_remote_confirm=True)
    assert not report.permanent_ok and "Hash" in report.local_message


def test_unknown_record_id_must_not_be_deleted(local):
    before, _, _ = local
    result = p.save_records_mutation_fast(before,deleted_ids=["not-real"],previous_count=3,require_remote_confirm=True)
    assert not result.permanent_ok and "record_id" in result.local_message


def test_no_firestore_and_no_token_fail_closed(local, monkeypatch):
    before, _, _ = local
    monkeypatch.setattr(p,"github_config",lambda:{"token":"","owner":"demo","repo":"stock-app","branch":"runtime-data"})
    result=p.save_records_mutation_fast(before[1:],deleted_ids=["rec-a"],previous_count=3,require_remote_confirm=True)
    assert not result.permanent_ok and "GITHUB_TOKEN" in result.local_message


def test_403_is_not_a_cas_conflict_or_retry(local, monkeypatch):
    monkeypatch.setattr(p,"_github_meta_sha",lambda *args:("sha-test",200,"ok"))
    monkeypatch.setattr(p,"_github_url",lambda name:"https://api.github.com/mock")
    called=[]
    class Resp:
        status_code=403
        text='{"message":"Resource not accessible by personal access token"}'
        content=b''
    def fake_put(*args, **kwargs):
        called.append(1)
        return Resp()
    monkeypatch.setattr(p.requests,"put",fake_put)
    monkeypatch.setattr(p.time,"sleep",lambda *a: (_ for _ in ()).throw(AssertionError("must not retry 403")))
    ok,msg=p.write_github_json("test_authority.json",{"ok":1})
    assert not ok and "403" in msg and "Contents" in msg and len(called)==1


def test_superset_recovery_same_business_date_even_if_local_activity_newer(local, monkeypatch):
    before, local_state, root=local
    large=before+[{"record_id":"rec-d","股票代號":"2254","推薦日期":"2026-11-13","推薦模式":"研究"}]
    state4=remote_state(large)
    # The local 3-row state looks more recently stamped than the 4-row GitHub
    # state, but it was not an explicit verified delete.
    local_state["updated_at_epoch"] += 3000
    local_state["revision"] += 3_000_000_000_000
    assert p.write_local_json_atomic(p.RECORDS_STATE_FILE,local_state)[0]
    def fetch(name,default):
        return (state4,"verified state") if name==p.RECORDS_STATE_FILE else (large,"full 4 rows")
    monkeypatch.setattr(p,"read_github_json",fetch)
    monkeypatch.setattr(p,"_records_history_gap_audit_needed",lambda *a:(False,"not needed"))
    result,details,restored=p.ensure_records_local_authority_current()
    assert restored and len(result)==4
    assert any("H161不採用局部權威" in x for x in details)
    assert len(p.read_local_json(p.RECORDS_FILE,[])[0])==4
    assert list((root/".cache/h161_authority_pre_restore").glob("local_*.json"))


def test_explicit_verified_firestore_delete_not_resurrected_by_stale_github(local, monkeypatch):
    before, _, _=local
    shortened=[r for r in before if r["record_id"]!="rec-a"]
    local_state=remote_state(shortened,mutation_kind="destructive",github_pending=True,mutation_deleted_count=1)
    assert p.write_local_json_atomic(p.RECORDS_FILE,shortened)[0]
    assert p.write_local_json_atomic(p.RECORDS_STATE_FILE,local_state)[0]
    monkeypatch.setattr(p,"firebase_configured",lambda:True)
    monkeypatch.setattr(p,"_read_records_firestore_summary",lambda:(local_state,"verified FS"))
    monkeypatch.setattr(p,"_records_history_gap_audit_needed",lambda *a:(False,"not needed"))
    outcome,details,restored=p.ensure_records_local_authority_current()
    assert not restored and len(outcome)==2


def test_page8_status_not_falsely_green():
    src=(ROOT/'pages'/'8_股神推薦紀錄.py').read_text(encoding='utf-8')
    assert 'H161刪除未執行' in src
    assert 'H161精確record_id核驗' in src
    assert 'getattr(st, _h161_del_kind)' in src
    ast.parse(src)
