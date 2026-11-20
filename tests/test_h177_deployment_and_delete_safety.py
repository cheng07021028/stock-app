# -*- coding: utf-8 -*-
"""H177: production deploy signature and fail-closed record deletion checks."""
from __future__ import annotations
import ast
import importlib
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
if "streamlit" not in sys.modules:
    fake = types.ModuleType("streamlit")
    fake.secrets = {}
    sys.modules["streamlit"] = fake
p = importlib.import_module("godpick_persistence_service")


def base_records():
    return [
        {"record_id": "rid-1", "股票代號": "1477", "推薦日期": "2026-11-19", "hidden": 1},
        {"record_id": "rid-2", "股票代號": "2317", "推薦日期": "2026-11-19", "hidden": 2},
    ]


@pytest.fixture
def authorities(monkeypatch):
    old = base_records()
    state = p._new_state("H177-test", old, count=2)
    monkeypatch.setattr(p, "github_config", lambda: {"token": "configured"})
    monkeypatch.setattr(p, "firebase_configured", lambda: True)
    monkeypatch.setattr(p, "read_github_json", lambda *args, **kwargs: (state, "verified Github state"))
    return old, state


def test_firebase_configured_but_summary_unavailable_must_not_use_github_only(authorities, monkeypatch):
    old, _ = authorities
    monkeypatch.setattr(p, "_read_records_firestore_summary", lambda: ({}, "HTTP 429 配額限制"))
    remaining = [old[1]]
    ok, reason = p._h141_prevent_remote_rollback(
        remaining, deleted_ids=["rid-1"], previous_rows=old, previous_count=2
    )
    assert not ok
    assert "H177" in reason and "Firestore" in reason
    assert "HTTP 429" in reason


def test_firebase_summary_wrong_hash_still_uses_h176_verified_reconcile(authorities, monkeypatch):
    old, state = authorities
    monkeypatch.setattr(p, "_read_records_firestore_summary", lambda: ({"count": 1, "payload_hash": "older"}, "older"))
    called = []
    def safe_repair(rows, *, expected_local_hash):
        called.append(expected_local_hash)
        return True, "H176 verified repair", state
    monkeypatch.setattr(p, "_h176_repair_firestore_gap_for_verified_delete", safe_repair)
    ok, reason = p._h141_prevent_remote_rollback(
        [old[1]], deleted_ids=["rid-1"], previous_rows=old, previous_count=2
    )
    assert ok, reason
    assert called == [p._json_hash(old)]


def test_remote_summary_hash_mismatch_cannot_bypass_h176(authorities, monkeypatch):
    old, _ = authorities
    monkeypatch.setattr(p, "_read_records_firestore_summary", lambda: ({"count": 1, "payload_hash": "older"}, "older"))
    monkeypatch.setattr(p, "_h176_repair_firestore_gap_for_verified_delete", lambda *a, **k: (False,"H176差異仍存在",{}))
    ok, reason = p._h141_prevent_remote_rollback(
        [old[1]], deleted_ids=["rid-1"], previous_rows=old, previous_count=2
    )
    assert not ok and "H176差異仍存在" in reason


def test_good_two_authorities_allow_selected_id_delete(authorities, monkeypatch):
    old, state = authorities
    monkeypatch.setattr(p, "_read_records_firestore_summary", lambda: (state, "verified"))
    ok, reason = p._h141_prevent_remote_rollback(
        [old[1]], deleted_ids=["rid-1"], previous_rows=old, previous_count=2
    )
    assert ok and "H161精確刪除預檢成功" in reason


def test_ghost_delete_or_missing_predelete_snapshot_blocked(authorities, monkeypatch):
    old, state = authorities
    monkeypatch.setattr(p, "_read_records_firestore_summary", lambda: (state, "verified"))
    ok, msg = p._h141_prevent_remote_rollback([old[1]],deleted_ids=["ghost"],previous_rows=old,previous_count=2)
    assert not ok and "身分" in msg
    ok, msg = p._h141_prevent_remote_rollback([old[1]],deleted_ids=["rid-1"],previous_rows=None,previous_count=2)
    assert not ok and "完整權威" in msg


def test_page8_visible_version_and_no_destructive_legacy_fallback():
    page = (ROOT / 'pages' / '8_股神推薦紀錄.py').read_text('utf-8')
    ast.parse(page)
    assert 'H177｜刪除修正版已載入' in page
    assert 'if deleted_ids:\n        _set_status("H177安全阻擋' in page
    assert 'local_ok = bool(local_ok and getattr(report, "permanent_ok", False))' in page


def test_source_only_does_not_embed_secrets_or_recommendations():
    package = [ROOT/'godpick_persistence_service.py',ROOT/'godpick_h176_delete_reconcile.py',ROOT/'pages'/'8_股神推薦紀錄.py']
    for f in package:
        assert f.is_file() and f.stat().st_size > 50
        ast.parse(f.read_text('utf8'))
