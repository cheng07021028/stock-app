# -*- coding: utf-8 -*-
"""H158 offline: data-freshness, weekend semantics and advisory final gate."""
import copy
import sys
import types
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import godpick_auto_scheduler as s
import godpick_auto_update_tasks as tasks

TZ = ZoneInfo("Asia/Taipei")


def _cfg():
    x = copy.deepcopy(s.DEFAULT_SETTINGS)
    x["enabled"] = True
    x["weekdays_only"] = True
    return x


def test_weekend_not_falsely_overdue():
    cfg = _cfg()
    rows = s.next_run_rows(cfg, {"jobs": {}, "completed_run_keys": []}, now=datetime(2026, 11, 14, 10, 55, tzinfo=TZ))
    assert len(rows) == 14
    assert all("到期待執行" not in row["下次預計"] and "到期待執行" not in row["到期狀態"] for row in rows)
    assert all("2026-11-16" in row["下次預計"] for row in rows)


def test_weekday_overdue_preserved():
    cfg = _cfg()
    rows = s.next_run_rows(cfg, {"jobs": {}, "completed_run_keys": []}, now=datetime(2026, 11, 13, 23, 50, tzinfo=TZ))
    assert any("到期待執行/補跑" in row["下次預計"] for row in rows)


def _final_state(optional_state="FAILED", core_state="SUCCESS", optional_at="2026-11-13 21:40"):
    cfg = _cfg()
    for j, conf in cfg["jobs"].items():
        conf["enabled"] = j in {"stock_master", "record_performance", "godpick_recommendation"}
        conf["times"] = ["21:40"]
    jobs = {
        "stock_master": {"last_status": core_state, "last_slot": optional_at, "last_run_at": "2026-11-13 21:42:00"},
        "record_performance": {"last_status": optional_state, "last_slot": optional_at, "last_run_at": "2026-11-13 21:49:00"},
    }
    completed = ["stock_master|2026-11-13 21:40"] if core_state == "SUCCESS" else []
    if optional_state in ("SUCCESS", "WARNING"):
        completed.append("record_performance|2026-11-13 21:40")
    return cfg, {"jobs": jobs, "completed_run_keys": completed}


def test_optional_failed_after_actual_attempt_warns_not_blocks():
    cfg, status = _final_state()
    ok, msg = s._recommendation_final_gate(cfg, status, datetime(2026, 11, 13, 22, 50, tzinfo=TZ), "godpick_recommendation")
    assert ok and "record" not in msg and "FAILED" in msg


def test_core_failure_still_blocks():
    cfg, status = _final_state(core_state="FAILED")
    ok, msg = s._recommendation_final_gate(cfg, status, datetime(2026, 11, 13, 22, 50, tzinfo=TZ), "godpick_recommendation")
    assert not ok and "股票主檔" in msg


def test_optional_earlier_slot_must_not_bypass():
    cfg, status = _final_state(optional_at="2026-11-13 20:35")
    status["jobs"]["stock_master"]["last_slot"] = "2026-11-13 21:40"
    ok, msg = s._recommendation_final_gate(cfg, status, datetime(2026, 11, 13, 22, 50, tzinfo=TZ), "godpick_recommendation")
    assert not ok and "績效" in msg


def test_h155_manual_strict_gate_kept():
    rows = [{"job": k, "status": "SUCCESS"} for k in s.H155_REQUIRED_REFRESH_JOBS]
    rows[-2]["status"] = "WARNING"
    assert not s.h155_verify_same_run_dependencies(rows)[0]


def test_record_performance_requires_sync_remote_ack(monkeypatch):
    from types import SimpleNamespace
    calls = []
    monkeypatch.setattr(tasks, "_ensure_records_authority_safe", lambda: ([{"record_id":"safe"}], ["OK"], False))
    monkeypatch.setitem(sys.modules, "godpick_perf_fast_update_v77", SimpleNamespace(update_recommendation_perf_fast_v77=lambda **kw: {"ok": True}))
    def fake_save(rows, **kw):
        calls.append(kw)
        return SimpleNamespace(permanent_ok=False, github_pending=True, github_message="queued", firestore_message="not configured")
    monkeypatch.setitem(sys.modules, "godpick_persistence_service", SimpleNamespace(save_records_sync_fast=fake_save))
    res = tasks.task_record_performance({})
    assert calls and calls[0]["require_remote_confirm"] is True
    assert res["ok"] is False
    assert "永久同步未確認" in res["message"]


def test_record_performance_confirmed_success(monkeypatch):
    monkeypatch.setattr(tasks, "_ensure_records_authority_safe", lambda: ([{"record_id":"safe"}], [], False))
    monkeypatch.setitem(sys.modules, "godpick_perf_fast_update_v77", types.SimpleNamespace(update_recommendation_perf_fast_v77=lambda **kw: {"ok": True}))
    monkeypatch.setitem(sys.modules, "godpick_persistence_service", types.SimpleNamespace(save_records_sync_fast=lambda *a, **kw: types.SimpleNamespace(permanent_ok=True, github_pending=False)))
    res = tasks.task_record_performance({})
    assert res["ok"] and "遠端已確認" in res["message"]


def test_latest_price_permissions_unchanged():
    src = (ROOT / "godpick_auto_scheduler.py").read_text(encoding="utf-8")
    assert '"godpick_recommendation": {' in src
    assert "H155_REQUIRED_REFRESH_JOBS" in src
    assert "H126" in src
