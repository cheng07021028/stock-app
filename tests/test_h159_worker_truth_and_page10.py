# -*- coding: utf-8 -*-
"""H159 offline regression: do not touch production JSON, credentials, providers."""
from __future__ import annotations

import ast
import copy
import os
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import godpick_auto_scheduler as sched


def test_transient_remote_failure_does_not_stop_unrelated_tracking():
    assert not sched._h1533_remote_failure(
        "recommend_list_performance",
        {"ok": False, "message": "推薦清單績效更新完成；永久同步失敗", "details": {"sync_messages": ["GitHub：409 conflict"]}},
    )
    assert not sched._h1533_remote_failure(
        "recommend_list_performance", {"ok": False, "message": "永久同步失敗", "details": {"sync_messages": ["遠端回讀待確認／timeout"]}},
    )


def test_confirmed_token_or_permission_error_still_opens_circuit():
    for error in ("HTTP 401", "HTTP 403", "Bad credentials", "resource not accessible by integration"):
        assert sched._h1533_remote_failure(
            "recommend_list_performance", {"ok": False, "message": "永久同步失敗", "details": {"sync_messages": [error]}},
        )
    assert not sched._h1533_remote_failure("super_ai_context", {"ok": False, "message": "永久同步失敗｜HTTP 403"})


def test_full_validation_rejects_optional_failures_even_after_recommendation():
    plan = sched.h156_plan_manual_batch(sched.DEFAULT_SETTINGS, "full_refresh_and_recommend")
    records = [{"job": k, "status": "SUCCESS"} for k in plan]
    assert sched.h156_manual_batch_summary(records, planned_jobs=plan)["all_success"]
    for bad in ("FAILED", "WARNING", "BLOCKED"):
        samples = [dict(x) for x in records]
        samples[[x["job"] for x in samples].index("recommend_list_performance")]["status"] = bad
        result = sched.h156_manual_batch_summary(samples, planned_jobs=plan)
        assert result["essential_success"]
        assert not result["all_success"]
    assert sched.h156_manual_batch_summary(records[:-1], planned_jobs=plan)["recommendation_status"] == "NOT_RUN"


def test_full_manual_optional_failure_runs_recommendation_as_warning_not_fake_green(monkeypatch):
    cfg = copy.deepcopy(sched.DEFAULT_SETTINGS)
    cfg["enabled"] = True
    cfg["weekdays_only"] = True
    plan = sched.h156_plan_manual_batch(cfg, "full_refresh_and_recommend")
    calls = []
    monkeypatch.setattr(sched, "load_settings", lambda **_: copy.deepcopy(cfg))
    monkeypatch.setattr(sched, "load_status", lambda **_: {"jobs": {}, "completed_run_keys": []})
    monkeypatch.setattr(sched, "load_history", lambda **_: [])
    monkeypatch.setattr(sched, "_acquire_lock", lambda **_: (True, ""))
    monkeypatch.setattr(sched, "_release_lock", lambda: None)
    monkeypatch.setattr(sched, "_checkpoint_runtime", lambda *a, **k: None)
    monkeypatch.setattr(sched, "_touch_lock", lambda *a, **k: None)
    def fake_executor(job, *args):
        calls.append(job)
        return {"ok": job != "recommend_list_performance", "message": "永久同步未確認（模擬）" if job == "recommend_list_performance" else "模擬正常", "finished_at": "2026-11-13 23:05:00"}
    monkeypatch.setattr(sched, "_execute_one", fake_executor)
    output = sched.run_due_jobs(
        now=datetime(2026, 11, 13, 23, 5, tzinfo=ZoneInfo("Asia/Taipei")),
        force_all_enabled=True, selected_jobs=plan, execution_order_override=plan,
        temporary_enable_jobs=plan, strict_same_run=True,
        manual_batch_mode="full_refresh_and_recommend",
    )
    statuses = {row["job"]: row["status"] for row in output["executed"]}
    assert statuses["recommend_list_performance"] == "FAILED"
    assert statuses["godpick_recommendation"] == "WARNING", statuses
    assert "H159全模組" in next(r["message"] for r in output["executed"] if r["job"] == "godpick_recommendation")
    assert not output["status"]["last_manual_batch"]["validation"]["all_success"]
    assert calls[-1] == "godpick_recommendation"
    assert not output["ok"]


def test_missing_core_dependency_prevents_recommendation(monkeypatch):
    cfg = copy.deepcopy(sched.DEFAULT_SETTINGS)
    plan = sched.h156_plan_manual_batch(cfg, "market_refresh_and_recommend")
    monkeypatch.setattr(sched, "load_settings", lambda **_: copy.deepcopy(cfg))
    monkeypatch.setattr(sched, "load_status", lambda **_: {"jobs": {}, "completed_run_keys": []})
    monkeypatch.setattr(sched, "load_history", lambda **_: [])
    monkeypatch.setattr(sched, "_acquire_lock", lambda **_: (True, ""))
    monkeypatch.setattr(sched, "_release_lock", lambda: None)
    monkeypatch.setattr(sched, "_checkpoint_runtime", lambda *a, **k: None)
    monkeypatch.setattr(sched, "_touch_lock", lambda *a, **k: None)
    seen = []
    def fake(job, *args):
        seen.append(job)
        return {"ok": job != "official_factors", "message": "模擬失敗", "finished_at": "2026-11-13 23:07:00"}
    monkeypatch.setattr(sched, "_execute_one", fake)
    out = sched.run_due_jobs(
        now=datetime(2026, 11, 13, 23, 7, tzinfo=ZoneInfo("Asia/Taipei")),
        force_all_enabled=True, selected_jobs=plan, execution_order_override=plan,
        temporary_enable_jobs=plan, strict_same_run=True, manual_batch_mode="market_refresh_and_recommend",
    )
    assert "godpick_recommendation" not in seen
    assert out["executed"][-1]["status"] == "BLOCKED"


def test_page17_real_buttons_dispatch_to_worker_without_local_force():
    tree = ast.parse((ROOT / "pages/17_系統健康檢查.py").read_text(encoding="utf-8"))
    funcs = {node.name: node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert "_h159_dispatch_central" in funcs
    src = (ROOT / "pages/17_系統健康檢查.py").read_text(encoding="utf-8")
    assert '_h159_dispatch_central(manual_job="", wake_source="page17_h159_due_only")' in src
    assert 'manual_job="full_refresh_and_recommend"' in src
    assert "_runall = _run_v191_with_live_progress(force_all=True)" not in src
    assert "_run = _run_v191_with_live_progress(force_all=False)" not in src


def test_page10_worker_upsert_requires_remote_confirm_and_tracks_authority():
    src = (ROOT / "pages/10_推薦清單.py").read_text(encoding="utf-8")
    assert "require_remote_confirm=_h159_worker" in src
    assert "_h159_history_ok = bool(rec_report.permanent_ok) if _h159_worker" in src
    assert "_h159_history_ok and list_report.permanent_ok and latest_report_ok" in src
    assert "if work.empty:" in src  # preserve no-data history guard
    assert "upsert_records_authority_fast(" in src


def test_original_workflows_and_h126_authority_intact():
    src = (ROOT / "godpick_auto_scheduler.py").read_text(encoding="utf-8")
    assert len(sched.JOB_LABELS) == 14
    assert "H155_REQUIRED_REFRESH_JOBS" in src
    assert "H126" in src
    assert "_recommendation_final_gate" in src
    assert "_h1533_sync_breaker" in src
    assert "runtime-data" in (ROOT / "pages/17_系統健康檢查.py").read_text(encoding="utf-8")


def test_manual_verdict_distinguishes_explicit_failure_and_routine_heartbeat():
    from tools.h159_manual_verdict import verify_manual_dispatch
    status = {
        "last_wakeup_run_id": "123",
        "last_wakeup_result": "COMPLETED",
        "last_summary": {"executed": 14, "success": 12, "warning": 1, "failed": 1, "blocked": 0},
        "last_manual_batch": {"batch_id": "123.1", "validation": {"all_success": False}},
    }
    assert verify_manual_dispatch(status, "123", "")[0]
    assert not verify_manual_dispatch(status, "123", "full_refresh_and_recommend")[0]
    status["last_summary"] = {"executed": 14, "success": 14, "warning": 0, "failed": 0, "blocked": 0}
    assert not verify_manual_dispatch(status, "123", "full_refresh_and_recommend")[0]
    status["last_manual_batch"]["validation"]["all_success"] = True
    assert verify_manual_dispatch(status, "123", "full_refresh_and_recommend")[0]
    assert not verify_manual_dispatch(status, "122", "full_refresh_and_recommend")[0]


def test_page10_worker_remote_ack_is_executed_not_just_source_text(monkeypatch):
    import pandas as pd
    from types import SimpleNamespace
    source = (ROOT / "pages/10_推薦清單.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    fn = next(x for x in module.body if isinstance(x, ast.FunctionDef) and x.name == "_sync_records")
    # Compile only the actual function so Streamlit UI/app auth is never invoked.
    code = compile(ast.Module(body=[fn], type_ignores=[]), "page10_h159_isolated", "exec")
    calls = []
    report = SimpleNamespace(local_ok=True, permanent_ok=False, messages=lambda: ["local only"])
    def upsert(rows, **kw):
        calls.append(kw)
        return report, {"before": 10, "after": 10, "added": 0, "updated": 1}
    def save(*args, **kw):
        return SimpleNamespace(permanent_ok=True, messages=lambda: ["remote confirmed"])
    env = {
        "pd": pd, "_ensure_record_columns": lambda df: df,
        "_k": lambda v: v, "st": SimpleNamespace(session_state={}),
        "save_named_json_permanent": save,
        "upsert_records_authority_fast": upsert,
        "load_named_json_permanent": lambda p,d: ({}, []),
        "_safe_str": str, "_now_text": lambda: "2026-11-13 23:00:00",
    }
    exec(code,env)
    data=pd.DataFrame([{"股票代號":"2330","推薦日期":"2026-11-13","資料來源":"本輪推薦清單"}])
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    ok,msg=env["_sync_records"](data)
    assert not ok
    assert calls[-1]["require_remote_confirm"] is True
    assert any("H159" in m for m in msg)
    report.permanent_ok=True
    ok,msg=env["_sync_records"](data)
    assert ok
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("GODPICK_PAGE17_FULL_REFRESH", raising=False)
    report.permanent_ok=False
    ok,msg=env["_sync_records"](data)
    assert ok and calls[-1]["require_remote_confirm"] is False  # original interactive behavior
    n=len(calls)
    assert env["_sync_records"](pd.DataFrame())[0]  # empty-list guard, no history erase
    assert len(calls)==n


def test_workflow_manual_verdict_is_conditional_and_scheduled_noise_guard_unchanged():
    import yaml
    config=yaml.safe_load((ROOT / ".github/workflows/godpick_auto_scheduler_v191.yml").read_text(encoding="utf-8"))
    steps=config["jobs"]["scheduler"]["steps"]
    verdict=[s for s in steps if "H159 explicit" in str(s.get("name", ""))]
    assert len(verdict)==1
    assert "github.event.inputs.manual_job != ''" in verdict[0]["if"]
    assert "h159_manual_verdict.py" in verdict[0]["run"]
    due=[s for s in steps if "Run V191 due checker" in str(s.get("name", ""))][0]
    assert due["continue-on-error"] is True  # routine 10-minute wakes keep soft-fail


def test_explicit_manual_verdict_requires_confirmed_remote_worker(monkeypatch):
    from tools import h159_manual_verdict as verdict
    monkeypatch.setenv("H159_MANUAL_JOB", "full_refresh_and_recommend")
    monkeypatch.setenv("H159_EXPECTED_RUN_ID", "123")
    monkeypatch.setenv("H159_REMOTE_VERIFIER_OUTCOME", "failure")
    assert verdict.main() == 1  # never accept local-only green
    yaml_src = (ROOT / ".github/workflows/godpick_auto_scheduler_v191.yml").read_text(encoding="utf-8")
    assert 'H159_REMOTE_VERIFIER_OUTCOME: ${{ steps.verify.outcome }}' in yaml_src
