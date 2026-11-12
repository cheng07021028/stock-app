# -*- coding: utf-8 -*-
"""H156 deterministic offline regression; never calls providers or writes runtime-data."""
from __future__ import annotations
import ast
import copy
import importlib.util
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import godpick_auto_scheduler as scheduler


def _cfg_all(enabled=False):
    cfg = copy.deepcopy(scheduler.DEFAULT_SETTINGS)
    cfg["enabled"] = False  # manual path must still work while unattended is OFF
    cfg["jobs"] = {k: {**v, "enabled": bool(enabled)} for k, v in cfg["jobs"].items()}
    return cfg


def test_core_only_six_in_canonical_order():
    cfg = _cfg_all(True)
    cfg["execution_order"] = list(reversed(scheduler.DEFAULT_EXECUTION_ORDER))
    assert scheduler.h156_plan_manual_batch(cfg, "market_refresh_and_recommend") == list(scheduler.H156_CORE_REFRESH_AND_RECOMMEND)
    assert len(scheduler.h156_plan_manual_batch(cfg, "market_refresh_and_recommend")) == 6


def test_full_mode_keeps_all_enabled_jobs_without_enabling_unrelated_jobs():
    cfg = _cfg_all(False)
    cfg["jobs"]["record_performance"]["enabled"] = True
    cfg["jobs"]["feedback_learning"]["enabled"] = True
    cfg["execution_order"] = list(reversed(scheduler.DEFAULT_EXECUTION_ORDER))
    plan = scheduler.h156_plan_manual_batch(cfg, "full_refresh_and_recommend")
    assert plan[:5] == list(scheduler.H155_REQUIRED_REFRESH_JOBS)
    assert plan[-1] == "godpick_recommendation"
    assert "record_performance" in plan and "feedback_learning" in plan
    assert "record_latest_price" not in plan  # permanently disabled noncritical work remains disabled


def test_unknown_manual_mode_rejected():
    try:
        scheduler.h156_plan_manual_batch(_cfg_all(), "typo")
    except ValueError:
        pass
    else:
        raise AssertionError("Unknown manual mode must fail, never silently run zero jobs")


def test_same_batch_summary_reports_truth():
    rows = [{"job": k, "status": "SUCCESS"} for k in scheduler.H155_REQUIRED_REFRESH_JOBS]
    summary = scheduler.h156_manual_batch_summary(rows)
    assert summary["essential_success"] and not summary["recommendation_executed"] and not summary["all_success"]
    rows += [{"job": "godpick_recommendation", "status": "WARNING"}]
    summary = scheduler.h156_manual_batch_summary(rows)
    assert summary["recommendation_executed"] and not summary["all_success"]
    rows[-1]["status"] = "SUCCESS"
    assert scheduler.h156_manual_batch_summary(rows)["all_success"]
    rows[1]["status"] = "FAILED"
    summary = scheduler.h156_manual_batch_summary(rows)
    assert not summary["essential_success"] and not summary["all_success"]


def test_manual_batch_execution_and_legacy_settings_untouched(monkeypatch):
    """Mock executor ensures prerequisite order and status identity, no network/JSON writes."""
    cfg = _cfg_all(False)
    cfg["execution_order"] = list(reversed(scheduler.DEFAULT_EXECUTION_ORDER))
    cfg["recommendation_run_last_after_all_enabled"] = False
    frozen = copy.deepcopy(cfg)
    prior_status = {
        "jobs": {k: {"last_status": "SUCCESS", "last_success_at": "2026-11-12 00:00:00"}
                 for k in scheduler.H155_REQUIRED_REFRESH_JOBS},
        "completed_run_keys": [],
    }
    monkeypatch.setattr(scheduler, "load_settings", lambda **kwargs: cfg)
    monkeypatch.setattr(scheduler, "load_status", lambda **kwargs: copy.deepcopy(prior_status))
    monkeypatch.setattr(scheduler, "load_history", lambda **kwargs: [])
    monkeypatch.setattr(scheduler, "_acquire_lock", lambda **kwargs: (True, ""))
    monkeypatch.setattr(scheduler, "_release_lock", lambda: None)
    monkeypatch.setattr(scheduler, "_checkpoint_runtime", lambda *a, **kw: None)
    monkeypatch.setattr(scheduler, "_touch_lock", lambda *a, **kw: None)
    monkeypatch.setattr(scheduler, "_execute_one", lambda job, *_: {"ok": True, "message": f"mock {job}", "finished_at": "2026-11-12 17:30:00"})
    plan = scheduler.h156_plan_manual_batch(cfg, "market_refresh_and_recommend")
    result = scheduler.run_due_jobs(force_all_enabled=True, selected_jobs=plan, execution_order_override=plan,
                                    temporary_enable_jobs=plan, strict_same_run=True,
                                    manual_batch_mode="market_refresh_and_recommend")
    assert result["ok"] and len(result["executed"]) == 6
    assert [row["job"] for row in result["executed"]] == plan
    assert all(row["status"] == "SUCCESS" for row in result["executed"])
    assert len(set(row["batch_id"] for row in result["executed"])) == 1
    assert result["status"]["last_manual_batch"]["validation"]["all_success"]
    assert cfg == frozen, "Temporary one-shot override must never save/mutate settings"


def test_strict_same_run_blocks_when_current_macro_failed(monkeypatch):
    cfg = _cfg_all(False)
    prior_status = {"jobs": {k: {"last_status": "SUCCESS", "last_success_at": "2026-11-12 14:25:00"} for k in scheduler.H155_REQUIRED_REFRESH_JOBS}, "completed_run_keys": []}
    monkeypatch.setattr(scheduler, "load_settings", lambda **kwargs: cfg)
    monkeypatch.setattr(scheduler, "load_status", lambda **kwargs: copy.deepcopy(prior_status))
    monkeypatch.setattr(scheduler, "load_history", lambda **kwargs: [])
    monkeypatch.setattr(scheduler, "_acquire_lock", lambda **kwargs: (True, ""))
    monkeypatch.setattr(scheduler, "_release_lock", lambda: None)
    monkeypatch.setattr(scheduler, "_checkpoint_runtime", lambda *a, **kw: None)
    monkeypatch.setattr(scheduler, "_touch_lock", lambda *a, **kw: None)
    monkeypatch.setattr(scheduler, "_execute_one", lambda job, *_: {"ok": job != "macro_full", "message": f"mock {job}", "finished_at": "2026-11-12 17:30:00"})
    plan = list(scheduler.H156_CORE_REFRESH_AND_RECOMMEND)
    result = scheduler.run_due_jobs(force_all_enabled=True, selected_jobs=plan, execution_order_override=plan,
                                    temporary_enable_jobs=plan, strict_same_run=True,
                                    manual_batch_mode="market_refresh_and_recommend")
    assert not result["ok"]
    assert result["executed"][1]["status"] == "FAILED"
    assert result["executed"][-1]["status"] == "BLOCKED"
    assert "H155本輪前置未全部成功" in result["executed"][-1]["message"]
    assert not result["status"]["last_manual_batch"]["validation"]["recommendation_executed"]


def test_full_batch_recommendation_must_wait_every_predecessor():
    plan = list(scheduler.H156_CORE_REFRESH_AND_RECOMMEND)
    plan.insert(-1, "record_performance")
    rows = [{"job": k, "status": "SUCCESS"} for k in plan[:-1]]
    ok, msg = scheduler.h156_verify_full_batch_predecessors(rows, plan)
    assert ok and not msg
    rows[-1]["status"] = "WARNING"
    ok, msg = scheduler.h156_verify_full_batch_predecessors(rows, plan)
    assert not ok and "推薦" in msg and "WARNING" in msg
    rows[-1]["status"] = "FAILED"
    ok, msg = scheduler.h156_verify_full_batch_predecessors(rows, plan)
    assert not ok and "FAILED" in msg


def test_super_ai_partial_not_green(monkeypatch):
    import godpick_auto_update_tasks as tasks
    fake = types.SimpleNamespace(refresh_super_ai_market_context=lambda fetch_etf: (
        {"margin_by_stock": {"2330": {"x": 1}}, "taifex": {}, "etf": {}}, ["TAIFEX failed"]))
    monkeypatch.setitem(sys.modules, "godpick_super_ai_market_context", fake)
    res = tasks.task_super_ai_context({"fetch_etf": True})
    assert res["ok"] and res["warning"] and not res["details"]["complete"]


def test_super_ai_complete_success(monkeypatch):
    import godpick_auto_update_tasks as tasks
    fake = types.SimpleNamespace(refresh_super_ai_market_context=lambda fetch_etf: (
        {"margin_by_stock": {"2330": {}}, "taifex": {"PCR": 99.0}, "etf": {}}, ["OK"]))
    monkeypatch.setitem(sys.modules, "godpick_super_ai_market_context", fake)
    res = tasks.task_super_ai_context({})
    assert res["ok"] and not res["warning"] and res["details"]["complete"]


def test_super_ai_both_missing_failed(monkeypatch):
    import godpick_auto_update_tasks as tasks
    fake = types.SimpleNamespace(refresh_super_ai_market_context=lambda fetch_etf: (
        {"margin_by_stock": {}, "taifex": {}, "etf": {}}, ["failed"]))
    monkeypatch.setitem(sys.modules, "godpick_super_ai_market_context", fake)
    res = tasks.task_super_ai_context({})
    assert not res["ok"] and not res["warning"]


def test_worker_single_manual_id_and_nonexistent_validation(monkeypatch):
    path = ROOT / "tools" / "run_godpick_auto_scheduler_v191.py"
    spec = importlib.util.spec_from_file_location("h156_worker", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    calls = []
    monkeypatch.setattr(mod, "run_due_jobs", lambda **kw: calls.append(kw) or {"ok": True, "executed": []})
    assert mod.run_worker_dispatch("unknown_job")["ok"] is False
    assert calls == []
    mod.run_worker_dispatch("official_factors")
    assert calls[-1]["selected_jobs"] == ["official_factors"]
    assert calls[-1]["temporary_enable_jobs"] == ["official_factors"]
    mod.run_worker_dispatch("market_refresh_and_recommend")
    assert calls[-1]["strict_same_run"] is True
    assert len(calls[-1]["selected_jobs"]) == 6


def test_ui_route_and_legacy_tools_preserved():
    ui = (ROOT / "pages/17_系統健康檢查.py").read_text(encoding="utf-8")
    assert '_h156_dispatch_refresh("market_refresh_and_recommend")' in ui
    assert '_h156_dispatch_refresh("full_refresh_and_recommend")' in ui
    assert 'run_global_update(' in ui
    assert '解除逾時更新鎖定' in ui
    assert 'save_auto_scheduler_settings(' in ui
    assert 'last_manual_batch' in ui and 'batch_id' in ui
    assert 'if not global_result.get("ok") or not ok_count' in ui


def test_modified_files_parse():
    for rel in ("godpick_auto_scheduler.py", "godpick_auto_update_tasks.py", "pages/17_系統健康檢查.py", "tools/run_godpick_auto_scheduler_v191.py"):
        ast.parse((ROOT / rel).read_text(encoding="utf-8"), filename=rel)
