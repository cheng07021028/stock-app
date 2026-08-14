# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import ModuleType
from unittest.mock import patch
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print("PASS", msg)


def main() -> None:
    import godpick_auto_scheduler as sched

    # 1) Long production jobs must never inherit retry_count=5. A failed full-market
    # scan/latest-price refresh is retried by the NEXT 10-minute wake instead of
    # repeating the expensive business operation six times inside one Actions run.
    check(sched.DEFAULT_JOB_OPTIONS["stock_master"].get("scheduler_immediate_retries") == 0,
          "stock master disables same-wake immediate retries")
    check(sched.DEFAULT_JOB_OPTIONS["godpick_recommendation"].get("scheduler_immediate_retries") == 0,
          "Page07 full-market recommendation disables same-wake immediate retries")
    check(sched.DEFAULT_JOB_OPTIONS["record_latest_price"].get("scheduler_immediate_retries") == 0,
          "Page08 latest-price refresh disables same-wake immediate retries")

    calls = {"n": 0}
    fake_tasks = ModuleType("godpick_auto_update_tasks")
    def fail_handler(_cfg):
        calls["n"] += 1
        return {"ok": False, "message": "simulated failure"}
    fake_tasks.TASK_HANDLERS = {"godpick_recommendation": fail_handler}
    real_tasks = sys.modules.get("godpick_auto_update_tasks")
    sys.modules["godpick_auto_update_tasks"] = fake_tasks
    try:
        result = sched._execute_one(
            "godpick_recommendation",
            {"options": {"scheduler_immediate_retries": 0}},
            {"retry_count": 5, "retry_delay_seconds": 0},
        )
    finally:
        if real_tasks is None:
            sys.modules.pop("godpick_auto_update_tasks", None)
        else:
            sys.modules["godpick_auto_update_tasks"] = real_tasks
    check(calls["n"] == 1 and result.get("max_attempts") == 1,
          "global retry_count=5 cannot repeat Page07 six times in one wake")

    # 2) The durable active-run contract must list only jobs actually due now.
    cfg = sched.normalize_settings({
        "enabled": True,
        "weekdays_only": False,
        "catch_up_missed_same_day": True,
        "grace_minutes": 60,
        "retry_count": 5,
        "jobs": {
            "stock_master": {"enabled": True, "times": ["04:00"]},
            "godpick_recommendation": {"enabled": True, "times": ["22:10"]},
        },
    })
    for _job, _job_cfg in cfg["jobs"].items():
        if _job not in {"stock_master", "godpick_recommendation"}:
            _job_cfg["enabled"] = False
    events: list[dict] = []
    with patch.object(sched, "load_settings", return_value=cfg), \
         patch.object(sched, "load_status", return_value={"jobs": {}, "completed_run_keys": []}), \
         patch.object(sched, "load_history", return_value=[]):
        sched.run_due_jobs(
            now=datetime(2026, 8, 14, 20, 0, tzinfo=sched.TZ),
            simulate=True,
            progress_callback=lambda event: events.append(dict(event)),
        )
    start = next(x for x in events if x.get("event") == "run_start")
    check(start.get("pending_jobs") == ["stock_master"],
          "active/pending UI lists only actually-due jobs instead of all 14 enabled jobs")

    # 3) Persistence H18: central automation can request one blocking GitHub
    # verification. Pending background upload is not misclassified as a hard failure,
    # and no asynchronous worker is launched for that central save.
    if "streamlit" not in sys.modules:
        fake_st = ModuleType("streamlit")
        fake_st.secrets = {}
        fake_st.session_state = {}
        sys.modules["streamlit"] = fake_st
    import godpick_persistence_service as persist

    with tempfile.TemporaryDirectory() as td:
        old_base = persist.BASE_DIR
        direct_calls = {"n": 0}
        queued_calls = {"n": 0}
        persist.BASE_DIR = Path(td)
        try:
            def direct_sync(records, state, reason):
                direct_calls["n"] += 1
                return True, f"mock verified {len(records)} records"
            def should_not_queue(*_args, **_kwargs):
                queued_calls["n"] += 1
                return True, "unexpected queue"
            with patch.object(persist, "firebase_configured", return_value=False), \
                 patch.object(persist, "github_config", return_value={"token": "TEST_ONLY"}), \
                 patch.object(persist, "_sync_records_github_snapshot_v191_h8", side_effect=direct_sync), \
                 patch.object(persist, "schedule_records_github_sync", side_effect=should_not_queue):
                report = persist.save_records_mutation_fast(
                    [{"record_id": "r1", "股票代號": "2330", "推薦日期": "2026-08-14", "推薦模式": "股神正式推薦"}],
                    upsert_rows=[{"record_id": "r1", "股票代號": "2330", "推薦日期": "2026-08-14", "推薦模式": "股神正式推薦"}],
                    reason="H18 smoke",
                    require_remote_confirm=True,
                )
            check(report.local_ok and report.github_ok and report.permanent_ok,
                  "central record mutation succeeds only after blocking remote verification")
            check(not report.github_pending and direct_calls["n"] == 1 and queued_calls["n"] == 0,
                  "central persistence verifies GitHub once and does not spawn background pending sync")
        finally:
            persist.BASE_DIR = old_base

    # 4) Wiring: only headless central automation asks for blocking confirmation;
    # manual UI calls keep the existing fast/background default.
    page7 = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    page8 = (ROOT / "pages" / "8_股神推薦紀錄.py").read_text(encoding="utf-8")
    tasks = (ROOT / "godpick_auto_update_tasks.py").read_text(encoding="utf-8")
    page17 = (ROOT / "pages" / "17_系統健康檢查.py").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "godpick_auto_scheduler_v191.yml").read_text(encoding="utf-8")

    check("require_remote_confirm=True" in page7 and "require_remote_confirm: bool = False" in page7,
          "Page07 central runner requests verified record authority while reusable helper stays parameterized")
    check("def _save_records_dual(df: pd.DataFrame, *, require_remote_confirm: bool = False)" in page8,
          "Page08 manual save remains fast/background by default")
    check('ns["_save_records_dual"](updated, require_remote_confirm=True)' in tasks,
          "Page08 scheduled latest-price job requires one verified remote save")
    check("last_progress_at" in page17 and "current_job" in page17 and "worker 正在執行" in page17,
          "Page17 distinguishes active worker progress from stale wake heartbeat")
    check("本輪實際待處理" in page17,
          "Page17 explains actual due-job count instead of claiming all enabled jobs are pending")
    check("timeout-minutes: 60" in workflow and "cancel-in-progress: false" in workflow,
          "Actions keeps non-cancelling concurrency but has a 60-minute hard stall watchdog")

    print("H18 scheduler stall recovery smoke: ALL PASS")


if __name__ == "__main__":
    main()
