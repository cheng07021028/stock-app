# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print("PASS", msg)


def _cfg(sched):
    cfg = sched.normalize_settings({
        "enabled": True,
        "weekdays_only": False,
        "catch_up_missed_same_day": True,
        "grace_minutes": 35,
        "jobs": {
            "godpick_recommendation": {"enabled": True, "times": ["22:10"]},
            "record_latest_price": {"enabled": True, "times": ["20:25"]},
        },
    })
    for job, row in cfg["jobs"].items():
        if job not in {"godpick_recommendation", "record_latest_price"}:
            row["enabled"] = False
    return cfg


def main() -> None:
    import godpick_auto_scheduler as sched

    now = datetime(2026, 8, 14, 22, 20, tzinfo=sched.TZ)
    cfg = _cfg(sched)

    # Due work + no healthy worker + stale wake => Page17 should enqueue one external wake.
    status = {
        "jobs": {},
        "completed_run_keys": [],
        "last_wakeup_at": "2026-08-14 21:58:00",
        "updated_at": "2026-08-14 21:58:00",
    }
    decision = sched.scheduler_wakeup_decision(cfg, status, now)
    check(decision["should_dispatch"] and set(decision["due_jobs"]) == {"godpick_recommendation", "record_latest_price"},
          "Page17 requests catch-up wake when due work has no healthy/recent worker")

    # A healthy worker must suppress duplicate dispatches, even though rows remain due until checkpointed complete.
    active_status = {
        **status,
        "active_run": {
            "mode": "scheduled",
            "pending_jobs": ["godpick_recommendation", "record_latest_price"],
            "planned_jobs": ["godpick_recommendation", "record_latest_price"],
            "current_job": "godpick_recommendation",
            "current_job_started_at": "2026-08-14 22:18:00",
            "last_progress_at": "2026-08-14 22:19:30",
            "updated_at": "2026-08-14 22:19:30",
            "completed_jobs": [],
            "failed_jobs": [],
            "blocked_jobs": [],
        },
    }
    active_decision = sched.scheduler_wakeup_decision(cfg, active_status, now)
    check(not active_decision["should_dispatch"] and active_decision["active_healthy"] and active_decision["reason"] == "active_worker",
          "healthy active worker suppresses duplicate Page17 workflow_dispatch")

    # If Actions was just woken but active_run has not checkpointed yet, allow startup time instead of dispatch storm.
    recent_status = {**status, "last_wakeup_at": "2026-08-14 22:16:00"}
    recent_decision = sched.scheduler_wakeup_decision(cfg, recent_status, now)
    check(not recent_decision["should_dispatch"] and recent_decision["recent_wakeup"] and recent_decision["reason"] == "recent_wakeup",
          "recent wake suppresses duplicate dispatch while worker is starting")

    # Table itself must reveal current work and queue order, not merely '到期待執行'.
    rows = sched.next_run_rows(cfg, active_status, now)
    by_id = {row["工作ID"]: row for row in rows}
    check(by_id["godpick_recommendation"]["到期狀態"] == "▶ 執行中" and by_id["godpick_recommendation"]["本輪順序"] == "1/2",
          "scheduler table marks exact current job and batch position")
    check(by_id["record_latest_price"]["到期狀態"] == "⏳ 本輪排隊" and by_id["record_latest_price"]["本輪順序"] == "2/2",
          "scheduler table marks queued job instead of looking idle")

    # Dispatch service is separately unit-tested: 204 is success, auth failure is surfaced,
    # and the PAT remains in the Authorization header instead of the URL.
    import godpick_scheduler_wakeup_service as wake_service
    calls = []
    class Resp:
        def __init__(self, code): self.status_code = code
    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return Resp(204)
    ok, msg = wake_service.dispatch_scheduler_wakeup(
        token="TEST_TOKEN_ONLY", owner="owner", repo="repo", branch="main", http_post=fake_post
    )
    check(ok and calls and calls[0][0].endswith("/actions/workflows/godpick_auto_scheduler_v191.yml/dispatches"),
          "H19 wake service submits the expected workflow_dispatch endpoint")
    check("TEST_TOKEN_ONLY" not in calls[0][0] and calls[0][1]["headers"]["Authorization"] == "Bearer TEST_TOKEN_ONLY",
          "H19 wake service keeps token in Authorization header, never URL")
    denied, denied_msg = wake_service.dispatch_scheduler_wakeup(
        token="TEST_TOKEN_ONLY", owner="owner", repo="repo", http_post=lambda *a, **k: Resp(403)
    )
    check(not denied and "HTTP 403" in denied_msg,
          "H19 wake service reports permission failure without running jobs inline")

    # Wiring/UI contract: automatic dispatch is asynchronous and refreshes status without running heavy work inline.
    page17 = (ROOT / "pages" / "17_系統健康檢查.py").read_text(encoding="utf-8")
    check('scheduler_wakeup_decision as auto_wakeup_decision' in page17,
          "Page17 uses pure H19 wakeup decision contract")
    check("dispatch_auto_scheduler_wakeup" in page17 and "page17_auto_catchup" in page17,
          "Page17 delegates catch-up to isolated workflow wake service")
    check('run_every="8s"' in page17 and '_v191_h19_autocatchup_try_epoch' in page17,
          "Page17 live panel auto-refreshes and rate-limits catch-up dispatches")
    check('不會在 Streamlit UI 執行 07/08 大型任務' in page17,
          "Page17 documents that UI never performs heavy catch-up inline")
    check("hotfix19_page17_autocatchup" in sched.VERSION,
          "scheduler version advances to H19")

    print("H19 Page17 auto catch-up/live status smoke: ALL PASS")


if __name__ == "__main__":
    main()
