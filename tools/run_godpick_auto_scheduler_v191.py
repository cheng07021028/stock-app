# -*- coding: utf-8 -*-
"""V191-H156: central worker entry; Page17 manual scopes remain auditable."""
from pathlib import Path
import json, sys, os
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_auto_scheduler import (
    JOB_LABELS, H155_REQUIRED_REFRESH_JOBS, H156_MANUAL_BATCH_MODES,
    h156_plan_manual_batch, load_settings, run_due_jobs,
)


def run_worker_dispatch(manual_job: str):
    manual_job = str(manual_job or "").strip()
    if manual_job in H156_MANUAL_BATCH_MODES:
        plan = h156_plan_manual_batch(load_settings(), manual_job)
        os.environ["GODPICK_PAGE17_FULL_REFRESH"] = "1"
        return run_due_jobs(
            force_all_enabled=True,
            selected_jobs=plan,
            execution_order_override=plan,
            temporary_enable_jobs=[*H155_REQUIRED_REFRESH_JOBS, "godpick_recommendation"],
            strict_same_run=True,
            manual_batch_mode=manual_job,
        )
    if manual_job:
        if manual_job not in JOB_LABELS:
            return {"ok": False, "message": f"未知中央手動工作：{manual_job}；未執行任何工作", "executed": [], "skipped": True}
        # H156: manual refresh still runs if its unattended schedule is disabled.
        return run_due_jobs(force_all_enabled=True, selected_jobs=[manual_job], temporary_enable_jobs=[manual_job])
    return run_due_jobs()


if __name__ == "__main__":
    res = run_worker_dispatch(os.environ.get("GODPICK_MANUAL_JOB") or "")
    print(json.dumps({
        "ok": res.get("ok"), "message": res.get("message"),
        "batch_id": res.get("batch_id"), "manual_batch_mode": res.get("manual_batch_mode"),
        "executed": res.get("executed", []),
    }, ensure_ascii=False, indent=2, default=str))
    # Preserve H33: the always-on 10-minute wake shall not produce email noise
    # for provider failures. Runtime status/history is the operational authority.
    raise SystemExit(0)
