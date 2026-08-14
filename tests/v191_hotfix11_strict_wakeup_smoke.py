from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
PAGE17 = ROOT / "pages" / "17_系統健康檢查.py"
WORKFLOW = ROOT / ".github" / "workflows" / "godpick_auto_scheduler_v191.yml"
SCHED = ROOT / "godpick_auto_scheduler.py"
WIN = ROOT / "tools" / "windows"


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print("PASS", msg)


def main():
    page = PAGE17.read_text(encoding="utf-8")
    wf = WORKFLOW.read_text(encoding="utf-8")
    sched_text = SCHED.read_text(encoding="utf-8")

    # Screenshot 1: the giant blue H3-H10 changelog block must be gone.
    check("中央排程只負責『到期檢查與依序執行』" not in page, "Page17 removes screenshot-1 giant information block")
    check("WARNING不是FAILED" not in page, "Page17 no longer renders verbose hotfix changelog in the top scheduler panel")

    # GitHub fallback remains every 10m but is offset from top-of-hour hotspot.
    check('cron: "2-52/10 * * * *"' in wf, "GitHub fallback cron is 10-minute off-peak schedule")
    check('cron: "*/10 * * * *"' not in wf, "old top-of-hour-aligned */10 cron removed")
    check("workflow_dispatch:" in wf and "wakeup_source:" in wf, "workflow accepts external strict wake source")
    check("GODPICK_WAKEUP_EVENT" in wf and "GODPICK_WAKEUP_SOURCE" in wf, "workflow propagates wake source to scheduler")

    # Scheduler records source for Page17 diagnostics.
    check("last_wakeup_source" in sched_text, "scheduler records durable wake source")
    check("hotfix11_dual_wakeup" in sched_text, "scheduler H11 version marker present")

    # Windows strict 10-minute dispatcher safety contract.
    trigger = (WIN / "Invoke-GodPickStrictWakeupV191.ps1").read_text(encoding="utf-8-sig")
    install = (WIN / "Install-GodPickStrictWakeupV191.ps1").read_text(encoding="utf-8-sig")
    uninstall = (WIN / "Uninstall-GodPickStrictWakeupV191.ps1").read_text(encoding="utf-8-sig")
    check("workflow_dispatch" in trigger and "/dispatches" in trigger, "strict dispatcher calls GitHub workflow_dispatch endpoint")
    check('wakeup_source = "windows_task_scheduler"' in trigger, "strict dispatcher labels wake source")
    check("ConvertTo-SecureString" in trigger and "ZeroFreeBSTR" in trigger, "dispatcher decrypts token only in memory and zeroes BSTR")
    check("ConvertFrom-SecureString" in install, "installer stores PAT using Windows DPAPI encrypted form")
    check("New-TimeSpan -Minutes 10" in install, "Windows task repeats every 10 minutes")
    check("AddMinutes(7)" in install, "Windows strict clock is offset from GitHub fallback")
    check("StartWhenAvailable" in install and "MultipleInstances IgnoreNew" in install, "Windows task recovers missed starts without overlapping local dispatches")
    check("Unregister-ScheduledTask" in uninstall, "uninstaller removes strict wake task")
    check("ghp_" not in trigger + install and "github_pat_" not in trigger + install, "no literal GitHub token is embedded")

    # Runtime behavior: wake source is checkpointed in scheduler status.
    import godpick_auto_scheduler as gs
    fake_cfg = gs.normalize_settings({"enabled": True, "weekdays_only": False, "jobs": {}})
    with patch.object(gs, "load_settings", return_value=fake_cfg), \
         patch.object(gs, "load_status", return_value={}), \
         patch.object(gs, "load_history", return_value=[]), \
         patch.dict(os.environ, {"GODPICK_WAKEUP_SOURCE": "windows_task_scheduler"}, clear=False):
        out = gs.run_due_jobs(now=datetime(2026, 8, 14, 13, 7, tzinfo=gs.TZ), simulate=True)
    check(out["status"].get("last_wakeup_source") == "windows_task_scheduler", "runtime status checkpoints strict wake source")

    print("H11 strict wakeup smoke: ALL PASS")


if __name__ == "__main__":
    main()
