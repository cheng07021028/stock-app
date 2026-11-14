# -*- coding: utf-8 -*-
"""H159 explicit-manual Actions verdict; does not write runtime data.

Recurring Windows/GitHub wakeups have no manual_job and retain H33 soft-fail.
"""
from __future__ import annotations
import json
import os
from pathlib import Path


def verify_manual_dispatch(status: dict, run_id: str, manual_job: str) -> tuple[bool, str]:
    if not manual_job:
        return True, "routine wake: explicit-manual verdict not applicable"
    if str(status.get("last_wakeup_run_id") or "") != str(run_id or "") or not run_id:
        return False, "Scheduler status does not match this GitHub Actions run; old success cannot substitute"
    result = str(status.get("last_wakeup_result") or "")
    if result != "COMPLETED":
        return False, "Explicit manual job incomplete: " + result + "｜" + str(status.get("last_wakeup_message") or "")[:240]
    summary = status.get("last_summary") if isinstance(status.get("last_summary"), dict) else {}
    if int(summary.get("executed") or 0) == 0 or int(summary.get("failed") or 0) or int(summary.get("blocked") or 0):
        return False, "Manual batch not fully finished: " + str(summary)
    if manual_job in {"full_refresh_and_recommend", "market_refresh_and_recommend"}:
        batch = status.get("last_manual_batch") if isinstance(status.get("last_manual_batch"), dict) else {}
        if str(batch.get("batch_id") or "").split(".")[0] != run_id:
            return False, "Batch receipt does not match this GitHub run"
        validation = batch.get("validation") if isinstance(batch.get("validation"), dict) else {}
        if not validation.get("all_success"):
            return False, "Core/full batch not fully verified: " + json.dumps(validation, ensure_ascii=False)[:900]
    elif int(summary.get("success") or 0) < 1:
        return False, "Manual single job has no success evidence: " + str(summary)
    return True, "Explicit manual run fully verified"


def main() -> int:
    job = str(os.environ.get("H159_MANUAL_JOB") or "").strip()
    if not job:
        print("routine wake: no manual verdict required")
        return 0
    if str(os.environ.get("H159_REMOTE_VERIFIER_OUTCOME") or "").lower() != "success":
        print("::error::H159 manual run not remotely confirmed by scheduler verifier")
        return 1
    path = Path(__file__).resolve().parents[1] / "godpick_auto_scheduler_status.json"
    try:
        status = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        print("::error::Cannot read scheduler status for this run: " + type(exc).__name__)
        return 1
    ok, detail = verify_manual_dispatch(status, str(os.environ.get("H159_EXPECTED_RUN_ID") or ""), job)
    print(("H159 VERIFIED: " if ok else "::error::H159 MANUAL FAILED: ") + detail)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
