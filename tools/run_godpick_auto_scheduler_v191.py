# -*- coding: utf-8 -*-
from pathlib import Path
import json,sys,os
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_auto_scheduler import run_due_jobs
if __name__=="__main__":
    manual_job=str(os.environ.get("GODPICK_MANUAL_JOB") or "").strip()
    if manual_job:
        # H71: Page16/Page17 manual official-factor refresh must execute in the
        # same GitHub/runtime-data worker as unattended scheduling.  A selected
        # manual job uses force semantics so an already-completed scheduled slot
        # does not suppress the explicit operator request.
        res=run_due_jobs(force_all_enabled=True, selected_jobs=[manual_job])
    else:
        res=run_due_jobs()
    print(json.dumps({"ok":res.get("ok"),"message":res.get("message"),"executed":res.get("executed",[])},ensure_ascii=False,indent=2,default=str))
    # Individual job failures are persisted and must remain visible; fail the Action
    # only for scheduler infrastructure errors, not a single market-source miss.
    raise SystemExit(0)
