# -*- coding: utf-8 -*-
from pathlib import Path
import json,sys,os
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_auto_scheduler import run_due_jobs
if __name__=="__main__":
    manual_job=str(os.environ.get("GODPICK_MANUAL_JOB") or "").strip()
    if manual_job == "full_refresh_and_recommend":
        # H155: this one-click is one batch, not a fast market snapshot refresh.
        # The scheduler uses the exact Page01 canonical macro_full handler and
        # the existing Page07 canonical recommendation runner.  A dependency
        # that only passed in an earlier run today does NOT count.
        from godpick_auto_scheduler import H155_REQUIRED_REFRESH_JOBS
        os.environ["GODPICK_PAGE17_FULL_REFRESH"] = "1"
        res=run_due_jobs(
            force_all_enabled=True,
            temporary_enable_jobs=[*H155_REQUIRED_REFRESH_JOBS, "godpick_recommendation"],
            strict_same_run=True,
        )
    elif manual_job:
        # H71 existing single-job authority is unchanged.
        res=run_due_jobs(force_all_enabled=True, selected_jobs=[manual_job])
    else:
        res=run_due_jobs()
    print(json.dumps({"ok":res.get("ok"),"message":res.get("message"),"executed":res.get("executed",[])},ensure_ascii=False,indent=2,default=str))
    # Individual job failures are persisted and must remain visible; fail the Action
    # only for scheduler infrastructure errors, not a single market-source miss.
    raise SystemExit(0)
