# -*- coding: utf-8 -*-
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_auto_scheduler import run_due_jobs
if __name__=="__main__":
    res=run_due_jobs()
    print(json.dumps({"ok":res.get("ok"),"message":res.get("message"),"executed":res.get("executed",[])},ensure_ascii=False,indent=2,default=str))
    # Individual job failures are persisted and must remain visible; fail the Action
    # only for scheduler infrastructure errors, not a single market-source miss.
    raise SystemExit(0)
