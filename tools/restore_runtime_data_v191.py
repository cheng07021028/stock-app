# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import subprocess
import json
import os
import tempfile
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_auto_scheduler import RUNTIME_ARTIFACTS

def main():
    # All control-plane files must be readable before a worker may use them.
    required={"data/config/godpick_auto_scheduler_settings.json", "godpick_auto_scheduler_status.json", "godpick_auto_scheduler_history.json"}
    restored=0; skipped=0; bad=[]
    for rel in RUNTIME_ARTIFACTS:
        if rel=="godpick_records.json":
            # H241's sharded runtime head is the only record authority.
            skipped+=1;continue
        target=ROOT/rel
        try:
            proc=subprocess.run(["git","show",f"origin/runtime-data:{rel}"],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,check=False,timeout=15)
            if proc.returncode!=0 or not proc.stdout:raise ValueError('missing runtime file')
            decoded=json.loads(proc.stdout.decode('utf-8-sig'))
            if rel in required and not isinstance(decoded,dict):raise ValueError('invalid control file')
            if rel=="data/config/godpick_auto_scheduler_settings.json" and (not isinstance(decoded.get("enabled"),bool) or not isinstance(decoded.get("jobs"),dict)):raise ValueError('invalid scheduler settings')
            if rel=="godpick_auto_scheduler_status.json" and not isinstance(decoded.get("jobs"),dict):raise ValueError('invalid status')
            if rel=="godpick_auto_scheduler_history.json" and not isinstance(decoded.get("records"),list):raise ValueError('invalid history')
            target.parent.mkdir(parents=True,exist_ok=True)
            fd,tmp=tempfile.mkstemp(dir=target.parent,prefix='.h243-')
            try:
                with os.fdopen(fd,'wb') as f:f.write(proc.stdout)
                os.replace(tmp,target)
            finally:
                if os.path.exists(tmp):os.unlink(tmp)
            restored+=1
        except Exception as exc:
            skipped+=1
            if rel in required:bad.append(rel+':'+type(exc).__name__)
    print(f"V191 H243 runtime restore: restored={restored} skipped={skipped}")
    if bad:raise SystemExit('H243 control authority restore failed: '+'; '.join(bad))
if __name__=="__main__": main()
