# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_auto_scheduler import RUNTIME_ARTIFACTS

def main():
    restored=0; skipped=0
    for rel in RUNTIME_ARTIFACTS:
        target=ROOT/rel
        try:
            proc=subprocess.run(["git","show",f"origin/runtime-data:{rel}"],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,check=False)
            if proc.returncode==0 and proc.stdout:
                target.parent.mkdir(parents=True,exist_ok=True); target.write_bytes(proc.stdout); restored+=1
            else: skipped+=1
        except Exception: skipped+=1
    print(f"V191 runtime restore: restored={restored} skipped={skipped}")
if __name__=="__main__": main()
