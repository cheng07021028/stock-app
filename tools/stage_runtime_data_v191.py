# -*- coding: utf-8 -*-
from pathlib import Path
import shutil,sys
ROOT=Path(__file__).resolve().parents[1]
OUT=Path(sys.argv[1] if len(sys.argv)>1 else "/tmp/godpick-v191-runtime")
sys.path.insert(0,str(ROOT))
from godpick_auto_scheduler import RUNTIME_ARTIFACTS
if OUT.exists(): shutil.rmtree(OUT)
OUT.mkdir(parents=True,exist_ok=True)
count=0
for rel in RUNTIME_ARTIFACTS:
    src=ROOT/rel
    if src.exists() and src.is_file():
        dst=OUT/rel; dst.parent.mkdir(parents=True,exist_ok=True); shutil.copy2(src,dst); count+=1
print(f"staged={count} to {OUT}")
