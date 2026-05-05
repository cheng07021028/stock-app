# -*- coding: utf-8 -*-
from pathlib import Path
ROOT=Path(__file__).resolve().parent
need=[ROOT/'streamlit_app.py']+sorted((ROOT/'pages').glob('*.py'))
missing=[]
for p in need:
    txt=p.read_text(encoding='utf-8', errors='ignore')
    if 'APP_AUTH_GUARD_V77' not in txt and 'APP_AUTH_GUARD_V76' not in txt:
        missing.append(p.relative_to(ROOT))
if missing:
    print('未鎖定檔案：')
    for m in missing: print('-', m)
    raise SystemExit(1)
print('OK：所有 streamlit_app.py 與 pages/*.py 都已加入登入鎖。')
