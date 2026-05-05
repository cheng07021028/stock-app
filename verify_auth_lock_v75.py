# -*- coding: utf-8 -*-
from pathlib import Path
ROOT = Path(__file__).resolve().parent
targets = [ROOT / 'streamlit_app.py'] + sorted((ROOT / 'pages').glob('*.py'))
missing=[]
for p in targets:
    if p.name == 'app_auth.py':
        continue
    txt = p.read_text(encoding='utf-8', errors='ignore')
    ok = ('from app_auth import require_login' in txt and 'APP_AUTH_GUARD_V75' in txt)
    print(('OK      ' if ok else 'MISSING ') + str(p.relative_to(ROOT)))
    if not ok:
        missing.append(str(p.relative_to(ROOT)))
if missing:
    raise SystemExit('未鎖定檔案：' + ', '.join(missing))
print('全部頁面都有登入鎖。')
