# -*- coding: utf-8 -*-
from pathlib import Path
ROOT = Path(__file__).resolve().parent
files = [ROOT / "streamlit_app.py"] + sorted((ROOT / "pages").glob("*.py"))
missing = []
for p in files:
    if not p.exists():
        continue
    txt = p.read_text(encoding="utf-8", errors="ignore")
    ok = "# >>> APP_AUTH_GUARD_V76" in txt and "require_login()" in txt
    print(("OK      " if ok else "MISSING ") + str(p.relative_to(ROOT)))
    if not ok:
        missing.append(p)
if missing:
    raise SystemExit(1)
print("ALL LOCKED")
