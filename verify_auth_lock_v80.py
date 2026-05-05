# -*- coding: utf-8 -*-
from pathlib import Path
ROOT = Path(__file__).resolve().parent
MISSING = []
for p in [ROOT / "streamlit_app.py", *sorted((ROOT / "pages").glob("*.py"))]:
    if not p.exists() or p.name in {"app_auth.py"}:
        continue
    t = p.read_text(encoding="utf-8", errors="ignore")
    if "APP_AUTH_GUARD_V80" not in t or "require_login()" not in t:
        MISSING.append(str(p.relative_to(ROOT)))
if MISSING:
    print("MISSING AUTH GUARD:")
    print("\n".join(MISSING))
    raise SystemExit(1)
print("All target pages have APP_AUTH_GUARD_V80")
