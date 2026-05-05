# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TARGETS = [ROOT / "streamlit_app.py"] + sorted((ROOT / "pages").glob("*.py"))
IMPORT_LINE = "from app_auth import require_login\n"
CALL_LINE = "require_login()\n"

for path in TARGETS:
    if not path.exists():
        continue
    text = path.read_text(encoding="utf-8")
    if "from app_auth import require_login" in text:
        continue
    lines = text.splitlines(True)
    insert = 0
    # 保留 coding / future import 在最上面
    for i, line in enumerate(lines[:30]):
        if line.startswith("#") or line.strip() == "" or line.startswith("from __future__"):
            insert = i + 1
            continue
        break
    lines.insert(insert, IMPORT_LINE)
    lines.insert(insert + 1, CALL_LINE)
    path.write_text("".join(lines), encoding="utf-8")
    print("patched", path.relative_to(ROOT))
