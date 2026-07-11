# -*- coding: utf-8 -*-
from __future__ import annotations

import compileall
import shutil
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PAGES = ROOT / "pages"
NEW_PAGE = PAGES / "8_股神推薦紀錄.py"
LEGACY_PAGE = PAGES / "8_#U80a1#U795e#U63a8#U85a6#U7d00#U9304.py"
BACKUP_DIR = ROOT / "patch_backup_v158_utf8"


def main() -> int:
    if not NEW_PAGE.exists():
        print("ERROR: pages/8_股神推薦紀錄.py not found. Extract this ZIP into the project root first.")
        return 1

    if LEGACY_PAGE.exists():
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = BACKUP_DIR / f"8_legacy_{stamp}.py.bak"
        shutil.move(str(LEGACY_PAGE), str(backup))
        print(f"Moved legacy page to backup: {backup.relative_to(ROOT)}")
    else:
        print("Legacy encoded page was not found; no cleanup was needed.")

    ok = compileall.compile_dir(str(ROOT), quiet=1)
    if not ok:
        print("ERROR: Python compile check failed.")
        return 2

    print("Patch applied successfully.")
    print("Active page: pages/8_股神推薦紀錄.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
