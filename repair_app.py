# -*- coding: utf-8 -*-
from __future__ import annotations

import compileall
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PAGES = ROOT / "pages"
PAYLOAD = ROOT / "payload"
ENCODED_PAGE = PAGES / "8_#U80a1#U795e#U63a8#U85a6#U7d00#U9304.py"
CHINESE_PAGE = PAGES / "8_股神推薦紀錄.py"
TARGET_FEEDBACK = ROOT / "godpick_performance_feedback.py"
SOURCE_PAGE = PAYLOAD / "page8_fixed.py"
SOURCE_FEEDBACK = PAYLOAD / "godpick_performance_feedback.py"
BACKUP_ROOT = ROOT / "patch_backup_v158_app_repair"


def backup_file(path: Path, backup_dir: Path) -> None:
    if not path.exists():
        return
    backup_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, backup_dir / path.name)


def main() -> int:
    if not PAGES.is_dir() or not (ROOT / "streamlit_app.py").exists():
        print("ERROR: 請把修復包解壓縮到 stock-app-main 專案根目錄後再執行。")
        return 1
    if not SOURCE_PAGE.exists() or not SOURCE_FEEDBACK.exists():
        print("ERROR: 修復包 payload 不完整，請重新解壓縮。")
        return 2

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = BACKUP_ROOT / stamp
    backup_file(ENCODED_PAGE, backup_dir)
    backup_file(CHINESE_PAGE, backup_dir)
    backup_file(TARGET_FEEDBACK, backup_dir)

    # 專案目前所有頁面都使用 #Uxxxx 命名；第 8 頁必須維持相同規則。
    shutil.copy2(SOURCE_PAGE, ENCODED_PAGE)
    shutil.copy2(SOURCE_FEEDBACK, TARGET_FEEDBACK)

    # 移除上一包新增的中文重複頁面，避免部署時產生雙頁面或頁面索引不一致。
    if CHINESE_PAGE.exists():
        CHINESE_PAGE.unlink()
        print("已移除重複頁面：pages/8_股神推薦紀錄.py")

    ok = compileall.compile_dir(str(ROOT), quiet=1, force=True)
    if not ok:
        print("ERROR: Python 語法檢查失敗，已保留備份：", backup_dir)
        return 3

    # 輕量匯入檢查：不啟動 Streamlit、不連網、不改資料。
    sys.path.insert(0, str(ROOT))
    try:
        import godpick_performance_feedback  # noqa: F401
        import godpick_formal_recommendation_engine  # noqa: F401
    except Exception as exc:
        print(f"ERROR: 核心模組匯入失敗：{exc}")
        print("已保留備份：", backup_dir)
        return 4

    print("修復完成。")
    print("有效頁面：pages/8_#U80a1#U795e#U63a8#U85a6#U7d00#U9304.py")
    print("備份位置：", backup_dir.relative_to(ROOT))
    print("請重新啟動或 Reboot Streamlit App。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
