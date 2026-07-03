# -*- coding: utf-8 -*-
from __future__ import annotations

"""一次性修正 pages 內 #UXXXX 亂碼檔名。

用法：
1. 把本檔放在專案根目錄 stock-app-main/。
2. 執行：python fix_page_filenames_unicode.py
3. 會把 pages/17_#U7cfb...py 這種檔名改成 pages/17_系統健康檢查.py。
4. 若目標中文檔名已存在，會略過避免覆蓋。
"""

import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PAGES_DIR = BASE_DIR / "pages"
PATTERN = re.compile(r"#U([0-9a-fA-F]{4})")


def decode_name(name: str) -> str:
    return PATTERN.sub(lambda m: chr(int(m.group(1), 16)), name)


def main() -> None:
    if not PAGES_DIR.exists():
        print(f"找不到 pages 資料夾：{PAGES_DIR}")
        return

    changed = 0
    skipped = 0
    for path in sorted(PAGES_DIR.iterdir()):
        if not path.is_file():
            continue
        new_name = decode_name(path.name)
        if new_name == path.name:
            continue
        target = path.with_name(new_name)
        if target.exists():
            print(f"略過，目標已存在：{path.name} -> {target.name}")
            skipped += 1
            continue
        path.rename(target)
        print(f"已修正：{path.name} -> {target.name}")
        changed += 1

    print(f"完成。已修正 {changed} 個檔案，略過 {skipped} 個檔案。")


if __name__ == "__main__":
    main()
