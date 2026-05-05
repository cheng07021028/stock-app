# -*- coding: utf-8 -*-
from __future__ import annotations

"""
v75：修正 st.session_state.get_k AttributeError
原因：
Streamlit 的 session_state 沒有 get_k() 方法，正確要用 st.session_state.get("key", default)。
修正：
將 pages/7_股神推薦.py 內所有 st.session_state.get_k(...) 改成 st.session_state.get(...)
"""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
TARGETS = [
    ROOT / "pages" / "7_股神推薦.py",
    ROOT / "pages" / "8_股神推薦紀錄.py",
    ROOT / "pages" / "10_推薦清單.py",
    ROOT / "pages" / "14_股神權重校正.py",
]

def patch_text(text: str) -> str:
    text = text.replace("st.session_state.get_k(", "st.session_state.get(")
    text = text.replace("session_state.get_k(", "session_state.get(")
    return text

def main() -> int:
    changed = False

    for path in TARGETS:
        if not path.exists():
            print(f"skip missing: {path.relative_to(ROOT)}")
            continue

        old = path.read_text(encoding="utf-8", errors="replace")
        new = patch_text(old)

        if new != old:
            backup = path.with_suffix(path.suffix + ".v75_getk_bak")
            backup.write_text(old, encoding="utf-8")
            path.write_text(new, encoding="utf-8")
            changed = True
            print(f"patched: {path.relative_to(ROOT)}")
        else:
            print(f"unchanged: {path.relative_to(ROOT)}")

    print("\ncompile checking...")
    errors = []
    for path in TARGETS:
        if not path.exists():
            continue
        try:
            compile(path.read_text(encoding="utf-8", errors="replace"), str(path), "exec")
        except SyntaxError as e:
            errors.append((path, e))
            print(f"COMPILE ERROR: {path.relative_to(ROOT)} line {e.lineno}: {e.msg}")
            if e.text:
                print(e.text.rstrip())
                print(" " * max((e.offset or 1) - 1, 0) + "^")

    if errors:
        return 1

    print("OK: syntax checked.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
