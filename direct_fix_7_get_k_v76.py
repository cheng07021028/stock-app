# -*- coding: utf-8 -*-
from __future__ import annotations

"""
v76 直接修復版：修正 pages/7_股神推薦.py 的 st.session_state.get_k 錯誤。
這支是「直接修檔並回寫」用，不改其他邏輯、不刪任何功能。
"""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
TARGET = ROOT / "pages" / "7_股神推薦.py"

def main() -> int:
    if not TARGET.exists():
        print("找不到 pages/7_股神推薦.py")
        return 1

    text = TARGET.read_text(encoding="utf-8", errors="replace")
    old = text

    # 修正 Streamlit SessionState 不存在 get_k() 的錯誤
    text = text.replace("st.session_state.get_k(", "st.session_state.get(")
    text = text.replace("session_state.get_k(", "session_state.get(")

    # 額外保險：若前一版修成 _safe_str(st.session_state.get("pick_strategy"), "結合版")
    # 這種會讓 _safe_str 多一個參數，改成正確 default 寫法。
    text = text.replace(
        '_safe_str(st.session_state.get("pick_strategy"), "結合版")',
        '_safe_str(st.session_state.get("pick_strategy", "結合版"))'
    )
    text = text.replace(
        "_safe_str(st.session_state.get('pick_strategy'), '結合版')",
        "_safe_str(st.session_state.get('pick_strategy', '結合版'))"
    )

    if text != old:
        backup = TARGET.with_suffix(".py.v76_getk_backup")
        backup.write_text(old, encoding="utf-8")
        TARGET.write_text(text, encoding="utf-8")
        print("已修正 pages/7_股神推薦.py")
    else:
        print("pages/7_股神推薦.py 沒有 get_k 需要修正")

    try:
        compile(TARGET.read_text(encoding="utf-8", errors="replace"), str(TARGET), "exec")
        print("語法檢查 OK")
    except SyntaxError as e:
        print(f"語法錯誤：line {e.lineno}: {e.msg}")
        if e.text:
            print(e.text.rstrip())
            print(" " * max((e.offset or 1) - 1, 0) + "^")
        return 1

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
