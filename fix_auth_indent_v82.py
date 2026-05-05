# -*- coding: utf-8 -*-
"""
v82：修復登入鎖造成的 __future__ import 位置錯誤，並修復已知 f-string 語法錯誤。
用途：
1. 移除舊版 APP_AUTH_GUARD 片段
2. 將 from __future__ import annotations 移回檔案最前段
3. 在 __future__ 後安全插入 require_login()
4. 修復 7_股神推薦.py 已知 f-string 雙引號巢狀錯誤
5. 編譯檢查 streamlit_app.py 與 pages/*.py
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TARGETS = [ROOT / "streamlit_app.py"] + sorted((ROOT / "pages").glob("*.py"))

AUTH_BLOCK = """# >>> APP_AUTH_GUARD_V82
try:
    from app_auth import require_login
    require_login()
except Exception as _auth_e:
    import streamlit as st
    st.error(f"登入系統載入失敗：{_auth_e}")
    st.stop()
# <<< APP_AUTH_GUARD_V82

"""

AUTH_RE = re.compile(
    r"\n?# >>> APP_AUTH_GUARD_V\d+.*?# <<< APP_AUTH_GUARD_V\d+\n?",
    re.DOTALL,
)

OLD_AUTH_PATTERNS = [
    re.compile(r"^\s*from app_auth import require_login\s*\n\s*require_login\(\)\s*\n?", re.MULTILINE),
]

KNOWN_REPLACEMENTS = [
    # Python 3.11 f-string parser 對同層雙引號較容易炸掉；改成單引號 tuple。
    ('st.session_state.get(("pick_strategy", "結合版"))', "st.session_state.get(('pick_strategy', '結合版'))"),
    ('st.session_state.get(("pick_strategy","結合版"))', "st.session_state.get(('pick_strategy', '結合版'))"),
]

def _strip_old_auth(text: str) -> str:
    text = AUTH_RE.sub("\n", text)
    for pat in OLD_AUTH_PATTERNS:
        text = pat.sub("", text)
    # 清掉殘留空行過多
    text = re.sub(r"\n{4,}", "\n\n\n", text)
    return text.lstrip("\ufeff")

def _extract_future_imports(text: str) -> tuple[str, list[str]]:
    lines = text.splitlines(keepends=True)
    futures = []
    kept = []
    for line in lines:
        if re.match(r"^\s*from\s+__future__\s+import\s+", line):
            norm = line.strip()
            if norm not in [x.strip() for x in futures]:
                futures.append(line if line.endswith("\n") else line + "\n")
        else:
            kept.append(line)
    return "".join(kept), futures

def _split_header(text: str) -> tuple[str, str]:
    """
    保留 shebang / encoding / module docstring 在最前面；future import 接在它們後面。
    """
    lines = text.splitlines(keepends=True)
    header = []
    i = 0

    # shebang / encoding / initial comments
    while i < len(lines):
        s = lines[i]
        if i == 0 and s.startswith("#!"):
            header.append(s); i += 1; continue
        if re.match(r"^#.*coding[:=]\s*[-\w.]+", s):
            header.append(s); i += 1; continue
        if s.strip() == "":
            # 開頭空白先保留，但避免堆太多
            header.append(s); i += 1; continue
        break

    rest = "".join(lines[i:])

    # module docstring
    try:
        mod = ast.parse(rest)
        if (
            mod.body
            and isinstance(mod.body[0], ast.Expr)
            and isinstance(getattr(mod.body[0], "value", None), ast.Constant)
            and isinstance(mod.body[0].value.value, str)
        ):
            doc_end = mod.body[0].end_lineno or 0
            rest_lines = rest.splitlines(keepends=True)
            header.extend(rest_lines[:doc_end])
            rest = "".join(rest_lines[doc_end:])
            # docstring 後接一個空行
            if not header[-1].endswith("\n"):
                header[-1] += "\n"
            if rest and not rest.startswith("\n"):
                header.append("\n")
    except SyntaxError:
        # 若檔案本來語法錯，先不要解析 docstring，後續 replacement 可能會修好
        pass

    header_text = "".join(header)
    body = rest.lstrip("\n")
    return header_text, body

def _insert_auth_after_future(text: str) -> str:
    text = _strip_old_auth(text)
    for old, new in KNOWN_REPLACEMENTS:
        text = text.replace(old, new)

    text_no_future, futures = _extract_future_imports(text)
    header, body = _split_header(text_no_future)

    out = header
    if futures:
        # 去重
        uniq = []
        seen = set()
        for f in futures:
            k = f.strip()
            if k not in seen:
                uniq.append(f if f.endswith("\n") else f + "\n")
                seen.add(k)
        out += "".join(uniq)
        out += "\n"
    out += AUTH_BLOCK
    out += body.lstrip("\n")
    return out

def repair_file(path: Path) -> None:
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8", errors="replace")
    new_text = _insert_auth_after_future(text)
    if new_text != text:
        path.write_text(new_text, encoding="utf-8")
        print(f"patched: {path.relative_to(ROOT)}")
    else:
        print(f"unchanged: {path.relative_to(ROOT)}")

def compile_check(paths: list[Path]) -> bool:
    ok = True
    for p in paths:
        try:
            compile(p.read_text(encoding="utf-8", errors="replace"), str(p), "exec")
        except SyntaxError as e:
            ok = False
            print(f"COMPILE ERROR: {p.relative_to(ROOT)} line {e.lineno}: {e.msg}")
            if e.text:
                print(e.text.rstrip())
                print(" " * max((e.offset or 1) - 1, 0) + "^")
    return ok

def main() -> int:
    for path in TARGETS:
        repair_file(path)

    print("\ncompile checking...")
    ok = compile_check(TARGETS)
    if not ok:
        print("\n仍有語法錯誤；請依上方檔名與行號修正。")
        return 1
    print("OK: all target files compiled.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
