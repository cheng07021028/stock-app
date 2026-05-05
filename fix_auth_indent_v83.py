# -*- coding: utf-8 -*-
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TARGETS = [ROOT / "streamlit_app.py"] + sorted((ROOT / "pages").glob("*.py"))

AUTH_BLOCK = """# >>> APP_AUTH_GUARD_V83
try:
    from app_auth import require_login
    require_login()
except Exception as _auth_e:
    import streamlit as st
    st.error(f"登入系統載入失敗：{_auth_e}")
    st.stop()
# <<< APP_AUTH_GUARD_V83

"""

AUTH_RE = re.compile(
    r"\n?# >>> APP_AUTH_GUARD_V\d+.*?# <<< APP_AUTH_GUARD_V\d+\n?",
    re.DOTALL,
)

def strip_old_auth(text: str) -> str:
    text = AUTH_RE.sub("\n", text)
    text = re.sub(r"^\s*from app_auth import require_login\s*\n\s*require_login\(\)\s*\n?", "", text, flags=re.MULTILINE)
    return text.lstrip("\ufeff")

def fix_known_syntax(text: str) -> str:
    # 修正 7_股神推薦.py 已知 f-string 錯誤：
    # _safe_str(st.session_state.get_k("pick_strategy"), "結合版")
    # 這段在 f-string 裡使用同層雙引號會造成 unmatched '('
    text = text.replace(
        'st.caption(f"目前顯示的是已保存推薦結果｜保存時間：{saved_at}｜策略：{_safe_str(st.session_state.get_k("pick_strategy"), "結合版")}")',
        'st.caption(f"目前顯示的是已保存推薦結果｜保存時間：{saved_at}｜策略：{_safe_str(st.session_state.get_k(\'pick_strategy\'), \'結合版\')}")'
    )
    # 更保險：凡是 f-string 裡出現 get_k("pick_strategy"), "結合版" 都改單引號
    text = text.replace('st.session_state.get_k("pick_strategy"), "結合版"', "st.session_state.get_k('pick_strategy'), '結合版'")
    text = text.replace('st.session_state.get("pick_strategy"), "結合版"', "st.session_state.get('pick_strategy'), '結合版'")
    return text

def extract_future_imports(text: str):
    futures, kept = [], []
    for line in text.splitlines(keepends=True):
        if re.match(r"^\s*from\s+__future__\s+import\s+", line):
            if line.strip() not in [x.strip() for x in futures]:
                futures.append(line if line.endswith("\n") else line + "\n")
        else:
            kept.append(line)
    return "".join(kept), futures

def split_header(text: str):
    lines = text.splitlines(keepends=True)
    header = []
    i = 0
    while i < len(lines):
        s = lines[i]
        if i == 0 and s.startswith("#!"):
            header.append(s); i += 1; continue
        if re.match(r"^#.*coding[:=]\s*[-\w.]+", s):
            header.append(s); i += 1; continue
        if s.strip() == "":
            header.append(s); i += 1; continue
        break
    rest = "".join(lines[i:])
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
            if rest and not rest.startswith("\n"):
                header.append("\n")
    except SyntaxError:
        pass
    return "".join(header), rest.lstrip("\n")

def patch_text(text: str) -> str:
    text = strip_old_auth(text)
    text = fix_known_syntax(text)
    text_no_future, futures = extract_future_imports(text)
    header, body = split_header(text_no_future)

    out = header
    if futures:
        out += "".join(futures)
        out += "\n"
    out += AUTH_BLOCK
    out += body.lstrip("\n")
    return out

def compile_one(path: Path):
    compile(path.read_text(encoding="utf-8", errors="replace"), str(path), "exec")

def main():
    for p in TARGETS:
        if not p.exists():
            continue
        old = p.read_text(encoding="utf-8", errors="replace")
        new = patch_text(old)
        if new != old:
            p.write_text(new, encoding="utf-8")
            print(f"patched: {p.relative_to(ROOT)}")
        else:
            print(f"unchanged: {p.relative_to(ROOT)}")

    print("\ncompile checking...")
    errors = []
    for p in TARGETS:
        try:
            compile_one(p)
        except SyntaxError as e:
            errors.append((p, e))
            print(f"COMPILE ERROR: {p.relative_to(ROOT)} line {e.lineno}: {e.msg}")
            if e.text:
                print(e.text.rstrip())
                print(" " * max((e.offset or 1) - 1, 0) + "^")
    if errors:
        raise SystemExit(1)
    print("OK: all target files compiled.")

if __name__ == "__main__":
    main()
