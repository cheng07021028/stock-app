# -*- coding: utf-8 -*-
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TARGETS = [ROOT / "streamlit_app.py"] + sorted((ROOT / "pages").glob("*.py"))

AUTH_BLOCK = """# >>> APP_AUTH_GUARD_V84
try:
    from app_auth import require_login
    require_login()
except Exception as _auth_e:
    import streamlit as st
    st.error(f"登入系統載入失敗：{_auth_e}")
    st.stop()
# <<< APP_AUTH_GUARD_V84

"""

AUTH_RE = re.compile(
    r"\n?# >>> APP_AUTH_GUARD_V\d+.*?# <<< APP_AUTH_GUARD_V\d+\n?",
    re.DOTALL,
)

def strip_old_auth(text: str) -> str:
    text = AUTH_RE.sub("\n", text)
    text = re.sub(r"^\s*from app_auth import require_login\s*\n\s*require_login\(\)\s*\n?", "", text, flags=re.MULTILINE)
    return text.lstrip("\ufeff")

def fix_7_known_bad_caption(text: str) -> str:
    """
    強制修正 7_股神推薦.py 這類錯誤：
    st.caption(f"目前顯示的是已保存推薦結果｜保存時間：{saved_at}｜策略：{_safe_str(st.session_state.get_k("pick_strategy"), "結合版")}")
    因為外層 f-string 是雙引號，內層 get_k("...") 會造成 f-string unmatched '('。
    """
    fixed_lines = []
    for line in text.splitlines(keepends=True):
        if (
            "st.caption(f" in line
            and "目前顯示的是已保存推薦結果" in line
            and "pick_strategy" in line
            and "saved_at" in line
        ):
            indent = line[: len(line) - len(line.lstrip())]
            newline = "\n" if line.endswith("\n") else ""
            fixed_lines.append(
                indent
                + 'strategy_label = _safe_str(st.session_state.get_k("pick_strategy"), "結合版")'
                + newline
            )
            fixed_lines.append(
                indent
                + 'st.caption(f"目前顯示的是已保存推薦結果｜保存時間：{saved_at}｜策略：{strategy_label}")'
                + newline
            )
        else:
            fixed_lines.append(line)
    text = "".join(fixed_lines)

    # 額外保險：若還有同型態字串，轉成單引號
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

def patch_text(text: str, path: Path) -> str:
    text = strip_old_auth(text)
    if path.name == "7_股神推薦.py":
        text = fix_7_known_bad_caption(text)

    text_no_future, futures = extract_future_imports(text)
    header, body = split_header(text_no_future)

    out = header
    if futures:
        out += "".join(futures)
        out += "\n"
    out += AUTH_BLOCK
    out += body.lstrip("\n")
    return out

def compile_check(paths):
    errors = []
    for p in paths:
        try:
            compile(p.read_text(encoding="utf-8", errors="replace"), str(p), "exec")
        except SyntaxError as e:
            errors.append((p, e))
            print(f"COMPILE ERROR: {p.relative_to(ROOT)} line {e.lineno}: {e.msg}")
            if e.text:
                print(e.text.rstrip())
                print(" " * max((e.offset or 1) - 1, 0) + "^")
    return errors

def main():
    for p in TARGETS:
        if not p.exists():
            continue
        old = p.read_text(encoding="utf-8", errors="replace")
        new = patch_text(old, p)
        if new != old:
            p.write_text(new, encoding="utf-8")
            print(f"patched: {p.relative_to(ROOT)}")
        else:
            print(f"unchanged: {p.relative_to(ROOT)}")

    print("\ncompile checking...")
    errors = compile_check(TARGETS)
    if errors:
        raise SystemExit(1)
    print("OK: all target files compiled.")

if __name__ == "__main__":
    main()
