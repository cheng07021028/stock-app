# -*- coding: utf-8 -*-
"""
v80 全站登入鎖縮排修復工具
- 移除 v74/v75/v76/v77 可能插壞的登入鎖片段
- 將 require_login() 安全插入 st.set_page_config(...) 之後
- 編譯檢查 streamlit_app.py 與 pages/*.py
"""
from __future__ import annotations

import os
import re
import py_compile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
AUTH_IMPORT = "from app_auth import require_login"
GUARD_START = "# >>> APP_AUTH_GUARD_V80"
GUARD_END = "# <<< APP_AUTH_GUARD_V80"

SKIP_NAMES = {
    "app_auth.py",
    "fix_auth_indent_v80.py",
    "verify_auth_lock_v80.py",
}

OLD_TOOL_PATTERNS = [
    "apply_auth_to_all_pages_v74.py",
    "apply_auth_to_all_pages_v74.bat",
    "apply_auth_to_all_pages_v75.py",
    "apply_auth_to_all_pages_v75.bat",
    "apply_auth_to_all_pages_v76.py",
    "apply_auth_to_all_pages_v76.bat",
    "force_auth_lock_v76.py",
    "verify_auth_lock_v76.py",
]

OLD_WORKFLOWS = [
    ".github/workflows/force_auth_patch_v75.yml",
    ".github/workflows/force_auth_lock_v76.yml",
]


def _is_target(path: Path) -> bool:
    if path.name in SKIP_NAMES:
        return False
    if path.name == "streamlit_app.py":
        return True
    return path.parent.name == "pages" and path.suffix == ".py"


def _strip_old_guards(text: str) -> str:
    # Remove any explicit guarded block from earlier versions.
    text = re.sub(
        r"\n?\s*# >>> APP_AUTH_GUARD[^\n]*\n.*?# <<< APP_AUTH_GUARD[^\n]*\n?",
        "\n",
        text,
        flags=re.DOTALL,
    )
    # Remove stray auth import and standalone require_login() lines.
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == AUTH_IMPORT:
            continue
        if stripped == "require_login()":
            continue
        lines.append(line)
    return "\n".join(lines).rstrip() + "\n"


def _insert_auth_import(text: str) -> str:
    if AUTH_IMPORT in text:
        return text
    lines = text.splitlines()
    insert_at = 0
    # keep encoding comment / shebang at top
    while insert_at < len(lines) and (
        lines[insert_at].startswith("#!")
        or "coding" in lines[insert_at][:50]
        or lines[insert_at].strip() == ""
    ):
        insert_at += 1

    # Keep module docstring first if present.
    if insert_at < len(lines) and lines[insert_at].lstrip().startswith(('"""', "'''")):
        quote = lines[insert_at].lstrip()[:3]
        insert_at += 1
        while insert_at < len(lines):
            if quote in lines[insert_at]:
                insert_at += 1
                break
            insert_at += 1

    # Future imports must stay before normal imports.
    while insert_at < len(lines) and lines[insert_at].startswith("from __future__ import"):
        insert_at += 1

    lines.insert(insert_at, AUTH_IMPORT)
    return "\n".join(lines).rstrip() + "\n"


def _find_page_config_end(lines: list[str]) -> int | None:
    for i, line in enumerate(lines):
        if "st.set_page_config" not in line:
            continue
        # locate end of call using parenthesis balance
        balance = 0
        seen = False
        for j in range(i, len(lines)):
            # remove strings roughly unnecessary; simple balance is enough for common Streamlit calls
            balance += lines[j].count("(") - lines[j].count(")")
            if "(" in lines[j]:
                seen = True
            if seen and balance <= 0:
                return j
    return None


def _find_main_insert(lines: list[str]) -> tuple[int, str] | None:
    for i, line in enumerate(lines):
        if re.match(r"^def\s+main\s*\(\s*\)\s*:", line):
            return i + 1, "    "
    return None


def _find_import_block_end(lines: list[str]) -> int:
    idx = 0
    while idx < len(lines):
        s = lines[idx].strip()
        if not s or s.startswith("#") or s.startswith("import ") or s.startswith("from "):
            idx += 1
            continue
        break
    return idx


def _insert_guard(text: str) -> str:
    lines = text.splitlines()
    page_end = _find_page_config_end(lines)
    if page_end is not None:
        indent = re.match(r"^(\s*)", lines[page_end]).group(1)
        insert_at = page_end + 1
    else:
        main_pos = _find_main_insert(lines)
        if main_pos is not None:
            insert_at, indent = main_pos
        else:
            insert_at = _find_import_block_end(lines)
            indent = ""

    guard = [
        f"{indent}{GUARD_START}",
        f"{indent}require_login()",
        f"{indent}{GUARD_END}",
    ]
    lines[insert_at:insert_at] = guard
    return "\n".join(lines).rstrip() + "\n"


def patch_file(path: Path) -> bool:
    original = path.read_text(encoding="utf-8")
    text = _strip_old_guards(original)
    text = _insert_auth_import(text)
    text = _insert_guard(text)
    if text != original:
        path.write_text(text, encoding="utf-8")
        return True
    return False


def cleanup_bad_tools() -> None:
    for rel in OLD_TOOL_PATTERNS:
        p = ROOT / rel
        if p.exists():
            p.rename(ROOT / f"DISABLED_{p.name}")
    for rel in OLD_WORKFLOWS:
        p = ROOT / rel
        if p.exists():
            p.write_text(
                "# Disabled by v80. Old auth patch workflow intentionally disabled.\n"
                "name: DISABLED old auth patch workflow\n"
                "on:\n  workflow_dispatch:\n"
                "jobs:\n  disabled:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo disabled\n",
                encoding="utf-8",
            )


def iter_targets() -> list[Path]:
    targets = []
    main = ROOT / "streamlit_app.py"
    if main.exists():
        targets.append(main)
    pages = ROOT / "pages"
    if pages.exists():
        targets.extend(sorted(p for p in pages.glob("*.py") if _is_target(p)))
    return targets


def compile_targets(targets: list[Path]) -> list[str]:
    errors = []
    for p in targets:
        try:
            py_compile.compile(str(p), doraise=True)
        except Exception as exc:
            errors.append(f"{p.relative_to(ROOT)} -> {exc}")
    return errors


def main() -> None:
    cleanup_bad_tools()
    targets = iter_targets()
    changed = []
    for p in targets:
        if patch_file(p):
            changed.append(str(p.relative_to(ROOT)))
    errors = compile_targets(targets)
    print("v80 auth indent fix completed")
    print(f"targets: {len(targets)}")
    print("changed:")
    for c in changed:
        print(f"  - {c}")
    if errors:
        print("\nCOMPILE ERRORS:")
        for e in errors:
            print(f"  - {e}")
        raise SystemExit(1)
    print("compile: OK")


if __name__ == "__main__":
    main()
