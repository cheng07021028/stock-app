# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
PAGES = ROOT / "pages"
TARGETS = [ROOT / "streamlit_app.py"]
if PAGES.exists():
    TARGETS += sorted(PAGES.glob("*.py"))

GUARD = (
    "# >>> APP_AUTH_GUARD_V74\n"
    "from app_auth import require_login\n"
    "require_login()\n"
    "# <<< APP_AUTH_GUARD_V74\n\n"
)

BLOCK_RE = re.compile(
    r"# >>> APP_AUTH_GUARD_V\d+\n.*?# <<< APP_AUTH_GUARD_V\d+\n\n?",
    flags=re.DOTALL,
)


def _remove_old_guard(text: str) -> str:
    text = BLOCK_RE.sub("", text)
    lines = []
    for line in text.splitlines(True):
        s = line.strip()
        # 移除舊版單行注入，避免重複；不移除 app_auth 其他管理函式 import。
        if s in {
            "from app_auth import require_login",
            "from app_auth import require_login; require_login()",
            "require_login()",
        }:
            continue
        lines.append(line)
    return "".join(lines)


def _skip_module_docstring(lines: list[str], i: int) -> int:
    # 跳過空白 / shebang / coding 註解
    while i < len(lines):
        s = lines[i].strip()
        if i == 0 and s.startswith("#!"):
            i += 1
            continue
        if "coding" in s and s.startswith("#"):
            i += 1
            continue
        if s == "":
            i += 1
            continue
        break

    if i >= len(lines):
        return i

    s = lines[i].lstrip()
    if s.startswith('"""') or s.startswith("'''"):
        quote = '"""' if s.startswith('"""') else "'''"
        # 單行 docstring
        if s.count(quote) >= 2 and len(s.split(quote)) >= 3:
            return i + 1
        i += 1
        while i < len(lines):
            if quote in lines[i]:
                return i + 1
            i += 1
    return i


def _find_insert_index(text: str) -> int:
    lines = text.splitlines(True)
    i = _skip_module_docstring(lines, 0)

    # __future__ import 必須在非 docstring 程式碼最前面，guard 放它後面。
    while i < len(lines):
        s = lines[i].strip()
        if s == "" or s.startswith("#"):
            i += 1
            continue
        if s.startswith("from __future__ import"):
            i += 1
            continue
        break
    return i


def patch_file(path: Path) -> tuple[bool, str]:
    if not path.exists() or path.name.startswith("."):
        return False, "skip"
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return False, "not utf-8"

    # 帳號管理頁也要保護；app_auth.py 本身不能 patch。
    if path.name == "app_auth.py":
        return False, "skip app_auth"

    clean = _remove_old_guard(text)
    idx = _find_insert_index(clean)
    lines = clean.splitlines(True)
    new_text = "".join(lines[:idx]) + GUARD + "".join(lines[idx:])
    if new_text == text:
        return False, "unchanged"
    path.write_text(new_text, encoding="utf-8")
    return True, "patched"


def main() -> None:
    print("v74 全站登入鎖定工具")
    print("ROOT =", ROOT)
    patched = 0
    for path in TARGETS:
        ok, msg = patch_file(path)
        rel = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
        print(f"{msg:12s} {rel}")
        patched += 1 if ok else 0
    print("完成。已更新檔案數：", patched)
    print("請 Commit / Push 到 GitHub，然後 Streamlit Cloud Clear cache → Reboot app。")


if __name__ == "__main__":
    main()
