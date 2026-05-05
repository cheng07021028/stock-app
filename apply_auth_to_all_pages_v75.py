# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent
PAGES = ROOT / "pages"
TARGETS = [ROOT / "streamlit_app.py"]
if PAGES.exists():
    TARGETS += sorted(PAGES.glob("*.py"))

AUTH_IMPORT = "from app_auth import require_login\n"
GUARD_CALL = (
    "# >>> APP_AUTH_GUARD_V75\n"
    "require_login()\n"
    "# <<< APP_AUTH_GUARD_V75\n"
)
IMPORT_BLOCK_RE = re.compile(r"^from app_auth import require_login\s*$", re.M)
GUARD_BLOCK_RE = re.compile(r"# >>> APP_AUTH_GUARD_V\d+\n.*?# <<< APP_AUTH_GUARD_V\d+\n?", re.S)


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8-sig")


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8", newline="")


def _remove_old_auth(text: str) -> str:
    text = GUARD_BLOCK_RE.sub("", text)
    lines = []
    for line in text.splitlines(True):
        s = line.strip()
        if s == "from app_auth import require_login":
            continue
        if s in {"require_login()", "from app_auth import require_login; require_login()"}:
            continue
        lines.append(line)
    # 清掉過多空白，但不要破壞格式
    return "".join(lines)


def _skip_initial_comments_docstring(lines: list[str], i: int = 0) -> int:
    # shebang / coding / leading blanks
    while i < len(lines):
        s = lines[i].strip()
        if i == 0 and s.startswith("#!"):
            i += 1
            continue
        if s.startswith("#") and "coding" in s.lower():
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
        q = '"""' if s.startswith('"""') else "'''"
        if s.count(q) >= 2 and len(s.strip()) > 3:
            return i + 1
        i += 1
        while i < len(lines):
            if q in lines[i]:
                return i + 1
            i += 1
    return i


def _after_future_imports(lines: list[str], i: int) -> int:
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


def _find_import_insert_index(lines: list[str]) -> int:
    return _after_future_imports(lines, _skip_initial_comments_docstring(lines, 0))


def _paren_end_line(lines: list[str], start: int) -> int:
    depth = 0
    seen = False
    in_string = None
    escape = False
    # 簡易括號掃描：足以處理 st.set_page_config(...) 多行呼叫
    for j in range(start, len(lines)):
        line = lines[j]
        for ch in line:
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == in_string:
                    in_string = None
                continue
            if ch in {'"', "'"}:
                in_string = ch
                continue
            if ch == "(":
                depth += 1
                seen = True
            elif ch == ")":
                depth -= 1
                if seen and depth <= 0:
                    return j + 1
    return start + 1


def _find_top_level_set_page_config_end(lines: list[str]) -> int | None:
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        if indent == 0 and "st.set_page_config" in stripped:
            return _paren_end_line(lines, i)
    return None


def _find_call_insert_index(lines: list[str]) -> int:
    spc = _find_top_level_set_page_config_end(lines)
    if spc is not None:
        return spc

    # 沒有 set_page_config：放在 top-level import 區塊後
    i = _find_import_insert_index(lines)
    if i < len(lines) and lines[i].strip() == AUTH_IMPORT.strip():
        i += 1
    # 繼續跳過一般 import 區塊
    while i < len(lines):
        s = lines[i].strip()
        if s == "" or s.startswith("#"):
            i += 1
            continue
        if s.startswith("import ") or s.startswith("from "):
            i += 1
            continue
        break
    return i


def patch_file(path: Path) -> tuple[bool, str]:
    if not path.exists() or path.name == "app_auth.py":
        return False, "skip"
    if path.suffix != ".py":
        return False, "skip"
    text = _read(path)
    original = text
    text = _remove_old_auth(text)
    lines = text.splitlines(True)

    import_idx = _find_import_insert_index(lines)
    lines.insert(import_idx, AUTH_IMPORT)
    text = "".join(lines)
    lines = text.splitlines(True)

    call_idx = _find_call_insert_index(lines)
    # 避免插在 import 那一行前面造成亂序
    insert_text = GUARD_CALL + "\n"
    lines.insert(call_idx, insert_text)
    new = "".join(lines)

    if new == original:
        return False, "unchanged"
    _write(path, new)
    return True, "patched"


def verify_file(path: Path) -> tuple[bool, str]:
    if not path.exists() or path.name == "app_auth.py" or path.suffix != ".py":
        return True, "skip"
    text = _read(path)
    if "from app_auth import require_login" not in text or "APP_AUTH_GUARD_V75" not in text:
        return False, "missing auth guard"
    return True, "ok"


def main() -> int:
    print("v75 全站強制登入鎖定工具")
    print("ROOT =", ROOT)
    patched = 0
    for path in TARGETS:
        ok, msg = patch_file(path)
        rel = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
        print(f"{msg:12s} {rel}")
        patched += 1 if ok else 0

    print("\n驗證結果：")
    failed = []
    for path in TARGETS:
        ok, msg = verify_file(path)
        rel = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
        print(f"{msg:20s} {rel}")
        if not ok:
            failed.append(str(rel))
    print("\n完成。已更新檔案數：", patched)
    if failed:
        print("仍未鎖定：", failed)
        return 1
    print("全部目標頁面已插入登入鎖。請 Commit / Push，並在 Streamlit Cloud Clear cache → Reboot app。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
