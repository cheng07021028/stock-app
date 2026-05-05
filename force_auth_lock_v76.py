# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent
TARGETS = [ROOT / "streamlit_app.py"] + sorted((ROOT / "pages").glob("*.py"))
START = "# >>> APP_AUTH_GUARD_V76"
END = "# <<< APP_AUTH_GUARD_V76"
GUARD = f"\n{START}\nfrom app_auth import require_login\nrequire_login()\n{END}\n"


def strip_old_guards(text: str) -> str:
    markers = [
        ("# >>> APP_AUTH_GUARD_V74", "# <<< APP_AUTH_GUARD_V74"),
        ("# >>> APP_AUTH_GUARD_V75", "# <<< APP_AUTH_GUARD_V75"),
        (START, END),
    ]
    for a, b in markers:
        while a in text and b in text:
            s = text.index(a)
            e = text.index(b) + len(b)
            text = text[:s].rstrip() + "\n" + text[e:].lstrip("\n")
    return text


def find_page_config_insert(lines: list[str]) -> int | None:
    for i, line in enumerate(lines):
        if "st.set_page_config" in line:
            depth = 0
            started = False
            for j in range(i, len(lines)):
                for ch in lines[j]:
                    if ch == "(":
                        depth += 1
                        started = True
                    elif ch == ")":
                        depth -= 1
                if started and depth <= 0:
                    return j + 1
    return None


def find_import_insert(lines: list[str]) -> int:
    # 保留 shebang / encoding / module docstring / future import 在最前面
    idx = 0
    if lines and lines[0].startswith("#!"):
        idx = 1
    while idx < len(lines) and ("coding" in lines[idx] or lines[idx].strip() == ""):
        idx += 1
    if idx < len(lines) and lines[idx].lstrip().startswith(('"""', "'''")):
        quote = '"""' if '"""' in lines[idx] else "'''"
        idx += 1
        while idx < len(lines) and quote not in lines[idx]:
            idx += 1
        idx += 1
    while idx < len(lines) and lines[idx].startswith("from __future__ import"):
        idx += 1
    # imports 區後插入，但若後面才 set_page_config，不能在這裡插入，會破壞 set_page_config first rule
    return idx


def patch_file(path: Path) -> bool:
    if not path.exists() or path.name.startswith("__"):
        return False
    text = path.read_text(encoding="utf-8", errors="ignore")
    old = text
    text = strip_old_guards(text)
    lines = text.splitlines(True)
    insert_at = find_page_config_insert(lines)
    if insert_at is None:
        insert_at = find_import_insert(lines)
    lines.insert(insert_at, GUARD)
    new = "".join(lines)
    if new != old:
        path.write_text(new, encoding="utf-8")
        return True
    return False


def main() -> None:
    changed = []
    for p in TARGETS:
        if patch_file(p):
            changed.append(str(p.relative_to(ROOT)))
    print("AUTH PATCH DONE")
    for x in changed:
        print("UPDATED", x)
    if not changed:
        print("NO CHANGE")

if __name__ == "__main__":
    main()
