# -*- coding: utf-8 -*-
"""
force_auth_lock_v77.py
安全版全站登入鎖注入工具：
- 修正 v76 可能造成 streamlit_app.py 縮排錯誤的問題
- streamlit_app.py：require_login() 放在 main() 裡 st.set_page_config 後
- pages/*.py：require_login() 放在 st.set_page_config 後；沒有 set_page_config 才放 import 區後
"""
from __future__ import annotations

from pathlib import Path
import re
import py_compile
import sys

ROOT = Path(__file__).resolve().parent
AUTH_IMPORT = "from app_auth import require_login"
GUARD_START = "# >>> APP_AUTH_GUARD_V77"
GUARD_END = "# <<< APP_AUTH_GUARD_V77"

OLD_GUARD_RE = re.compile(r"\n?\s*# >>> APP_AUTH_GUARD_V\d+\n.*?\n\s*# <<< APP_AUTH_GUARD_V\d+\n?", re.S)


def _remove_old_guards(text: str) -> str:
    text = OLD_GUARD_RE.sub("\n", text)
    # Remove stray guard imports/calls created by older tools, but keep our import added later.
    lines = []
    for line in text.splitlines():
        s = line.strip()
        if s == AUTH_IMPORT:
            continue
        if s == "require_login()":
            continue
        lines.append(line)
    return "\n".join(lines).rstrip() + "\n"


def _add_import_after_streamlit(text: str) -> str:
    if AUTH_IMPORT in text:
        return text
    lines = text.splitlines()
    out = []
    inserted = False
    for line in lines:
        out.append(line)
        if not inserted and line.strip() == "import streamlit as st":
            out.append(AUTH_IMPORT)
            inserted = True
    if not inserted:
        # fallback: after future/import block top-level only
        idx = 0
        for i, line in enumerate(lines[:80]):
            s = line.strip()
            if line and not line.startswith((' ', '\t')) and (s.startswith('import ') or s.startswith('from ')):
                idx = i + 1
        lines.insert(idx, AUTH_IMPORT)
        return "\n".join(lines).rstrip() + "\n"
    return "\n".join(out).rstrip() + "\n"


def _find_matching_paren(lines, start_idx):
    # Simple paren balance from st.set_page_config line.
    bal = 0
    started = False
    for j in range(start_idx, len(lines)):
        line = lines[j]
        # strip comments roughly
        code = line.split('#', 1)[0]
        if '(' in code:
            started = True
        bal += code.count('(') - code.count(')')
        if started and bal <= 0:
            return j
    return start_idx


def _insert_after_set_page_config(text: str, indent: str) -> str | None:
    lines = text.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('st.set_page_config'):
            end = _find_matching_paren(lines, i)
            guard = [
                f"{indent}{GUARD_START}",
                f"{indent}require_login()",
                f"{indent}{GUARD_END}",
            ]
            lines[end+1:end+1] = guard
            return "\n".join(lines).rstrip() + "\n"
    return None


def patch_streamlit_app(path: Path):
    text = path.read_text(encoding='utf-8')
    text = _remove_old_guards(text)
    text = _add_import_after_streamlit(text)
    # In streamlit_app, require_login belongs inside main(), after set_page_config.
    # Find def main section and patch its st.set_page_config.
    lines = text.splitlines()
    main_start = None
    for i, line in enumerate(lines):
        if line.startswith('def main('):
            main_start = i
            break
    if main_start is not None:
        for i in range(main_start, len(lines)):
            if lines[i].startswith('    st.set_page_config'):
                end = _find_matching_paren(lines, i)
                guard = [
                    f"    {GUARD_START}",
                    f"    require_login()",
                    f"    {GUARD_END}",
                ]
                lines[end+1:end+1] = guard
                path.write_text("\n".join(lines).rstrip() + "\n", encoding='utf-8')
                return
    patched = _insert_after_set_page_config(text, '')
    if patched is None:
        # fallback after imports
        patched = text.replace(AUTH_IMPORT, AUTH_IMPORT + f"\n{GUARD_START}\nrequire_login()\n{GUARD_END}", 1)
    path.write_text(patched, encoding='utf-8')


def patch_page(path: Path):
    text = path.read_text(encoding='utf-8')
    text = _remove_old_guards(text)
    text = _add_import_after_streamlit(text)
    patched = _insert_after_set_page_config(text, '')
    if patched is None:
        # Insert right after auth import if no set_page_config in page.
        patched = text.replace(AUTH_IMPORT, AUTH_IMPORT + f"\n{GUARD_START}\nrequire_login()\n{GUARD_END}", 1)
    path.write_text(patched, encoding='utf-8')


def main():
    targets = []
    sp = ROOT / 'streamlit_app.py'
    if sp.exists():
        patch_streamlit_app(sp)
        targets.append(sp)
    pages = ROOT / 'pages'
    if pages.exists():
        for p in sorted(pages.glob('*.py')):
            # account management also locked; app_auth allows admin/user checks inside page.
            patch_page(p)
            targets.append(p)
    errors = []
    for p in targets:
        try:
            py_compile.compile(str(p), doraise=True)
        except Exception as e:
            errors.append((p, e))
    if errors:
        print('以下檔案語法檢查失敗：')
        for p, e in errors:
            print(f'- {p.relative_to(ROOT)}: {e}')
        sys.exit(1)
    print(f'完成：已安全加入登入鎖並通過語法檢查，共 {len(targets)} 個檔案。')

if __name__ == '__main__':
    main()
