# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import argparse
import hashlib

EXPECTED = [
    'streamlit_app.py',
    'godpick_h204_sidebar_navigation.py',
    'tests/test_h204_sidebar_navigation.py',
]


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description='H204 numbered sidebar preflight')
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    root = Path(args.root)
    missing = [rel for rel in EXPECTED if not (root / rel).exists()]
    if missing:
        print('MISSING')
        for rel in missing:
            print(rel)
        return 1
    src = (root / 'streamlit_app.py').read_text('utf-8-sig')
    ok = 'run_numbered_navigation' in src and 'godpick_h204_sidebar_navigation' in src
    print('READY_APPLY_H204' if ok else 'NOT_READY_H204')
    for rel in EXPECTED:
        p = root / rel
        print(f'{rel} {sha256_of(p)}')
    return 0 if ok else 2


if __name__ == '__main__':
    raise SystemExit(main())
