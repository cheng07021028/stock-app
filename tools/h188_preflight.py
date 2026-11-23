# -*- coding: utf-8 -*-
"""V191-H188 exact-file, no-regression preflight and local installer.

Usage from unpacked ZIP:
  python tools/h188_preflight.py --root D:\\stock-app
  python tools/h188_preflight.py --root D:\\stock-app --apply
The installer never accesses the network or modifies JSON/history/secrets.
"""
from __future__ import annotations
import argparse
import hashlib
import os
import shutil
from datetime import datetime
from pathlib import Path

FILES = {
    'godpick_auto_scheduler.py': ('1da81d8f74492107f4e1d0910c09ceb82e23eefc7135bb7d0cea6bca6a75307d', '1fee724aa4bb9a1457f8ff8d932223fed09be539d66ad8f4df7a067c5d9895d8'),
    'godpick_auto_update_tasks.py': ('69af97606d78883fdcb3972654f95baa18cb559a42f74176008293693154f045', '13df1417db340992d241e5b219ea4edb6f886b488c8b07f245a9a953133d4c15'),
    'godpick_super_ai_market_context.py': ('e48221adb40bc5466dcf8d4e86cf7fbe8064f2da3a5637cf5ce707c10117cd5a', '2afc7d83478d5e990410464d72b86327cd892abd200aa5613a527e3af81e823d'),
    'pages/10_推薦清單.py': ('df4bd080563250432919330bb0bb2bfbf1fb9f545ac97a387c9f168835a8cac3', '4a44f49145d57173c2e48af8c069fd288ed40c37b0697552061cb9a0aa89300a'),
}
BUNDLE_ROOT = Path(__file__).resolve().parent.parent


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inspect(root: Path) -> dict[str, str]:
    result = {}
    for name, (before, after) in FILES.items():
        target = root / name
        source = BUNDLE_ROOT / name
        if not target.is_file() or not source.is_file() or sha(source) != after:
            result[name] = 'BLOCKED_MISSING_OR_BAD_PAYLOAD'
        else:
            h = sha(target)
            result[name] = 'BASELINE_H187' if h == before else ('H188_ALREADY_INSTALLED' if h == after else 'BLOCKED_UNKNOWN_OR_NEWER')
    return result


def install(root: Path) -> dict[str, str]:
    statuses = inspect(root)
    if any(x.startswith('BLOCKED') for x in statuses.values()):
        raise RuntimeError('H188_SAFE_BLOCK: unknown, newer, or incomplete files detected; nothing modified')
    to_write = [k for k, v in statuses.items() if v == 'BASELINE_H187']
    if not to_write:
        return statuses
    backup = root / '_h188_rollback' / datetime.now().strftime('%Y%m%d_%H%M%S')
    for name in to_write:
        dest = backup / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / name, dest)
    # Preflight all files and make all backups before starting replacement.
    for name in to_write:
        target = root / name
        tmp = target.with_name(target.name + '.h188_tmp')
        try:
            tmp.write_bytes((BUNDLE_ROOT / name).read_bytes())
            os.replace(tmp, target)
        finally:
            if tmp.exists():
                tmp.unlink()
    return inspect(root)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description='H188 exact-hash preflight, no downgrades')
    ap.add_argument('--root', required=True, help='Your real stock-app repository folder')
    ap.add_argument('--apply', action='store_true', help='Backup and install only known baseline production files')
    args = ap.parse_args(argv)
    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        print('BLOCKED_ROOT_MISSING: ' + str(root)); return 2
    statuses = inspect(root)
    for k, v in statuses.items():
        print(f'{k}: {v}')
    if any(x.startswith('BLOCKED') for x in statuses.values()):
        print('BLOCKED_UNKNOWN_VERSION; nothing changed'); return 2
    if not args.apply:
        print('H188_ALREADY_INSTALLED' if all(v == 'H188_ALREADY_INSTALLED' for v in statuses.values()) else 'READY_APPLY_H188')
        return 0
    try:
        result = install(root)
    except (OSError, RuntimeError) as exc:
        print('BLOCKED_INSTALL: ' + str(exc)); return 2
    if all(x == 'H188_ALREADY_INSTALLED' for x in result.values()):
        print('H188_APPLIED_OK'); return 0
    print('BLOCKED_POSTVERIFY'); return 2


if __name__ == '__main__':
    raise SystemExit(main())
