# -*- coding: utf-8 -*-
"""Build bounded H138 manager operational brief from same-day H137 audit."""
from __future__ import annotations
import argparse
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import sys
import os
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h138_daily_brief import build_brief
from godpick_h134_decision_vault import _json_bytes


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime-root', type=Path, required=True)
    parser.add_argument('--asof', type=date.fromisoformat,
                        default=datetime.now(ZoneInfo('Asia/Taipei')).date())
    args = parser.parse_args(argv)
    audit_file = args.runtime_root / 'data' / 'h137' / 'H137_audit.json'
    try:
        audit = json.loads(audit_file.read_bytes())
        if audit.get('schema') != 'H137-audit-1' or audit.get('asof_tw') != args.asof.isoformat():
            raise ValueError('AUDIT_NOT_CURRENT')
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        audit = {'status': 'AUDIT_MISSING', 'findings': ['H137_AUDIT_UNAVAILABLE']}
    brief = build_brief(root=args.runtime_root, asof=args.asof, audit=audit)
    output = args.runtime_root / 'data' / 'h138' / 'H138_manager_brief.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.h138_', suffix='.tmp', dir=output.parent)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(_json_bytes(brief))
            f.flush(); os.fsync(f.fileno())
        os.replace(name, output)
    finally:
        if os.path.exists(name): os.unlink(name)
    print(json.dumps({'status': brief['status'], 'asof': brief['asof_tw']}, ensure_ascii=False))
    return 0  # The H137 audit remains the job's failure authority.


if __name__ == '__main__':
    raise SystemExit(main())
