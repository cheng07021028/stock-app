# -*- coding: utf-8 -*-
"""H137 nightly post-H136 audit. Read only source; writes a health receipt only.

No artificial market sessions, no new picks, no modification to H134/H136 data.
Exit nonzero on BLOCKED so Github Actions notifications can be configured.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import os
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h136_auto_hub import collect_local_archives, validate_artifacts, FILE_NAMES
from godpick_h134_decision_vault import _json_bytes

TW = ZoneInfo('Asia/Taipei')
SCHEMA = 'H137-audit-1'

def audit(*, root: Path, asof: date, market_closed: bool = False) -> dict:
    """Check recent live decisions and the latest H136 integrity/health.

    Only a runtime-data local checkout is used. A passing internal audit is NOT
    external attestation or independent PIT proof.
    """
    paths = root / 'data' / 'h136'
    snapshots, diagnostic = collect_local_archives(root, asof=asof)
    live = [s for s in snapshots if s.get('source_type') == 'LIVE_SESSION_H126'
            and s.get('research_day') == asof.isoformat()]
    statuses = []
    if not market_closed and not live:
        statuses.append('H126_LIVE_REPORT_NOT_ARCHIVED_FOR_TODAY')
    health_path = paths / 'H136_health.json'
    if not health_path.is_file() or health_path.is_symlink():
        statuses.append('H136_HEALTH_MISSING')
    else:
        try:
            if health_path.stat().st_size > 100_000:
                raise ValueError('HEALTH_OVERSIZED')
            health = json.loads(health_path.read_bytes())
            if health.get('schema') != 'H136-health-1':
                raise ValueError('INVALID_HEALTH_SCHEMA')
            if health.get('run_day_tw') != asof.isoformat():
                statuses.append('H136_NOT_UPDATED_TODAY')
            if health.get('status') != 'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY':
                statuses.append('H136_LATEST_RUN_NOT_READY')
            if health.get('status') == 'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY':
                names = ('H136_manifest.json', *FILE_NAMES.values())
                if any(not (paths / name).is_file() or (paths / name).is_symlink() for name in names):
                    statuses.append('H136_CACHE_MISSING')
                else:
                    blob = {}
                    for name in names:
                        path = paths / name
                        if path.stat().st_size > 10_000_000:
                            raise ValueError('OVERSIZED_CACHE')
                        blob[name] = path.read_bytes()
                    _, manifest = validate_artifacts(blob, asof=asof)
                    if manifest.get('asof') != asof.isoformat():
                        statuses.append('H136_CACHE_OUTDATED')
                    # "Nightly sync ran" is not "today's market data arrived".
                    # A refreshed manifest based solely on old prices must fail
                    # the daily readiness audit on an expected trading day.
                    if not market_closed and (manifest.get('latest_stock_date') != asof.isoformat()
                                              or manifest.get('latest_benchmark_date') != asof.isoformat()):
                        statuses.append('H136_EOD_PRICE_DATA_NOT_CURRENT')
        except (OSError, ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError):
            statuses.append('H136_HEALTH_OR_CACHE_INVALID')
    return {'schema': SCHEMA, 'asof_tw': asof.isoformat(),
            'audit_time_tw': datetime.now(TW).isoformat(timespec='seconds'),
            'status': 'BLOCKED_REQUIRES_ATTENTION' if statuses else 'INTERNAL_CHECK_PASS_NOT_PIT',
            'findings': statuses, 'same_day_live_h126_archives': len(live),
            'archive_valid': diagnostic['archive_valid'], 'archive_rejected': diagnostic['archive_rejected'],
            'PIT': 'NOT_INDEPENDENTLY_VERIFIED',
            'note': 'Internal only. Holiday scheduling may require manual market_closed flag; cannot auto-authorize trades.'}


def _atomic(path: Path, blob: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.h137_', suffix='.tmp')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(blob); f.flush(); os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime-root', type=Path, required=True)
    parser.add_argument('--asof', type=date.fromisoformat, default=datetime.now(TW).date())
    parser.add_argument('--market-closed', action='store_true',
                        help='manual market-closed override; do not mark data synced by guessing a holiday')
    args = parser.parse_args()
    status = audit(root=args.runtime_root, asof=args.asof, market_closed=args.market_closed)
    _atomic(args.runtime_root / 'data' / 'h137' / 'H137_audit.json', _json_bytes(status))
    print(json.dumps(status, ensure_ascii=False))
    return 1 if status['status'] == 'BLOCKED_REQUIRES_ATTENTION' else 0


if __name__ == '__main__':
    raise SystemExit(main())
