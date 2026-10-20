# -*- coding: utf-8 -*-
"""H139 bounded post-sync repair and research settlement CLI (no brokerage)."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import sys
import tempfile
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h134_decision_vault import _json_bytes
from godpick_h139_settlement import load_verified_h136, build_settlement, _base
from tools.h136_nightly_sync import sync

TW = ZoneInfo('Asia/Taipei')


def _atomic(path: Path, blob: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.h139_', suffix='.tmp')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(blob); f.flush(); os.fsync(f.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _choose_market(source: Path, runtime: Path, asof: date) -> Path:
    """Prefer a complete current-day H117 3-source checkout over old CSV."""
    def complete(root: Path) -> bool:
        return all((root / 'data' / 'h117' / 'daily' / name /
                    (asof.isoformat() + '.json.gz')).is_file() for name in ('TWSE', 'TPEX', 'TAIEX'))
    if complete(source): return source
    if complete(runtime): return runtime
    # No assumption that file presence = official validity: sync re-verifies manifests.
    # Try existing H116 fallback from the root with both CSVs, never mix roots.
    for candidate in (source, runtime):
        if all((candidate / 'data' / 'h116' / x).is_file()
               for x in ('verified_ohlc.csv', 'benchmark_ohlc.csv')):
            return candidate
    return source


def recovery_once(*, source_root: Path, runtime_root: Path, asof: date,
                  sync_fn=sync) -> dict:
    """Zero or ONE rebuild. Status never upgraded unless cache and EOD are valid."""
    report = {'schema': 'H139-recovery-1', 'asof_tw': asof.isoformat(),
              'checked_at_tw': datetime.now(TW).isoformat(timespec='seconds'),
              'status': 'UNKNOWN', 'attempts': 0,
              'staged_validation_before_publish': True,
              'not_a_market_source_attestation': True}
    try:
        load_verified_h136(runtime_root, asof=asof)
        report['status'] = 'ALREADY_READY_NO_RETRY'
    except (ValueError, OSError) as first:
        report['initial_blocker'] = str(first)[:160]
        market_root = _choose_market(source_root, runtime_root, asof)
        report['market_checkout'] = ('source' if market_root == source_root else 'runtime')
        report['attempts'] = 1
        try:
            # Build in an isolated directory. NEVER let a failed retry replace
            # an existing H136 manifest/last-good CSVs before new data passes.
            with TemporaryDirectory(prefix='h139_staged_recovery_') as temporary:
                stage_root = Path(temporary)
                sync_fn(vault_root=runtime_root, market_root=market_root,
                        output_root=stage_root, asof=asof)
                load_verified_h136(stage_root, asof=asof)
                names = ('H136_signals.csv', 'H136_stock.csv', 'H136_benchmark.csv',
                         'H136_manifest.json', 'H136_health.json')
                for filename in names:
                    source = stage_root / 'data' / 'h136' / filename
                    if source.is_symlink() or not source.is_file():
                        raise ValueError('STAGING_CACHE_FILE_UNSAFE')
                    _atomic(runtime_root / 'data' / 'h136' / filename, source.read_bytes())
            load_verified_h136(runtime_root, asof=asof)
            report['status'] = 'RECOVERED_INTERNAL_SHA_EOD_CURRENT'
        except (ValueError, OSError, RuntimeError, KeyError) as exc:
            # No retry loop and no fabricated SUCCESS. A missing source is not reparable here.
            report['status'] = 'RETRY_FAILED_STILL_NOT_READY'
            report['remaining_blocker'] = f'{type(exc).__name__}:{str(exc)[:120]}'
    _atomic(runtime_root / 'data' / 'h139' / 'H139_recovery.json', _json_bytes(report))
    return report


def settle_once(*, runtime_root: Path, asof: date) -> dict:
    try:
        frames, manifest = load_verified_h136(runtime_root, asof=asof)
        path = runtime_root / 'data' / 'h137' / 'H137_audit.json'
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 100_000:
            raise ValueError('H137_CURRENT_AUDIT_NOT_AVAILABLE')
        audit = json.loads(path.read_bytes())
        report = build_settlement(asof=asof, frames=frames, manifest=manifest, audit=audit)
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError) as exc:
        report = _base(asof)
        report['reason'] = f'{type(exc).__name__}:{str(exc)[:150]}'
    _atomic(runtime_root / 'data' / 'h139' / 'H139_settlement.json', _json_bytes(report))
    return report


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--stage', required=True, choices=('recovery','settlement'))
    p.add_argument('--source-root', type=Path)
    p.add_argument('--runtime-root', type=Path, required=True)
    p.add_argument('--asof', type=date.fromisoformat, default=datetime.now(TW).date())
    args = p.parse_args(argv)
    if args.stage == 'recovery':
        if args.source_root is None:
            p.error('--source-root required for recovery')
        result = recovery_once(source_root=args.source_root, runtime_root=args.runtime_root, asof=args.asof)
    else:
        result = settle_once(runtime_root=args.runtime_root, asof=args.asof)
    print(json.dumps({'stage': args.stage, 'asof': args.asof.isoformat(),
                      'status': result['status']}, ensure_ascii=False))
    # H137 audit remains the pipeline blocker. Publishing blocked receipts is expected.
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
