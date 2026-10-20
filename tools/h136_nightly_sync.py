# -*- coding: utf-8 -*-
"""Nightly H136 synchronized read-only H134/H117 research dataset (no web scraper).

Run against CHECKED OUT runtime-data (vault) and source branch (H117 official
EOD). Requires no secrets except GitHub Actions checkout permissions. Never
promote old H102 rankings or fabricate missing sessions.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import os
import sys
import tempfile

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h136_auto_hub import (collect_local_archives, create_bundle, make_artifacts,
                                   validate_artifacts, _json_bytes)


def _load_csv(root: Path, name: str) -> pd.DataFrame:
    path = root / 'data' / 'h116' / name
    if not path.is_file() or path.is_symlink() or path.stat().st_size > 25_000_000:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype={'code': str, '股票代號': str}, low_memory=False)
    except (OSError, ValueError, UnicodeError, pd.errors.ParserError):
        return pd.DataFrame()


def _load_h117_archived(root: Path, archives: list[dict], *, asof: date) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Derive exactly matched H134 watchlist from existing full-market H117 EOD.

    Do not guess stock bars from H116's older tracked selection.  Require
    TWSE+TPEx+TAIEX complete per session, verified manifest, source URL and
    settled collector timestamp. A corrupt daily source cannot be promoted.
    """
    folder = root / 'data' / 'h117' / 'daily'
    if not folder.is_dir():
        return pd.DataFrame(), pd.DataFrame(), 'H117_DAILY_ARCHIVE_NOT_FOUND'
    from godpick_h117_official_market_closure import (verify_manifest, MIN_COUNTS, MARKET_URLS)
    from datetime import time
    from zoneinfo import ZoneInfo
    import gzip
    import re
    codes = {str(row.get('股票代號')) for snap in archives for row in snap.get('rows', [])
             if re.fullmatch(r'[0-9]{4}', str(row.get('股票代號', '')))}
    if not codes:
        return pd.DataFrame(), pd.DataFrame(), 'NO_H134_RESEARCH_STOCKS'
    roots = {src: folder / src for src in ('TWSE', 'TPEX', 'TAIEX')}
    if any(not v.is_dir() for v in roots.values()):
        return pd.DataFrame(), pd.DataFrame(), 'H117_THREE_MARKETS_INCOMPLETE'
    sets = []
    for src, path in roots.items():
        sets.append({p.name[:-8] for p in path.glob('*.json.gz')
                     if re.fullmatch(r'\d{4}-\d{2}-\d{2}\.json\.gz', p.name)})
    common = sorted(set.intersection(*sets))[-260:]
    stock_rows, bench_rows = [], []
    for day in common:
        if day > asof.isoformat():
            continue
        cohort = {}
        valid = True
        for src, base in roots.items():
            path = base / (day + '.json.gz')
            try:
                if path.is_symlink() or path.stat().st_size > 20_000_000:
                    raise ValueError('UNSAFE_ARCHIVE')
                with gzip.open(path, 'rt', encoding='utf-8') as f:
                    data = json.load(f)
                when = datetime.fromisoformat(str(data.get('collected_at', '')).replace('Z', '+00:00'))
                if when.tzinfo is None or when.astimezone(ZoneInfo('Asia/Taipei')).date().isoformat() != day or when.astimezone(ZoneInfo('Asia/Taipei')).time() < time(15):
                    raise ValueError('EOD_TIMESTAMP_NOT_VALID')
                if (not verify_manifest(data) or data.get('source') != src or data.get('date') != day
                    or data.get('source_url') != MARKET_URLS[src] or not isinstance(data.get('rows'), list)
                    or len(data['rows']) < MIN_COUNTS[src]):
                    raise ValueError('H117_MANIFEST_INVALID')
                cohort[src] = data['rows']
            except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, EOFError):
                valid = False
                break
        if not valid or len(cohort['TAIEX']) != 1:
            continue
        index = cohort['TAIEX'][0]
        if index.get('date') != day or index.get('code') != 'BENCHMARK':
            continue
        bench_rows.append(index)
        for src in ('TWSE', 'TPEX'):
            for item in cohort[src]:
                if item.get('code') in codes and item.get('date') == day:
                    stock_rows.append(item)
    if not bench_rows:
        return pd.DataFrame(), pd.DataFrame(), 'H117_NO_COMPLETE_VERIFIED_DAYS'
    return pd.DataFrame(stock_rows), pd.DataFrame(bench_rows), 'H117_FULL_EOD_LOCAL_HASH_VERIFIED'


def _atomic(path: Path, blob: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.h136_', suffix='.tmp')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(blob)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def sync(*, vault_root: Path, market_root: Path, output_root: Path, asof: date) -> dict:
    archives, diagnostics = collect_local_archives(vault_root, asof=asof)
    stocks, benchmark, market_status = _load_h117_archived(market_root, archives, asof=asof)
    if stocks.empty or benchmark.empty:
        stocks = _load_csv(market_root, 'verified_ohlc.csv')
        benchmark = _load_csv(market_root, 'benchmark_ohlc.csv')
        market_status = market_status + ';FALLBACK_H116_TRACKED_CSV'
    frames, quality = create_bundle(archives, stocks, benchmark, asof=asof)
    quality['actual_market_source_status'] = market_status
    quality['vault'] = diagnostics
    quality['market_root_type'] = 'USER_SELECTED_CHECKOUT_NOT_SOURCE_ATTESTATION'
    ready = quality['status'] == 'RESEARCH_DATA_AVAILABLE'
    folder = output_root / 'data' / 'h136'
    folder.mkdir(parents=True, exist_ok=True)
    if ready:
        files = make_artifacts(frames, quality)
        validate_artifacts(files, asof=asof)
        # Write data files before manifest, so consumers detect partial updates via SHA.
        for filename in ('H136_signals.csv', 'H136_stock.csv', 'H136_benchmark.csv'):
            _atomic(folder / filename, files[filename])
        _atomic(folder / 'H136_manifest.json', files['H136_manifest.json'])
    # Never replace a last-good cache with a fabricated/empty dataset.
    # This health receipt records the LATEST run and flags stale cache as such.
    health = {'schema': 'H136-health-1', 'run_day_tw': asof.isoformat(),
              'run_at_tw': datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds'),
              'status': 'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY' if ready else 'NOT_READY_OLD_DATA_PRESERVED',
              'reason': quality, 'cache_updated': ready,
              'note': 'Not independently PIT verified; no trading authority.'}
    _atomic(folder / 'H136_health.json', _json_bytes(health))
    return health


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--vault-root', type=Path, required=True)
    p.add_argument('--market-root', type=Path, required=True)
    p.add_argument('--output-root', type=Path, required=True)
    p.add_argument('--asof', type=date.fromisoformat, default=datetime.now(ZoneInfo('Asia/Taipei')).date())
    a = p.parse_args()
    result = sync(vault_root=a.vault_root, market_root=a.market_root,
                  output_root=a.output_root, asof=a.asof)
    print(json.dumps({'status': result['status'], 'summary': result['reason']}, ensure_ascii=False, default=str))
    return 0  # no data is valid fail-safe, not a hard network error


if __name__ == '__main__':
    raise SystemExit(main())
