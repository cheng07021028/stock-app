# -*- coding: utf-8 -*-
"""H136: bounded, auditable auto-data bridge for H133/H135 research.

Existing H134 create-only archives and H117 EOD CSVs are the ONLY inputs.
The nightly job does not invent quotes, backdate PIT evidence, trade, or fetch
private portfolios. The UI reads a SHA-checked derived cache from runtime-data.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
import base64
import hashlib
import json
import re

import pandas as pd
import requests

from godpick_h134_decision_vault import _json_bytes, REMOTE_DIRECTORY
from godpick_h135_signal_governance import _archive_integrity, lifecycle_report

VERSION = 'V191-H136.0'
SCHEMA = 'H136-1'
HUB_PATH = 'data/h136'
FILE_NAMES = {'signals': 'H136_signals.csv', 'stock': 'H136_stock.csv',
              'benchmark': 'H136_benchmark.csv'}
MAX_ARCHIVES = 250
MAX_PRICE_ROWS = 200_000
MAX_BYTES = 10_000_000
MAX_ARCHIVE_BYTES = 360_000
GITHUB_API = 'https://api.github.com/repos'


def _hash(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _today() -> date:
    return datetime.now(ZoneInfo('Asia/Taipei')).date()


def _parse_dates(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors='coerce').dt.date


def _price_table(raw: pd.DataFrame, *, asof: date, benchmark: bool) -> tuple[pd.DataFrame, str]:
    cols = ['日期', '收盤價'] if benchmark else ['股票代號', '日期', '收盤價']
    empty = pd.DataFrame(columns=cols)
    if not isinstance(raw, pd.DataFrame) or raw.empty:
        return empty, 'SOURCE_EMPTY'
    if len(raw) > MAX_PRICE_ROWS:
        return empty, 'PRICE_ROW_LIMIT'
    date_col = next((c for c in ('date', '日期', 'Date') if c in raw), None)
    close_col = next((c for c in ('close', '收盤價', 'Close') if c in raw), None)
    code_col = next((c for c in ('code', '股票代號', 'Code') if c in raw), None)
    if not date_col or not close_col or (not benchmark and not code_col):
        return empty, 'SOURCE_SCHEMA_MISSING'
    p = pd.DataFrame({'日期': _parse_dates(raw[date_col]),
                      '收盤價': pd.to_numeric(raw[close_col].astype(str).str.replace(',', '', regex=False), errors='coerce')})
    if not benchmark:
        p['股票代號'] = raw[code_col].astype(str).str.strip().str.removesuffix('.0')
        p = p[p['股票代號'].str.fullmatch(r'[0-9]{4}')]
    p = p.dropna(subset=['日期', '收盤價'])
    p = p[(p['日期'] <= asof) & (p['收盤價'] > 0)]
    if p.empty:
        return empty, 'NO_VALID_PRICES'
    if p['日期'].map(lambda d: d.weekday() >= 5).any():
        return empty, 'WEEKEND_PRICE_SOURCE'
    key = ['日期'] if benchmark else ['股票代號', '日期']
    if p.groupby(key, dropna=False)['收盤價'].nunique().gt(1).any():
        return empty, 'CONFLICTING_CLOSE'
    p = p.drop_duplicates(key).sort_values(key).reset_index(drop=True)
    if benchmark and p['收盤價'].pct_change().abs().gt(.4).any():
        return empty, 'BENCHMARK_JUMP_RECHECK'
    if not benchmark and p.groupby('股票代號')['收盤價'].pct_change().abs().gt(.4).any():
        return empty, 'STOCK_SPLIT_OR_EXTREME_MOVE_RECHECK'
    return p[cols], 'H117_DERIVED_NOT_INDEPENDENTLY_ATTESTED'


def collect_local_archives(root: Path, *, asof: date) -> tuple[list[dict], dict[str, Any]]:
    folder = Path(root) / REMOTE_DIRECTORY
    files = sorted(folder.glob('H134_*.json')) if folder.is_dir() else []
    matching = [f for f in files if re.fullmatch(r'H134_\d{8}_[0-9a-f]{32}\.json', f.name)]
    matching = matching[-MAX_ARCHIVES:]
    rows: list[dict] = []
    skipped = 0
    for path in matching:
        try:
            if path.is_symlink() or path.stat().st_size > MAX_ARCHIVE_BYTES:
                skipped += 1
                continue
            data = json.loads(path.read_bytes())
            _, state, _ = _archive_integrity(data)
            if state != 'VALID_INTERNAL_HASH_NOT_INDEPENDENT_PIT':
                skipped += 1
                continue
            # The filename binds the actual content fingerprint, as in H134's remote readback.
            if not path.name.endswith(str(data.get('decision_fingerprint_sha256', ''))[:32] + '.json'):
                skipped += 1
                continue
            if date.fromisoformat(data['research_day']) > asof:
                skipped += 1
                continue
            rows.append(data)
        except (OSError, ValueError, TypeError, json.JSONDecodeError, KeyError):
            skipped += 1
    return rows, {'archive_files': len(matching), 'archive_valid': len(rows), 'archive_rejected': skipped,
                  'archive_limit_reached': len(files) > MAX_ARCHIVES}


def create_bundle(archives: list[dict], stocks: pd.DataFrame, benchmark: pd.DataFrame,
                  *, asof: date) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """Pure, offline derivation. Only H134 pre-existing A/B signals qualify."""
    lifecycle = lifecycle_report(archives, asof=asof)
    signals = lifecycle.get('歷史研究訊號', pd.DataFrame())
    if not isinstance(signals, pd.DataFrame):
        signals = pd.DataFrame()
    s, ss = _price_table(stocks, asof=asof, benchmark=False)
    b, bs = _price_table(benchmark, asof=asof, benchmark=True)
    dates = sorted(set(_parse_dates(signals['訊號研究日']).dropna())) if not signals.empty else []
    signal_count = len(signals)
    paired_days = (set(s['日期']) & set(b['日期'])) if not s.empty and not b.empty else set()
    aligned_signal_days = len(set(dates) & paired_days)
    complete = bool(signal_count and len(dates) >= 2 and aligned_signal_days >= 2)
    summary = {
        'schema': SCHEMA, 'asof': asof.isoformat(), 'status': 'RESEARCH_DATA_AVAILABLE' if complete else 'INCOMPLETE_NO_VALID_BACKTEST',
        'lifecycle_status': lifecycle.get('狀態', ''), 'archive_valid_dates': int(lifecycle.get('有效封存數', 0)),
        'signal_rows': signal_count, 'signal_days': len(dates), 'aligned_signal_days': aligned_signal_days, 'stock_rows': len(s), 'stock_codes': int(s['股票代號'].nunique()) if not s.empty else 0,
        'benchmark_rows': len(b), 'stock_status': ss, 'benchmark_status': bs,
        'latest_stock_date': str(s['日期'].max()) if not s.empty else '',
        'latest_benchmark_date': str(b['日期'].max()) if not b.empty else '',
        'latest_signal_date': str(max(dates)) if dates else '',
        'PIT': 'NOT_INDEPENDENTLY_VERIFIED', 'corporate_action': 'NOT_RECONCILED',
        'market_source': 'H117_DERIVED_EOD_CSV_UNATTESTED',
        'trade_permission': 'UNCHANGED',
        'note': '研究用衍生資料：H134內部雜湊不等於獨立PIT；H117本地CSV來源仍需官方封存鏈查核。缺價、未成熟、除權息與成本由H133再稽核。',
    }
    return {'signals': signals, 'stock': s, 'benchmark': b}, summary


def _csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False, lineterminator='\n').encode('utf-8-sig')


def make_artifacts(frames: dict[str, pd.DataFrame], summary: dict[str, Any]) -> dict[str, bytes]:
    """Artifacts are self-contained with signed-by-hash manifest, no private data."""
    blobs = {name: _csv_bytes(frames[name]) for name in FILE_NAMES}
    if any(len(b) > MAX_BYTES for b in blobs.values()):
        raise ValueError('H136_BUNDLE_TOO_LARGE')
    manifest = dict(summary)
    manifest['files'] = {FILE_NAMES[name]: {'sha256': _hash(blob), 'bytes': len(blob), 'rows': len(frames[name])}
                         for name, blob in blobs.items()}
    return {FILE_NAMES[name]: blob for name, blob in blobs.items()} | {'H136_manifest.json': _json_bytes(manifest)}


def validate_artifacts(blobs: dict[str, bytes], *, asof: date | None = None) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """Treat remote cache as untrusted; verify every byte and reject missing rows."""
    try:
        manifest_raw = blobs['H136_manifest.json']
        if len(manifest_raw) > 80_000:
            raise ValueError('BAD_MANIFEST_SIZE')
        manifest = json.loads(manifest_raw)
        if manifest.get('schema') != SCHEMA or not re.fullmatch(r'20\d{2}-\d{2}-\d{2}', str(manifest.get('asof'))):
            raise ValueError('INVALID_MANIFEST')
        day = date.fromisoformat(manifest['asof'])
        if asof is not None and day > asof:
            raise ValueError('FUTURE_GENERATION')
        frames = {}
        for name, filename in FILE_NAMES.items():
            raw = blobs[filename]
            entry = manifest['files'][filename]
            if len(raw) > MAX_BYTES or entry['bytes'] != len(raw) or entry['sha256'] != _hash(raw):
                raise ValueError('CACHE_SHA_MISMATCH')
            frame = pd.read_csv(BytesIO(raw), encoding='utf-8-sig', dtype={'股票代號': str})
            if len(frame) != entry['rows']:
                raise ValueError('CACHE_ROW_COUNT_MISMATCH')
            frames[name] = frame
        # Rerun safety checks, not only file hash which an attacker can rewrite.
        _, ss = _price_table(frames['stock'], asof=day, benchmark=False)
        _, bs = _price_table(frames['benchmark'], asof=day, benchmark=True)
        if ss != 'H117_DERIVED_NOT_INDEPENDENTLY_ATTESTED' and len(frames['stock']):
            raise ValueError('INVALID_STOCK_CACHE')
        if bs != 'H117_DERIVED_NOT_INDEPENDENTLY_ATTESTED' and len(frames['benchmark']):
            raise ValueError('INVALID_BENCHMARK_CACHE')
        return frames, manifest
    except (KeyError, TypeError, ValueError, UnicodeError, OverflowError, json.JSONDecodeError) as exc:
        raise ValueError(f'H136_CACHE_REJECTED:{type(exc).__name__}') from exc


def remote_cache(*, config: dict | None = None, transport: Any = requests,
                 asof: date | None = None) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """GitHub runtime-data read-only; never fall back to an unverified old ranking."""
    if config is None:
        from godpick_persistence_service import github_config
        cfg = github_config()
    else:
        cfg = config
    if not all(re.fullmatch(r'[\w.-]{1,100}', str(cfg.get(k, ''))) for k in ('owner', 'repo', 'branch')) or not cfg.get('token'):
        return {}, {'status': 'GITHUB_AUTH_NOT_CONFIGURED'}
    base = f"{GITHUB_API}/{cfg['owner']}/{cfg['repo']}/contents/{HUB_PATH}"
    headers = {'Authorization': 'Bearer ' + cfg['token'], 'Accept': 'application/vnd.github+json',
               'X-GitHub-Api-Version': '2022-11-28'}
    blobs = {}
    try:
        for filename in ('H136_health.json', 'H136_manifest.json', *FILE_NAMES.values()):
            res = transport.get(f'{base}/{filename}', headers=headers, params={'ref': cfg['branch']}, timeout=(5, 12))
            if res.status_code != 200:
                return {}, {'status': f'REMOTE_FILE_UNAVAILABLE_{res.status_code}', 'missing_file': filename}
            obj = res.json()
            if not isinstance(obj, dict):
                return {}, {'status': 'REMOTE_METADATA_INVALID'}
            if obj.get('encoding') == 'base64' and isinstance(obj.get('content'), str):
                data = base64.b64decode(obj['content'], validate=True)
            elif obj.get('encoding') == 'none' and not obj.get('content'):
                # GitHub Contents API strips inline base64 for files >1MB.
                # Ask for authenticated raw bytes; never follow redirects with token.
                raw_headers = {**headers, 'Accept': 'application/vnd.github.raw+json'}
                binary = transport.get(f'{base}/{filename}', headers=raw_headers,
                                       params={'ref': cfg['branch']}, timeout=(5, 15), allow_redirects=False)
                if binary.status_code != 200:
                    return {}, {'status': 'REMOTE_RAW_UNAVAILABLE', 'file': filename}
                data = binary.content
                if not isinstance(data, bytes):
                    return {}, {'status': 'REMOTE_RAW_CONTENT_INVALID'}
            else:
                return {}, {'status': 'REMOTE_ENCODING_REJECTED'}
            if len(data) > MAX_BYTES:
                return {}, {'status': 'REMOTE_OVERSIZE'}
            blobs[filename] = data
        health = json.loads(blobs['H136_health.json'])
        if health.get('schema') != 'H136-health-1' or health.get('status') != 'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY':
            return {}, {'status': 'LAST_SYNC_NOT_READY', 'last_run_day': health.get('run_day_tw'), 'reason': health.get('reason')}
        frames, manifest = validate_artifacts(blobs, asof=asof or _today())
        if health.get('run_day_tw') != manifest.get('asof'):
            return {}, {'status': 'LATEST_RUN_AND_CACHE_DATE_MISMATCH'}
        last = date.fromisoformat(manifest['asof'])
        if (asof or _today()) - last > timedelta(days=14):
            return {}, {'status': 'CACHE_STALE_MORE_THAN_14_DAYS', 'last_run_day': str(last)}
        return frames, {**manifest, 'status': manifest['status'], 'remote_readback': 'SHA256_VERIFIED_NOT_PIT'}
    except (requests.RequestException, ValueError, KeyError, TypeError, json.JSONDecodeError, base64.binascii.Error):
        return {}, {'status': 'REMOTE_CACHE_REJECTED_OR_OFFLINE'}
