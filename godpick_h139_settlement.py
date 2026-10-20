# -*- coding: utf-8 -*-
"""H139: deterministic, fail-closed daily *research* settlement and drift advice.

Input: H136 verified derived research cache + H137 audit.  Not a trade ledger.
Returns are signal-date close -> future EOD close, NOT executable fills.
No trading authority, model weight mutation, or account access.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
import base64
import binascii
import hashlib
import json
import math
import re

import pandas as pd
import requests

from godpick_h134_decision_vault import API_ROOT, _json_bytes
from godpick_h136_auto_hub import FILE_NAMES, validate_artifacts
from godpick_h133_blackhorse_validation import (_normalize_signals, _benchmark,
                                                 _stock_prices, HORIZONS)

VERSION = 'V191-H139.0'
SCHEMA = 'H139-research-settlement-1'
TW = ZoneInfo('Asia/Taipei')
MAX_REMOTE = 120_000
MIN_DRIFT_BLOCK_DAYS = 15
ADVERSE_SHIFT_PP = 2.0  # fixed before inspecting any real validation data


def load_verified_h136(root: Path, *, asof: date) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    """Accept only *this* run's internally verified, EOD-current H136 cache."""
    base = Path(root) / 'data' / 'h136'
    health_path = base / 'H136_health.json'
    if health_path.is_symlink() or not health_path.is_file() or health_path.stat().st_size > 100_000:
        raise ValueError('H136_HEALTH_MISSING_OR_UNSAFE')
    try:
        health = json.loads(health_path.read_bytes())
    except (ValueError, UnicodeError) as exc:
        raise ValueError('H136_HEALTH_INVALID') from exc
    if (not isinstance(health, dict) or health.get('schema') != 'H136-health-1'
            or health.get('run_day_tw') != asof.isoformat()
            or health.get('status') != 'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY'):
        raise ValueError('H136_LATEST_RUN_NOT_READY')
    blobs: dict[str, bytes] = {}
    for filename in ['H136_manifest.json', *FILE_NAMES.values()]:
        path = base / filename
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 10_000_000:
            raise ValueError('H136_CACHE_MISSING_OR_UNSAFE')
        blobs[filename] = path.read_bytes()
    frames, manifest = validate_artifacts(blobs, asof=asof)
    if (manifest.get('asof') != asof.isoformat()
            or manifest.get('status') != 'RESEARCH_DATA_AVAILABLE'):
        raise ValueError('H136_DATA_NOT_CURRENT')
    if (manifest.get('latest_stock_date') != asof.isoformat()
            or manifest.get('latest_benchmark_date') != asof.isoformat()):
        raise ValueError('H136_EOD_PRICES_STALE')
    return frames, manifest


def _base(asof: date) -> dict[str, Any]:
    return {
        'schema': SCHEMA, 'asof_tw': asof.isoformat(),
        'generated_at_tw': datetime.now(TW).isoformat(timespec='seconds'),
        'status': 'BLOCKED_NO_VERIFIED_RESEARCH_DATA',
        'reason': '', 'strategies': {}, 'anomalies': [],
        'source': 'H136_INTERNAL_SHA_H134_H117_DERIVED',
        'PIT': 'NOT_INDEPENDENTLY_ATTESTED',
        'corporate_actions': 'NOT_FULLY_RECONCILED',
        'returns': 'RESEARCH_EOD_CLOSE_TO_CLOSE_NOT_ACTUAL_TRADE_PNL',
        'trade_permission': 'UNCHANGED', 'automatic_weights': 'DISABLED',
    }


def build_settlement(*, asof: date, frames: dict[str, pd.DataFrame],
                     manifest: dict[str, Any], audit: dict[str, Any]) -> dict[str, Any]:
    """Top3/Top5 fixed policies. Complete-case daily cohorts only, no fill-forward.

    Market horizons are BENCHMARK sessions, not N observed ticker bars.
    Partial cohorts are always excluded from average alpha; this blocks
    survivor-only overstatement. Drift alerts are descriptive only.
    """
    report = _base(asof)
    if (manifest.get('asof') != asof.isoformat()
            or manifest.get('status') != 'RESEARCH_DATA_AVAILABLE'
            or manifest.get('latest_stock_date') != asof.isoformat()
            or manifest.get('latest_benchmark_date') != asof.isoformat()):
        report['reason'] = 'H136_CACHE_NOT_READY_OR_EOD_OUTDATED'
        return report
    if (audit.get('schema') != 'H137-audit-1'
            or audit.get('asof_tw') != asof.isoformat()
            or audit.get('status') != 'INTERNAL_CHECK_PASS_NOT_PIT'):
        report['reason'] = 'H137_SAME_DAY_AUDIT_NOT_PASSED'
        return report
    signals, _ = _normalize_signals(frames.get('signals'), asof=asof)
    market, bs = _benchmark(frames.get('benchmark'), asof=asof)
    stocks, ps = _stock_prices(frames.get('stock'), asof=asof)
    if signals.empty or market.empty or stocks.empty:
        report['reason'] = 'SIGNALS_OR_PRICES_INVALID'
        report['anomalies'] = [x for x, bad in [('NO_VALID_SIGNALS', signals.empty),
                                                (bs, market.empty), (ps, stocks.empty)] if bad]
        return report
    sessions = list(market['日期'])
    if not sessions or sessions[-1] != asof:
        report['reason'] = 'BENCHMARK_LAST_SESSION_NOT_ASOF'
        return report
    pos = {day: n for n, day in enumerate(sessions)}
    close_b = dict(zip(market['日期'], market['大盤收盤價']))
    close_s = {(code, day): px for code, day, px in stocks[['code', 'day', 'close']].itertuples(index=False, name=None)}
    ordered = signals.sort_values(['訊號研究日', '當時研究分數', '股票代號'],
                                  ascending=[True, False, True], na_position='last')
    total_matured = 0
    for label, n in (('TOP3', 3), ('TOP5', 5)):
        chosen = ordered.groupby('訊號研究日', sort=False).head(n)
        rows: list[dict[str, Any]] = []
        for signal_day, cohort in chosen.groupby('訊號研究日', sort=True):
            signal_day = signal_day if isinstance(signal_day, date) else date.fromisoformat(str(signal_day))
            if signal_day not in pos:
                continue
            for h in HORIZONS:
                position = pos[signal_day] + h
                if position >= len(sessions):
                    continue  # NOT_MATURED; never fill with last known price
                target = sessions[position]
                errors = []
                alphas = []
                gains = []
                for ticker in cohort['股票代號']:
                    p0 = close_s.get((ticker, signal_day))
                    p1 = close_s.get((ticker, target))
                    if p0 is None or p1 is None or p0 <= 0 or p1 <= 0:
                        errors.append('STOCK_PRICE_MISSING_NO_SURVIVOR_FILL')
                        continue
                    stock_return = (p1 / p0 - 1) * 100
                    benchmark_return = (close_b[target] / close_b[signal_day] - 1) * 100
                    if (not math.isfinite(stock_return) or not math.isfinite(benchmark_return)
                            or abs(stock_return) > 35 or abs(benchmark_return) > 20):
                        errors.append('SPLIT_OR_EXTREME_RETURN_RECHECK')
                        continue
                    gains.append(stock_return)
                    alphas.append(stock_return - benchmark_return)
                complete = len(alphas) == len(cohort) and not errors
                rows.append({'signal_day': signal_day.isoformat(), 'matured_on': target.isoformat(),
                             'horizon': f'T+{h}', 'expected_stocks': len(cohort),
                             'observed_stocks': len(alphas),
                             'status': 'COMPLETE_RESEARCH_COHORT' if complete else 'QUARANTINED_PARTIAL_COHORT',
                             'mean_alpha_pp': round(sum(alphas)/len(alphas), 4) if complete else None,
                             'mean_close_change_pct': round(sum(gains)/len(gains), 4) if complete else None,
                             'reason': '|'.join(sorted(set(errors))) if not complete else ''})
        stats: dict[str, Any] = {}
        for h in HORIZONS:
            related = [r for r in rows if r['horizon'] == f'T+{h}']
            completed = [r for r in related if r['status'] == 'COMPLETE_RESEARCH_COHORT']
            newly = [r for r in completed if r['matured_on'] == asof.isoformat()]
            incomplete_today = [r for r in related if r['matured_on'] == asof.isoformat() and
                                r['status'] != 'COMPLETE_RESEARCH_COHORT']
            total_matured += len(newly)
            vals = [float(x['mean_alpha_pp']) for x in completed]
            # Fixed chronological blocks; no looking at winning subset.
            prev = vals[-2*MIN_DRIFT_BLOCK_DAYS:-MIN_DRIFT_BLOCK_DAYS]
            recent = vals[-MIN_DRIFT_BLOCK_DAYS:]
            coverage = len(completed)/len(related) if related else 0.0
            drift = 'INSUFFICIENT_INDEPENDENT_DAYS_OR_COVERAGE'
            if len(prev) == MIN_DRIFT_BLOCK_DAYS and len(recent) == MIN_DRIFT_BLOCK_DAYS and coverage >= 0.8:
                shift = sum(recent)/len(recent) - sum(prev)/len(prev)
                drift = ('ADVERSE_SHIFT_RESEARCH_REVIEW_ONLY' if shift <= -ADVERSE_SHIFT_PP and
                         sum(recent)/len(recent) < 0 else 'DESCRIPTIVE_NO_CONFIRMED_ADVERSE_SHIFT')
            stats[f'T+{h}'] = {
                'historical_matured_cohorts': len(related), 'full_cohort_days': len(completed),
                'partial_or_missing_days': len(related)-len(completed),
                'coverage_pct': round(coverage*100, 2),
                'mean_research_alpha_pp': round(sum(vals)/len(vals), 4) if vals else None,
                'newly_settled_research_days': len(newly),
                'newly_settled_mean_alpha_pp': round(sum(x['mean_alpha_pp'] for x in newly)/len(newly), 4) if newly else None,
                'newly_quarantined_days': len(incomplete_today),
                'drift_advice': drift,
                'allow_auto_weight_update': False,
            }
        report['strategies'][label] = {'horizons': stats,
                                      'recent_cohorts': rows[-15:],
                                      'all_complete_days_are_research_not_fills': True}
    report['status'] = 'RESEARCH_SETTLED_NO_NEW_MATURITY' if total_matured == 0 else 'RESEARCH_MATURED_OBSERVATIONS'
    report['reason'] = ('Close-to-close research observations only; H134 time-stamps not independent PIT '
                        'and corporate actions/transaction costs not fully attested.')
    report['h136_manifest_sha256'] = hashlib.sha256(_json_bytes(manifest)).hexdigest()
    return report


def read_remote_settlement(*, config: dict | None = None, transport: Any = requests,
                           asof: date | None = None) -> dict[str, Any]:
    """Read-only H139 GitHub receipt; reject missing/stale/noncanonical data."""
    if config is None:
        from godpick_persistence_service import github_config
        config = github_config()
    cfg = config or {}
    if not cfg.get('token') or not all(re.fullmatch(r'[\w.-]{1,100}', str(cfg.get(k, '')))
                                           for k in ('owner', 'repo', 'branch')):
        return {'status': 'NOT_CONFIGURED'}
    url = f"{API_ROOT}/{cfg['owner']}/{cfg['repo']}/contents/data/h139/H139_settlement.json"
    try:
        res = transport.get(url, params={'ref': cfg['branch']}, timeout=(5, 12),
                            allow_redirects=False,
                            headers={'Authorization': 'Bearer ' + cfg['token'],
                                     'Accept': 'application/vnd.github+json'})
        if res.status_code != 200:
            return {'status': 'REMOTE_UNAVAILABLE'}
        meta = res.json()
        if not isinstance(meta, dict) or meta.get('encoding') != 'base64':
            return {'status': 'REMOTE_INVALID_ENCODING'}
        data = base64.b64decode(meta.get('content', ''), validate=True)
        if len(data) > MAX_REMOTE:
            return {'status': 'REMOTE_TOO_LARGE'}
        obj = json.loads(data)
        if (not isinstance(obj, dict) or obj.get('schema') != SCHEMA
                or _json_bytes(obj) != data):
            return {'status': 'REMOTE_INVALID_RECEIPT'}
        day = date.fromisoformat(str(obj.get('asof_tw')))
        now = asof or datetime.now(TW).date()
        if day > now or (now-day).days > 5:
            return {'status': 'OUTDATED_RECEIPT', 'last_day': day.isoformat()}
        if obj.get('status') not in ('RESEARCH_MATURED_OBSERVATIONS', 'RESEARCH_SETTLED_NO_NEW_MATURITY',
                                     'BLOCKED_NO_VERIFIED_RESEARCH_DATA'):
            return {'status': 'INVALID_STATUS'}
        return obj
    except (ValueError, TypeError, KeyError, UnicodeError, binascii.Error,
            requests.RequestException, json.JSONDecodeError):
        return {'status': 'REMOTE_READ_FAILED'}


def read_remote_recovery(*, config: dict | None = None, transport: Any = requests,
                         asof: date | None = None) -> dict[str, Any]:
    """Read-only H139 one-shot recovery receipt; absence is not success."""
    if config is None:
        from godpick_persistence_service import github_config
        config = github_config()
    cfg = config or {}
    if not cfg.get('token') or not all(re.fullmatch(r'[\w.-]{1,100}', str(cfg.get(k, '')))
                                          for k in ('owner', 'repo', 'branch')):
        return {'status': 'NOT_CONFIGURED'}
    url = f"{API_ROOT}/{cfg['owner']}/{cfg['repo']}/contents/data/h139/H139_recovery.json"
    try:
        res = transport.get(url, params={'ref': cfg['branch']}, timeout=(5, 12),
                            allow_redirects=False,
                            headers={'Authorization': 'Bearer ' + cfg['token'],
                                     'Accept': 'application/vnd.github+json'})
        if res.status_code != 200:
            return {'status': 'REMOTE_UNAVAILABLE'}
        meta = res.json()
        if not isinstance(meta, dict) or meta.get('encoding') != 'base64':
            return {'status': 'REMOTE_INVALID_ENCODING'}
        data = base64.b64decode(meta.get('content', ''), validate=True)
        if len(data) > 20_000:
            return {'status': 'REMOTE_TOO_LARGE'}
        receipt = json.loads(data)
        if (not isinstance(receipt, dict) or receipt.get('schema') != 'H139-recovery-1'
                or _json_bytes(receipt) != data or
                receipt.get('status') not in ('ALREADY_READY_NO_RETRY',
                                               'RECOVERED_INTERNAL_SHA_EOD_CURRENT',
                                               'RETRY_FAILED_STILL_NOT_READY') or
                receipt.get('attempts') not in (0, 1)):
            return {'status': 'REMOTE_INVALID_RECEIPT'}
        day = date.fromisoformat(str(receipt.get('asof_tw')))
        now = asof or datetime.now(TW).date()
        if day > now or (now-day).days > 5:
            return {'status': 'OUTDATED_RECEIPT', 'last_day': day.isoformat()}
        return receipt
    except (ValueError, TypeError, KeyError, UnicodeError, binascii.Error,
            requests.RequestException, json.JSONDecodeError):
        return {'status': 'REMOTE_READ_FAILED'}
