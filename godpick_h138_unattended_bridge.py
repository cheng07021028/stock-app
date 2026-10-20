# -*- coding: utf-8 -*-
"""H138 guarded bridge: existing Page07 scheduler -> current-run H126 -> H134.

No second stock-picking algorithm, no UI click simulation, no invented prices.
No archival unless all three H117 official EOD sessions, current H97 compact
scan authority and GitHub create-only readback have been independently checked
by their respective *internal* validators. SHA alone is not independent PIT.
"""
from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo
import gzip
import json
from typing import Any

import pandas as pd
from godpick_h117_official_market_closure import MARKET_URLS, MIN_COUNTS, verify_manifest
from godpick_h134_decision_vault import build_snapshot, archive_once
from godpick_h126_authority_report import attach_authority

VERSION = 'V191-H138.0'
TW = ZoneInfo('Asia/Taipei')


def _state(code: str, **kw: Any) -> dict[str, Any]:
    return {'status': code, 'confirmed': False, 'trading_authority': False,
            'pit_independently_attested': False, **kw}


def verified_eod(root: Path, day: date, *, now: datetime) -> dict[str, Any]:
    """Fail closed on missing EOD; weekdays alone cannot prove trading."""
    if now.tzinfo is None or now.astimezone(TW).date() != day:
        return _state('REJECTED_CLOCK_OR_DATE')
    if day.weekday() >= 5:
        return _state('CLOSED_WEEKEND')
    if now.astimezone(TW).time() < time(15, 0):
        return _state('WAIT_FOR_MARKET_CLOSE')
    base = Path(root) / 'data' / 'h117' / 'daily'
    for source, expected_url in MARKET_URLS.items():
        path = base / source / (day.isoformat() + '.json.gz')
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 20_000_000:
                return _state('EOD_NOT_VERIFIED', missing=source)
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                item = json.load(f)
            observed = datetime.fromisoformat(str(item['collected_at']).replace('Z', '+00:00'))
            if (observed.tzinfo is None or observed.astimezone(TW).date() != day
                    or observed.astimezone(TW).time() < time(15, 0)):
                return _state('EOD_COLLECTION_TIMESTAMP_INVALID', missing=source)
            if (not verify_manifest(item) or item.get('source') != source
                    or item.get('source_url') != expected_url or item.get('date') != day.isoformat()
                    or not isinstance(item.get('rows'), list)
                    or len(item['rows']) < MIN_COUNTS[source]
                    or int(item.get('row_count', -1)) != len(item['rows'])):
                return _state('EOD_PROVENANCE_INVALID', missing=source)
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError, EOFError, UnicodeError):
            return _state('EOD_READ_FAILED', missing=source)
    return _state('EOD_THREE_MARKETS_INTERNAL_HASH_VERIFIED',
                  market_date=day.isoformat())


def seal_scheduled_decision(result: dict, namespace: dict[str, Any], *,
                            now: datetime | None = None, root: Path | None = None,
                            config: dict | None = None, transport: Any = None,
                            eod_checker: Any = verified_eod) -> dict[str, Any]:
    """Seal only the *same scheduled execution*, never a saved/previous scan.

    All closures and disk/remote clients may be injected for offline tests. A
    successful Page07 scan and a successful H134 seal remain separate truths.
    """
    clock = now or datetime.now(TW)
    if clock.tzinfo is None:
        return _state('INVALID_CLOCK')
    clock = clock.astimezone(TW)
    if not isinstance(result, dict) or not result.get('ok'):
        return _state('SCAN_FAILED_NO_SEAL')
    ctx = result.get('execution_context')
    if (not isinstance(ctx, dict) or ctx.get('owner') != '07_股神推薦'
            or ctx.get('trigger') != 'V191中央自動排程'
            or result.get('execution_owner') != 'pages/7_股神推薦.py'):
        return _state('NOT_CANONICAL_SCHEDULED_RUN')
    day = clock.date()
    if ctx.get('run_date') != day.isoformat():
        return _state('STALE_RUN_DATE')
    try:
        start = datetime.strptime(str(ctx.get('started_at')), '%Y-%m-%d %H:%M:%S').replace(tzinfo=TW)
    except (ValueError, TypeError):
        return _state('RUN_TIME_MISSING')
    elapsed = (clock - start).total_seconds()
    if elapsed < 0 or elapsed > 3600:
        return _state('RUN_NOT_CURRENT', elapsed_seconds=round(elapsed))
    checker = eod_checker(Path(root or Path(__file__).resolve().parent), day, now=clock)
    if checker.get('status') != 'EOD_THREE_MARKETS_INTERNAL_HASH_VERIFIED':
        return _state('MARKET_DAY_UNVERIFIED', eod_status=checker.get('status', 'UNKNOWN'))
    try:
        st = namespace['__headless_st__']
        key = namespace['_k']
        session = st.session_state
        same = session.get(key('recommend_execution_context_v191'), {})
        if not isinstance(same, dict) or same.get('run_id') != ctx.get('run_id') or not ctx.get('run_id'):
            return _state('SESSION_SCAN_ID_MISMATCH')
        candidate = session.get(key('candidate_diagnosis_store'))
        if not isinstance(candidate, pd.DataFrame) or candidate.empty:
            return _state('NO_CURRENT_CANDIDATE_EVIDENCE')
        compact_key = namespace['H85_H79_CORE_SESSION_KEY']
        raw = session.get(key(compact_key))
        raw = namespace['_h90_normalize_core_snapshot'](raw)
        validate = namespace.get('validate_h97_core_for_scan')
        if not raw or not callable(validate):
            return _state('H97_CURRENT_CORE_MISSING')
        valid, why = validate(raw, candidate, run_id=ctx['run_id'], require_audit=True)
        if not valid:
            return _state('H97_CURRENT_CORE_REJECTED', reason=str(why)[:160])
        tables = namespace['_h85_decode_h79_core_tables'](raw)
        if (not isinstance(tables, dict) or not tables or
                not any(isinstance(tables.get(k), pd.DataFrame) and not tables[k].empty
                        for k in ('audit', 'h112_audit_radar', 'waiting', 'research'))):
            return _state('CURRENT_H126_INPUT_MISSING')
        decorator = namespace.get('decorate_h94_decision_tables')
        if callable(decorator):
            tables = decorator(tables, candidate_df=candidate,
                               scan_report=session.get(key('scan_quality_report'), {}),
                               sector_df=session.get(key('category_strength_store')))
        authority = attach_authority(tables).get('h126_authority')
        if not isinstance(authority, pd.DataFrame) or authority.empty:
            return _state('H126_NO_AUTHORITY')
        bundle = build_snapshot(authority, research_day=day,
                                source='SCHEDULED_PAGE07_H126', captured_at=clock)
        freshness = bundle['payload']['freshness']
        if freshness != 'REPORTED_DAY_MATCHES_UNVERIFIED':
            return _state('H126_EVIDENCE_NOT_CURRENT', freshness=freshness)
        if transport is None:
            receipt = archive_once(bundle, config=config)
        else:
            receipt = archive_once(bundle, config=config, transport=transport)
        return _state('REMOTE_CONFIRMED' if receipt.get('confirmed') else 'REMOTE_UNCONFIRMED',
                      confirmed=bool(receipt.get('confirmed')), path=receipt.get('path', ''),
                      remote_status=receipt.get('status', ''), research_day=day.isoformat(),
                      research_count=int(bundle['payload']['summary'].get('研究股', 0)),
                      run_id=ctx['run_id'], freshness=freshness)
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        return _state('H138_GUARDED_BUILD_FAILED', reason=type(exc).__name__)
