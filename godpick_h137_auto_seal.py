# -*- coding: utf-8 -*-
"""H137 - guarded event-driven H126 archiving on an actual manager report build.

This module does not schedule or create daily picks. Only an authenticated admin's
newly generated H126 manager report may trigger a create-only GitHub H134 archive.
A failed remote write is NEVER represented as a permanent save. Not PIT/WORM.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from godpick_h134_decision_vault import build_snapshot, archive_once

VERSION = 'V191-H137.0'
TW = ZoneInfo('Asia/Taipei')


def _result(status: str, **detail: Any) -> dict[str, Any]:
    return {'status': status, 'confirmed': False, 'mode': 'H137_EVENT_ARCHIVE_ONLY', **detail}


def seal_new_manager_report(authority: pd.DataFrame, *, role: str,
                            generated_at: datetime, now: datetime | None = None,
                            config: dict | None = None, transport: Any = requests) -> dict[str, Any]:
    """Attempt once per newly built manager report, NEVER a stale downloaded cache.

    `generated_at` is a timezone-aware local generation time, not an alleged
    original recommendation timestamp; its research_day is the local calendar
    day of generation. The H134 snapshot marks PIT as unverified.
    """
    if role != 'admin':
        return _result('SKIPPED_ADMIN_ONLY')
    if not isinstance(authority, pd.DataFrame) or authority.empty:
        return _result('SKIPPED_NO_H126_ROWS')
    if not {'股票代號', 'H126權威層別'}.issubset(authority.columns):
        return _result('REJECTED_H126_SCHEMA')
    actual_now = now or datetime.now(TW)
    if actual_now.tzinfo is None or generated_at.tzinfo is None:
        return _result('REJECTED_TIMEZONE_MISSING')
    current = actual_now.astimezone(TW)
    created = generated_at.astimezone(TW)
    if created.date() != current.date() or abs(current - created) > timedelta(minutes=5):
        return _result('SKIPPED_NOT_FRESH_REPORT', report_day=created.date().isoformat())
    if created.weekday() >= 5:
        return _result('SKIPPED_NON_TRADING_WEEKEND')
    # A report constructed today can use older evidence; label as such. Reject
    # future or malformed data instead of claiming a usable research timestamp.
    if 'H126研究資訊截止日' in authority:
        dates = authority['H126研究資訊截止日'].dropna().astype(str).str.strip()
        dates = dates[dates != '']
        for value in dates:
            try:
                observed = date.fromisoformat(value[:10])
            except ValueError:
                return _result('REJECTED_EVIDENCE_DATE_INVALID')
            if observed > created.date():
                return _result('REJECTED_FUTURE_EVIDENCE')
    try:
        bundle = build_snapshot(authority, research_day=created.date(),
                                source='LIVE_SESSION_H126', captured_at=created)
        receipt = archive_once(bundle, config=config, transport=transport)
        return {'mode': 'H137_EVENT_ARCHIVE_ONLY', 'confirmed': bool(receipt.get('confirmed')),
                'status': receipt.get('status', 'REMOTE_UNCONFIRMED'),
                'path': receipt.get('path', ''),
                'research_day': created.date().isoformat(),
                'freshness': bundle['payload']['freshness'],
                'PIT': 'NOT_INDEPENDENTLY_ATTESTED'}
    except (ValueError, TypeError, KeyError, requests.RequestException):
        return _result('ARCHIVE_NOT_CONFIRMED')


def read_remote_audit(*, config: dict | None = None, transport: Any = requests,
                      asof: date | None = None) -> dict[str, Any]:
    """Read *status* from remote runtime-data only; not a signed authority source."""
    import base64
    import json
    import re
    from godpick_h134_decision_vault import API_ROOT
    if config is None:
        from godpick_persistence_service import github_config
        config = github_config()
    if not isinstance(config, dict) or not config.get('token') or not all(
        re.fullmatch(r'[\w.-]{1,100}', str(config.get(key, ''))) for key in ('owner','repo','branch')
    ):
        return _result('H137_REMOTE_NOT_CONFIGURED')
    url = f"{API_ROOT}/{config['owner']}/{config['repo']}/contents/data/h137/H137_audit.json"
    headers = {'Authorization': 'Bearer ' + config['token'],
               'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'}
    try:
        res = transport.get(url, params={'ref': config['branch']}, headers=headers,
                            timeout=(5, 12), allow_redirects=False)
        if res.status_code != 200:
            return _result('H137_REMOTE_UNAVAILABLE')
        meta = res.json()
        if not isinstance(meta, dict) or meta.get('encoding') != 'base64':
            return _result('H137_REMOTE_ENCODING_INVALID')
        blob = base64.b64decode(meta.get('content') or '', validate=True)
        if len(blob) > 25000:
            return _result('H137_REMOTE_OVERSIZED')
        data = json.loads(blob)
        if not isinstance(data, dict) or data.get('schema') != 'H137-audit-1':
            return _result('H137_REMOTE_SCHEMA_INVALID')
        if data.get('status') not in ('BLOCKED_REQUIRES_ATTENTION', 'INTERNAL_CHECK_PASS_NOT_PIT'):
            return _result('H137_REMOTE_STATUS_INVALID')
        day = date.fromisoformat(str(data.get('asof_tw')))
        reference = asof or datetime.now(TW).date()
        if day > reference:
            return _result('H137_REMOTE_FUTURE_DATE')
        if (reference - day).days > 4:
            return _result('H137_REMOTE_STALE', last_day=day.isoformat())
        findings = data.get('findings', [])
        if not isinstance(findings, list) or len(findings) > 40 or not all(
            isinstance(v, str) and len(v) < 100 for v in findings):
            return _result('H137_REMOTE_FINDINGS_INVALID')
        return {'status': data['status'], 'asof_tw': data['asof_tw'],
                'same_day_live_h126_archives': int(data.get('same_day_live_h126_archives', 0)),
                'findings': findings, 'verification': 'REMOTE_CONTENT_ONLY_NOT_INDEPENDENT_SHA'}
    except (ValueError, TypeError, KeyError, requests.RequestException,
            json.JSONDecodeError, base64.binascii.Error):
        return _result('H137_REMOTE_READ_FAILED')
