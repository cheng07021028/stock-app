# -*- coding: utf-8 -*-
"""H138 safe manager brief from existing sealed research and H137 audit.

This is an operational/research *status* report, not generated trades or
performance. No network scraping, forecasts, model claims or brokerage access.
"""
from __future__ import annotations
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any
import base64
import binascii
import json
import re
import requests

from godpick_h134_decision_vault import _json_bytes, API_ROOT
from godpick_h136_auto_hub import collect_local_archives

TW = ZoneInfo('Asia/Taipei')
SCHEMA = 'H138-manager-brief-1'


def build_brief(*, root: Path, asof: date, audit: dict[str, Any]) -> dict[str, Any]:
    archives, report = collect_local_archives(root, asof=asof)
    today = [x for x in archives if x.get('research_day') == asof.isoformat() and
             x.get('source_type') in ('LIVE_SESSION_H126', 'SCHEDULED_PAGE07_H126')]
    distinct = {x.get('decision_fingerprint_sha256') for x in today}
    conflict = len(distinct) > 1
    current = today[0] if len(distinct) == 1 else None
    summary = current.get('summary', {}) if current else {}
    checks = audit.get('findings') if isinstance(audit.get('findings'), list) else ['H137_AUDIT_UNAVAILABLE']
    status = ('REVIEW_REQUIRED' if conflict or checks or not current or
              audit.get('status') != 'INTERNAL_CHECK_PASS_NOT_PIT'
              else 'READY_FOR_MANUAL_RESEARCH_NOT_TRADES')
    notes = list(checks[:35])
    if conflict: notes.append('MULTIPLE_H126_SNAPSHOTS_SAME_DAY')
    if not current: notes.append('TODAY_H126_ARCHIVE_NOT_CONFIRMED')
    return {
        'schema': SCHEMA,
        'asof_tw': asof.isoformat(),
        'prepared_at_tw': datetime.now(TW).isoformat(timespec='seconds'),
        'status': status,
        'official_market_verified_by': 'H137_INTERNAL_AUDIT_NOT_EXTERNAL_ATTESTATION',
        'decision_source': current.get('source_type') if current else '',
        'decision_sha256': current.get('decision_fingerprint_sha256') if current else '',
        'decision_summary': {str(k): int(v) for k,v in summary.items()
                             if isinstance(v, int) and not isinstance(v, bool)},
        'today_confirmed_archives': len(today),
        'archive_rejected': report.get('archive_rejected', 0),
        'findings': list(dict.fromkeys(notes)),
        'headline': ('當日研究封存與資料內部檢查可供主管檢閱；不等於准許買進' if status.startswith('READY')
                     else '決策封存或盤後資料未完成驗收；請先查核，不得視為今日推薦'),
        'returns_verification': 'NOT_COMPUTED_NOT_TRADE_PNL',
        'pit': 'NOT_INDEPENDENTLY_ATTESTED',
    }


def read_remote_brief(*, config: dict | None = None, transport: Any = requests,
                      asof: date | None = None) -> dict[str, Any]:
    if config is None:
        from godpick_persistence_service import github_config
        config = github_config()
    cfg = config or {}
    if (not cfg.get('token') or not all(re.fullmatch(r'[\w.-]{1,100}', str(cfg.get(k, '')))
            for k in ('owner', 'repo', 'branch'))):
        return {'status': 'NOT_CONFIGURED'}
    uri = (f"{API_ROOT}/{cfg['owner']}/{cfg['repo']}/contents/"
           'data/h138/H138_manager_brief.json')
    try:
        res = transport.get(uri, params={'ref': cfg['branch']}, timeout=(5, 12),
                            allow_redirects=False,
                            headers={'Authorization': 'Bearer ' + cfg['token'],
                                     'Accept': 'application/vnd.github+json'})
        if res.status_code != 200:
            return {'status': 'REMOTE_UNAVAILABLE'}
        meta = res.json()
        if not isinstance(meta, dict) or meta.get('encoding') != 'base64':
            return {'status': 'REMOTE_INVALID_ENCODING'}
        data = base64.b64decode(meta.get('content', ''), validate=True)
        if len(data) > 20000: return {'status': 'REMOTE_TOO_LARGE'}
        obj = json.loads(data)
        if not isinstance(obj, dict) or obj.get('schema') != SCHEMA:
            return {'status': 'REMOTE_INVALID_SCHEMA'}
        if _json_bytes(obj) != data:
            return {'status': 'REMOTE_NONCANONICAL'}
        day = date.fromisoformat(str(obj.get('asof_tw')))
        reference = asof or datetime.now(TW).date()
        if day > reference or (reference - day).days > 5:
            return {'status': 'OUTDATED_BRIEF', 'last_day': str(day)}
        if obj.get('status') not in ('REVIEW_REQUIRED', 'READY_FOR_MANUAL_RESEARCH_NOT_TRADES'):
            return {'status': 'INVALID_STATUS'}
        if not isinstance(obj.get('findings'), list) or len(obj['findings']) > 40:
            return {'status': 'INVALID_FINDINGS'}
        return obj
    except (ValueError, TypeError, KeyError, requests.RequestException, json.JSONDecodeError, binascii.Error):
        return {'status': 'REMOTE_READ_FAILED'}
