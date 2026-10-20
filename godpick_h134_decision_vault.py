# -*- coding: utf-8 -*-
"""H134 manager decision vault: create-only GitHub archives and readback audit.

Only an independently verified external timestamp + source archives can prove full
point-in-time integrity. GitHub file creation/readback is tamper-evident, NOT
immutable storage or a trading permission. No persistence without explicit click.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any
from urllib.parse import quote
import base64
import hashlib
import json
import math
import re

import pandas as pd
import requests

VERSION = 'V191-H134.0'
REMOTE_DIRECTORY = 'godpick_h134_decision_vault'
MAX_ROWS = 250
MAX_BYTES = 360_000
MAX_RETRIEVE = 12
API_ROOT = 'https://api.github.com/repos'
SELECTED_COLUMNS = (
    '股票代號', '股票名稱', '市場別', 'H126順位', 'H126權威層別',
    'H126能否買進', 'H126認證資格', 'H126最後研究分',
    'H126策略來源', 'H126風險狀態', 'H126缺失與處置',
    'H126研究資訊截止日', 'H123法人實證狀態', 'H124大戶實證狀態',
    'H126證據來源',
)


def _plain(value: Any) -> str | int | float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    return str(value).strip()[:700]


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _valid_code(value: Any) -> str:
    value = str(value or '').strip()
    if value.endswith('.0') and value[:-2].isdigit():
        value = value[:-2]
    return value if re.fullmatch(r'\d{4}', value) else ''


def _day(value: Any) -> date | None:
    try:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        return date.fromisoformat(str(value).strip()[:10])
    except (ValueError, TypeError):
        return None


def build_snapshot(frame: pd.DataFrame, *, research_day: date, source: str,
                   captured_at: datetime | None = None) -> dict[str, Any]:
    """Make bounded deterministic content and untrusted-origin metadata.

    Fingerprint excludes captured_at: clicking twice cannot generate a different
    object for identical research decision and day. A second distinct decision
    produces a new archive, never silently replacing the first one.
    """
    if not isinstance(frame, pd.DataFrame) or not {'股票代號', 'H126權威層別'}.issubset(frame.columns):
        raise ValueError('NO_H126_AUTHORITY')
    if not isinstance(research_day, date) or isinstance(research_day, datetime):
        raise ValueError('INVALID_RESEARCH_DAY')
    if len(frame) > MAX_ROWS:
        raise ValueError('TOO_MANY_ROWS')
    if source not in ('LIVE_SESSION_H126', 'UPLOADED_H126_UNVERIFIED', 'SCHEDULED_PAGE07_H126'):
        raise ValueError('UNKNOWN_SOURCE')
    now = captured_at or datetime.now(ZoneInfo('Asia/Taipei'))
    if now.tzinfo is None:
        raise ValueError('CAPTURE_TIME_MISSING_TIMEZONE')
    now_tw = now.astimezone(ZoneInfo('Asia/Taipei'))
    if research_day > now_tw.date():
        raise ValueError('FUTURE_RESEARCH_DAY')
    if research_day < date(2020, 1, 1):
        raise ValueError('RESEARCH_DAY_TOO_OLD')
    rows: list[dict[str, Any]] = []
    codes: set[str] = set()
    for item in frame.to_dict('records'):
        code = _valid_code(item.get('股票代號'))
        if not code:
            if len(frame) == 1 and not str(item.get('股票代號') or '').strip():
                # H126's placeholder row for an empty research shortlist.
                continue
            raise ValueError('INVALID_STOCK_CODE')
        if code in codes:
            raise ValueError('DUPLICATED_STOCK_CODE')
        codes.add(code)
        row = {field: _plain(item.get(field)) for field in SELECTED_COLUMNS}
        row['股票代號'] = code
        layer = str(row.get('H126權威層別') or '')
        if not (layer.startswith(('A', 'B', 'C', 'Ａ', 'Ｂ', 'Ｃ', '禁止'))):
            raise ValueError('INVALID_H126_LAYER')
        rows.append(row)
    rows.sort(key=lambda r: r['股票代號'])
    dates = sorted({str(r['H126研究資訊截止日'])[:10] for r in rows if r.get('H126研究資訊截止日')})
    parsed_days = [_day(v) for v in dates]
    if any(v is None for v in parsed_days):
        freshness = 'MISSING_OR_INVALID_EVIDENCE_DATE'
    elif any(v > research_day for v in parsed_days):
        freshness = 'EVIDENCE_FROM_FUTURE_RECHECK'
    elif not dates:
        freshness = 'NO_EVIDENCE_DATE'
    elif max(parsed_days) < research_day:
        freshness = 'EVIDENCE_OLDER_THAN_RESEARCH_DAY'
    else:
        freshness = 'REPORTED_DAY_MATCHES_UNVERIFIED'
    summary = summarize(pd.DataFrame(rows))
    stable = {'schema': 'H134-1', 'research_day': research_day.isoformat(),
              'source_type': source, 'rows': rows, 'summary': summary, 'freshness': freshness}
    fingerprint = _sha(_json_bytes(stable))
    path = f'{REMOTE_DIRECTORY}/H134_{research_day:%Y%m%d}_{fingerprint[:32]}.json'
    pit_status = ('USER_BACKFILL_NOT_PIT' if source == 'UPLOADED_H126_UNVERIFIED'
                  else 'BACKDATED_SESSION_NOT_PIT' if research_day != now_tw.date()
                  else 'SCHEDULED_SCAN_NOT_INDEPENDENTLY_TIMESTAMPED' if source == 'SCHEDULED_PAGE07_H126'
                  else 'SAME_DAY_SESSION_NOT_INDEPENDENTLY_TIMESTAMPED')
    if research_day.weekday() >= 5:
        pit_status = 'NON_TRADING_DAY_NOT_PIT'
    payload = {**stable, 'decision_fingerprint_sha256': fingerprint,
               'captured_at_tw': now_tw.isoformat(timespec='seconds'),
               'pit_status': pit_status,
               'integrity_note': 'GitHub create-only and readback; admin can still modify/delete remote. Not independent PIT.'}
    if len(_json_bytes(payload)) > MAX_BYTES:
        raise ValueError('ARCHIVE_TOO_LARGE')
    return {'path': path, 'payload': payload, 'fingerprint': fingerprint,
            'payload_sha256': _sha(_json_bytes(payload))}


def summarize(frame: pd.DataFrame) -> dict[str, int]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return {'研究股': 0, 'A級研究': 0, 'B級觀察': 0, 'C級稽核': 0, '禁止新倉': 0,
                '法人待驗證': 0, 'TDCC待驗證': 0}
    def s(col: str) -> pd.Series:
        return frame[col].fillna('').astype(str) if col in frame else pd.Series([''] * len(frame))
    layers = s('H126權威層別')
    flows = s('H123法人實證狀態')
    tdcc = s('H124大戶實證狀態')
    return {'研究股': len(frame), 'A級研究': int(layers.str.startswith(('A', 'Ａ')).sum()),
            'B級觀察': int(layers.str.startswith(('B', 'Ｂ')).sum()),
            'C級稽核': int(layers.str.startswith(('C', 'Ｃ')).sum()),
            '禁止新倉': int(layers.str.startswith('禁止').sum()),
            '法人待驗證': int((flows != 'PROVEN_FLOW').sum()),
            'TDCC待驗證': int((tdcc != 'VERIFIED_RISING').sum())}


def _cfg(override: dict | None = None) -> dict[str, str]:
    if override is None:
        from godpick_persistence_service import github_config
        override = github_config()
    cfg = {k: str(override.get(k) or '') for k in ('owner', 'repo', 'branch', 'token')}
    if not cfg['token'] or not all(re.fullmatch(r'[\w.-]{1,100}', cfg[k]) for k in ('owner', 'repo', 'branch')):
        raise ValueError('GITHUB_CONFIG_MISSING_OR_INVALID')
    return cfg


def _url(cfg: dict[str, str], path: str) -> str:
    if not path.startswith(REMOTE_DIRECTORY + '/') or not re.fullmatch(r'[\w./-]+', path) or '..' in path:
        raise ValueError('UNSAFE_ARCHIVE_PATH')
    return f"{API_ROOT}/{quote(cfg['owner'])}/{quote(cfg['repo'])}/contents/{path}"


def _headers(token: str) -> dict[str, str]:
    return {'Authorization': 'Bearer ' + token,
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28'}


def _download(path: str, *, cfg: dict, transport: Any) -> tuple[dict | None, str]:
    try:
        res = transport.get(_url(cfg, path), headers=_headers(cfg['token']),
                            params={'ref': cfg['branch']}, timeout=(8, 20))
        if res.status_code == 404:
            return None, 'NOT_FOUND'
        if res.status_code != 200:
            return None, f'READ_HTTP_{res.status_code}'
        obj = res.json()
        if not isinstance(obj, dict) or obj.get('encoding') != 'base64' or not obj.get('content'):
            return None, 'INVALID_REMOTE_ENCODING'
        data = base64.b64decode(obj['content'], validate=False)
        if len(data) > MAX_BYTES:
            return None, 'REMOTE_TOO_LARGE'
        payload = json.loads(data)
        if not isinstance(payload, dict) or _json_bytes(payload) != data:
            return None, 'REMOTE_CANONICAL_MISMATCH'
        fingerprint = payload.get('decision_fingerprint_sha256')
        stable = {key: payload.get(key) for key in ('schema', 'research_day', 'source_type', 'rows', 'summary', 'freshness')}
        if fingerprint != _sha(_json_bytes(stable)):
            return None, 'REMOTE_FINGERPRINT_MISMATCH'
        if f'_{fingerprint[:32]}.json' not in path:
            return None, 'REMOTE_FILENAME_MISMATCH'
        return payload, 'READBACK_HASH_VERIFIED'
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, requests.RequestException, OverflowError):
        return None, 'REMOTE_READ_FAILED'


def archive_once(bundle: dict[str, Any], *, config: dict | None = None,
                 transport: Any = requests) -> dict[str, Any]:
    """GitHub create-only PUT; success only after remote readback hash verified.

    No SHA is EVER sent in PUT, hence existing path cannot be updated via this
    method. A new content version generates a different filename; old remains.
    """
    try:
        cfg = _cfg(config)
        path = bundle['path']
        value = bundle['payload']
        raw = _json_bytes(value)
        if len(raw) > MAX_BYTES or _sha(raw) != bundle['payload_sha256']:
            return {'status': 'REJECTED_BUNDLE', 'confirmed': False, 'path': path}
        existing, state = _download(path, cfg=cfg, transport=transport)
        if state == 'READBACK_HASH_VERIFIED':
            # Same decision content is idempotent even when a later click has
            # a newer local captured_at. NEVER rewrite the trusted first receipt.
            same = existing.get('decision_fingerprint_sha256') == bundle['fingerprint']
            return {'status': 'ALREADY_CONFIRMED' if same else 'ARCHIVE_CONFLICT',
                    'confirmed': same, 'path': path}
        if state != 'NOT_FOUND':
            return {'status': state, 'confirmed': False, 'path': path}
        body = {'message': f'H134 seal H126 decision {value["research_day"]} {bundle["fingerprint"][:12]}',
                'content': base64.b64encode(raw).decode('ascii'),
                'branch': cfg['branch']}
        response = transport.put(_url(cfg, path), headers=_headers(cfg['token']), json=body, timeout=(10, 35))
        if response.status_code not in (201, 409, 422):
            return {'status': f'WRITE_HTTP_{response.status_code}', 'confirmed': False, 'path': path}
        observed, read_status = _download(path, cfg=cfg, transport=transport)
        confirmed = (read_status == 'READBACK_HASH_VERIFIED' and
                     observed.get('decision_fingerprint_sha256') == bundle['fingerprint'])
        return {'status': 'REMOTE_WRITE_READBACK_CONFIRMED' if confirmed and response.status_code == 201
                else 'ALREADY_CONFIRMED' if confirmed else 'WRITE_UNCONFIRMED_' + read_status,
                'confirmed': confirmed, 'path': path}
    except (ValueError, KeyError, TypeError, requests.RequestException):
        return {'status': 'REMOTE_UNAVAILABLE_OR_CONFIG', 'confirmed': False, 'path': str(bundle.get('path', ''))}


def list_recent(*, config: dict | None = None, transport: Any = requests,
                limit: int = MAX_RETRIEVE) -> tuple[list[dict], str]:
    """List independently stored archives, checking each file fingerprint.

    Do not treat entries as PIT certified. GitHub archive commit time and branch
    protection must be independently audited for stronger claims.
    """
    try:
        cfg = _cfg(config)
        res = transport.get(_url(cfg, REMOTE_DIRECTORY + '/'), headers=_headers(cfg['token']),
                            params={'ref': cfg['branch']}, timeout=(8, 20))
        if res.status_code == 404:
            return [], 'REMOTE_DIRECTORY_EMPTY'
        if res.status_code != 200:
            return [], f'LIST_HTTP_{res.status_code}'
        entries = res.json()
        if not isinstance(entries, list):
            return [], 'LIST_FORMAT_INVALID'
        output: list[dict] = []
        matches = sorted((e for e in entries if isinstance(e, dict) and
                          re.fullmatch(r'H134_\d{8}_[a-f0-9]{32}\.json', str(e.get('name') or ''))
                          and e.get('type') == 'file'),
                         key=lambda e: e['name'], reverse=True)[:max(1, min(MAX_RETRIEVE, limit))]
        for entry in matches:
            path = f"{REMOTE_DIRECTORY}/{entry['name']}"
            payload, state = _download(path, cfg=cfg, transport=transport)
            output.append({'檔案': entry['name'], '封存路徑': path,
                           '遠端驗證': state, '研究日': payload.get('research_day') if payload else '',
                           '建立時間': payload.get('captured_at_tw') if payload else '',
                           '股票數': payload.get('summary', {}).get('研究股') if payload else None,
                           'PIT狀態': payload.get('pit_status') if payload else 'NOT_CONFIRMED',
                           '來源': payload.get('source_type') if payload else ''})
        return output, 'LIST_OK' if len(entries) < 1000 else 'LIST_TRUNCATION_RISK'
    except (ValueError, KeyError, TypeError, requests.RequestException):
        return [], 'REMOTE_LIST_FAILED'


def read_archive(path: str, *, config: dict | None = None,
                 transport: Any = requests) -> tuple[dict | None, str]:
    if not re.fullmatch(rf'{REMOTE_DIRECTORY}/H134_\d{{8}}_[0-9a-f]{{32}}\.json', str(path)):
        return None, 'INVALID_ARCHIVE_PATH'
    try:
        return _download(path, cfg=_cfg(config), transport=transport)
    except (ValueError, TypeError, requests.RequestException):
        return None, 'REMOTE_READ_FAILED'


def snapshot_signals(snapshots: list[dict]) -> pd.DataFrame:
    """Build an explicitly NON-PIT-VERIFIED historical research input for H133."""
    rows: list[dict[str, Any]] = []
    for payload in snapshots:
        if not isinstance(payload, dict) or payload.get('schema') != 'H134-1':
            continue
        day = _day(payload.get('research_day'))
        if day is None:
            continue
        for row in payload.get('rows', []):
            layer = str(row.get('H126權威層別') or '')
            if not layer.startswith(('A', 'B', 'Ａ', 'Ｂ')):
                continue
            code = _valid_code(row.get('股票代號'))
            if not code:
                continue
            rows.append({'股票代號': code, '訊號研究日': day.isoformat(),
                         'H126權威層別': layer,
                         'H126最後研究分': row.get('H126最後研究分'),
                         'H109決策權威': '',
                         'H123法人實證狀態': row.get('H123法人實證狀態'),
                         'H124大戶實證狀態': row.get('H124大戶實證狀態'),
                         '訊號產生時間': payload.get('captured_at_tw'),
                         'H134_PIT狀態': 'NOT_INDEPENDENTLY_VERIFIED',
                         'H134_封存SHA256': payload.get('decision_fingerprint_sha256')})
    return pd.DataFrame(rows)


def compare_decisions(current: pd.DataFrame, previous: pd.DataFrame) -> pd.DataFrame:
    """Explain research-layer transitions without interpreting them as trades."""
    columns = ['股票代號', '本期層別', '前期層別', '變化類型', '本期研究分', '前期研究分']
    def indexed(frame: pd.DataFrame) -> dict[str, dict]:
        if not isinstance(frame, pd.DataFrame) or '股票代號' not in frame or 'H126權威層別' not in frame:
            return {}
        out = {}
        for item in frame.to_dict('records'):
            code = _valid_code(item.get('股票代號'))
            if code:
                out[code] = item
        return out
    newer = indexed(current)
    older = indexed(previous)
    results = []
    for code in sorted(set(newer) | set(older)):
        a, b = newer.get(code, {}), older.get(code, {})
        a_layer, b_layer = str(a.get('H126權威層別') or ''), str(b.get('H126權威層別') or '')
        change = '新增研究' if not b else '移出研究' if not a else '層別變化' if a_layer != b_layer else '持續追蹤'
        results.append({'股票代號': code, '本期層別': a_layer, '前期層別': b_layer,
                        '變化類型': change, '本期研究分': _plain(a.get('H126最後研究分')),
                        '前期研究分': _plain(b.get('H126最後研究分'))})
    return pd.DataFrame(results, columns=columns)
