# -*- coding: utf-8 -*-
"""H140: read-only, fail-closed operational reconciler for H136-H139.

Never asserts real GitHub Actions execution from source-code presence. A receipt
proves only that its bytes were present in this runtime-data checkout.
No recommendation, portfolio, credentials or model-weight mutation.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
import base64
import binascii
import json
import re

import requests
from godpick_h134_decision_vault import API_ROOT, _json_bytes

VERSION = 'V191-H140.0'
SCHEMA = 'H140-ops-monitor-1'
TW = ZoneInfo('Asia/Taipei')
MAX_RECEIPT = 100_000
STAGES = {
    'H136': ('data/h136/H136_health.json', 'H136-health-1', 'run_day_tw',
             ('UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY',)),
    'H137': ('data/h137/H137_audit.json', 'H137-audit-1', 'asof_tw',
             ('INTERNAL_CHECK_PASS_NOT_PIT',)),
    'H138': ('data/h138/H138_manager_brief.json', 'H138-manager-brief-1', 'asof_tw',
             ('READY_FOR_MANUAL_RESEARCH_NOT_TRADES',)),
    'H139_RECOVERY': ('data/h139/H139_recovery.json', 'H139-recovery-1', 'asof_tw',
                      ('ALREADY_READY_NO_RETRY', 'RECOVERED_INTERNAL_SHA_EOD_CURRENT')),
    'H139_SETTLEMENT': ('data/h139/H139_settlement.json', 'H139-research-settlement-1', 'asof_tw',
                        ('RESEARCH_MATURED_OBSERVATIONS', 'RESEARCH_SETTLED_NO_NEW_MATURITY')),
}
PLAYBOOK = {
    'H136': '手動執行 H136 workflow，檢查 H134 當日封存及 H117 個股/大盤盤後資料、runtime-data 寫入權限。',
    'H137': '執行 H137 workflow，查閱 runtime-data/data/h137/H137_audit.json 的 findings。',
    'H138': '先確認 H137 稽核與本期 H126 封存；再檢查 H138 晨報生成步驟。',
    'H139_RECOVERY': '查 H139 重建 receipt 的 remaining_blocker；核對 H136 cache 的日期與來源，勿強行覆寫舊資料。',
    'H139_SETTLEMENT': '查 H139 settlement 的 reason；需要 H136 當期完整資料及 H137 稽核通過。',
}


def _read(path: Path, expected_schema: str, date_key: str, asof: date) -> tuple[dict, str]:
    try:
        if path.is_symlink() or not path.is_file():
            return {}, 'MISSING'
        if path.stat().st_size < 3 or path.stat().st_size > MAX_RECEIPT:
            return {}, 'UNSAFE_SIZE'
        blob = path.read_bytes()
        item = json.loads(blob)
        if not isinstance(item, dict) or item.get('schema') != expected_schema:
            return {}, 'BAD_SCHEMA'
        if blob != _json_bytes(item):
            return {}, 'NONCANONICAL'
        written_day = date.fromisoformat(str(item[date_key]))
        if written_day > asof:
            return {}, 'FUTURE_DATE'
        if written_day < asof:
            return {'receipt_day': written_day.isoformat()}, 'STALE'
        return item, 'CURRENT'
    except (ValueError, TypeError, OSError, UnicodeError, KeyError, json.JSONDecodeError):
        return {}, 'INVALID'


def diagnose(*, runtime_root: Path, asof: date, market_closed: bool = False,
             workflow_run_id: str = '', source_ref: str = '') -> dict[str, Any]:
    """Read source status *without* modifying or rerunning jobs.

    market_closed is an explicit human override, NEVER proof of closure.
    There is no PASS on a closure override, only SKIPPED_UNVERIFIED.
    """
    stages: dict[str, dict] = {}
    findings: list[str] = []
    for name, (relpath, schema, date_key, good) in STAGES.items():
        item, state = _read(Path(runtime_root) / relpath, schema, date_key, asof)
        declared = item.get('status', '') if state == 'CURRENT' else ''
        # Any unsupported status is blocked, not treated as success.
        ok = state == 'CURRENT' and declared in good
        stages[name] = {'state': 'OK' if ok else state if state != 'CURRENT' else 'BLOCKED',
                        'reported_status': declared if state == 'CURRENT' else '',
                        'receipt_day': asof.isoformat() if state == 'CURRENT' else item.get('receipt_day',''),
                        'recommended_action': '' if ok else PLAYBOOK[name]}
        if not ok:
            findings.append(f'{name}_{state if state != "CURRENT" else declared or "UNKNOWN_STATUS"}')
    # A valid current H137 'pass' must never override outdated H136 freshness.
    # Likewise, a mature settlement may not elevate a blocked daily audit.
    if market_closed:
        status = 'MARKET_CLOSED_MANUAL_OVERRIDE_NOT_ATTESTED'
    else:
        status = 'INTERNAL_PIPELINE_READY_NOT_LIVE_ATTESTED' if not findings else 'BLOCKED_NEEDS_OPERATOR'
    summary = {
        'schema': SCHEMA, 'asof_tw': asof.isoformat(),
        'generated_at_tw': datetime.now(TW).isoformat(timespec='seconds'),
        'status': status, 'stages': stages, 'findings': findings,
        'pipeline_stages_ok': sum(x['state'] == 'OK' for x in stages.values()),
        'pipeline_stages_total': len(stages),
        'workflow_run_id': str(workflow_run_id)[:32] if re.fullmatch(r'\d{1,32}', str(workflow_run_id)) else '',
        'source_ref': str(source_ref)[:100] if re.fullmatch(r'[\w./-]{1,100}', str(source_ref)) else '',
        'recommendation_authority_unchanged': True,
        'pit_status': 'NOT_INDEPENDENTLY_ATTESTED',
        'trade_pnl_status': 'NOT_A_TRADE_LEDGER',
        'provenance': 'RECEIPTS_IN_LOCAL_RUNTIME_CHECKOUT_NOT_INDEPENDENT_CLOUD_RUN_ATTESTATION',
    }
    return summary


def read_remote_monitor(*, asof: date | None = None, config: dict | None = None,
                        transport: Any = requests) -> dict:
    """Strict read-only GitHub lookup; no cached stale-success fallback."""
    if config is None:
        from godpick_persistence_service import github_config
        config = github_config()
    cfg = config or {}
    if (not cfg.get('token') or not all(re.fullmatch(r'[\w.-]{1,100}', str(cfg.get(k, '')))
            for k in ('owner', 'repo', 'branch'))):
        return {'status': 'NOT_CONFIGURED'}
    uri = (f"{API_ROOT}/{cfg['owner']}/{cfg['repo']}/contents/"
           'data/h140/H140_monitor.json')
    try:
        response = transport.get(uri, params={'ref': cfg['branch']},
                                 headers={'Authorization': 'Bearer ' + cfg['token'],
                                          'Accept': 'application/vnd.github+json'},
                                 timeout=(5, 12), allow_redirects=False)
        if response.status_code != 200:
            return {'status': 'REMOTE_NOT_AVAILABLE'}
        payload = response.json()
        if not isinstance(payload, dict) or payload.get('encoding') != 'base64':
            return {'status': 'REMOTE_INVALID_ENCODING'}
        blob = base64.b64decode(payload.get('content', ''), validate=True)
        if len(blob) > MAX_RECEIPT:
            return {'status': 'REMOTE_OVERSIZED'}
        obj = json.loads(blob)
        if not isinstance(obj, dict) or obj.get('schema') != SCHEMA or blob != _json_bytes(obj):
            return {'status': 'REMOTE_INVALID_SCHEMA'}
        reference = asof or datetime.now(TW).date()
        dated = date.fromisoformat(str(obj.get('asof_tw')))
        if dated > reference or dated != reference:
            return {'status': 'OUTDATED_OR_FUTURE_MONITOR', 'last_day': dated.isoformat()}
        if obj.get('status') not in ('BLOCKED_NEEDS_OPERATOR','INTERNAL_PIPELINE_READY_NOT_LIVE_ATTESTED',
                                     'MARKET_CLOSED_MANUAL_OVERRIDE_NOT_ATTESTED'):
            return {'status': 'REMOTE_UNKNOWN_STATUS'}
        if not isinstance(obj.get('stages'),dict) or set(obj['stages']) != set(STAGES):
            return {'status': 'REMOTE_INVALID_STAGES'}
        return obj
    except (ValueError, TypeError, KeyError, OSError, UnicodeError, binascii.Error,
            requests.RequestException, json.JSONDecodeError):
        return {'status': 'REMOTE_READ_FAILED'}
