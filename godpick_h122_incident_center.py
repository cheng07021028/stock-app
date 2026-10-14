# -*- coding: utf-8 -*-
"""V191-H122 Operations incident governance; read-only, fail closed.

No securities decisions, portfolio allocation, undocumented APIs or inferred trading days.
"""
from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any, Mapping
import hashlib
import json
import re

VERSION = 'V191-H122.1-INCIDENT-CENTER'
NETWORK_TRANSIENT = re.compile(r'(?i)(?:HTTP\s*(?:429|500|502|503|504)\b|\b(?:connecttimeout|readtimeout|connectionerror|temporarily unavailable|timed?\s*out|connection reset)\b)')
NON_RETRYABLE = re.compile(r'(?i)(?:HTTP\s*(?:400|401|403|404|422)\b|IMMUTABLE|HASH|CONFLICT|INVALID|FUTURE|DATE_MISMATCH|ROW_COUNT|schema|格式|雜湊|交易日|矛盾|驗證失敗)')
VALID_TASKS = ('market_eod', 'corporate_raw')
STEP_VALUES = {'success', 'failure', 'skipped', 'cancelled', 'unknown'}


def retry_decision(task: str, *, exit_code: int, result: Mapping[str, Any] | None, output: str = '') -> dict[str, Any]:
    """Only retry immutable official collection on provably transient source failures.

    An exit code of 0 is insufficient for H119: partial capture must be treated
    as failure.  Unexpected/malformed output remains unverified, not retryable.
    """
    if task not in VALID_TASKS: raise ValueError('UNAUTHORIZED_RETRY_TASK')
    result = result if isinstance(result, Mapping) else {}
    status = str(result.get('status', ''))
    errors = result.get('errors', {})
    errors = errors if isinstance(errors, Mapping) else {}
    if task == 'corporate_raw':
        good = exit_code == 0 and status == 'RAW_CAPTURED_REVIEW_REQUIRED' and not errors and len(result.get('captures') or {}) == 2
    else:
        good = exit_code == 0 and status in ('COLLECTED_LOCALLY_GIT_COMMIT_STILL_REQUIRED', 'NO_NEW_MARKET_SESSION', 'UNCHANGED_VERIFIED')
        if status == 'NO_NEW_MARKET_SESSION':
            return {'ok': True, 'retry': False, 'status': 'NO_NEW_SESSION_UNVERIFIED_HOLIDAY', 'code': 'MARKET_NOT_PUBLISHED'}
    if good: return {'ok': True, 'retry': False, 'status': 'COLLECTED_OR_VERIFIED', 'code': 'OK'}
    # Do not retry from unstructured stdout or unverified schema.
    reason = ' | '.join(str(v)[:300] for v in errors.values())
    if not reason:
        reason = str(result.get('reason') or result.get('error') or '')[:300]
    if reason and NETWORK_TRANSIENT.search(reason) and not NON_RETRYABLE.search(reason):
        return {'ok': False, 'retry': True, 'status': 'TRANSIENT_RETRY_ALLOWED', 'code': 'SOURCE_TRANSIENT'}
    return {'ok': False, 'retry': False, 'status': 'MANUAL_REVIEW_REQUIRED', 'code': 'SOURCE_INCOMPLETE_OR_INVALID'}


def _sanitize_codes(codes: list[str]) -> list[str]:
    result = []
    for code in codes:
        code = re.sub('[^A-Z0-9_]', '_', str(code).upper())[:70]
        if code and code not in result: result.append(code)
    return result[:45]


def build_dashboard(health: Mapping[str, Any], receipt: Mapping[str, Any] | None = None,
                    remote: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Build deterministic safe dashboard. No dynamic timestamps, token, raw errors.

    Remote verification is only attributed to exactly matching H121 receipt
    and verified Git HEAD; never assume a checkout-local file is durable.
    """
    receipt = receipt if isinstance(receipt, Mapping) else {}
    remote = remote if isinstance(remote, Mapping) else {}
    market = health.get('market', {}) if isinstance(health.get('market'), Mapping) else {}
    raw_issues = health.get('issues', []) if isinstance(health.get('issues'), list) else []
    codes = _sanitize_codes([i.get('code', '') for i in raw_issues if isinstance(i, Mapping)])
    severity = 'BLOCKED' if health.get('status') == 'BLOCKED_EVIDENCE_INVALID' else ('PENDING' if codes else 'READY_FOR_REVIEW')
    steps = {key: str(receipt.get(key, 'unknown')).lower() for key in ('collect_step', 'corporate_step', 'review_step')}
    for key in steps:
        if steps[key] not in STEP_VALUES: steps[key] = 'unknown'
    if any(value in ('failure', 'cancelled') for value in steps.values()):
        severity = 'BLOCKED'; codes = _sanitize_codes(codes + ['NIGHTLY_PIPELINE_STEP_FAILED'])
    elif any(value in ('unknown', 'skipped') for value in steps.values()):
        if severity != 'BLOCKED': severity = 'PENDING'
        codes = _sanitize_codes(codes + ['NIGHTLY_PIPELINE_NOT_CONFIRMED'])
    last_day = market.get('last_complete_session')
    if not isinstance(last_day, str) or not re.fullmatch(r'20\d{2}-\d\d-\d\d', last_day): last_day = None
    partial = market.get('incomplete_session_count')
    last = market.get('last_day_by_source') if isinstance(market.get('last_day_by_source'), Mapping) else {}
    sources = {name: (last.get(name) if isinstance(last.get(name), str) else None) for name in ('TWSE','TPEX','TAIEX')}
    if partial is None: partial = 0
    remote_ok = bool(remote.get('remote_verified') is True and remote.get('status') == 'REMOTE_HEAD_MATCH')
    # H121 remote receipt is artifact-only and may be from a previous run.
    # No remote assertion unless run-specific matching commit can be proven.
    local_head = remote.get('local_head')
    remote_head = remote.get('remote_head')
    remote_proof = ('REMOTE_HEAD_MATCH_ONLY' if remote_ok and isinstance(local_head, str)
                    and re.fullmatch('[0-9a-f]{40}', local_head) and local_head == remote_head
                    else 'REMOTE_UNVERIFIED')
    if remote_proof != 'REMOTE_HEAD_MATCH_ONLY':
        codes = _sanitize_codes(codes + ['REMOTE_PERSISTENCE_NOT_PROVEN'])
        if severity == 'READY_FOR_REVIEW': severity = 'PENDING'
    snapshot_count = market.get('decision_snapshot_count')
    proof_days = market.get('h120_proof_accepted_days')
    fingerprint_input = {'severity': severity, 'codes': codes, 'last_complete': last_day,
                         'last_sources': sources, 'snapshot_count': snapshot_count,
                         'proof_days': proof_days, 'steps': steps, 'remote': remote_proof}
    digest = hashlib.sha256(json.dumps(fingerprint_input, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()
    return {'version':VERSION, 'status': severity, 'alert_codes': codes,
            'data_freshness': {'last_three_source_day':last_day, 'last_source_days':sources,
                               'partial_day_count':int(partial) if str(partial).isdigit() else None},
            'recommendation_snapshots': snapshot_count,
            'h120_attested_days': proof_days,
            'pipeline_steps': steps, 'persistence':remote_proof,
            'safe_retry_tasks':list(VALID_TASKS), 'auto_trading':False, 'auto_weights':False,
            'strategy_approved':False, 'fingerprint_sha256':digest}


def alert_transition(previous: Mapping[str, Any] | None, current: Mapping[str, Any]) -> str:
    """A new error/fingerprint change warrants notice; identical state is quiet."""
    previous = previous if isinstance(previous, Mapping) else {}
    old = str(previous.get('status', 'UNKNOWN'))
    new = str(current.get('status', 'UNKNOWN'))
    if new == 'BLOCKED' and (old != new or previous.get('fingerprint_sha256') != current.get('fingerprint_sha256')):
        return 'ALERT'
    if old == 'BLOCKED' and new != 'BLOCKED': return 'RECOVERY_REQUIRES_REVIEW'
    return 'NO_CHANGE'


def to_markdown(data: Mapping[str, Any]) -> str:
    lines = ['# H122 AI 股神｜營運健康戰情', '',
             f"**狀態：{data.get('status','UNKNOWN')}**　｜　**遠端證明：{data.get('persistence','REMOTE_UNVERIFIED')}**", '',
             '| 來源 | 最近封存交易日 |', '|---|---|']
    days = data.get('data_freshness', {}).get('last_source_days', {})
    for source in ('TWSE','TPEX','TAIEX'):
        lines.append(f'| {source} | {days.get(source) or "尚無"} |')
    lines += ['', f"- 三市場同日封存：{data.get('data_freshness',{}).get('last_three_source_day') or '尚無'}",
              f"- 真實推薦快照批次：{data.get('recommendation_snapshots')}",
              f"- H120 人工核對日數：{data.get('h120_attested_days')}",
              f"- 阻礙代碼：{', '.join(data.get('alert_codes', [])) or '無'}", '',
              '> 狀態不代表交易績效已獲認證；本頁沒有任何買進權限。', '']
    return '\n'.join(lines)


def to_html(data: Mapping[str, Any]) -> str:
    """Standalone static HTML artifact; escape all evidence, no JS, no external URLs."""
    status = escape(str(data.get('status', 'UNKNOWN')))
    source = data.get('data_freshness', {}).get('last_source_days', {})
    lines = ''.join('<tr><th>'+escape(s)+'</th><td>'+escape(str(source.get(s) or '尚無'))+'</td></tr>' for s in ('TWSE','TPEX','TAIEX'))
    codes = ', '.join(escape(str(x)) for x in data.get('alert_codes', [])) or '無'
    return ('<!doctype html><html lang="zh-Hant"><meta charset="utf-8"><title>H122 股神營運健康戰情</title>'
            '<style>body{font:16px system-ui;max-width:960px;margin:40px auto;padding:0 20px;background:#0b1426;color:#e6edf5}'
            'section{border:1px solid #3b4c67;border-radius:14px;padding:24px;margin:20px 0}table{width:100%;border-collapse:collapse}'
            'th,td{padding:12px;border-bottom:1px solid #344459;text-align:left}small{color:#b0c4dd}</style>'
            f'<h1>AI 股神 H122｜營運健康戰情</h1><section><h2>{status}</h2><p>遠端資料狀態：'
            f'{escape(str(data.get("persistence", "REMOTE_UNVERIFIED")))}</p>'
            f'<p>真實推薦快照：{escape(str(data.get("recommendation_snapshots")))}</p>'
            f'<p>H120 核對日數：{escape(str(data.get("h120_attested_days")))}</p></section>'
            f'<section><h2>官方來源</h2><table>{lines}</table></section>'
            f'<section><h2>待處理問題</h2><p>{codes}</p></section>'
            '<small>只讀稽核；未取得經驗證的行情或企業行動資料時，不會推定投資績效。</small></html>')
