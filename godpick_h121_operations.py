# -*- coding: utf-8 -*-
"""H121 independent operations audit; read-only validation of H116--H120 evidence.

Do not infer a holiday, genuine remote persistence, certified entitlements, or
strategy advantage from a local checkout.  No changes to trading decisions.
"""
from __future__ import annotations

from datetime import datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
import hashlib
import json
import re

VERSION = 'V191-H121.1-OPERATIONS-PROVENANCE'
TZ = ZoneInfo('Asia/Taipei')
SOURCES = ('TWSE', 'TPEX', 'TAIEX')


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _last(items: list[str]) -> str | None:
    return max(items) if items else None


def _append(issues: list[dict[str, str]], code: str, message: str, severity: str = 'PENDING') -> None:
    issues.append({'code': code, 'severity': severity, 'message': message})


def inspect_repository(root: Path, *, now: datetime | None = None) -> dict[str, Any]:
    """Stable byte-provenance report; no live HTTP/clock value in stable digest.

    A warning is not a signal to reduce quality gates.  Invalid hashed artifacts
    fail closed. The state alone NEVER certifies actual GitHub remote storage.
    """
    from godpick_h117_official_market_closure import load_official_days, MIN_COUNTS, verify_manifest
    from godpick_h116_daily_feedback import load_snapshot_files

    root = Path(root)
    now = now or datetime.now(TZ)
    now = now.astimezone(TZ) if now.tzinfo else now.replace(tzinfo=TZ)
    issues: list[dict[str, str]] = []
    stats: dict[str, Any] = {}
    dayroot = root / 'data/h117/daily'
    official = {s: {} for s in SOURCES}
    try:
        official = load_official_days(dayroot)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        _append(issues, 'INVALID_OFFICIAL_ARCHIVE', f'官方行情封存無法驗證：{type(exc).__name__}', 'BLOCKED')
    days = {src: sorted(official[src]) for src in SOURCES}
    latest = {src: _last(days[src]) for src in SOURCES}
    stats['market_days_by_source'] = {src: len(days[src]) for src in SOURCES}
    stats['last_day_by_source'] = latest
    common = set(days[SOURCES[0]])
    for src in SOURCES[1:]:
        common.intersection_update(days[src])
    stats['complete_session_count'] = len(common)
    stats['last_complete_session'] = max(common) if common else None
    latest_union = set().union(*(set(days[s]) for s in SOURCES))
    incomplete = sorted(latest_union - common)
    stats['incomplete_session_count'] = len(incomplete)
    stats['incomplete_recent_dates'] = incomplete[-12:]
    if not common:
        _append(issues, 'NO_THREE_SOURCE_DAY', '缺少上市、上櫃與大盤同日完整行情')
    if incomplete:
        _append(issues, 'PARTIAL_OFFICIAL_SESSIONS', f'有 {len(incomplete)} 個市場資料不完整日期')
    for src in SOURCES:
        for day, manifest in official[src].items():
            if not verify_manifest(manifest):
                _append(issues, 'INVALID_DAILY_HASH', f'{src}/{day} 封存雜湊不符', 'BLOCKED')
                break
            if int(manifest.get('row_count') or 0) != len(manifest.get('rows') or []):
                _append(issues, 'DAILY_ROW_COUNT_MISMATCH', f'{src}/{day} 行數與封存不符', 'BLOCKED')
                break
            if len(manifest.get('rows') or []) < MIN_COUNTS[src]:
                _append(issues, 'DAILY_TOO_FEW_ROWS', f'{src}/{day} 行數未達最低品質門檻', 'BLOCKED')
                break
    last_complete = stats['last_complete_session']
    if last_complete and last_complete > now.date().isoformat():
        _append(issues, 'FUTURE_MARKET_DATE', '封存交易日超過目前日期', 'BLOCKED')
    if now.weekday() < 5 and now.time() >= time(18, 15):
        if last_complete != now.date().isoformat():
            _append(issues, 'TODAY_OFFICIAL_EOD_NOT_VERIFIED',
                    '今天收盤資料尚未三市場一致；可能為休市／官方延遲，未經交易日曆驗證不得推定已完成')
    try:
        snaps = load_snapshot_files(root / 'data/h116/decisions')
        stats['decision_snapshot_count'] = len(snaps)
        stats['verified_evidence_rows'] = sum(int(s.get('verified_asof_rows') or 0) for s in snaps)
        stats['decision_rows'] = sum(len(s.get('decisions') or []) for s in snaps)
        if not snaps:
            _append(issues, 'NO_IMMUTABLE_DECISIONS', '缺少真實掃描快照，不能產出推薦歷史績效')
        elif stats['verified_evidence_rows'] == 0:
            _append(issues, 'NO_PIT_VERIFIED_DECISIONS', '快照存在但無可確認事前時間的候選')
    except (ValueError, TypeError, OSError, KeyError, json.JSONDecodeError) as exc:
        stats['decision_snapshot_count'] = None
        _append(issues, 'INVALID_DECISION_SNAPSHOT', f'推薦快照驗證失敗：{type(exc).__name__}', 'BLOCKED')

    manifest_path = root / 'reports/h116/H116_source_manifest.json'
    if not manifest_path.is_file():
        _append(issues, 'NO_H116_PERFORMANCE_MANIFEST', '尚未產生可追溯績效來源清單')
        stats['h116_manifest'] = 'MISSING'
    else:
        try:
            m = json.loads(manifest_path.read_text(encoding='utf-8'))
            sources = (
                ('price_input_sha256', root / 'data/h116/verified_ohlc.csv'),
                ('benchmark_input_sha256', root / 'data/h116/benchmark_ohlc.csv'),
                ('h120_details_csv_sha256', root / 'reports/h116/H116_T1_T3_T5_details.csv'),
            )
            evidence_hash_ok = True
            for key, path in sources:
                expected = m.get(key)
                if not isinstance(expected, str) or not re.fullmatch(r'[0-9a-f]{64}', expected) or not path.is_file() or _sha(path) != expected:
                    evidence_hash_ok = False
                    _append(issues, 'PERFORMANCE_SOURCE_HASH_MISMATCH', f'{key} 找不到相符的現存來源檔', 'BLOCKED')
            stats['h116_manifest'] = 'HASH_MATCH' if evidence_hash_ok else 'INVALID'
            stats['h118_quality_policy'] = m.get('h118_quality_policy', 'UNKNOWN')
            stats['h120_policy'] = m.get('h120_policy', 'UNKNOWN')
            stats['h120_proof_accepted_days'] = int(m.get('h120_proof_accepted_days') or 0)
            if m.get('h118_quality_policy') != 'STRICT' or m.get('h120_policy') != 'MANUAL_RAW_PROOF_ENFORCED':
                _append(issues, 'UNVERIFIED_PERFORMANCE_POLICY', '既有績效不是 H118+H120 嚴格證據政策')
            if not stats['h120_proof_accepted_days']:
                _append(issues, 'NO_H120_ATTESTED_DAYS', '尚無經人工對帳的企業行動證據，不能認證策略優勢')
            else:
                # A count in the H116 manifest is not a certification. Replay
                # the current manually-attested raw and catalog reconciliation,
                # and reject a stale source manifest if any proof changed.
                from godpick_h118_performance_authority import (read_csv, load_actions, load_coverage,
                                                                CSV_ACTION_COLUMNS, CSV_COVERAGE_COLUMNS)
                from godpick_h120_evidence_reconciliation import PROOF_COLUMNS, reconcile_coverage
                audit = reconcile_coverage(
                    raw_root=root / 'data/h119/raw',
                    proofs=read_csv(root / 'data/h120/reviewed/day_proofs.csv', PROOF_COLUMNS),
                    actions=load_actions(read_csv(root / 'data/h118/corporate_actions.csv', CSV_ACTION_COLUMNS)),
                    coverage=load_coverage(read_csv(root / 'data/h118/daily_coverage.csv', CSV_COVERAGE_COLUMNS)),
                )
                if (audit['accepted_days'] != stats['h120_proof_accepted_days'] or
                        audit['evidence_fingerprint_sha256'] != m.get('h120_raw_proof_fingerprint_sha256')):
                    _append(issues, 'H120_ATTESTATION_PROVENANCE_MISMATCH',
                            '績效來源引用的人工核對資料已改動或不符合當前原始封存', 'BLOCKED')
            refs = m.get('snapshot_runs')
            if not isinstance(refs, list):
                _append(issues, 'MISSING_SNAPSHOT_PROVENANCE', '績效來源缺少決策快照清單', 'BLOCKED')
            else:
                byrun = {s['run_id']: s['snapshot_sha256'] for s in snaps} if isinstance(stats['decision_snapshot_count'], int) else {}
                if any(not isinstance(item, list) or len(item) != 2 or byrun.get(item[0]) != item[1] for item in refs):
                    _append(issues, 'SNAPSHOT_PROVENANCE_MISMATCH', '績效引用的快照不存在或雜湊不符', 'BLOCKED')
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            _append(issues, 'INVALID_PERFORMANCE_MANIFEST', f'績效來源無法解析：{type(exc).__name__}', 'BLOCKED')
            stats['h116_manifest'] = 'INVALID'
    if any(x['severity'] == 'BLOCKED' for x in issues): status = 'BLOCKED_EVIDENCE_INVALID'
    elif issues: status = 'PENDING_VALIDATION'
    else: status = 'READY_FOR_HUMAN_REVIEW_NOT_STRATEGY_APPROVAL'
    body = {'version': VERSION, 'status': status, 'market': stats, 'issues': issues,
            'remote_commit_verified': False, 'auto_trade': False, 'auto_weight_change': False,
            'policy': 'RESEARCH_ONLY_FAIL_CLOSED;LOCAL_STATE_NOT_REMOTE_PROOF'}
    serialized = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')
    body['state_sha256'] = hashlib.sha256(serialized).hexdigest()
    return body
