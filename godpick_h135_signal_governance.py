# -*- coding: utf-8 -*-
"""V191-H135 | H134 decision lifecycle and H133 research-policy oversight.

READ ONLY. Neither GitHub JSON SHA-256 nor a claimed timestamp proves independent PIT.
This code cannot alter H109/H126 trade authorization, weights, or order execution.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any
import hashlib
import math
import re

import pandas as pd

from godpick_h134_decision_vault import _json_bytes, _valid_code, REMOTE_DIRECTORY
from godpick_h133_blackhorse_validation import evaluate_blackhorse

VERSION = 'V191-H135.0'
MAX_ARCHIVES = 500
MAX_ROWS_PER_ARCHIVE = 250
MAX_TOTAL_ROWS = 50000
RISK_WORDS = ('FADE', 'SHOCK', 'QUARANTINE', 'VETO', 'BLOCK', '禁止', '硬否決')
TIERS = ('A', 'B', 'C', 'ISOLATED')
EMPTY_AUDIT = ['封存檔', '研究日', '狀態', '說明']
EMPTY_TRANSITIONS = ['研究日', '股票代號', '本期層別', '前期層別', '生命週期事件',
                     '連續封存出現次數', '前後封存間隔日', '封存來源', 'PIT獨立證明']


def _day(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


def _tier(layer: Any, risk: Any) -> str:
    text = str(layer or '').strip().upper()
    danger = str(risk or '').strip().upper()
    if any(word in danger for word in RISK_WORDS) or any(word in text for word in RISK_WORDS):
        return 'ISOLATED'
    if text.startswith(('A', 'Ａ')):
        return 'A'
    if text.startswith(('B', 'Ｂ')):
        return 'B'
    if text.startswith(('C', 'Ｃ')):
        return 'C'
    if text.startswith('隔離'):
        return 'ISOLATED'
    return 'UNKNOWN'


def _archive_integrity(payload: Any) -> tuple[date | None, str, dict[str, dict] | None]:
    """Checks *internal* integrity only; the provenance may still be self-asserted."""
    if not isinstance(payload, dict) or payload.get('schema') != 'H134-1':
        return None, 'NOT_H134_SCHEMA', None
    try:
        day = _day(payload.get('research_day'))
        if not day or day.weekday() >= 5 or day < date(2020, 1, 1):
            return day, 'INVALID_RESEARCH_SESSION', None
        stable = {k: payload.get(k) for k in ('schema', 'research_day', 'source_type', 'rows', 'summary', 'freshness')}
        expected = hashlib.sha256(_json_bytes(stable)).hexdigest()
        if payload.get('decision_fingerprint_sha256') != expected:
            return day, 'FINGERPRINT_MISMATCH', None
        rows = payload.get('rows')
        if not isinstance(rows, list) or len(rows) > MAX_ROWS_PER_ARCHIVE:
            return day, 'ROWS_INVALID_OR_TOO_LARGE', None
        capture = pd.Timestamp(payload.get('captured_at_tw'))
        if pd.isna(capture) or capture.tzinfo is None:
            return day, 'CAPTURE_TIME_UNTRUSTED', None
        capture = capture.tz_convert('Asia/Taipei')
        if capture.date() < day:
            return day, 'CAPTURE_BEFORE_RESEARCH_DAY', None
        # Backfilled archives can be displayed, but must never be promoted to PIT.
        typ = str(payload.get('source_type') or '')
        if typ not in ('LIVE_SESSION_H126', 'UPLOADED_H126_UNVERIFIED'):
            return day, 'INVALID_SOURCE_TYPE', None
        seen = {}
        for item in rows:
            if not isinstance(item, dict):
                return day, 'ROW_FORMAT_INVALID', None
            code = _valid_code(item.get('股票代號'))
            if not code or code in seen:
                return day, 'INVALID_OR_DUPLICATE_STOCK', None
            tier = _tier(item.get('H126權威層別'), item.get('H126風險狀態'))
            if tier == 'UNKNOWN':
                return day, 'UNKNOWN_RESEARCH_LAYER', None
            evidence_value = item.get('H126研究資訊截止日')
            evidence_day = _day(evidence_value)
            if evidence_value is not None and str(evidence_value).strip() and evidence_day is None:
                return day, 'INVALID_EVIDENCE_DATE', None
            if evidence_day is not None and evidence_day > day:
                return day, 'FUTURE_EVIDENCE_DAY', None
            seen[code] = {'tier': tier, 'row': item}
        return day, 'VALID_INTERNAL_HASH_NOT_INDEPENDENT_PIT', seen
    except (TypeError, ValueError, OverflowError, KeyError):
        return None, 'MALFORMED_ARCHIVE', None


def lifecycle_report(archives: list[dict], *, asof: date) -> dict[str, Any]:
    """Chronological changes, with conflicting days quarantined rather than last-wins.

    An exit means absent from next *archived* list; it is NOT a sell instruction.
    A run of archive observations is NOT a run of uninterrupted market sessions.
    """
    result = {'狀態': 'NO_ARCHIVES', '封存稽核': pd.DataFrame(columns=EMPTY_AUDIT),
              '逐日生命週期': pd.DataFrame(columns=EMPTY_TRANSITIONS),
              '有效封存數': 0, '摘要': pd.DataFrame(), '歷史研究訊號': pd.DataFrame()}
    if not isinstance(asof, date) or isinstance(asof, datetime):
        result['狀態'] = 'INVALID_ASOF'
        return result
    if not isinstance(archives, list) or len(archives) > MAX_ARCHIVES:
        result['狀態'] = 'ARCHIVE_LIMIT'
        return result
    audit = []
    by_day: dict[date, dict[str, tuple[dict, dict[str, dict]]]] = {}
    for idx, payload in enumerate(archives):
        day, state, rows = _archive_integrity(payload)
        file_name = f'input_{idx + 1}'
        if isinstance(payload, dict):
            file_name = str(payload.get('decision_fingerprint_sha256', file_name))[:32]
        if day and day > asof:
            state, rows = 'FUTURE_RESEARCH_DATE', None
        audit.append({'封存檔': file_name, '研究日': str(day or ''), '狀態': state,
                      '說明': '內容雜湊只驗檔案內一致性，未驗簽或獨立PIT' if rows is not None else '封存未列入研究'})
        if rows is None:
            continue
        fingerprint = payload['decision_fingerprint_sha256']
        by_day.setdefault(day, {})[fingerprint] = (payload, rows)
    dates = sorted(by_day)
    good = {}
    for day in dates:
        possible = by_day[day]
        if len(possible) != 1:
            audit.append({'封存檔': '*', '研究日': str(day), '狀態': 'SAME_DAY_CONFLICT_QUARANTINED',
                          '說明': '同一研究日有不同H126決策，當日全部隔離，不選擇事後較好版本'})
            continue
        good[day] = next(iter(possible.values()))
    if sum(len(x[1]) for x in good.values()) > MAX_TOTAL_ROWS:
        result['狀態'] = 'TOTAL_ROW_LIMIT'
        result['封存稽核'] = pd.DataFrame(audit, columns=EMPTY_AUDIT)
        return result
    prev: dict[str, dict] = {}
    last_day = None
    streaks: dict[str, int] = {}
    transitions, signals = [], []
    rank = {'ISOLATED': 0, 'C': 1, 'B': 2, 'A': 3}
    for day in sorted(good):
        payload, today = good[day]
        gap = (day - last_day).days if last_day else None
        for code in sorted(set(today) | set(prev)):
            current = today.get(code, {})
            previous = prev.get(code, {})
            current_tier = current.get('tier', 'ABSENT')
            previous_tier = previous.get('tier', 'ABSENT')
            if not previous:
                event = 'BASELINE_FIRST_ARCHIVE' if last_day is None else 'NEW_OR_REENTRY'
                streaks[code] = 1
            elif not current:
                event = 'EXITED_FROM_ARCHIVED_RESEARCH_NOT_SELL'
                streaks.pop(code, None)
            elif current_tier == previous_tier:
                event = 'HELD_SAME_RESEARCH_LAYER'
                streaks[code] = streaks.get(code, 0) + 1
            elif current_tier == 'ISOLATED':
                event = 'RISK_QUARANTINED'
                streaks[code] = streaks.get(code, 0) + 1
            elif previous_tier == 'ISOLATED':
                event = 'REMOVED_QUARANTINE_RECHECK_NOT_BUY'
                streaks[code] = streaks.get(code, 0) + 1
            elif rank[current_tier] > rank[previous_tier]:
                event = 'RESEARCH_LAYER_UP'
                streaks[code] = streaks.get(code, 0) + 1
            else:
                event = 'RESEARCH_LAYER_DOWN'
                streaks[code] = streaks.get(code, 0) + 1
            transitions.append({'研究日': day.isoformat(), '股票代號': code,
                                '本期層別': current_tier, '前期層別': previous_tier,
                                '生命週期事件': event, '連續封存出現次數': streaks.get(code, 0),
                                '前後封存間隔日': gap,
                                '封存來源': payload.get('source_type'), 'PIT獨立證明': 'NOT_VERIFIED'})
            if current and current_tier in ('A', 'B'):
                row = current['row']
                # H133 will also reject backfilled timestamps; no rewriting provenance.
                signals.append({'股票代號': code, '訊號研究日': day.isoformat(),
                                'H126權威層別': row.get('H126權威層別'),
                                'H126最後研究分': row.get('H126最後研究分'),
                                'H109決策權威': row.get('H126風險狀態', ''),
                                'H123法人實證狀態': row.get('H123法人實證狀態'),
                                'H124大戶實證狀態': row.get('H124大戶實證狀態'),
                                '訊號產生時間': payload.get('captured_at_tw'),
                                'PIT狀態': 'NOT_INDEPENDENTLY_VERIFIED',
                                '封存SHA256': payload['decision_fingerprint_sha256']})
        prev = today
        last_day = day
    result['封存稽核'] = pd.DataFrame(audit, columns=EMPTY_AUDIT)
    result['逐日生命週期'] = pd.DataFrame(transitions, columns=EMPTY_TRANSITIONS)
    result['歷史研究訊號'] = pd.DataFrame(signals)
    result['有效封存數'] = len(good)
    if not result['逐日生命週期'].empty:
        tab = result['逐日生命週期']
        result['摘要'] = tab.groupby('生命週期事件').size().reset_index(name='筆數')
    result['狀態'] = 'RESEARCH_ONLY_PIT_UNVERIFIED' if good else 'NO_USABLE_ARCHIVES'
    return result


def research_policy_review(h133: dict[str, Any], *,
                           strategy: str = 'H126_DAILY_TOP5',
                           coverage_threshold: float = 80.0,
                           min_independent_days: int = 20) -> pd.DataFrame:
    """Predeclared research governance; never auto-promote, auto-adjust or auto-trade."""
    if not 50 <= coverage_threshold <= 100 or not 10 <= min_independent_days <= 500:
        raise ValueError('INVALID_POLICY_THRESHOLD')
    rows = []
    if not isinstance(h133, dict):
        return pd.DataFrame([{'策略': strategy, '研究判定': 'INPUT_INVALID', '正式交易權限': 'UNCHANGED'}])
    perf = h133.get('樣本外績效')
    if not isinstance(perf, pd.DataFrame) or perf.empty:
        return pd.DataFrame([{'策略': strategy, '研究判定': 'MISSING_OOS_PERFORMANCE', '正式交易權限': 'UNCHANGED'}])
    for item in perf.to_dict('records'):
        horizon = str(item.get('期間', 'UNKNOWN'))
        days = pd.to_numeric(item.get('有效獨立交易日'), errors='coerce')
        cov = pd.to_numeric(item.get('涵蓋率%'), errors='coerce')
        low = pd.to_numeric(item.get('Bootstrap95%下界pp'), errors='coerce')
        high = pd.to_numeric(item.get('Bootstrap95%上界pp'), errors='coerce')
        mean = pd.to_numeric(item.get('日等權超額pp'), errors='coerce')
        if pd.isna(days) or pd.isna(cov) or int(days) < min_independent_days or cov < coverage_threshold:
            verdict = 'SAMPLE_OR_COVERAGE_INSUFFICIENT'
        elif pd.isna(low) or pd.isna(high) or pd.isna(mean):
            verdict = 'UNCERTAINTY_NOT_ESTIMABLE'
        elif high < 0:
            verdict = 'REVIEW_FOR_RESEARCH_SUSPENSION_NEGATIVE_OOS'
        elif low > 0:
            verdict = 'RESEARCH_POSITIVE_REQUIRES_INDEPENDENT_PIT'
        else:
            verdict = 'NO_RELIABLE_EDGE_DEMONSTRATED'
        if h133.get('狀態') != 'RETROSPECTIVE_RESEARCH_ONLY':
            verdict = 'DATA_OR_ADJUSTMENT_UNVERIFIED'
        rows.append({'策略': strategy, '期間': horizon,
                     '有效研究交易日': None if pd.isna(days) else int(days),
                     '樣本覆蓋率%': None if pd.isna(cov) else float(cov),
                     '等權超額pp': None if pd.isna(mean) else float(mean),
                     '95%下界pp': None if pd.isna(low) else float(low),
                     '95%上界pp': None if pd.isna(high) else float(high),
                     '研究判定': verdict, 'PIT獨立證明': 'NOT_VERIFIED',
                     '正式交易權限': 'UNCHANGED', '自動調權': 'DISABLED'})
    return pd.DataFrame(rows)


def fixed_challenger_comparison(signals: pd.DataFrame | None,
                                stocks: pd.DataFrame | None,
                                benchmark: pd.DataFrame | None, *, asof: date,
                                adjusted_confirmed: bool = False,
                                min_oos_days: int = 20) -> dict[str, Any]:
    """Predeclared Top3 vs Top5; no winner-picking from OOS data."""
    research = {}
    reviews = []
    for strategy, n in (('H126_TOP3_RESEARCH', 3), ('H126_TOP5_REFERENCE', 5)):
        report = evaluate_blackhorse(signals, stocks, benchmark, asof=asof,
                                     adjusted_confirmed=adjusted_confirmed,
                                     top_k=n, min_oos_days=min_oos_days)
        research[strategy] = report
        reviews.append(research_policy_review(report, strategy=strategy,
                                              min_independent_days=min_oos_days))
    comparison = pd.concat(reviews, ignore_index=True)
    return {'策略研究': research, '比較決策': comparison,
            '研究限制': ('Top3與Top5為預先固定研究政策；樣本外不能用來挑勝者後聲稱前瞻績效。'
                     '缺少獨立PIT、調整價／含息與成交成本核對，禁止自動晉升正式買進或調權。')}
