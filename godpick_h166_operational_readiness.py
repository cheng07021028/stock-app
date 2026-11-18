# -*- coding: utf-8 -*-
"""V191-H166 | conservative, side-effect-free production readiness scoreboard.

Separates job execution, same-day verified market context, H126 transaction
permission and actual out-of-sample trade performance. A heartbeat is never a
recommendation, and a research ranking is never a hit-rate estimate.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo

VERSION = 'V191-H166-OPERATIONAL-READINESS'
_TW = ZoneInfo('Asia/Taipei')
_REQUIRED = ('stock_master', 'macro_full', 'official_factors', 'super_ai_context', 'watchlist_runtime')


def _as_date(v: Any) -> date | None:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    text = str(v or '').strip().replace('/', '-')
    try:
        return date.fromisoformat(text[:10])
    except (ValueError, TypeError):
        return None


def expected_finished_session(now: datetime | None = None) -> date:
    """Conservative business-day reference, not a Taiwan exchange holiday calendar."""
    t = (now or datetime.now(_TW)).astimezone(_TW)
    d = t.date()
    if t.weekday() < 5 and t.hour < 17:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def context_evidence(context: Mapping[str, Any] | None, now: datetime | None = None) -> dict[str, Any]:
    """Market/TAIFEX freshness requires a consistent confirmed session.

    API 'verified' only establishes source parsing; the date still must match
    the latest expected complete Taiwan market session (weekdays-only proxy).
    """
    context = context if isinstance(context, Mapping) else {}
    margin = context.get('market_margin') or {}
    margin = margin if isinstance(margin, Mapping) else {}
    tai = context.get('taifex') or {}
    tai = tai if isinstance(tai, Mapping) else {}
    # Distinct official release windows: TAIFEX institutional/PCR feed is
    # generally published after the afternoon market; TWSE margin sometimes
    # settles later in the evening. Never demand unpublished margin data at 17h,
    # or silently treat yesterday's TAIFEX as current after the release window.
    t = (now or datetime.now(_TW)).astimezone(_TW)
    wanted = expected_finished_session(now)
    previous = t.date() - timedelta(days=1)
    while previous.weekday() >= 5:
        previous -= timedelta(days=1)
    margin_expected = t.date() if t.weekday() < 5 and (t.hour, t.minute) >= (21, 30) else previous
    taifex_expected = t.date() if t.weekday() < 5 and (t.hour, t.minute) >= (16, 30) else previous
    margin_day = _as_date(margin.get('TWSE融資券資料日期'))
    pcr_day = _as_date(tai.get('TAIFEX_PCR資料日期'))
    fut_day = _as_date(tai.get('TAIFEX期貨資料日期'))
    stocks = context.get('margin_by_stock') or {}
    stocks = stocks if isinstance(stocks, Mapping) else {}
    count = len(stocks)
    coverage = context.get('source_coverage') or {}
    coverage = coverage if isinstance(coverage, Mapping) else {}
    # Existing H158 snapshots predate explicit TPEx counters. Reconstruct a
    # conservative old-snapshot coverage estimate from each stock's market tag.
    tpex_count = int(coverage.get('TPEx融資券檔數') or 0) if coverage else sum(
        isinstance(v, Mapping) and str(v.get('市場別') or '') in {'上櫃', 'TPEx'}
        for v in stocks.values())
    try:
        from godpick_h158_taifex_openapi import complete as official_complete
        official = official_complete(dict(tai), now=now)
    except (ImportError, ValueError, TypeError):
        official = False
    good = bool(count >= 100 and tpex_count >= 30 and official and margin_day and pcr_day and fut_day
                and margin_day >= margin_expected
                and pcr_day >= taifex_expected and fut_day >= taifex_expected)
    missing = []
    if count < 100:
        missing.append(f'融資券個股樣本 {count}<100')
    if tpex_count < 30:
        missing.append(f'TPEx上櫃融資券覆蓋不足（{tpex_count}檔）；不可僅用TWSE宣稱全市場情境完成')
    if not official:
        missing.append('TAIFEX官方PCR／外資TX淨未平倉數值未完全核實')
    for label, d, required in [
        ('融資券', margin_day, margin_expected),
        ('PCR', pcr_day, taifex_expected),
        ('外資臺指期', fut_day, taifex_expected),
    ]:
        if d is None or d < required:
            missing.append(f'{label}資料日 {d.isoformat() if d else "未知"}，預期至少 {required.isoformat()}')
    return {
        'verified': good,
        'reference_day': wanted.isoformat(),
        'margin_expected_day': margin_expected.isoformat(),
        'taifex_expected_day': taifex_expected.isoformat(),
        'margin_day': margin_day.isoformat() if margin_day else '',
        'pcr_day': pcr_day.isoformat() if pcr_day else '',
        'futures_day': fut_day.isoformat() if fut_day else '',
        'stock_count': count,
        'tpex_count': tpex_count,
        'reasons': missing,
    }


def evaluate_operational_readiness(settings: Mapping[str, Any] | None,
                                   status: Mapping[str, Any] | None,
                                   context: Mapping[str, Any] | None = None,
                                   now: datetime | None = None,
                                   records_state: Mapping[str, Any] | None = None) -> dict[str, Any]:
    settings = settings if isinstance(settings, Mapping) else {}
    status = status if isinstance(status, Mapping) else {}
    records_state = records_state if isinstance(records_state, Mapping) else {}
    jobs = status.get('jobs') or {}
    jobs = jobs if isinstance(jobs, Mapping) else {}
    refer = expected_finished_session(now)
    evidence = context_evidence(context, now=now)
    failures = []
    advisories = []
    if not settings.get('enabled'):
        failures.append('中央總開關已停用；Worker 喚醒不會進行正式到期工作')
    # A current completed *success* is required for each essential upstream.
    missing_status = []
    for name in _REQUIRED:
        data = jobs.get(name) or {}
        data = data if isinstance(data, Mapping) else {}
        ok = (str(data.get('last_status') or '') == 'SUCCESS'
              and (_as_date(data.get('last_success_at')) or date.min) >= refer)
        if not ok:
            missing_status.append(name)
    if missing_status:
        failures.append('必要資料工作未具有足夠新的 SUCCESS：' + ', '.join(missing_status))
    if not evidence['verified']:
        failures.append('SuperAI來源／同日資料不完整：' + '；'.join(evidence['reasons'][:4]))
    rec = jobs.get('godpick_recommendation') or {}
    rec = rec if isinstance(rec, Mapping) else {}
    result = str(rec.get('last_status') or '')
    last_ok = _as_date(rec.get('last_success_at'))
    rec_fresh = result == 'SUCCESS' and bool(last_ok and last_ok >= refer)
    if not rec_fresh:
        failures.append(f'07推薦未達最近完成交易日成功：{result or "尚無結果"}｜最近成功：{last_ok or "無"}')
    # Pending means remote durability is not confirmed; never imply data loss.
    pending = records_state.get('github_pending') is True
    if pending:
        advisories.append('08歷史 GitHub 備份尚待遠端驗證，不能稱為已完成永久同步')
    return {
        'ready_for_current_research': not failures,
        'status': 'NOT_READY' if failures else 'READY_FOR_RESEARCH_ONLY',
        'reference_day': refer.isoformat(),
        'scheduler_enabled': bool(settings.get('enabled')),
        'super_ai': evidence,
        'recommendation_status': result or 'UNKNOWN',
        'last_recommendation_success': last_ok.isoformat() if last_ok else '',
        'blockers': failures,
        'advisories': advisories,
        'h126_trade_authority': 'NOT_EVALUATED_HERE',
        'out_of_sample_performance': 'NOT_VERIFIED',
        'notice': 'READY_FOR_RESEARCH_ONLY不等於H126允許買進；未經獨立樣本外績效測試，不得宣稱隔日命中率。',
    }


__all__ = ['VERSION', 'expected_finished_session', 'context_evidence', 'evaluate_operational_readiness']
