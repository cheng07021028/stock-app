# -*- coding: utf-8 -*-
"""V191-H171 research-only PIT financial evidence & T+N performance.

Does not modify rankings, H109/H126 decisions, persistent recommendation records,
or previous modules. Uses FIRST_OBSERVED archived evidence: an official report
period is not automatically a historically available observation.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from math import isfinite
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo
import re

VERSION = 'V191-H171-PIT-TTM-BREAKOUT-FORWARD-RESEARCH'
TZ = ZoneInfo('Asia/Taipei')
FINANCIALS = ('eps_ytd', 'revenue_ytd', 'gross_profit_ytd', 'operating_profit_ytd', 'net_profit_ytd')
ALLOWED_PROVENANCE = {'FIRST_OBSERVED', 'PUBLICATION_VERIFIED'}


def _num(v: Any) -> float | None:
    if isinstance(v, bool) or v is None or str(v).strip() in {'', '--', 'nan', 'None'}:
        return None
    try:
        x = float(str(v).replace(',', '').strip())
        return x if isfinite(x) and abs(x) < 1e15 else None
    except (ValueError, OverflowError, TypeError):
        return None


def _day(v: Any) -> date | None:
    if isinstance(v, datetime):
        return v.astimezone(TZ).date() if v.tzinfo else None
    if isinstance(v, date):
        return v
    s = str(v or '').strip()
    try:
        if re.fullmatch(r'\d{7}', s):
            return date(int(s[:3]) + 1911, int(s[3:5]), int(s[5:7]))
        if re.fullmatch(r'\d{8}', s):
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _moment(v: Any) -> datetime | None:
    if not v:
        return None
    try:
        t = datetime.fromisoformat(str(v).replace('Z', '+00:00'))
        return t.astimezone(TZ) if t.tzinfo else None
    except (ValueError, TypeError, OverflowError):
        return None


def _period(v: Any) -> tuple[int, int] | None:
    m = re.fullmatch(r'(20\d{2})Q([1-4])', str(v or '').strip())
    return (int(m[1]), int(m[2])) if m else None


def _pnum(p: tuple[int, int]) -> int:
    return p[0] * 4 + p[1] - 1


def _pstr(n: int) -> str:
    return f'{n // 4}Q{n % 4 + 1}'


def _code(v: Any) -> str:
    s = str(v or '').strip().removesuffix('.0')
    return s if re.fullmatch(r'[1-9]\d{3}', s) else ''


def _evidence(row: Mapping, decision_day: date) -> bool:
    """Date-only research: first observed MUST precede research day."""
    observed = _moment(row.get('available_at'))
    report = _day(row.get('report_date'))
    provenance = str(row.get('availability_basis') or '')
    return bool(observed and report and observed.date() < decision_day
                and report <= observed.date() and provenance in ALLOWED_PROVENANCE)


SOURCE_URLS = {
    'TWSE': 'https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ci',
    'TPEx': 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_ci',
}


def normalize_official_ytd(raw_rows: Any, *, market: str, observed_at: str) -> list[dict]:
    """Parse official general-industry cumulative statements without inventing fields.

    Unknown financial line labels remain None. Historical PIT is never inferred
    from the report date; available_at is the actual collection timestamp.
    """
    observed = _moment(observed_at)
    if market not in SOURCE_URLS or observed is None or not isinstance(raw_rows, list):
        raise ValueError('official market, aware observation time and list are required')
    aliases = {
        'eps_ytd': ('基本每股盈餘（元）', '基本每股盈餘', 'BasicEPS'),
        'revenue_ytd': ('營業收入合計', '營業收入', '營業收入淨額'),
        'gross_profit_ytd': ('營業毛利（毛損）', '營業毛利(毛損)', '營業毛利'),
        'operating_profit_ytd': ('營業利益（損失）', '營業利益(損失)', '營業利益'),
        'net_profit_ytd': ('本期淨利（淨損）', '本期淨利(淨損)', '本期淨利', '本期稅後淨利'),
    }
    rows = []
    for item in raw_rows:
        if not isinstance(item, Mapping):
            continue
        code = _code(item.get('公司代號') or item.get('CompanyCode'))
        year = str(item.get('年度') or item.get('Year') or '').strip()
        quarter = str(item.get('季別') or item.get('Season') or item.get('Quarter') or '').strip().upper().removeprefix('Q')
        report_date = _day(item.get('出表日期') or item.get('Date'))
        if not code or not year.isdigit() or not quarter.isdigit() or not report_date:
            continue
        y, q = int(year), int(quarter)
        if y < 1911:
            y += 1911
        if not 2000 <= y <= 2100 or not 1 <= q <= 4 or report_date > observed.date():
            continue
        fields = {}
        for key, keys in aliases.items():
            fields[key] = next((_num(item[k]) for k in keys if k in item), None)
        if all(value is None for value in fields.values()):
            continue
        rows.append(dict(code=code, market='上市' if market == 'TWSE' else '上櫃',
                         year_quarter=f'{y}Q{q}', report_date=report_date.isoformat(),
                         available_at=observed.isoformat(), availability_basis='FIRST_OBSERVED',
                         source=market, source_url=SOURCE_URLS[market],
                         unit_eps='TWD_PER_SHARE', financial_unit='SOURCE_DISCLOSED_UNIT',
                         financial_line_labels=list(item.keys()), **fields))
    return rows


def _ledger(rows: Iterable[Mapping], *, code: str, market: str, study_day: str,
            needed: tuple[str, ...]) -> tuple[dict[int, dict], dict]:
    day = _day(study_day)
    if day is None or not code or not market:
        return {}, {'status': 'INVALID_RESEARCH_ID_OR_DATE'}
    matching: dict[int, list[dict]] = defaultdict(list)
    rejected = defaultdict(int)
    for row in rows:
        if not isinstance(row, Mapping) or _code(row.get('code')) != code or row.get('market') != market:
            continue
        p = _period(row.get('year_quarter'))
        if not p:
            rejected['INVALID_QUARTER'] += 1
            continue
        if str(row.get('source') or '') not in {'TWSE', 'TPEx'}:
            rejected['UNVERIFIED_SOURCE'] += 1
            continue
        if (market == '上市' and row['source'] != 'TWSE') or (market == '上櫃' and row['source'] != 'TPEx'):
            rejected['SOURCE_MARKET_MISMATCH'] += 1
            continue
        if not _evidence(row, day):
            rejected['NOT_POINT_IN_TIME'] += 1
            continue
        if any(_num(row.get(k)) is None for k in needed):
            rejected['MISSING_FINANCIAL_FIELD'] += 1
            continue
        matching[_pnum(p)].append(dict(row))
    selected: dict[int, dict] = {}
    for period, candidates in matching.items():
        # A restated YTD series must not be silently mixed with another version.
        distinct = {tuple(round(_num(r.get(k)), 4) for k in needed) for r in candidates}
        if len(distinct) > 1:
            rejected['CONFLICTING_RESTATEMENTS'] += 1
            continue
        selected[period] = min(candidates, key=lambda r: _moment(r['available_at']))
    return selected, {'status': 'LEDGER_CHECKED', 'rejected': dict(rejected),
                      'usable_quarters': len(selected)}


def _quarter_amount(series: Mapping[int, Mapping], n: int, field: str) -> float | None:
    curr = series.get(n)
    if curr is None:
        return None
    value = _num(curr.get(field))
    if value is None:
        return None
    if n % 4 == 0:
        return value  # Q1
    prev = series.get(n - 1)
    prior = _num(prev.get(field)) if prev is not None else None
    return None if prior is None else value - prior


def ttm_eps(rows: Iterable[Mapping], *, code: str, market: str, study_day: str) -> dict:
    """Four sequential single-quarter EPS derived from official YTD EPS.

    The reconstructed figure is RESEARCH ONLY: per-share amounts can be affected
    by share counts, stock splits and reported restatements. Not H115 authority.
    """
    code = _code(code)
    ledger, meta = _ledger(rows, code=code, market=market, study_day=study_day,
                           needed=('eps_ytd',))
    out = {'code': code, 'market': market, 'study_day': study_day,
           'eps_ttm': None, 'status': 'INSUFFICIENT_PIT_QUARTER_HISTORY',
           'used_periods': [], 'trade_authority_changed': False,
           'source_status': meta, 'basis': 'FOUR_QUARTERS_DERIVED_FROM_OFFICIAL_YTD_RESEARCH_ONLY'}
    if not ledger:
        return out
    latest = max(ledger)
    periods = list(range(latest - 3, latest + 1))
    amounts = [_quarter_amount(ledger, n, 'eps_ytd') for n in periods]
    out['used_periods'] = [_pstr(n) for n in periods]
    out['latest_period'] = _pstr(latest)
    if any(v is None for v in amounts):
        out['missing_ytd_periods'] = sorted({_pstr(p) for n in periods for p in
                                            ([n] if n % 4 == 0 else [n - 1, n]) if p not in ledger})
        return out
    out['eps_ttm'] = round(sum(amounts), 4)
    out['quarter_eps'] = { _pstr(n): round(v, 4) for n, v in zip(periods, amounts) }
    out['status'] = 'RECONSTRUCTED_TTM_PIT_RESEARCH_ONLY'
    out['notice'] = '累計EPS差分重建；須另核實股本變動與追溯重編，不自動構成H115/H126買進授權。'
    return out


def profit_breakout(rows: Iterable[Mapping], *, code: str, market: str, study_day: str) -> dict:
    """Gross/operating margin five-quarter breakout AND net profit positive YoY.

    Works with cumulative revenue/gross/operating/net-profit fields from archived
    official statements. Requires all five single-quarter periods, PIT.
    """
    code = _code(code)
    names = ('revenue_ytd', 'gross_profit_ytd', 'operating_profit_ytd', 'net_profit_ytd')
    ledger, meta = _ledger(rows, code=code, market=market, study_day=study_day,
                           needed=names)
    out = {'code': code, 'market': market, 'study_day': study_day,
           'status': 'INSUFFICIENT_PIT_FIVE_QUARTER_HISTORY',
           'five_quarter_margin_breakout': False, 'profit_growth_yoy': False,
           'certified_for_research': False, 'trade_authority_changed': False,
           'source_status': meta}
    if not ledger:
        return out
    latest = max(ledger)
    periods = list(range(latest - 4, latest + 1))
    vals = {k: [_quarter_amount(ledger, n, k) for n in periods] for k in names}
    out['used_periods'] = [_pstr(n) for n in periods]
    if any(v is None for amounts in vals.values() for v in amounts):
        return out
    if any(x <= 0 for x in vals['revenue_ytd']):
        out['status'] = 'NONPOSITIVE_REVENUE_CANNOT_COMPARE_MARGINS'
        return out
    gross = [g / r for g, r in zip(vals['gross_profit_ytd'], vals['revenue_ytd'])]
    op = [g / r for g, r in zip(vals['operating_profit_ytd'], vals['revenue_ytd'])]
    net = vals['net_profit_ytd']
    out['gross_margin_last'] = round(gross[-1] * 100, 4)
    out['operating_margin_last'] = round(op[-1] * 100, 4)
    # Latest exceeds each prior quarter, not merely >=, with tolerance.
    out['five_quarter_margin_breakout'] = (gross[-1] > max(gross[:-1]) + 1e-9 or
                                            op[-1] > max(op[:-1]) + 1e-9)
    out['profit_growth_yoy'] = net[-1] > 0 and net[-1] > net[0]
    out['net_profit_yoy_change'] = round(net[-1] - net[0], 4)
    out['certified_for_research'] = out['five_quarter_margin_breakout'] and out['profit_growth_yoy']
    out['status'] = ('PROFIT_BREAKOUT_PIT_RESEARCH_CERTIFIED' if out['certified_for_research']
                     else 'NO_VERIFIED_PROFIT_BREAKOUT')
    return out


def forward_performance(signals: Iterable[Mapping], prices: Iterable[Mapping],
                        market_sessions: Iterable[str], *, evaluated_at: str,
                        horizons: tuple[int, ...] = (1, 5, 10)) -> dict:
    """T+N EXCHANGE sessions, not N available stock bars or calendar days.

    Evaluating a matured recommendation is not a trade signal and must never be
    fed into that historical recommendation's feature set.
    """
    cutoff = _moment(evaluated_at)
    if cutoff is None:
        raise ValueError('evaluated_at must have an explicit timezone')
    if not horizons or any(type(n) is not int or n < 1 or n > 252 for n in horizons):
        raise ValueError('invalid horizons')
    parsed = [_day(d) for d in market_sessions]
    if not parsed or any(d is None for d in parsed) or parsed != sorted(set(parsed)):
        raise ValueError('market_sessions must be unique ascending valid dates')
    sessions = {d: i for i, d in enumerate(parsed)}
    quotes: dict[tuple[str, date], list[tuple[float, datetime]]] = defaultdict(list)
    for p in prices:
        if not isinstance(p, Mapping):
            continue
        code = _code(p.get('code'))
        d = _day(p.get('trade_date'))
        px = _num(p.get('close'))
        observed = _moment(p.get('available_at'))
        if code and d and d in sessions and px is not None and px > 0 and observed and observed <= cutoff and observed.date() >= d:
            quotes[(code, d)].append((px, observed))
    clean_quotes: dict[tuple[str, date], float] = {}
    for key, hits in quotes.items():
        # Distinct values for same day require a source adjustment/reconciliation.
        if len({round(v[0], 5) for v in hits}) == 1:
            clean_quotes[key] = hits[0][0]
    results = []
    summary: dict[str, dict] = {}
    for signal in signals:
        if not isinstance(signal, Mapping):
            continue
        code = _code(signal.get('code'))
        sig_day = _day(signal.get('signal_date'))
        run_id = str(signal.get('run_id') or '')
        if not code or not sig_day or not run_id:
            results.append({'code': code, 'run_id': run_id, 'status': 'INVALID_SIGNAL', 'returns': {}})
            continue
        origin = sessions.get(sig_day)
        entry = clean_quotes.get((code, sig_day))
        values = {}
        for horizon in horizons:
            key = f'T+{horizon}'
            if origin is None:
                values[key] = {'status': 'SIGNAL_DAY_NOT_TRADING_SESSION', 'return_pct': None}
                continue
            if entry is None:
                values[key] = {'status': 'MISSING_OR_CONFLICTING_ENTRY_CLOSE', 'return_pct': None}
                continue
            if origin + horizon >= len(parsed):
                values[key] = {'status': 'NOT_MATURED', 'return_pct': None}
                continue
            target = parsed[origin + horizon]
            if target > cutoff.date():
                values[key] = {'status': 'NOT_MATURED', 'return_pct': None}
                continue
            exit_price = clean_quotes.get((code, target))
            if exit_price is None:
                values[key] = {'status': 'MISSING_OR_CONFLICTING_EXIT_CLOSE', 'return_pct': None,
                               'target_session': target.isoformat()}
                continue
            values[key] = {'status': 'EVALUATED_PRICE_ONLY', 'return_pct': round((exit_price / entry - 1) * 100, 4),
                           'target_session': target.isoformat()}
        results.append({'code': code, 'run_id': run_id, 'signal_date': sig_day.isoformat(),
                        'returns': values, 'trade_authority_changed': False})
    for horizon in horizons:
        key = f'T+{horizon}'
        vals = [x['returns'][key]['return_pct'] for x in results if key in x['returns']
                and x['returns'][key]['status'] == 'EVALUATED_PRICE_ONLY']
        summary[key] = {'evaluated': len(vals), 'win_rate_pct': round(100 * sum(v > 0 for v in vals) / len(vals), 2) if vals else None,
                        'mean_return_pct': round(sum(vals) / len(vals), 4) if vals else None,
                        'total_signals': len(results)}
    return {'version': VERSION, 'evaluated_at': cutoff.isoformat(), 'status': 'RESEARCH_RETROSPECTIVE_ONLY',
            'basis': 'CLOSE_TO_CLOSE_PRICE_RETURN_EXCLUDES_DIVIDENDS_FEES_TAXES',
            'horizons': list(horizons), 'signals': results, 'summary': summary,
            'trade_authority_changed': False}


__all__ = ['VERSION', 'normalize_official_ytd', 'ttm_eps', 'profit_breakout', 'forward_performance']
