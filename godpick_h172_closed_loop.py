# -*- coding: utf-8 -*-
"""H172: read-only bridge from H116 signed decisions and H117 signed EOD to H171 T+N.

Only creates a separately versioned research report. It never writes/changes
`godpick_records.json`, H109/H126 decisions, historical scores or recommendations.
A price-only reference from market-day close is NOT an executable entry price.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, time
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo
import json
import re

from godpick_h116_daily_feedback import load_snapshot_files, verify_snapshot
from godpick_h117_official_market_closure import load_official_days, verify_manifest
from godpick_h171_pit_validation import forward_performance

VERSION = 'V191-H172-H116-H117-IMMUTABLE-TPLUS-RESEARCH'
TZ = ZoneInfo('Asia/Taipei')
SOURCES = ('TWSE', 'TPEX', 'TAIEX')


def _moment(value: Any) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(value).strip().replace('Z', '+00:00'))
        return dt.astimezone(TZ) if dt.tzinfo else None
    except (ValueError, TypeError, OverflowError):
        return None


def _digest(body: Mapping) -> str:
    b = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                   allow_nan=False).encode('utf-8')
    return sha256(b).hexdigest()


def build_research_report(snapshots: list[Mapping], daily: Mapping[str, Mapping], *,
                          evaluated_at: str) -> dict[str, Any]:
    """Reject incomplete/unverifiable provenance; never silently skip missing sessions.

    All three H117 sources must exist on each *observed* index trading day;
    a partial day remains in the session axis so the T+N index cannot compress.
    Historical holiday/absence coverage is NOT independently certified.
    """
    cutoff = _moment(evaluated_at)
    if cutoff is None:
        raise ValueError('evaluated_at must include timezone')
    if not isinstance(snapshots, list) or not isinstance(daily, Mapping):
        raise ValueError('invalid signed source containers')
    if any(s not in daily or not isinstance(daily[s], Mapping) for s in SOURCES):
        raise ValueError('all official TWSE/TPEX/TAIEX source containers required')
    if any(not verify_snapshot(s) for s in snapshots):
        raise ValueError('H116 immutable decision hash verification failed')
    checked: dict[tuple[str,str], Mapping] = {}
    for src in SOURCES:
        for day, m in daily[src].items():
            if not isinstance(m, Mapping) or not verify_manifest(m) or m.get('source') != src or m.get('date') != day:
                raise ValueError(f'H117 manifest integrity/source/date failed: {src} {day}')
            at = _moment(m.get('collected_at'))
            if at is None or at.date().isoformat() < day:
                raise ValueError(f'H117 first-observed time invalid: {src} {day}')
            if at <= cutoff:
                checked[(src,day)] = m
    # Retain partial index days in the axis. A missing stock market must NOT
    # cause T+5 to become T+4 or claim complete quote coverage.
    sessions = sorted(day for src,day in checked if src=='TAIEX')
    complete = [day for day in sessions if all((s,day) in checked for s in SOURCES)]
    incomplete = sorted(set(sessions)-set(complete))
    invalid = []
    prices = []
    for day in complete:
        codes: set[str] = set()
        for src in ('TWSE','TPEX'):
            manifest = checked[(src,day)]
            for bar in manifest.get('rows',[]):
                code = str(bar.get('code') or '') if isinstance(bar, Mapping) else ''
                if not re.fullmatch(r'[1-9]\d{3}',code):
                    continue
                if bar.get('date')!=day or bar.get('exchange')!=src:
                    raise ValueError(f'H117 daily bar not consistent with manifest: {src} {day}')
                if code in codes:
                    raise ValueError(f'cross-market duplicate code {code} on {day}')
                codes.add(code)
                prices.append({'code':code,'trade_date':day,'close':bar.get('close'),
                               'available_at':manifest['collected_at']})
    seen_runs = {}
    seen_events = {}
    eligible = []
    excluded = []
    for s in snapshots:
        rid = str(s.get('run_id') or '')
        old = seen_runs.get(rid)
        if old is not None and old != s['snapshot_sha256']:
            raise ValueError('same H116 run_id has conflicting immutable snapshots')
        if old is not None:
            continue
        seen_runs[rid] = s['snapshot_sha256']
        for e in s.get('decisions',[]):
            if not isinstance(e, Mapping):
                raise ValueError('invalid H116 decision row')
            eid = str(e.get('event_id') or '')
            if not re.fullmatch(r'[0-9a-f]{24}', eid):
                raise ValueError('invalid immutable decision event_id')
            if eid in seen_events:
                if seen_events[eid] != e:
                    raise ValueError('H116 duplicate event_id has conflicting contents')
                continue
            seen_events[eid] = e
            code = str(e.get('code') or '')
            day = str(e.get('market_day') or s.get('market_day') or '')
            decision = _moment(e.get('decided_at'))
            evidence = _moment(e.get('latest_evidence_at'))
            why = None
            if e.get('permission') != 'RESEARCH_ONLY_NO_TRADE':
                why = 'UNEXPECTED_TRADE_PERMISSION'
            elif not re.fullmatch(r'[1-9]\d{3}',code):
                why = 'NON_FOUR_DIGIT_STOCK_CODE'
            elif not decision or not evidence or evidence > decision or e.get('asof_status') != 'VERIFIED_PER_ROW':
                why = 'UNVERIFIED_ASOF_EVIDENCE'
            elif e.get('run_id') != rid:
                why = 'SOURCE_RUN_ID_MISMATCH'
            elif day > decision.date().isoformat() or not re.fullmatch(r'20\d{2}-\d{2}-\d{2}',day):
                why = 'INVALID_DECISION_MARKET_DAY'
            elif day not in sessions:
                why = 'NO_VERIFIED_MARKET_SESSION_FOR_REFERENCE'
            if why:
                excluded.append({'event_id':eid,'code':code,'run_id':rid,'status':why})
                continue
            eligible.append({'event_id':eid,'code':code,'run_id':rid,'signal_date':day,
                             'strategy':str(e.get('strategy') or '')[:80],
                             'tier':str(e.get('tier') or '')[:80],
                             'source_table':str(e.get('source_table') or '')[:80],
                             'decided_at':decision.isoformat(),
                             'latest_evidence_at':evidence.isoformat()})
    # H171 does not mutate signal records or use the result as historical ranking.
    calculated = forward_performance(
        [dict(code=e['code'],signal_date=e['signal_date'],run_id=e['event_id']) for e in eligible],
        prices, sessions, evaluated_at=cutoff.isoformat()) if sessions and eligible else None
    by_event = {row['run_id']:row for row in (calculated or {}).get('signals',[])}
    results = []
    for event in eligible:
        values = by_event[event['event_id']]['returns']
        results.append({**event,'returns':values,
                        'status':'PIT_RESEARCH_REFERENCE_ONLY',
                        'entry_basis':'MARKET_DAY_CLOSE_PRICE_ONLY_NOT_EXECUTABLE',
                        'trade_authority_changed':False})
    summary = (calculated or {}).get('summary') or {
        'T+'+str(n):{'evaluated':0,'win_rate_pct':None,'mean_return_pct':None,'total_signals':0}
        for n in (1,5,10)}
    by_strategy = {}
    for strategy in sorted({r['strategy'] for r in results}):
        group = [r for r in results if r['strategy']==strategy]
        by_strategy[strategy] = {'events':len(group)}
        for n in (1,5,10):
            key=f'T+{n}'
            wins=[x['returns'][key]['return_pct'] for x in group
                  if x['returns'][key]['status']=='EVALUATED_PRICE_ONLY']
            by_strategy[strategy][key] = {'evaluated':len(wins),'win_rate_pct':
                round(100*sum(v>0 for v in wins)/len(wins),2) if wins else None,
                'mean_return_pct':round(sum(wins)/len(wins),4) if wins else None,
                'sufficient_sample_for_review':len(wins)>=30}
    source_hashes = sorted(set(s['snapshot_sha256'] for s in snapshots))
    manifest_hashes = sorted(m['content_sha256'] for m in checked.values())
    report = {'version':VERSION,'as_of':cutoff.isoformat(),
        'status':'RESEARCH_ONLY_PARTIAL_CALENDAR_COVERAGE_NOT_TRADE_BACKTEST',
        'h116_snapshots':len(seen_runs),'h116_source_hashes':source_hashes,
        'h117_manifest_hashes':manifest_hashes,
        'official_index_observed_sessions':len(sessions),
        'fully_observed_market_sessions':len(complete),
        'partial_official_market_sessions':incomplete,
        'independent_holiday_calendar_verified':False,
        'eligible_research_events':len(results), 'excluded_events':excluded,
        'results':results,'summary':summary,'by_strategy':by_strategy,
        'legacy_godpick_records_modified':False,'trade_authority_changed':False,
        'caveat':'僅H116逐股PIT確認事件與H117三來源盤後快照。T+N為觀察交易日的收盤對收盤未調整漲跌，非可執行買點；除權息/手續費/稅負未納入。交易所完整假日曆未獨立證實，缺日不可宣稱正式完整回測；不回寫歷史排名及H126。'}
    report['report_sha256'] = _digest(report)
    return report


def verify_research_report(report: Mapping) -> bool:
    if not isinstance(report,Mapping) or not report.get('report_sha256'):
        return False
    payload = dict(report)
    checksum = payload.pop('report_sha256')
    return (report.get('version')==VERSION and report.get('trade_authority_changed') is False
            and report.get('legacy_godpick_records_modified') is False
            and _digest(payload)==checksum)


def load_verified_report(path: Path) -> dict | None:
    p = Path(path)
    if not p.is_file():
        return None
    if p.stat().st_size > 25_000_000:
        raise ValueError('H172 performance report too large')
    result = json.loads(p.read_text(encoding='utf-8-sig'))
    if not verify_research_report(result):
        raise ValueError('H172 report sha256 invalid or out of contract')
    return result


__all__=['VERSION','build_research_report','verify_research_report','load_verified_report']
