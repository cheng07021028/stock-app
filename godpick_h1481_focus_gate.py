# -*- coding: utf-8 -*-
"""V191-H148.1: current-run, evidence-first strong-sector research board.

Only CURATES the existing H113/H148 evidence. Never creates H126 permission,
fetches quotes, writes history, re-runs scanning or claims whole-market coverage.
Quarantine is visible separately rather than filling the front 12 with stale or
FADE-vetoed stocks. Equal H110 scores remain ties, never fake ranks.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Mapping
import pandas as pd

from godpick_h147_five_gate_review import _code, _day, _n, _s, evaluate_five_gates
from godpick_h148_strength_provenance import _evidence_time

VERSION = 'V191-H148.1-VERIFIED-STRONG-SECTOR-BOARD'
EXTRA_COLS = ('H1481研究分層', 'H1481有效量價', 'H1481主流與籌碼', 'H1481入榜／隔離依據')
STRONG_KINDS = {'突破確認研究／非買點', '已發動健康續攻研究／防追高'}
SECTOR_SUPPORT = 'H108內部雙指標支持／尚未獨立驗證'
FLOW_SUPPORT = '五日來源雜湊齊／連買標記（原H123）'
HOLDER_SUPPORT = '兩週集保級距週增已核（非特定大戶鎖單）'
NO_VETO = {'RESEARCH_ELIGIBLE', 'ALLOW', 'PASS', 'NONE'}


def curate_focus_board(full_board: pd.DataFrame, audit: pd.DataFrame, trace: pd.DataFrame,
                       *, research_day: Any, limit: int = 12,
                       trace_meta: Mapping[str, Any] | None = None
                       ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Return (research-board, quarantined-audit, diagnostics).

    Any contradiction across sources, absence of same-day >=60 bars, amount,
    volume or sector breadth/flow proof makes a stock ineligible for the main
    *strong-sector research board*. Such stocks are not judged fundamentally bad.
    H148 diagnostics are internal, not a license to claim official sector data.
    """
    board = full_board if isinstance(full_board, pd.DataFrame) else pd.DataFrame()
    audit = audit if isinstance(audit, pd.DataFrame) else pd.DataFrame()
    trace = trace if isinstance(trace, pd.DataFrame) else pd.DataFrame()
    research_date = _day(research_day)
    raw_rows: dict[str, list[dict]] = {}
    for row in audit.to_dict('records'):
        code = _code(row.get('股票代號'))
        if code:
            raw_rows.setdefault(code, []).append(row)
    trail: dict[str, list[dict]] = {}
    for row in trace.to_dict('records'):
        code = _code(row.get('股票代號'))
        if code:
            trail.setdefault(code, []).append(row)
    known_board = board.drop_duplicates(subset=['股票代號'], keep='first') if '股票代號' in board else pd.DataFrame()
    rank_rows, blocked_rows = [], []
    reasons = Counter()
    available_h148 = bool(len(trail))
    for rec in known_board.to_dict('records'):
        code = _code(rec.get('股票代號'))
        if not code: continue
        raw_list = raw_rows.get(code, [])
        trace_list = trail.get(code, [])
        why = []
        if len(raw_list) != 1 or len(trace_list) != 1:
            why.append('同代號來源缺失或重複')
        raw = raw_list[0] if len(raw_list) == 1 else {}
        evidence = trace_list[0] if len(trace_list) == 1 else {}
        if _s(rec.get('排序可信度')) == '同代號資料衝突':
            why.append('H110／H109資料衝突')
        if '矛盾' in _s(rec.get('H126正式資格')):
            why.append('H126同代號決策衝突／需回權威來源查核')
        cutoff = research_date
        evidence_status = _evidence_time(raw, cutoff)
        if evidence_status != '同日完整OHLCV（內部標記）':
            why.append('缺同研究交易日有效K線：' + evidence_status)
        if _s(raw.get('H113流動性合格')) != '是':
            why.append('H113既有流動性條件未通過或未提供')
        gates = evaluate_five_gates(raw, research_day=cutoff)
        if gates['成交量能'] != '量能支持':
            why.append('未驗證當日成交額／20日均額／500張及放量1.2倍')
        # H148 read-only breakout/continuation requires validated, synchronized
        # H114 same-day evidence and no unknown/future dates.
        kind = _s(evidence.get('H148研究分類'))
        if kind not in STRONG_KINDS:
            why.append('非已核實突破或健康續攻')
        if _s(evidence.get('H148量價狀態')) != '同日完整OHLCV（內部標記）':
            why.append('H148跨池量價來源未通過')
        if _s(evidence.get('H148風控')) not in NO_VETO or _s(rec.get('H109風控')) not in NO_VETO:
            why.append('H109風控否決或不同來源不一致')
        sector = _s(evidence.get('H148族群證據'))
        if sector != SECTOR_SUPPORT:
            why.append('未核實H108同輪跨期族群廣度與資金流支持')
        if not available_h148:
            why.append('缺H148同輪稽核結果')
        flow = _s(evidence.get('H148法人證據')) == FLOW_SUPPORT
        tdcc = _s(evidence.get('H148大戶週證')) == HOLDER_SUPPORT
        fresh = bool(research_date and _day(raw.get('H114K線證據日')) == research_date)
        # Extra conservative consistency check: don't infer strong trend solely
        # from a stage label if MA20/60 technical evidence is unavailable.
        gap20 = _n(raw, '收盤距MA20%', 'MA20乖離%')
        gap60 = _n(raw, '收盤距MA60%', 'MA60乖離%')
        if gap20 is None or gap60 is None or not (-6 <= gap20 <= 12 and gap60 >= 0):
            why.append('均線結構未證或偏離風險')
        rec = dict(rec)
        rec['H1481有效量價'] = ('同日K線60根以上／150+150百萬／500張／量比≥1.2' if fresh and
                              gates['成交量能'] == '量能支持' and evidence_status == '同日完整OHLCV（內部標記）'
                              else '量價未取得完整同日認證')
        rec['H1481主流與籌碼'] = ('H108內部跨期雙指標支持' if sector == SECTOR_SUPPORT else 'H108來源尚缺') + (
            '｜法人五日已核' if flow else '｜法人待補') + ('｜TDCC週增已核' if tdcc else '｜TDCC待補')
        if why:
            rec['H1481研究分層'] = '隔離查核／不得占據強勢主榜'
            rec['H1481入榜／隔離依據'] = '；'.join(why)
            blocked_rows.append(rec)
            reasons.update(set(why))
        else:
            rec['H1481研究分層'] = '強勢族群＋突破/續攻｜籌碼雙核' if flow and tdcc else '量價＋內部主流支持｜籌碼待補'
            rec['H1481入榜／隔離依據'] = kind+'；'+('法人與週持股已核' if flow and tdcc else '尚不能聲稱法人連買／大戶鎖單')
            # All rows are RESEARCH-ONLY; never replace the H126 authority.
            rec['觀察順位'] = '同層並列｜非買進排名'
            rec['排序可信度'] = '同輪量價／族群條件分層，非H110分數排序'
            rec['買進資格'] = '禁止依本榜買進'
            rec['_tier'] = 0 if flow and tdcc else 1
            rec['_kind'] = 0 if kind == '突破確認研究／非買點' else 1
            rank_rows.append(rec)
    rank_rows.sort(key=lambda r:(r['_tier'], r['_kind'], r['股票代號']))
    for row in rank_rows:
        row.pop('_tier', None); row.pop('_kind', None)
    cols = list(board.columns) + [x for x in EXTRA_COLS if x not in board.columns]
    take = max(1, min(20, int(limit)))
    main = pd.DataFrame(rank_rows[:take], columns=cols)
    quarantine = pd.DataFrame(blocked_rows, columns=cols)
    sample_missed = trace.loc[
        trace['H148研究分類'].isin(STRONG_KINDS) &
        (trace['H148漏網位置'] == '診斷候選未進H113稽核／待查原因'),
        '股票代號'
    ].astype(str).drop_duplicates().tolist() if {'H148研究分類','H148漏網位置','股票代號'}.issubset(trace.columns) else []
    meta = {
        'version': VERSION, 'research_day': str(research_date) if research_date else '未核實',
        'full_audit': len(raw_rows), 'h148_trace': len(trail),
        'eligible': len(rank_rows), 'displayed': len(main),
        'quarantine': len(quarantine), 'quarantine_reasons': dict(reasons),
        'h113_missed_strong_research_count': len(sample_missed),
        'h113_missed_examples': sample_missed[:12],
        'scope_proven': bool(isinstance(trace_meta, Mapping) and trace_meta.get('proven_diagnosis_pool') is True),
        'warning': ('本輪0檔符合已驗同日量價＋內部族群雙指標的強勢研究入榜條件；原候選保留於隔離查核，不代表全市場沒有強勢股。'
                    if not rank_rows else f'僅有{len(rank_rows)}檔符合強勢研究證據條件，非正式買進清單。'),
    }
    if not meta['scope_proven']:
        meta['warning'] += ' 本輪診斷母體尚未完整證明，不能宣稱全市場覆蓋或無漏網。'
    return main, quarantine, meta


__all__ = ['VERSION', 'EXTRA_COLS', 'curate_focus_board']
