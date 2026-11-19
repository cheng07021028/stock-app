# -*- coding: utf-8 -*-
"""Non-mutating H171 audit section for existing godpick Excel sheet 11.

Caller supplies original workbook; this appends below existing navigator and preserves
00/01/02/H126/other sheets, rankings, columns and formal trade permissions.
"""
from __future__ import annotations
from typing import Any, Mapping
from openpyxl.styles import Alignment, Font, PatternFill
from godpick_h171_pit_validation import ttm_eps, profit_breakout, forward_performance

NAV = '11_交易員重點導航'
SOURCE = '01_具名強勢研究'


def _text(v: Any) -> str:
    return str(v or '').strip()


def append_h171_research(wb: Any, *, financial_rows: list[Mapping], research_day: str,
                         performance: Mapping | None = None) -> dict:
    """Append ONLY research evidence; never auto-promote a stock to trade status."""
    if NAV not in wb.sheetnames or SOURCE not in wb.sheetnames:
        return {'status': 'MISSING_EXISTING_SHEETS', 'rows': 0, 'trade_authority_changed': False}
    ws = wb[NAV]
    source = wb[SOURCE]
    existing_names = wb.sheetnames[:]
    start = ws.max_row + 3
    ws.cell(start, 1, 'H171｜近四季 EPS × 五季獲利突破 × T+N 推薦績效研究')
    ws.cell(start, 1).font = Font(name='Microsoft JhengHei', bold=True, color='FFFFFF', size=12)
    ws.cell(start, 1).fill = PatternFill('solid', fgColor='17324D')
    ws.cell(start, 2, f'研究日：{research_day}｜僅研究用途；不修改 H109 / H126 買進權威')
    ws.row_dimensions[start].height = 26
    perf = performance or {}
    summary = perf.get('summary') if isinstance(perf, Mapping) else None
    summary = summary if isinstance(summary, Mapping) else {}
    ws.cell(start + 1, 1, 'T+N 為歷史收盤價績效，不是明日預測、交易建議或含股利總報酬')
    ws.cell(start + 2, 1, 'T+1／T+5／T+10 有效樣本數')
    ws.cell(start + 2, 2, ' / '.join(str((summary.get(f'T+{i}') or {}).get('evaluated', 0)) for i in (1, 5, 10)))
    ws.cell(start + 3, 1, 'T+1／T+5／T+10 歷史勝率')
    ws.cell(start + 3, 2, ' / '.join(
        ('--' if (summary.get(f'T+{i}') or {}).get('win_rate_pct') is None else
         f"{summary[f'T+{i}']['win_rate_pct']}%") for i in (1, 5, 10)))
    hdr = start + 5
    headings = ['股票代號', '股票名稱', '市場別', '研究日', 'TTM EPS', 'TTM EPS 證據狀態',
                '獲利突破狀態', '毛利率(%)', '營益率(%)', 'T+1 已實現(%)',
                'T+5 已實現(%)', 'T+10 已實現(%)', '正式買進權限']
    for j, heading in enumerate(headings, 1):
        cell = ws.cell(hdr, j, heading)
        cell.font = Font(name='Microsoft JhengHei', bold=True, color='FFFFFF')
        cell.fill = PatternFill('solid', fgColor='0B6E75')
        cell.alignment = Alignment(vertical='center', wrap_text=True)
    ws.row_dimensions[hdr].height = 28
    headers = [_text(cell.value) for cell in source[1]]
    cols = {col: headers.index(col) for col in ('股票代號', '股票名稱', '市場別') if col in headers}
    seen = set()
    certified = 0
    reconstructed = 0
    perf_index = {}
    for signal in perf.get('signals', []) if isinstance(perf, Mapping) else []:
        if not isinstance(signal, Mapping):
            continue
        code = str(signal.get('code') or '')
        date = str(signal.get('signal_date') or '')
        perf_index.setdefault((code, date), signal.get('returns') or {})
    for row in source.iter_rows(min_row=2, values_only=True):
        code = _text(row[cols['股票代號']]).removesuffix('.0') if '股票代號' in cols else ''
        if not code.isdigit() or len(code) != 4 or code in seen:
            continue
        seen.add(code)
        name = _text(row[cols['股票名稱']]) if '股票名稱' in cols else ''
        market = _text(row[cols['市場別']]) if '市場別' in cols else ''
        e = ttm_eps(financial_rows, code=code, market=market, study_day=research_day)
        p = profit_breakout(financial_rows, code=code, market=market, study_day=research_day)
        if e['eps_ttm'] is not None:
            reconstructed += 1
        if p['certified_for_research']:
            certified += 1
        returns = perf_index.get((code, research_day), {})
        def realized(n: int) -> float | None:
            x = returns.get(f'T+{n}') or {}
            return x.get('return_pct') if x.get('status') == 'EVALUATED_PRICE_ONLY' else None
        values = [code, name, market, research_day, e['eps_ttm'], e['status'], p['status'],
                  p.get('gross_margin_last'), p.get('operating_margin_last'),
                  realized(1), realized(5), realized(10), '遵照原 H126；H171 不授權買進']
        line = hdr + len(seen)
        for col, value in enumerate(values, 1):
            cell = ws.cell(line, col, value)
            cell.alignment = Alignment(vertical='center', wrap_text=col in (6, 7, 13))
            if col in (8, 9, 10, 11, 12) and isinstance(value, (float, int)):
                cell.number_format = '0.00'
            if col == 5 and isinstance(value, (float, int)):
                cell.number_format = '0.0000'
        ws.row_dimensions[line].height = 30
    assert wb.sheetnames == existing_names
    return {'status': 'H171_RESEARCH_SECTION_APPENDED', 'sheet': NAV,
            'start_row': start, 'research_rows': len(seen),
            'reconstructed_ttm_rows': reconstructed, 'profit_breakout_research_rows': certified,
            't_plus_n_sample_summary': summary, 'trade_authority_changed': False}


__all__ = ['append_h171_research']
