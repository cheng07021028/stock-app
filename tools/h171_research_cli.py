# -*- coding: utf-8 -*-
"""H171 workbook post-export audit, without modifying the production selector.

Example:
python tools/h171_research_cli.py --workbook original.xlsx --financial official_ytd_archive.json \
    --research-day 2026-11-18 --output verified_audit.xlsx

Optional T+N evaluation: --signals signals.json --prices price_history.json
   --sessions trading_sessions.json --evaluated-at 2026-11-19T17:30:00+08:00
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h171_pit_validation import forward_performance
from godpick_h171_workbook_audit import append_h171_research
from openpyxl import load_workbook


def load_list(path: Path | None, *keys: str) -> list:
    if path is None:
        return []
    obj = json.loads(path.read_text(encoding='utf-8-sig'))
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in keys:
            if isinstance(obj.get(k), list):
                return obj[k]
    raise ValueError(f'{path}: expected JSON array or list in {keys}')


def main(argv=None) -> dict:
    p = argparse.ArgumentParser(description='H171 existing-sheet evidence audit; not a trading signal')
    p.add_argument('--workbook', type=Path, required=True)
    p.add_argument('--financial', type=Path, required=True, help='Archived official YTD rows, not a fabricated quarterly EPS')
    p.add_argument('--research-day', required=True, help='YYYY-MM-DD; first observation must be before this day')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--signals', type=Path)
    p.add_argument('--prices', type=Path)
    p.add_argument('--sessions', type=Path)
    p.add_argument('--evaluated-at')
    args = p.parse_args(argv)
    if args.output.resolve() == args.workbook.resolve():
        raise ValueError('Output must differ from original workbook')
    financial = load_list(args.financial, 'rows', 'eps_ytd_rows')
    perf = None
    if any((args.signals,args.prices,args.sessions,args.evaluated_at)):
        if not all((args.signals,args.prices,args.sessions,args.evaluated_at)):
            raise ValueError('T+N requires --signals --prices --sessions --evaluated-at together')
        perf = forward_performance(load_list(args.signals,'signals'),
                                   load_list(args.prices,'prices'),
                                   load_list(args.sessions,'sessions'),
                                   evaluated_at=args.evaluated_at)
    wb = load_workbook(args.workbook)
    report = append_h171_research(wb, financial_rows=financial,
                                  research_day=args.research_day, performance=perf)
    if report['status'] != 'H171_RESEARCH_SECTION_APPENDED':
        raise ValueError('Required existing sheets absent; no workbook written')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    wb.save(args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report

if __name__=='__main__':
    main()
