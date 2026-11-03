# -*- coding: utf-8 -*-
"""Validate H152 cumulative deployment (H151.2 + H152, no online calls)."""
from __future__ import annotations
import ast
from pathlib import Path


def verify(root:Path)->tuple[bool,list[str]]:
    root=Path(root)
    import sys
    if str(root.resolve()) not in sys.path: sys.path.insert(0,str(root.resolve()))
    from tools.h1512_verify_deploy import verify as prior
    ok,notes=prior(root)
    problems=[] if ok else list(notes)
    needed=('godpick_h152_resilience_forecast.py','godpick_h114_parallel_alpha.py',
            'pages/7_股神推薦.py','tests/test_h152_resilience_forecast.py')
    for rel in needed:
        p=root/rel
        if not p.is_file():
            problems.append('MISSING_H152 '+rel);continue
        try:ast.parse(p.read_text('utf-8'),filename=str(p))
        except (SyntaxError,OSError,UnicodeError) as err:
            problems.append('INVALID_H152 '+rel+':'+type(err).__name__)
    if problems:return False,problems
    page=(root/'pages/7_股神推薦.py').read_text('utf-8')
    analyzer=(root/'godpick_h114_parallel_alpha.py').read_text('utf-8')
    signals=(root/'godpick_h152_resilience_forecast.py').read_text('utf-8')
    for text in ("_h152_research(","research=locals().get('h152_named_research'",
                 "research_summary=locals().get('h152_research_summary'",
                 "opportunities=locals().get('h152_opportunities'",
                 "_h1512_normalize_views(wb)","00_H126主管決策"):
        if text not in page:problems.append('MISSING_H152_PAGE_HOOK '+text)
    for text in ('H114前日收盤價','H114前次交易日','H114當日漲跌幅%',
                 'H114同型歷史樣本數'):
        if text not in analyzer:problems.append('MISSING_H114_SOURCE '+text)
    for text in ('formal_h126_modified', 'supplement_with_verified_h117',
                 'market_aligned','H109','非H126交易授權'):
        if text not in signals:problems.append('MISSING_H152_GUARD '+text)
    return not problems,problems or ['PASS H152 cumulative dependency, data-date and H126 controls']

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--root',default='.')
    args=p.parse_args();ok,notes=verify(Path(args.root))
    for line in notes:print(line)
    raise SystemExit(0 if ok else 1)
