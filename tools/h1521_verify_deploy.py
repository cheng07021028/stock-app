# -*- coding: utf-8 -*-
"""Validate H152.1 cumulative overlay and protect historic H126 workbook gate."""
from __future__ import annotations
import ast
from pathlib import Path

def verify(root:Path)->tuple[bool,list[str]]:
    root=Path(root)
    import sys
    if str(root.resolve()) not in sys.path:sys.path.insert(0,str(root.resolve()))
    from tools.h152_verify_deploy import verify as prev
    good,notices=prev(root)
    errors=[] if good else list(notices)
    required=['pages/7_股神推薦.py','godpick_h114_parallel_alpha.py',
              'godpick_h149_trade_intelligence.py','godpick_h152_resilience_forecast.py',
              'godpick_h1521_early_potential.py','tests/test_h1521_early_potential.py']
    for rel in required:
        file=root/rel
        if not file.is_file():errors.append('MISSING '+rel);continue
        try:ast.parse(file.read_text(encoding='utf-8'),filename=rel)
        except (SyntaxError,OSError,UnicodeError) as e:errors.append('INVALID '+rel+' '+type(e).__name__)
    if errors:return False,errors
    page=(root/required[0]).read_text('utf-8');h149=(root/required[2]).read_text('utf-8')
    h114=(root/required[1]).read_text('utf-8');module=(root/required[4]).read_text('utf-8')
    for token in ("from godpick_h1521_early_potential import build_early_potential",
                  'h1521_potential, h152_named_research, h1521_summary',
                  'h1521_opportunities',"_h1512_normalize_views(wb)",
                  "00_H126主管決策", "tables['h126_authority']"):
        if token not in page:errors.append('MISSING_H1521_PAGE_CONTRACT '+token)
    for token in ('P1｜','P2｜','0.55','蓄勢尚未確認突破'):
        if token not in h149:errors.append('MISSING_H149_POTENTIAL_CONTRACT '+token)
    for token in ('H114當前5日漲跌幅%','H114當日收盤價'):
        if token not in h114:errors.append('MISSING_H114_FEATURE '+token)
    for token in ('formal_h126_modified','非H126買進授權','H114波動收斂','H114量縮墊高'):
        if token not in module:errors.append('MISSING_POTENTIAL_GUARD '+token)
    return not errors,errors or ['PASS H152.1 full cumulative dependencies, workbook and H126 invariants']

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--root',default='.')
    args=p.parse_args();success,notes=verify(Path(args.root))
    for note in notes:print(note)
    raise SystemExit(0 if success else 1)
