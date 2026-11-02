# -*- coding: utf-8 -*-
"""H151 cumulative single-ZIP preflight without network or credentials."""
from __future__ import annotations
import ast
import argparse
import sys
from pathlib import Path


def verify(root: Path) -> tuple[bool, list[str]]:
    root = Path(root).resolve()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from tools.h150_verify_deploy import verify as verify_h150
    ok, notes = verify_h150(root)
    problems = [] if ok else list(notes)
    for name in ('godpick_h151_workbook_sections.py', 'pages/7_股神推薦.py'):
        p = root / name
        if not p.is_file():
            problems.append(f'MISSING: {name}')
            continue
        try:
            ast.parse(p.read_text('utf-8'), filename=name)
        except (SyntaxError, OSError, UnicodeError) as exc:
            problems.append(f'INVALID: {name}: {type(exc).__name__}: {exc}')
    if problems:
        return False, problems
    page = (root/'pages/7_股神推薦.py').read_text('utf-8')
    for term in ('_h151_section_sheets(', 'StockPick_H151_SplitSheets_',
                 '"00_主管重點觀察榜"', '"00_H126主管決策"'):
        if term not in page:
            problems.append('MISSING_H151: '+term)
    from godpick_h151_workbook_sections import CORE_TABS, CATEGORY_TABS
    if len(CATEGORY_TABS)!=10 or len({x[0] for x in CATEGORY_TABS})!=10:
        problems.append('INVALID_CATEGORY_TABS')
    if CORE_TABS != ('00_主管重點觀察榜', '00_H126主管決策'):
        problems.append('AUTHORITY_TAB_ORDER_CHANGED')
    return (not problems), problems or ['PASS H151: H150 cumulative dependencies and 12 independent Excel worksheets']


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1])
    args=parser.parse_args()
    ok, notes = verify(args.root)
    for line in notes: print(line)
    sys.exit(0 if ok else 1)
