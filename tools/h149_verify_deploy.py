# -*- coding: utf-8 -*-
"""H149 cumulative production preflight; read-only, no secrets or network."""
from __future__ import annotations
import argparse
import ast
from pathlib import Path
import sys

MODULES=(
 'godpick_h144_focus_board','godpick_h147_five_gate_review',
 'godpick_h147_scan_bridge','godpick_h148_strength_provenance',
 'godpick_h1481_focus_gate','godpick_h1482_source_audit',
 'godpick_h1483_research_recall','godpick_h114_parallel_alpha',
 'godpick_h149_trade_intelligence',
)


def verify(root: Path) -> tuple[bool,list[str]]:
    root=Path(root)
    missing=[]
    bad=[]
    page=root/'pages'/'7_股神推薦.py'
    required=[page]+[root/(n+'.py') for n in MODULES]
    for p in required:
        if not p.is_file():
            missing.append('MISSING: '+str(p.relative_to(root)))
        else:
            try:
                ast.parse(p.read_text('utf-8'),filename=str(p))
            except (OSError,SyntaxError,UnicodeError) as exc:
                bad.append(f'INVALID: {p.name}: {type(exc).__name__}: {exc}')
    notes=missing+bad
    if notes:
        return False,notes
    source=page.read_text('utf-8')
    # H151 changes ONLY the manager Excel layout contract: H149 six-pillar
    # evidence remains computed, but its block is no longer appended inside
    # the first worksheet. Accept both the legacy H149 single-sheet appendix
    # and the explicitly versioned H151 per-category workbook contract.
    split_layout = 'from godpick_h151_workbook_sections import add_research_section_sheets' in source
    tokens = ('build_six_pillar_review', 'godpick_h1482_source_audit')
    if split_layout:
        tokens += ('_h151_section_sheets(', 'StockPick_H151_SplitSheets_')
        split_file = root/'godpick_h151_workbook_sections.py'
        if not split_file.is_file():
            notes.append('MISSING: godpick_h151_workbook_sections.py')
        else:
            try:
                ast.parse(split_file.read_text('utf-8'),filename=str(split_file))
            except (OSError,SyntaxError,UnicodeError) as exc:
                notes.append(f'INVALID: {split_file.name}: {type(exc).__name__}')
    else:
        tokens += ('append_six_pillar_to_sheet', 'StockPick_H149_TradeResearch_')
    for token in tokens:
        if token not in source:
            notes.append('MISSING HOOK: '+token)
    if notes:return False,notes
    return True,['PASS: H149 cumulative 10 production files, Page07 Excel hooks and UTF-8 Python syntax']


def main() -> int:
    p=argparse.ArgumentParser()
    p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1])
    args=p.parse_args()
    ok,notes=verify(args.root)
    for line in notes:print(line)
    return 0 if ok else 1

if __name__=='__main__':sys.exit(main())
