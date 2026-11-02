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
    for token in ('build_six_pillar_review','append_six_pillar_to_sheet',
                  'StockPick_H149_TradeResearch_','godpick_h1482_source_audit'):
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
