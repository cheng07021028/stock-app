# -*- coding: utf-8 -*-
"""H153 cumulative packaging checks; H126 decisions are never revised."""
from __future__ import annotations
import ast
from pathlib import Path


def verify(root: Path) -> tuple[bool,list[str]]:
    root=Path(root).resolve()
    import sys
    if str(root) not in sys.path:sys.path.insert(0,str(root))
    from tools.h1521_verify_deploy import verify as older
    ok,notes=older(root)
    errs=[] if ok else list(notes)
    mandatory=(
        'pages/7_股神推薦.py','godpick_h114_parallel_alpha.py',
        'godpick_h115_pit_evidence.py','godpick_h153_growth_setup_rank.py',
        'godpick_h151_workbook_sections.py','godpick_h1512_excel_integrity.py',
        'tests/test_h153_growth_technical_ranking.py',
        'tests/test_h153_real_excel_ranked.py',
    )
    for name in mandatory:
        file=root/name
        if not file.is_file():errs.append('MISSING '+name);continue
        try:ast.parse(file.read_text(encoding='utf-8'),filename=name)
        except (SyntaxError,OSError,UnicodeError) as ex:errs.append('BAD_SYNTAX '+name+':'+type(ex).__name__)
    if errs:return False,errs
    page=(root/'pages/7_股神推薦.py').read_text(encoding='utf-8')
    h114=(root/'godpick_h114_parallel_alpha.py').read_text(encoding='utf-8')
    h153=(root/'godpick_h153_growth_setup_rank.py').read_text(encoding='utf-8')
    for token in ('build_growth_setup_ranking','build_ranked_five_plans',
                  '_h153_pit_evidence(_h152_root, _h148_research_day)',
                  'h152_named_research = _h153_attach_research',
                  'h152_opportunities = _h153_five_plans',
                  'research=locals().get(\'h152_named_research\'',
                  'opportunities=locals().get(\'h152_opportunities\'',
                  "tables['h126_authority']", '_h1512_normalize_views(wb)'):
        if token not in page:errs.append('MISSING_H153_WIRING '+token)
    for token in ('H114黃金交叉','H114箱型狀態','H114箱頂','H114箱底','H114技術失效條件'):
        if token not in h114:errs.append('MISSING_H114_TECH '+token)
    for token in ('同分並列','forecast_revenue_scenario','formal_h126_modified',
                  '研究證據分','H109風控','非H126買進授權'):
        if token not in h153:errs.append('MISSING_H153_GUARD '+token)
    # H115 PIT data provenance must be available rather than using a legacy
    # current-day fundamentals cache to fill historical ranking.
    if 'from godpick_h115_pit_evidence import build_h114_evidence' not in page:
        errs.append('INVALID_H115_PIT_SOURCE')
    return not errs, errs or ['PASS H153 full cumulative dependencies, 12-sheet rank, PIT and H126 controls']

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--root',default='.')
    args=parser.parse_args()
    status,details=verify(Path(args.root))
    for line in details:print(line)
    raise SystemExit(0 if status else 1)
