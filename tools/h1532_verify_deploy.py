#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only H153.2 cumulative install preflight; never opens user data or secrets."""
from __future__ import annotations
import argparse,ast,sys
from pathlib import Path

REQUIRED=(
    'pages/7_股神推薦.py', 'godpick_h114_parallel_alpha.py',
    'godpick_h153_growth_setup_rank.py', 'godpick_h1531_quality_repair.py',
    'godpick_h1532_technical_evidence.py', 'godpick_h1532_scope_repair.py',
    'godpick_h115_pit_evidence.py', 'godpick_h1531_multiday_resilience.py',
    'godpick_h151_workbook_sections.py', 'godpick_h1512_excel_integrity.py',
    'tools/h1531_verify_deploy.py',
    '.github/workflows/h150_data_orchestrator.yml',
)
def verify(root:Path)->list[str]:
    root=Path(root).resolve();errors=[]
    for name in REQUIRED:
        p=root/name
        if not p.is_file():errors.append('MISSING:'+name);continue
        if p.suffix=='.py':
            try:ast.parse(p.read_text(encoding='utf-8'),filename=name)
            except (OSError,UnicodeError,SyntaxError) as e:
                errors.append('BAD_PY:'+name+':'+type(e).__name__)
    if errors:return errors
    if str(root) not in sys.path:sys.path.insert(0,str(root))
    from tools.h1531_verify_deploy import verify as older
    errors.extend(older(root))
    page=(root/'pages/7_股神推薦.py').read_text('utf-8')
    for t in ('restore_research_scope as _h1532_scope','_h1532_scope_meta',
              'h153_ranked, h153_summary, h153_meta = _h153_rank(',
              "tables['h126_authority']",'_h1512_normalize_views(wb)'):
        if t not in page:errors.append('MISSING_PAGE_CONTRACT:'+t)
    tech=(root/'godpick_h114_parallel_alpha.py').read_text('utf-8')
    if 'evaluate_prebreakout(df)' not in tech:errors.append('MISSING_H114_INDICATOR_CALL')
    ranking=(root/'godpick_h153_growth_setup_rank.py').read_text('utf-8')
    for t in ('H1532布林收斂','H1532OBV量價背離','H1532ADX','classify_h109(risk)','build_ranked_five_plans'):
        if t not in ranking:errors.append('MISSING_RANK_CONTRACT:'+t)
    return errors

def main():
    pa=argparse.ArgumentParser();pa.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);a=pa.parse_args()
    errors=verify(a.root)
    if errors:
        print('FAIL H153.2:\n'+'\n'.join(errors));raise SystemExit(1)
    print('PASS H153.2 cumulative research modules, 12-sheet H126 authority and H151.2 Excel contracts')
if __name__=='__main__':main()
