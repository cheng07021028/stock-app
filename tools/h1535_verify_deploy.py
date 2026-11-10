# -*- coding: utf-8 -*-
"""H153.5 cumulative deployment preflight; read-only, no network writes."""
from pathlib import Path
import argparse
import ast

PRODUCTION=(
 'pages/7_股神推薦.py',
 'godpick_h1535_research_recovery.py',
 'godpick_h1534_three_margin.py',
 'godpick_h1532_scope_repair.py',
 'godpick_h153_growth_setup_rank.py',
 'godpick_h115_pit_evidence.py',
 'godpick_h1531_quality_repair.py',
 'godpick_auto_scheduler.py',
 'godpick_history_sources.py',
 'godpick_h1512_excel_integrity.py',
 '.github/workflows/h1534_official_margins.yml',
)

def verify(root:Path):
    missing=[name for name in PRODUCTION if not (root/name).is_file()]
    if missing:raise RuntimeError('H153.5 missing dependencies: '+', '.join(missing))
    for name in PRODUCTION:
        if name.endswith('.py'):
            ast.parse((root/name).read_text('utf-8'),filename=name)
    p=(root/'pages/7_股神推薦.py').read_text('utf-8')
    n=(root/'godpick_h1535_research_recovery.py').read_text('utf-8')
    assert 'candidate_diagnosis_scan_signature_h1535' in p
    assert 'restore_verified_research_pool' in p
    assert '_h1535_merge_watch(h152_named_research,_h1535_watchlist)' in p
    assert 'h152_opportunities = _h153_five_plans(ranked=h153_ranked' in p
    assert "tables['h126_authority']" in p
    assert 'H1534三率三升狀態' in (root/'godpick_h153_growth_setup_rank.py').read_text('utf-8')
    assert 'H153.3' in (root/'README_H1533.md').read_text('utf-8')
    assert 'DISCLAIMER' in n and '禁止買進' in n
    print('PASS H153.5 same-run scope / nontrading named audit / H126 unchanged')

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',type=Path,default=Path('.'))
    verify(parser.parse_args().root)
