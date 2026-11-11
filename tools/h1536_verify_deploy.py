# -*- coding: utf-8 -*-
"""H153.6 read-only cumulative deployment and non-regression preflight."""
import argparse
import ast
from pathlib import Path

PRODUCTION=(
 'pages/7_股神推薦.py','godpick_h1536_manager_clarity.py',
 'godpick_h1535_research_recovery.py','godpick_h1534_three_margin.py',
 'godpick_h1532_technical_evidence.py','godpick_h1532_scope_repair.py',
 'godpick_h153_growth_setup_rank.py','godpick_h1531_quality_repair.py',
 'godpick_h115_pit_evidence.py','godpick_auto_scheduler.py',
 'godpick_h1512_excel_integrity.py','godpick_h151_workbook_sections.py',
 '.github/workflows/h1534_official_margins.yml'
)

def verify(root:Path):
    missing=[x for x in PRODUCTION if not (root/x).is_file()]
    if missing:raise RuntimeError('H153.6 missing cumulative production: '+repr(missing))
    for filename in PRODUCTION:
        if filename.endswith('.py'):
            ast.parse((root/filename).read_text('utf-8'),filename=filename)
    page=(root/'pages/7_股神推薦.py').read_text('utf-8')
    watch=(root/'godpick_h1535_research_recovery.py').read_text('utf-8')
    clarity=(root/'godpick_h1536_manager_clarity.py').read_text('utf-8')
    assert 'existing_codes=_h1536_shown_codes' in page
    assert '_h1536_clarity(' in page
    assert 'h152_research_summary = pd.concat([_h1536_existing_summary,_h1536_audit]' in page
    assert 'for item in items if item[\'股票代號\'] not in excluded' in watch
    assert 'H153.6 H1532完整85根技術指標已核' in clarity
    assert "tables['h126_authority']" in page
    assert 'normalize_frozen_sheet_views' in (root/'godpick_h1512_excel_integrity.py').read_text('utf-8')
    print('PASS H153.6 cumulative dependencies / W dedup-before-cap / readable board / H126 intact')

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',type=Path,default=Path('.'))
    verify(parser.parse_args().root)
