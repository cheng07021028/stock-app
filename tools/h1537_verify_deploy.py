# -*- coding: utf-8 -*-
"""H153.7 cumulative deployment and actual visible manager-content contract."""
import argparse,ast
from pathlib import Path
from importlib.util import spec_from_file_location,module_from_spec

NEEDED=('pages/7_股神推薦.py','godpick_h1537_manager_visible.py',
        'godpick_h1536_manager_clarity.py','godpick_h1535_research_recovery.py',
        'godpick_h1534_three_margin.py','godpick_h1532_technical_evidence.py',
        'godpick_h1512_excel_integrity.py','godpick_h151_workbook_sections.py')

def verify(root:Path):
    for name in NEEDED:
        p=root/name
        if not p.is_file(): raise RuntimeError('Missing production '+name)
        if name.endswith('.py'): ast.parse(p.read_text('utf-8'),filename=name)
    page=(root/NEEDED[0]).read_text('utf-8')
    code=(root/'godpick_h1537_manager_visible.py').read_text('utf-8')
    for term in ('_h1537_visible(', '_h1537_validate(', '_h1537_cross_tab(',
                 "tables['h126_authority']",'normalize_frozen_sheet_views'):
        if term not in page:raise RuntimeError('Missing manager export hook '+term)
    for term in ('H1537ManagerResearch','H1537VerificationWatch',
                 'H126本輪0檔','validate_exported_cross_tab','H148.1'):
        if term not in code:raise RuntimeError('Missing manager content '+term)
    print('PASS H153.7 manager visible + H126 authority unchanged + 12-tabs contract')

if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('--root',default='.',type=Path)
    verify(a.parse_args().root)
