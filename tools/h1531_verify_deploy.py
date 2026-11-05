#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only cumulative H153.1 deployment preflight. Does not read Secrets or data."""
from __future__ import annotations
import argparse
import importlib
from pathlib import Path

NEEDED=(
 'pages/7_股神推薦.py','godpick_h114_parallel_alpha.py',
 'godpick_h115_pit_evidence.py','godpick_h153_growth_setup_rank.py',
 'godpick_h1531_quality_repair.py','godpick_h1531_multiday_resilience.py',
 'godpick_h151_workbook_sections.py','godpick_h1512_excel_integrity.py',
 'godpick_h152_resilience_forecast.py',
 '.github/workflows/h150_data_orchestrator.yml',
)

def verify(root:Path)->list[str]:
    root=Path(root)
    errors=[f'MISSING:{x}' for x in NEEDED if not (root/x).is_file()]
    if errors:return errors
    page=(root/'pages/7_股神推薦.py').read_text('utf-8')
    must=('_h153_pit_evidence(_h152_root, _h148_research_day)',
          '_h1531_pit_digest(_h152_root)',
          'from godpick_h1531_multiday_resilience import enrich_multiday_resilience',
          '_h153_attach_research(h152_named_research, h153_ranked)',
          '_h153_five_plans(ranked=h153_ranked',
          '_h1512_normalize_views(wb)',
          "tables['h126_authority']")
    errors += [f'PAGE_CONTRACT_MISSING:{s}' for s in must if s not in page]
    rank=(root/'godpick_h153_growth_setup_rank.py').read_text('utf-8')
    for s in ('classify_h109(risk)','liquidity_reasons(amount=amount',
              'raw_valuation_ok','同日同業PE樣本不足','明確否決／來源衝突隔離',
              'H153.1欄位對齊'):
        if s not in rank:errors.append('RANK_CONTRACT_MISSING:'+s)
    h115=(root/'godpick_h115_pit_evidence.py').read_text('utf-8')
    if h115.find('H115原始估值資料狀態')<0:errors.append('H115_RAW_PE_EPS_MISSING')
    return errors


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1])
    arg=ap.parse_args()
    errors=verify(arg.root)
    if errors:
        print('FAIL H153.1 deployment:\n'+'\n'.join(errors))
        raise SystemExit(1)
    print('PASS H153.1 cumulative deployment: dependencies, page, H109/H115/H126 contracts')
if __name__=='__main__':main()
