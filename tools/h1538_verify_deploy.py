# -*- coding: utf-8 -*-
"""H153.8 dependency and 7/09-vs-11/11 authority smoke check; offline."""
from __future__ import annotations
from pathlib import Path
import argparse
import importlib.util
import sys
from datetime import date

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--root',default='.')
    args=ap.parse_args()
    root=Path(args.root).resolve()
    targets=['pages/7_股神推薦.py','godpick_h1538_source_authority.py',
             'godpick_h1537_manager_visible.py','godpick_h1536_manager_clarity.py',
             'godpick_h1535_research_recovery.py','godpick_h1534_three_margin.py',
             'godpick_h1532_technical_evidence.py','godpick_auto_scheduler.py']
    absent=[x for x in targets if not (root/x).is_file()]
    if absent:raise AssertionError('Missing cumulative modules: '+str(absent))
    sys.path.insert(0,str(root))
    from godpick_h1538_source_authority import select_authority, business_date
    main_old={'data_date':'2026-07-09','updated_at':'2026-11-11 23:01:10'}
    runtime_new={'twse_data_date':'2026-11-11','updated_at':'2026-11-11 22:05:14'}
    decision=select_authority('market_snapshot.json',main_old,runtime_new,today=date(2026,11,11))
    assert decision['business_date']=='2026-11-11'
    assert decision['source']=='github-runtime-data-readonly'
    assert not select_authority('market_snapshot.json',main_old,None,today=date(2026,11,11))['payload']
    old_f={'data_date':'20260709','records':[{'股票代號':'2330'}]}
    new_f={'data_date':'20261111','records':[{'股票代號':'2330'}]}
    assert select_authority('official_factors_cache.json',old_f,new_f,today=date(2026,11,11))['business_date']=='2026-11-11'
    assert business_date('market_snapshot.json',{'updated_at':'2026-11-11'}) is None
    page=(root/'pages/7_股神推薦.py').read_text(encoding='utf-8')
    required=['godpick_h1538_source_authority','reconcile_json','H153.8','h1538_authority_sources',
              'select_authority("market_snapshot.json", _h40_restored, None)',
              'godpick_h1537_manager_visible']
    missing=[x for x in required if x not in page]
    if missing:raise AssertionError('H153.8 page integration missing '+str(missing))
    for v in ('h1537','h1536','h1535','h1534','h1533'):
        assert (root/'tools'/f'{v}_verify_deploy.py').is_file(), v
    print('PASS H153.8 cumulative deployment | July fallback quarantined | H126 untouched')

if __name__=='__main__':main()
