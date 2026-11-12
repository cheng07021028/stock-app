# -*- coding: utf-8 -*-
"""H153.9 cumulative, non-trading research release smoke."""
from __future__ import annotations
import argparse
from pathlib import Path
import sys
from datetime import date

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--root',default='.')
    root=Path(ap.parse_args().root).resolve()
    required=['pages/7_股神推薦.py','godpick_h1539_mainstream_freshness.py',
              'godpick_h1538_source_authority.py','godpick_h1537_manager_visible.py',
              'godpick_h1536_manager_clarity.py','godpick_h1535_research_recovery.py',
              'godpick_h1534_three_margin.py','godpick_h1533_scheduler_stall.py',
              'godpick_h153_growth_setup_rank.py','godpick_h1532_technical_evidence.py']
    # H153.3 implementation lives in scheduler; no separate H1533 module.
    required=[x for x in required if not x.endswith('godpick_h1533_scheduler_stall.py')]
    missing=[x for x in required if not (root/x).is_file()]
    assert not missing,'Missing cumulative dependency '+str(missing)
    page=(root/'pages/7_股神推薦.py').read_text(encoding='utf-8')
    for marker in ('godpick_h1539_mainstream_freshness','h153_ranked, _h1539_summary',
                   'godpick_h1538_source_authority','godpick_h1537_manager_visible'):
        assert marker in page,marker
    import pandas as pd
    sys.path.insert(0,str(root))
    from godpick_h1539_mainstream_freshness import audit_mainstream_repetition
    r=pd.DataFrame([{'股票代號':'2618','股票名稱':'長榮航',
          '排名等級':'B｜待補證','研究排名':1,'研究證據分':41,'反證與待補':'H126禁止買進'}])
    source=pd.DataFrame([{'股票代號':'2618','類別':'航運','H114K線證據日':'2026-11-11',
          'H114K線資料狀態':'VALID_OHLCV','H114K線有效根數':100,
          'H114當前5日漲跌幅%':1.0,'H113當日成交額百萬':300,
          'H114當日成交量張':800,'H109決策權威':'UNKNOWN'}])
    got,summary,meta=audit_mainstream_repetition(r,source,research_day=date(2026,11,11),snapshots=[])
    assert len(got)==1
    assert got.iloc[0]['H1539主流證據等級'].startswith('U｜')
    assert got.iloc[0]['研究證據分']==41
    assert got.iloc[0]['研究排名']==1
    assert len(summary)>=4
    print('PASS H153.9 cumulative | sector missing not called mainstream | H126 unchanged')

if __name__=='__main__':main()
