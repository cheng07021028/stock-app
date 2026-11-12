# -*- coding: utf-8 -*-
"""Verify V191-H154 cumulative install without touching permanent runtime-data."""
from __future__ import annotations
from pathlib import Path
from argparse import ArgumentParser
import sys
import pandas as pd


def main():
    p=ArgumentParser();p.add_argument('--root',default='.')
    root=Path(p.parse_args().root).resolve()
    required=[
        'pages/7_股神推薦.py', 'godpick_h154_export_parity.py',
        'godpick_h1539_mainstream_freshness.py',
        'godpick_h1538_source_authority.py',
        'godpick_h1537_manager_visible.py',
        'godpick_h153_growth_setup_rank.py',
        'godpick_h1536_manager_clarity.py',
        'godpick_h1535_research_recovery.py',
        'godpick_h1534_three_margin.py',
        'godpick_h1512_excel_integrity.py',
        'tests/test_h154_export_parity.py',
        'README_H154.md',
    ]
    missing=[x for x in required if not (root/x).is_file()]
    assert not missing,f'H154 missing cumulative dependencies: {missing}'
    page=(root/'pages/7_股神推薦.py').read_text(encoding='utf8')
    for key in ('godpick_h154_export_parity','_h154_audit','_h154_verify',
                'godpick_h1537_manager_visible','godpick_h1539_mainstream_freshness'):
        assert key in page,f'H154 page missing {key}'
    ranker=(root/'godpick_h1539_mainstream_freshness.py').read_text(encoding='utf8')
    assert 'H154研究排序分' in ranker
    sys.path.insert(0,str(root))
    from godpick_h154_export_parity import reconcile_run,append_audit_summary
    from godpick_h1539_mainstream_freshness import audit_mainstream_repetition
    demo=pd.DataFrame([
        {'股票代號':'2618','股票名稱':'長榮航','研究證據分':37,
         '排名等級':'B｜研究待補','反證與待補':'H126禁止買進'},
        {'股票代號':'2002','股票名稱':'中鋼','研究證據分':31,
         '排名等級':'B｜研究待補','反證與待補':'H126禁止買進'}])
    diag=pd.DataFrame([{'股票代號':x,'類別':'航運','H114K線證據日':'2026-11-12',
        'H114K線資料狀態':'VALID_OHLCV','H114K線有效根數':100,
        'H113當日成交額百萬':300,'H114當日成交量張':800,
        'H109決策權威':'UNKNOWN'} for x in ('2618','2002')])
    history=[{'date':'2026-11-09','rows':[{'股票代號':'2618'}]},
             {'date':'2026-11-10','rows':[{'股票代號':'2618'}]}]
    ranked,_,_=audit_mainstream_repetition(demo,diag,
        research_day='2026-11-12',snapshots=history)
    assert ranked.iloc[0]['股票代號']=='2002'
    r=ranked[ranked['股票代號']=='2618'].iloc[0]
    assert float(r['研究證據分'])==37 and float(r['H154研究排序分'])==30
    audit=reconcile_run(tables={'waiting':pd.DataFrame([{'股票代號':'4566','股票名稱':'時碩工業'}]),
                               'h126_authority':pd.DataFrame([{'H126能否買進':'禁止買進'}])},
        ranked=ranked,watch=[],research=ranked,plans=ranked.iloc[:1],six_summary=[],
        h145_status='BLOCKED_FINAL_RESULT_COUNT_GAP',run_id='same',scan_run_id='same')
    assert audit['h126_buyable']==0 and audit['h85_waiting']==1 and audit['ranked']==2
    assert audit['h85_waiting_named']==[('4566','時碩工業')]
    summary=append_audit_summary(pd.DataFrame(columns=['檢查項目','結果','說明']),audit)
    assert 'H154系統H85等待具名清單' in summary['檢查項目'].tolist()
    assert audit['status']=='FORMAL_BLOCKED_H145'
    print('PASS H154 cumulative deployment | research vs H85 vs H126 | score disclosed | permanent data untouched')

if __name__=='__main__':main()
