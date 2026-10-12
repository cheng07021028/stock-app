# -*- coding: utf-8 -*-
"""H110 offline: safe as-of, source/flow integrity, caps, Formal safety, Excel contract."""
import datetime as dt
import os
import sys
from pathlib import Path

os.environ['GODPICK_H109_OFFICIAL_FETCH'] = '0'
os.environ['GODPICK_H109_RISK_HISTORY'] = '0'
os.environ['GODPICK_H110_SCAN_MODE'] = 'SAFE'
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
from godpick_h110_evidence_integrity import VERSION, audit, research_asof
from godpick_h109_authority_governor import join_official
from godpick_h105_future_blackhorse_engine import _diversified_pick, decorate_decision_tables

asof, mode, live = research_asof('2026-10-08')
assert (asof, live) == (dt.date(2026,10,8),False), (asof,mode)
assert 'HISTORICAL' in mode

row={'股票代號':'3090', 'H109最終研究優先分':80, '營收資料日期':'202608',
     'H105主要理由':'三大法人短中期同向買超', '三大法人近3日合計':200, '三大法人近5日合計':-90}
audit_result=audit(row,market_day=asof,info_asof=dt.date(2026,10,11),asof_mode=mode)
assert audit_result['H110證據狀態'].startswith('CONFLICT'),audit_result
assert audit_result['H110最終研究優先分'] < 80
assert audit_result['H110營收資料年月']=='202608'
assert audit_result['H110主榜准入'].startswith('否')

official={"H109營收資料年月":"202609","H109營收出表日期":"2026-10-08","H109官方營收YoY%":"+25.5", "H109官方營收MoM%":"3.2", "H109營收來源":"https://openapi.twse.com.tw/v1/opendata/t187ap05_L"}
joined=join_official({'營收資料日期':'202609'},official)
assert joined['月營收YoY%_官方']==25.5 and joined['H109營收來源']==official['H109營收來源']
assert '月營收YoY%_官方' not in join_official({'營收資料日期':'202610'},official)
rev=audit(dict(joined,**{'H109最終研究優先分':80}),market_day=asof,info_asof=dt.date(2026,10,8),asof_mode=mode)
assert rev['H110營收來源驗證']=='TWSE_公告日期已核對',rev
future=audit({**joined,'H109最終研究優先分':80, 'H109營收出表日期':'2026-10-11'},market_day=asof,info_asof=asof,asof_mode=mode)
assert future['H110證據狀態'].startswith('CONFLICT'),future

# Top5 cap 2 for confirmed ignition; never use phase-10 override in top5.
ranks=pd.DataFrame([{'股票代號':f'99{i:02d}','類別':'被動元件' if i<=3 else f'族群{i}',
   'H108族群生命週期':'IGNITION｜族群點火','H108族群確認分':85} for i in range(1,6)])
selected=_diversified_pick(ranks,5)
assert (selected['類別']=='被動元件').sum()==2,selected[['股票代號','類別']]
# If no other sector exists, output may be short, never silently bypass the cap.
assert len(_diversified_pick(ranks.iloc[:3],5))==2

# Full decision tables: Formal must remain untouched and H110 must enter export schema.
from h108_full_regression import future as sample
rows=[sample('6175','立敦','被動元件'),sample('3090','日電貿','被動元件'),sample('3264','欣銓','半導體')]
for item in rows:
    item['H99市場資料日']='2026-10-08'
base=pd.DataFrame(rows)
result=decorate_decision_tables({'research':base.copy(),'waiting':pd.DataFrame(),
                                 'actionable':base.iloc[:1].copy(),'health':pd.DataFrame()},candidate_df=base)
assert result['actionable']['股票代號'].astype(str).tolist()==['6175']
assert 'H110最終研究優先分' in result['blackhorse_overview'].columns
assert 'H110證據狀態' in result['research'].columns
assert (result['research']['H110研究資訊截止日']=='2026-10-08').all()
assert result['health']['項目'].eq('H110交易日/資訊截止日').any()
print('H110_EVIDENCE_INTEGRITY_REGRESSION PASS')
