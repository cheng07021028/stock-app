# -*- coding: utf-8 -*-
"""H112 regression: sector monopoly, audit radar boundary and Formal safety."""
import os, sys
from pathlib import Path
os.environ['GODPICK_H109_OFFICIAL_FETCH']='0'
os.environ['GODPICK_H109_RISK_HISTORY']='0'
os.environ['GODPICK_H110_SCAN_MODE']='SAFE'
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pandas as pd
from godpick_h112_sector_balance import safe_audit_discovery,balance_research,balance_blackhorse
from godpick_h105_future_blackhorse_engine import _build_blackhorse_overview

def row(code,sector,priority,origin='research',valid=True):
    return {'股票代號':code,'股票名稱':code,'類別':sector,'市場別':'上市',
      'H110最終研究優先分':priority,'H105黑馬預發動分':priority-8,
      'H105黑馬層級':'S1｜BLACKHORSE-SETUP', 'H105發動階段':'BH2｜PRESSURE-BUILDING',
      'H105未來發動窗口':'1～5個交易日','H105今日強勢排除':'否',
      'H105核心證據完整度%':100,'H105過熱追高風險分':15,
      'H109決策權威':'RESEARCH_ELIGIBLE' if valid else 'FADE_VETO',
      'H110證據狀態':'PARTIAL｜來源/資料不足', 'H112候選來源': 'AUDIT_SECONDARY_RADAR' if origin=='audit' else '原Research／Waiting',
      'H107近期族群入選日數':3 if sector=='被動元件' else 0,
      'H108族群生命週期':'IGNITION｜族群點火'}

same=pd.DataFrame([row('3026','被動元件',83.24),row('2472','被動元件',82.60)])
short=balance_blackhorse(same,30)
assert len(short)==1 and short.iloc[0]['股票代號']=='3026',short
eligible=same.copy(); eligible['__h108']=eligible['H110最終研究優先分']
assert len(balance_research(eligible,eligible,2))==1

# Full evidence can reveal non-executable next-wave radar, never create Formal.
audit=pd.DataFrame([
 row('2327','被動元件',83.39,'audit'),row('3548','光學鏡頭',70.32,'audit'),
 row('8039','PCB載板',68.54,'audit'),row('2449','封測',74.0,'audit'),
 row('9999','光通訊',95.0,'audit',False),
 row('8888','散熱',90.0,'audit'),
])
audit.loc[audit['股票代號']=='8888','H105未來發動窗口']='未定'
added=safe_audit_discovery(audit)
assert set(added['股票代號'])=={'2327','3548','8039','2449'},added
assert added['H112研究權限'].str.contains('WATCH_ONLY').all()
source=pd.concat([same,added],ignore_index=True)
out=balance_blackhorse(source,30)
assert '3548' in set(out['股票代號']) and '8039' in set(out['股票代號']),out[['股票代號','類別']]
assert sum(out['類別']=='被動元件')<=2,out[['股票代號','類別']]
assert out.iloc[1]['類別']!='被動元件',out[['股票代號','類別','H112最終跨族群優先分']]
assert all(out.loc[out['H112候選來源'].str.contains('AUDIT'),'H112研究權限'].str.contains('WATCH_ONLY'))
# Inspect exact exported H105 table contract, without a Page7 rewrite.
overview=_build_blackhorse_overview({'research':same,'waiting':pd.DataFrame(),'actionable':pd.DataFrame(),'h112_audit_radar':added})
assert overview['類別'].nunique()>=3,overview[['股票代號','類別']]
assert 'H112候選來源' in overview.columns
print('H112_SECTOR_BALANCE_REGRESSION PASS')
