# -*- coding: utf-8 -*-
from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pandas as pd
from godpick_h105_future_blackhorse_engine import decorate_decision_tables
from godpick_h114_parallel_alpha import analyze_history

bars=[]
for i,dt in enumerate(pd.bdate_range(end='2026-10-13',periods=32)):
    p=90+i*0.2
    bars.append({'日期':str(dt.date()),'開盤價':p,'最高價':p+1.1,'最低價':p-.8,'收盤價':p+.35,'成交股數':1800000})
tech=analyze_history(pd.DataFrame(bars),asof='2026-10-13')
base={'股票代號':'1234','股票名稱':'合成測試','類別':'其他電子','市場別':'上市',
      '成交額百萬':350,'20日均成交額百萬':320,'最新成交量_張':1800,'20日均量_張':1800,
      'K線最後交易日':'2026-10-13','K線日期驗證基準':'2026-10-13',
      'H105黑馬預發動分':70,'H108族群生命週期':'PREHEAT'}
original=pd.DataFrame([{**base,'股票代號':'5678'}]);candidate=pd.DataFrame([{**base,**tech}])
tables={'actionable':original.copy(),'research':pd.DataFrame([base]),'waiting':pd.DataFrame(),
        'audit':pd.DataFrame([base]), 'health':pd.DataFrame([{'項目':'原紀錄','數值':'保留'}])}
out=decorate_decision_tables(tables,candidate_df=candidate)
assert 'h114_parallel_watch' in out
assert 'H114K線資料狀態' in out['research'].columns
assert 'H114量縮墊高' in out['audit'].columns
assert 'H114版本' in out['health']['項目'].astype(str).to_list()
assert out['actionable']['股票代號'].astype(str).tolist()==original['股票代號'].astype(str).tolist()
assert set(out['research'].columns)>= {'H114研究優先分','H114主導策略','H114入榜身分'} or out['research'].empty
print('H114 H105 pipeline integration PASS; research rows',len(out['research']),'formal unchanged')
