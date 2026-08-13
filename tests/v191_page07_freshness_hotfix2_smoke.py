# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_headless_page_loader import load_page_namespace
import official_factor_service as ofs
ns=load_page_namespace('pages/7_股神推薦.py',base_dir=ROOT)
row={'股票代號':'2330','官方資料日期':'20260811','官方因子資料日期':'20260811','法人資料日期':'20260812','估值資料日期':'20260812'}
fixed=ofs._repair_derived_daily_dates_v191({'data_date':'20260811','meta':{'data_date':'20260811'},'records':[row]})
ns['load_factor_cache']=lambda:fixed
ns['_load_latest_recommendation_authority_v185']=lambda:({
    'saved_at':'2026-08-13 10:30:00','kline_date':'2026-08-13',
    'recommendations':[{'股票代號':'2330','本輪市場最新交易日':'2026-08-13'}],
    'candidate_diagnosis':[]
},[])
orig_read=ns['_read_project_json_file']
def fake_read(path):
    text=str(path)
    if 'market_snapshot' in text: return {'market_date':'2026-08-13'}
    return orig_read(path)
ns['_read_project_json_file']=fake_read
ns['_expected_latest_trade_date_v173']=lambda:pd.Timestamp('2026-08-12')
snap=ns['_project_data_freshness_snapshot_v173']()
assert snap['official_date']=='2026-08-12',snap
assert snap['official_lag']==1,snap
assert not snap['hard_block'],snap
assert snap['ready'],snap
assert '盤中' in snap.get('kline_display',''),snap
print('PASS Page07 freshness: 8/13 live K-line + 8/12 official is normal T-1, stale 8/11 composite repaired')
