# -*- coding: utf-8 -*-
from datetime import date
from pathlib import Path
import sys
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_headless_page_loader import load_page_namespace
ns=load_page_namespace('pages/10_推薦清單.py',base_dir=ROOT)
# N-day backfill: deterministic one-row mature candidate.
row={"推薦日期":"2026-07-01","股票代號":"2330","股票名稱":"台積電","推薦價格":100.0}
df=pd.DataFrame([row])
ns['_row_needs_formal_n_day_update_v98']=lambda payload:True
ns['_calc_formal_n_day_metrics_v98']=lambda payload:{"推薦後1日%":1.2,"推薦後3日%":2.5,"追蹤更新時間":"2026-08-12 21:00:00"}
out,summary=ns['_update_formal_n_day_metrics_v98'](df,max_rows=300,show_progress=False)
assert summary['processed']==1 and summary['success']==1,summary
assert float(out.iloc[0]['推薦後1日%'])==1.2
# Next-day hit tracking: exact page function, mocked market history result.
ns['_calc_night_hit_for_row_v101']=lambda row,timeout=7:{"作戰追蹤狀態":"完成","進場點命中":"是","突破價命中":"否","作戰追蹤更新時間":"2026-08-12 21:01:00"}
out2,hit=ns['_update_night_hit_tracking_v101'](df,max_rows=300,show_progress=False)
assert hit['processed']==1 and hit['success']==1,hit
assert out2.iloc[0]['進場點命中']=='是'
print('PASS V191 Page10 adapters | N-day=1 | hit-tracking=1')
