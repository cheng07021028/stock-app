# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))

from godpick_headless_page_loader import load_page_namespace

ns=load_page_namespace(ROOT/'pages'/'7_股神推薦.py',base_dir=ROOT)
st=ns['__headless_st__']
k=ns['_k']

# Isolate the persistence side effect; this test inspects exactly what Page07 would hand to Page08 authority.
captured=[]
ns['_v191_actionable_tracking_frame']=lambda df:(df.copy(),True,['H9 synthetic partition'])
ns['assess_individual_sample_quality']=lambda row:(True,'ok','高')
ns['_append_godpick_records']=lambda rows,force_duplicate=False:(captured.extend(rows) or len(rows),['captured'])

st.session_state[k('recommend_execution_context_v191')]={
    'owner':'07_股神推薦','trigger':'H9模擬','started_at':'2026-08-12 14:42:10',
    'run_date':'2026-08-12','run_id':'gprun_h9_20260812_144210','automation_version':'V191-H9'
}

df=pd.DataFrame([{
    '股票代號':'5347','股票名稱':'世界','市場別':'上櫃','類別':'半導體業',
    '正式推薦分區':'盤中雷達追蹤','盤中雷達優先級':'R1-P｜強勢前兆核心雷達',
    '推薦模式':'飆股模式','推薦日期':'2026-08-11','最新價':160.0,
    '實戰觸發價':168.5,'觸發後守價':165.5,'主要進場路徑':'強勢前兆待觸發',
    '正式推薦版本':'V188'
}])
added,msgs=ns['_v159_auto_record_actionable_recommendations'](df,background_write=False)
assert added==1,(added,msgs,captured)
assert len(captured)==1,captured
r=captured[0]
# The historical first-seen date is preserved for provenance, while the actual Page07 batch is today's authority date.
assert r['原始推薦日期']=='2026-08-11',r
assert r['推薦日期']=='2026-08-12',r
assert r['推薦批次日期']=='2026-08-12',r
assert r['推薦執行ID']=='gprun_h9_20260812_144210',r
assert r['推薦執行來源']=='07_股神推薦',r
assert r['推薦執行版本']=='V191-H9',r
print('PASS H9 Page07 run provenance | first_seen=2026-08-11 | cohort=2026-08-12 | run_id preserved')
