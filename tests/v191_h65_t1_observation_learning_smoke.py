# -*- coding: utf-8 -*-
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))

from godpick_t1_trade_truth import TRUTH_VERSION, build_h57_h60_learning_summary, _truth_from_updated

EXPECTED_TRUTH="godpick_t1_trade_truth_v191_h65_multifactor_observation_learning_20260909"


def main():
    assert TRUTH_VERSION==EXPECTED_TRUTH
    rows=[
        {'T1成熟':True,'H65觀察層級':'W1｜重點觀察推薦','隔日候選漲跌%':2.0,'Selection Alpha%':1.0},
        {'T1成熟':True,'H65觀察層級':'W2｜提前卡位觀察','隔日候選漲跌%':-1.0,'Selection Alpha%':-0.5},
        {'T1成熟':True,'H65觀察層級':'W3｜候選追蹤','隔日候選漲跌%':1.0,'Selection Alpha%':0.3},
    ]
    s=build_h57_h60_learning_summary(rows)
    assert s['H65_W1成熟樣本']==1 and s['H65_W1正報酬率%']==100.0
    assert s['H65_W2成熟樣本']==1 and s['H65_W2正報酬率%']==0.0
    assert s['H65_W3成熟樣本']==1 and s['H65_W3平均SelectionAlpha%']==0.3
    original={
        '推薦日期':'2026-09-08','股票代號':'2330','股票名稱':'測試',
        'H65觀察層級':'W1｜重點觀察推薦','H65觀察推薦':'是｜重點觀察',
        'H65多因子觀察分':82,'H65全市場觀察百分位%':95,'H65資料覆蓋%':88,'H65風險扣分':2,
        'H65版本':'v191_h65_multifactor_observation_radar_20260909',
    }
    rec=_truth_from_updated(original,{'隔日候選漲跌%':1.2},{'history':[]},0.3)
    assert rec['H65觀察層級'].startswith('W1')
    assert rec['H65多因子觀察分']==82.0 and rec['H65資料覆蓋%']==88.0
    assert rec['H65版本']=='v191_h65_multifactor_observation_radar_20260909'
    print('PASS H65 T+1 observation cohort learning smoke')

if __name__=='__main__': main()
