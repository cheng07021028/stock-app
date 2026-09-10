# -*- coding: utf-8 -*-
import sys, types
# Isolated modified-file package does not include unchanged perf module. Stub it
# only for this smoke test; production uses the project's real module.
stub=types.ModuleType('godpick_perf_fast_update_v77')
stub.update_record_perf=lambda row,quote,track_days=None: dict(row)
sys.modules['godpick_perf_fast_update_v77']=stub
from godpick_t1_trade_truth import build_h66_rank_learning_summary, build_probability_calibration, TRUTH_VERSION
rows=[]
for day in ('2026-09-01','2026-09-02','2026-09-03'):
    for rank in range(1,11):
        alpha=2.5-(rank-1)*0.35
        rows.append({
            'T1成熟':True,'推薦日期':day,'股票代號':f'{day[-2:]}{rank:02d}',
            'H66T1全市場順位':rank,'H66T1自適應排序分':100-rank*4,
            'H66T1層級':'A1｜T+1優先觀察' if rank<=3 else 'A2｜T+1次優先' if rank<=6 else 'B1｜T+1等待確認',
            '隔日候選漲跌%':alpha+0.2,'Selection Alpha%':alpha,'推薦後2日%':alpha+0.5,
            'SuperAI校準後隔日上漲機率%':70-rank,
            '是否納入可執行績效':False,
        })
summary=build_h66_rank_learning_summary(rows)
assert summary['H66排名成熟交易日']==3
assert summary['H66平均RankIC'] is not None and summary['H66平均RankIC'] > 0.9
assert summary['H66_Top1正報酬率%']==100.0
assert summary['H66_Top3平均SelectionAlpha%'] > summary['H66_Top10平均SelectionAlpha%']
assert summary['H66_A1T2成熟樣本']==9
cal=build_probability_calibration(rows)
assert 'naive_base_rate_brier_score' in cal and 'brier_skill_vs_base_rate_pct' in cal
assert 'h67' in TRUTH_VERSION.lower()
print('PASS H66 T1 rank learning', summary)
