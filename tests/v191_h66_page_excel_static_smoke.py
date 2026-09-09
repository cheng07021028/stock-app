# -*- coding: utf-8 -*-
from pathlib import Path
p=Path(__file__).resolve().parents[1]/"pages"/"7_股神推薦.py"
s=p.read_text(encoding="utf-8")
checks=[
    'H66_TIMING_EXPECTED_VERSION = "v191_h66_adaptive_alpha_t1_timing_truth_20260909"',
    'from godpick_h66_adaptive_timing_engine import',
    'apply_h66_adaptive_timing',
    'build_h66_t1_timing_table',
    'build_h66_learning_governance_table',
    '"_H66T1優先", "H66T1自適應排序分"',
    'render_pro_section("超級AI T+1時機｜H66 Adaptive Alpha Ranking")',
    '("T+1時機雷達", h66_t1_df',
    '("H66學習治理", h66_learning_df',
    '"H66平均RankIC"',
    '"brier_skill_vs_base_rate_pct"',
    'V191-H66-ADAPTIVE-ALPHA-T1-TIMING-TRUTH-EXCEL',
]
missing=[x for x in checks if x not in s]
assert not missing, missing
# H66 is never inserted into the Formal authority condition.
formal_fragment='正式推薦仍須H64有效Formal＋H56盤前＋Entry/守價＋RR'
assert formal_fragment in s
print('PASS H66 page/excel static smoke')
