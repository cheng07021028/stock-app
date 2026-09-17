# -*- coding: utf-8 -*-
from pathlib import Path
p=Path(__file__).resolve().parents[1]/"pages"/"7_股神推薦.py"
s=p.read_text(encoding="utf-8")
checks=[
    'H74_FRESH_CAPITAL_EXPECTED_VERSION = "v191_h74_fresh_mainstream_capital_rotation_truth_20260917"',
    'render_pro_section("AI決策總覽｜H74 新鮮主流 × 資金加速")',
    '("AI決策總覽", h74_overview_df',
    '("Excel閱讀指南", h74_guide_df',
    '("H74新鮮主流資金", h74_research_df',
    '"_H64有效Formal優先", "_H74研究優先", "H74決策總分"',
    'V191-H74-FRESH-MAINSTREAM-CAPITAL-ROTATION-TRUTH-EXCEL',
    'PAGE07_SPEED_FIX_VERSION = "page07_v191_h74_fresh_mainstream_capital_rotation_truth_20260917"',
]
missing=[x for x in checks if x not in s]
assert not missing, missing
# Excel must put the decision overview before old H64/H73 detail sheets.
assert s.index('("AI決策總覽", h74_overview_df') < s.index('("超級AI最終決策", final_decision_df')
print("H74 PAGE/EXCEL STATIC: PASS")
