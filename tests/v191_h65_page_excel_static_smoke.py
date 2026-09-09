# -*- coding: utf-8 -*-
from pathlib import Path
import ast

ROOT=Path(__file__).resolve().parents[1]
PAGE=ROOT/'pages'/'7_股神推薦.py'
text=PAGE.read_text(encoding='utf-8')
ast.parse(text)

assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h65_multifactor_observation_radar_20260909"' in text
assert 'EXCEL_COLUMN_LAYOUT_VERSION = "V191-H65-MULTIFACTOR-OBSERVATION-RADAR-20260909"' in text
assert 'from godpick_h65_multifactor_observation_engine import (' in text
assert 'apply_h65_multifactor_observation' in text
assert 'build_h65_observation_radar_table' in text
assert 'build_h65_indicator_coverage_table' in text
assert '"_H64有效Formal優先", "_H65觀察優先", "H65多因子觀察分"' in text
assert '"多因子觀察雷達", h65_observation_df' in text
assert '"多因子指標覆蓋", h65_coverage_df' in text
assert 'H65 Excel保留10個核心活頁' in text
assert 'W1/W2/W3只做觀察研究；正式推薦仍須H64有效Formal' in text
# Formal export is still H63/H64-owned, never built from H65 W tiers.
assert 'formal_execution_df = build_h63_formal_execution_table' in text
assert 'build_h63_formal_execution_table(h65_observation_df' not in text
print('PASS H65 Page07 ranking/UI/10-sheet Excel static integration smoke')
