# -*- coding: utf-8 -*-
from pathlib import Path
p=Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py'
s=p.read_text(encoding='utf-8')
for token in [
    'H76_DAILY_ALPHA_EXPECTED_VERSION', 'apply_h76_daily_alpha_core_split', 'build_h76_daily_alpha_table',
    'build_h76_structural_core_table', 'build_h76_evidence_table',
    '"01_今日新Alpha"', '"02_結構核心監控"', '"03_今日主流資金"', '"04_推薦證據"', '"05_績效與健康"',
    '_h76_visible_sheets', 'wb.sheetnames.index("01_今日新Alpha")',
    'V191-H76-DAILY-ALPHA-CORE-SPLIT-REPEAT-EVIDENCE',
    '技術診斷｜H73 Leadership Breadth（非每日推薦榜）',
]:
    assert token in s, token
assert 'page07_v191_h76_daily_alpha_core_split_repeat_evidence_20260918' in s
print('PASS H76 page/excel static')

h76=(Path(__file__).resolve().parents[1]/'godpick_h76_daily_alpha_core_split.py').read_text(encoding='utf-8')
assert '2382' not in h76 and '廣達' not in h76
print('PASS H76 no stock hard-code')
