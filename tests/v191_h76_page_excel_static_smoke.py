# -*- coding: utf-8 -*-
from pathlib import Path
p=Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py'
s=p.read_text(encoding='utf-8')
for token in [
    'H76_DAILY_ALPHA_EXPECTED_VERSION', 'apply_h76_daily_alpha_core_split', 'build_h76_daily_alpha_table',
    'build_h76_structural_core_table', 'build_h76_evidence_table',
    '"01_今日驗證Alpha"', '"02_等待與核心監控"', '"03_今日主流資金"', '"04_驗證與風險證據"', '"05_績效煞車與健康"',
    '_h77_visible_sheets', 'wb.sheetnames.index("01_今日驗證Alpha")',
    'V191-H76-DAILY-ALPHA-CORE-SPLIT-REPEAT-EVIDENCE',
    '技術診斷｜H73 Leadership Breadth（非每日推薦榜）',
]:
    assert token in s, token
assert 'page07_v191_h77_verified_delta_chase_entry_performance_brake_20260918' in s
print('PASS H76 compatibility under H77 page/excel')

h76=(Path(__file__).resolve().parents[1]/'godpick_h76_daily_alpha_core_split.py').read_text(encoding='utf-8')
assert '2382' not in h76 and '廣達' not in h76
print('PASS H76 no stock hard-code')
