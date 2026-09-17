# -*- coding: utf-8 -*-
from pathlib import Path
p=Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py'
s=p.read_text(encoding='utf-8')
for token in [
    'H75_EXECUTIVE_EXPECTED_VERSION', 'build_h75_today_ai_table',
    '"01_今日AI推薦"', '"02_今日主流資金"', '"03_推薦證據"', '"04_推薦績效"', '"05_系統健康"',
    '_h75_visible_sheets', '_ws.sheet_state = "hidden"', 'wb.sheetnames.index("01_今日AI推薦")',
    'V191-H75-EXECUTIVE-DECISION-EXPORT',
]:
    assert token in s, token
print('PASS H75 page/excel static')
