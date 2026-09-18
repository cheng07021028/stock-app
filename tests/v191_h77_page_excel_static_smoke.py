# -*- coding: utf-8 -*-
from pathlib import Path


root = Path(__file__).resolve().parents[1]
engine = (root / "godpick_h77_verified_delta_execution_gate.py").read_text(encoding="utf-8")
page = (root / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")

required = [
    "H77_VERIFIED_DELTA_EXPECTED_VERSION",
    "apply_h77_verified_delta_execution_gate",
    "01_今日驗證Alpha",
    "02_等待與核心監控",
    "04_驗證與風險證據",
    "05_績效煞車與健康",
    "V191-H77-VERIFIED-DELTA-CHASE-ENTRY-PERFORMANCE-BRAKE",
]
for token in required:
    assert token in page, token

assert "2382" not in engine and "廣達" not in engine
assert "H64/H63" in engine and "H68" in engine
assert "今日漲幅%" in engine and "Entry進場買點分" in engine
print("PASS H77 Page07/Excel static integration")
