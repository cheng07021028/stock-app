# -*- coding: utf-8 -*-
from pathlib import Path
p=Path(__file__).resolve().parents[1]/"pages"/"7_股神推薦.py"
s=p.read_text(encoding="utf-8")
checks=[
    "v191_h73_leadership_breadth_distribution_truth_20260916",
    "apply_h73_leadership_breadth",
    '"_H73研究優先", "H73研究排序分"',
    "H73領先共振",
    "H73治理摘要",
    "H73分布矛盾中位扣分",
    "H73只做研究排序",
    "V191-H73-LEADERSHIP-BREADTH-DISTRIBUTION-TRUTH-20260916",
]
for x in checks:
    assert x in s, x
assert s.index('"_H64有效Formal優先", "_H73研究優先"') >= 0
print("PASS H73 Page07/Excel static authority")
