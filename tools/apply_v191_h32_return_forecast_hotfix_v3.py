# -*- coding: utf-8 -*-
"""V191-H32 v3 integration + zero-return truth fix."""
from __future__ import annotations
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

from apply_v191_h32_return_forecast_hotfix_v2 import main as apply_v2

FORECAST = ROOT / "godpick_return_forecast_engine.py"


def patch_zero_return_truth() -> None:
    src = FORECAST.read_text(encoding="utf-8-sig")
    old = '''def _truth_actual(row: dict[str, Any], horizon: int) -> float | None:\n    if horizon == 1:\n        return _f(row.get("隔日候選漲跌%") or row.get("推薦後1日%"))\n    for key in (\n'''
    new = '''def _truth_actual(row: dict[str, Any], horizon: int) -> float | None:\n    if horizon == 1:\n        # H32-v3: 0.00% is a valid realized return, never a missing value.\n        # Using ``a or b`` silently discards zero and biases validation samples.\n        first = _f(row.get("隔日候選漲跌%"))\n        if first is not None:\n            return first\n        return _f(row.get("推薦後1日%"))\n    for key in (\n'''
    if new in src:
        return
    if old not in src:
        raise RuntimeError("H32-v3 zero-return anchor not found")
    FORECAST.write_text(src.replace(old, new, 1), encoding="utf-8")


def main() -> None:
    apply_v2()
    patch_zero_return_truth()
    print("PASS V191-H32 v3 integration + zero-return truth fix")


if __name__ == "__main__":
    main()
