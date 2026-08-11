# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _load_rows() -> list[dict]:
    payload = json.loads((ROOT / "godpick_latest_recommendations.json").read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ["recommendations", "records", "data", "items", "rows"]:
            if isinstance(payload.get(key), list):
                return payload[key]
    return []


def main() -> None:
    import sys
    sys.path.insert(0, str(ROOT))
    import godpick_formal_recommendation_engine as formal

    rows = _load_rows()
    assert rows, "packaged recommendation fixture missing"
    base = pd.DataFrame(rows)
    # 57 * 10 ~= 570 rows is enough to catch accidental Series.apply regressions
    sample = pd.concat([base] * 10, ignore_index=True)
    started = time.perf_counter()
    out = formal.apply_formal_recommendation_engine(sample)
    elapsed = time.perf_counter() - started
    assert len(out) == len(sample)
    assert {"正式推薦分區", "股神推薦優先分", "AI模型版本"}.issubset(out.columns)
    # This environment runs the optimized path in roughly 4-5s. Keep generous
    # headroom so the smoke test detects a return to the old 10s+/500-row path
    # without becoming hardware-flaky.
    assert elapsed < 9.0, f"V181 final engine performance regression: {elapsed:.3f}s / {len(sample)} rows"

    page7 = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'decision_frame_store_v181' in page7
    assert 'decision_frame_scan_signature_v181' in page7
    assert 'v181_decision_cache_hits' in page7
    assert 'and not _v181_ai_ready' in page7, "duplicate learning overlay guard missing"
    assert 'K線掃描完成' in page7 and '最終結果運算' in page7
    assert '_page07_record_authority_executor_v181' in page7
    assert '_v159_auto_record_actionable_recommendations(auto_source, background_write=False)' in page7, 'V185 requires synchronous permanent authority verification for daily recommendation records'
    print(f"PASS page7 V181 final pipeline｜rows={len(sample)}｜elapsed={elapsed:.3f}s")


if __name__ == "__main__":
    main()
