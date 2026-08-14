# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "pages" / "7_股神推薦.py"

def _helper_source() -> str:
    src = PAGE.read_text(encoding="utf-8")
    start = src.index("def _v191_h23_missing_json_value_mask")
    end = src.index("def _records_to_df_for_json", start)
    return src[start:end]

def _load_helpers():
    ns = {"pd": pd, "json": __import__("json"), "Any": object}
    src = _helper_source()
    # Strip only the session-state diagnostic block dependency by giving
    # no-op stand-ins; serializer behavior itself is executed unchanged.
    class _State(dict): pass
    class _St: session_state = _State()
    ns.update({"st": _St(), "_k": lambda x: x, "_now_text": lambda: "2026-08-15 00:00:00"})
    exec(src, ns, ns)
    return ns

def test_duplicate_columns_coalesce_rightmost_nonblank():
    ns = _load_helpers()
    df = pd.DataFrame([["oldA", "newA", 1], ["oldB", "", 2]], columns=["官方資料日期", "官方資料日期", "score"])
    clean, dups = ns["_v191_h23_unique_json_frame"](df)
    assert list(clean.columns) == ["官方資料日期", "score"]
    assert dups == ["官方資料日期"]
    assert clean["官方資料日期"].tolist() == ["newA", "oldB"]
    rows = ns["_df_to_records_for_json"](df)
    assert rows[0]["官方資料日期"] == "newA"
    assert rows[1]["官方資料日期"] == "oldB"

def test_unique_columns_unchanged():
    ns = _load_helpers()
    df = pd.DataFrame([{"a": 1, "b": "x"}])
    rows = ns["_df_to_records_for_json"](df)
    assert rows == [{"a": 1, "b": "x"}]
