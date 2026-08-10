# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib
import json
import sys
import tempfile
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    # Minimal Streamlit stub: persistence only needs secrets for this local test.
    fake = types.ModuleType("streamlit")
    fake.secrets = {}
    sys.modules.setdefault("streamlit", fake)
    sys.path.insert(0, str(ROOT))
    svc = importlib.import_module("godpick_persistence_service")

    assert callable(getattr(svc, "restore_records_snapshot", None))
    assert callable(getattr(svc, "recover_records_authority", None))

    with tempfile.TemporaryDirectory(prefix="godpick_v181_") as td:
        old_base = svc.BASE_DIR
        svc.BASE_DIR = Path(td)
        try:
            rows = [
                {"record_id": "dup", "股票代號": "2330", "推薦日期": "2026-08-06", "推薦模式": "股神推薦", "推薦價格": 100},
                {"record_id": "dup", "股票代號": "2330", "推薦日期": "2026-08-07", "推薦模式": "股神推薦", "推薦價格": 101},
            ]
            ok, msg = svc.restore_records_snapshot(rows, source="firestore_test")
            assert ok, msg
            restored = json.loads((Path(td) / svc.RECORDS_FILE).read_text(encoding="utf-8"))
            assert len(restored) == 2
            assert len({r["record_id"] for r in restored}) == 2, "duplicate IDs were not repaired"
            assert (Path(td) / svc.RECORDS_STATE_FILE).exists()
            assert (Path(td) / svc.RECORDS_MANIFEST_FILE).exists()
        finally:
            svc.BASE_DIR = old_base
    print("PASS persistence V181 restore_records_snapshot compatibility")


if __name__ == "__main__":
    main()
