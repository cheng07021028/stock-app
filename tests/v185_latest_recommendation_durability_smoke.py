from __future__ import annotations

import ast
import copy
import sys
import types
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGE7 = ROOT / "pages" / "7_股神推薦.py"


def _load_persistence_with_streamlit_stub():
    if "streamlit" not in sys.modules:
        st = types.ModuleType("streamlit")
        st.secrets = {}
        sys.modules["streamlit"] = st
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import godpick_persistence_service as gps
    return gps


def test_business_date_beats_deployment_timestamp():
    gps = _load_persistence_with_streamlit_stub()
    old = {"saved_at": "2026-07-09 16:14:38", "recommendation_date": "2026-07-09"}
    new = {"saved_at": "2026-08-11 16:30:00", "recommendation_date": "2026-08-11"}
    old_state = {"updated_at_epoch": 9_999_999_999.0, "revision": 9_999_999_999_000_000_000}
    new_state = {"updated_at_epoch": 1.0, "revision": 1}
    assert gps._named_json_authority_key(
        "godpick_latest_recommendations.json", "firestore", new, new_state, datetime.min
    ) > gps._named_json_authority_key(
        "godpick_latest_recommendations.json", "local", old, old_state, datetime.now()
    )


def test_loader_selects_newer_business_snapshot_even_if_old_local_state_is_newer():
    gps = _load_persistence_with_streamlit_stub()
    old = {"saved_at": "2026-07-09 16:14:38", "recommendation_date": "2026-07-09", "recommendations": [{"股票代號": "1111"}]}
    new = {"saved_at": "2026-08-11 16:30:00", "recommendation_date": "2026-08-11", "recommendations": [{"股票代號": "2330"}]}
    old_state = {"payload_hash": gps._json_hash(old), "updated_at_epoch": 9_999_999_999.0, "revision": 9_999_999_999_000_000_000}
    new_state = {"payload_hash": gps._json_hash(new), "updated_at_epoch": 1.0, "revision": 1}

    original = {
        "read_local_json": gps.read_local_json,
        "read_github_json": gps.read_github_json,
        "_read_named_firestore": gps._read_named_firestore,
        "write_local_json_atomic": gps.write_local_json_atomic,
    }
    try:
        def read_local(name, default):
            if name == "godpick_latest_recommendations.json":
                return copy.deepcopy(old), "local old", datetime.now()
            if name == "godpick_latest_recommendations_sync_state.json":
                return copy.deepcopy(old_state), "local state", datetime.now()
            return copy.deepcopy(default), "local missing", datetime.min

        def read_github(name, default):
            if name == "godpick_latest_recommendations.json":
                return copy.deepcopy(old), "github old"
            if name == "godpick_latest_recommendations_sync_state.json":
                return copy.deepcopy(old_state), "github state"
            return copy.deepcopy(default), "github missing"

        gps.read_local_json = read_local
        gps.read_github_json = read_github
        gps._read_named_firestore = lambda doc, default: (copy.deepcopy(new), "firestore new", datetime.min, copy.deepcopy(new_state))
        gps.write_local_json_atomic = lambda *args, **kwargs: (True, "ok")
        payload, details = gps.load_named_json_permanent("godpick_latest_recommendations.json", {})
        assert payload["saved_at"].startswith("2026-08-11"), (payload, details)
        assert payload["recommendations"][0]["股票代號"] == "2330"
        assert details[0] == "權威來源：firestore"
    finally:
        for name, value in original.items():
            setattr(gps, name, value)


def test_page7_wiring_prevents_0709_rollback_and_requires_verified_history_write():
    src = PAGE7.read_text(encoding="utf-8")
    assert 'GODPICK_LATEST_ANCHOR_FILE = "godpick_latest_run_anchor.json"' in src
    assert 'persist_json_permanent as _persist_anchor_v185' in src
    assert '未設定 GitHub/Firebase 遠端永久層' in src
    assert 'return bool(local_ok and anchor_ok), msgs' in src
    assert '_load_latest_recommendation_authority_v185()' in src
    assert 'background_write=False' in src
    assert 'latest_pack_permanent_ok' in src
    assert 'full_snapshot_pending_or_older' in src


def main():
    test_business_date_beats_deployment_timestamp()
    test_loader_selects_newer_business_snapshot_even_if_old_local_state_is_newer()
    test_page7_wiring_prevents_0709_rollback_and_requires_verified_history_write()
    print("PASS V185 latest recommendation durability｜business-date authority, durable run anchor, no 7/9 rollback, synchronous history authority")


if __name__ == "__main__":
    main()
