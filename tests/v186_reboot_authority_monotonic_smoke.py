from __future__ import annotations

import copy
import os
import sys
import tempfile
import types
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGE7 = ROOT / "pages" / "7_股神推薦.py"
OFFICIAL = ROOT / "official_factor_service.py"


def _load_persistence():
    if "streamlit" not in sys.modules:
        st = types.ModuleType("streamlit")
        st.secrets = {}
        sys.modules["streamlit"] = st
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import godpick_persistence_service as gps
    return gps


def _factor_payload(date: str, code: str = "2330") -> dict:
    return {
        "version": "test",
        "updated_at": f"{date} 18:30:00",
        "data_date": date,
        "record_count": 1,
        "records": [{"股票代號": code, "官方因子資料日期": date, "官方資料完整度": 90}],
        "meta": {"data_date": date},
    }


def test_business_date_beats_reboot_timestamp_for_official_factor():
    gps = _load_persistence()
    old = _factor_payload("2026-07-11", "1111")
    new = _factor_payload("2026-08-11", "2330")
    old_state = {"updated_at_epoch": 9_999_999_999.0, "revision": 9_999_999_999_000_000_000}
    new_state = {"updated_at_epoch": 1.0, "revision": 1}
    assert gps._named_json_authority_key(
        "official_factors_cache.json", "local", new, new_state, datetime.min
    ) > gps._named_json_authority_key(
        "official_factors_cache.json", "github", old, old_state, datetime.now()
    )


def test_new_legacy_local_beats_old_valid_remote():
    """A missing sidecar must never let a stale remote cache roll back a newer local cache."""
    gps = _load_persistence()
    local_new = _factor_payload("2026-08-11", "2330")
    remote_old = _factor_payload("2026-07-11", "1111")
    remote_state = {
        "payload_hash": gps._json_hash(remote_old),
        "updated_at_epoch": 9_999_999_999.0,
        "revision": 9_999_999_999_000_000_000,
    }
    original = {
        "read_local_json": gps.read_local_json,
        "read_github_json": gps.read_github_json,
        "_read_named_firestore": gps._read_named_firestore,
        "write_local_json_atomic": gps.write_local_json_atomic,
    }
    try:
        def read_local(name, default):
            if name == "official_factors_cache.json":
                return copy.deepcopy(local_new), "local new without state", datetime.now()
            if name == "official_factors_cache_sync_state.json":
                return {}, "no local state", datetime.min
            return copy.deepcopy(default), "missing", datetime.min

        def read_github(name, default):
            if name == "official_factors_cache.json":
                return copy.deepcopy(remote_old), "remote old"
            if name == "official_factors_cache_sync_state.json":
                return copy.deepcopy(remote_state), "remote valid state"
            return copy.deepcopy(default), "missing"

        gps.read_local_json = read_local
        gps.read_github_json = read_github
        gps._read_named_firestore = lambda doc, default: (copy.deepcopy(default), "none", datetime.min, {})
        gps.write_local_json_atomic = lambda *args, **kwargs: (True, "ok")
        payload, details = gps.load_named_json_permanent("official_factors_cache.json", {})
        assert payload["data_date"] == "2026-08-11", (payload, details)
        assert payload["records"][0]["股票代號"] == "2330"
        assert details[0] == "權威來源：local"
    finally:
        for name, value in original.items():
            setattr(gps, name, value)


def test_new_runtime_remote_restores_old_packaged_local():
    gps = _load_persistence()
    local_old = _factor_payload("2026-07-11", "1111")
    remote_new = _factor_payload("2026-08-11", "2330")
    remote_state = {
        "payload_hash": gps._json_hash(remote_new),
        "updated_at_epoch": 2.0,
        "revision": 2,
    }
    original = {
        "read_local_json": gps.read_local_json,
        "read_github_json": gps.read_github_json,
        "_read_named_firestore": gps._read_named_firestore,
        "write_local_json_atomic": gps.write_local_json_atomic,
    }
    writes = {}
    try:
        def read_local(name, default):
            if name == "official_factors_cache.json":
                return copy.deepcopy(local_old), "packaged old", datetime.now()
            if name == "official_factors_cache_sync_state.json":
                return {}, "no local state", datetime.min
            return copy.deepcopy(default), "missing", datetime.min

        def read_github(name, default):
            if name == "official_factors_cache.json":
                return copy.deepcopy(remote_new), "runtime new"
            if name == "official_factors_cache_sync_state.json":
                return copy.deepcopy(remote_state), "runtime state"
            return copy.deepcopy(default), "missing"

        gps.read_local_json = read_local
        gps.read_github_json = read_github
        gps._read_named_firestore = lambda doc, default: (copy.deepcopy(default), "none", datetime.min, {})
        gps.write_local_json_atomic = lambda name, payload: (writes.__setitem__(name, copy.deepcopy(payload)) is None, "ok")
        payload, details = gps.load_named_json_permanent("official_factors_cache.json", {})
        assert payload["data_date"] == "2026-08-11", (payload, details)
        assert details[0] == "權威來源：github"
        assert writes["official_factors_cache.json"]["data_date"] == "2026-08-11"
    finally:
        for name, value in original.items():
            setattr(gps, name, value)


def test_runtime_branch_default_and_wiring():
    gps = _load_persistence()
    old_env = {k: os.environ.get(k) for k in ["GITHUB_RUNTIME_DATA_BRANCH", "GITHUB_REPO_BRANCH"]}
    try:
        os.environ["GITHUB_RUNTIME_DATA_BRANCH"] = "runtime-data"
        os.environ["GITHUB_REPO_BRANCH"] = "main"
        assert gps.github_config()["branch"] == "runtime-data"
    finally:
        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    official_src = OFFICIAL.read_text(encoding="utf-8-sig")
    page7_src = PAGE7.read_text(encoding="utf-8-sig")
    assert 'OFFICIAL_FACTOR_DURABLE_PATH = "official_factors_cache.json"' in official_src
    assert 'persist_json_permanent(\n            OFFICIAL_FACTOR_DURABLE_PATH' in official_src
    assert 'persist_json_async(str(CACHE_FILE)' not in official_src
    assert 'load_named_json_permanent(OFFICIAL_FACTOR_DURABLE_PATH, before)' in official_src
    assert 'official_payload = load_factor_cache() if callable(load_factor_cache)' in page7_src
    workflow_src = (ROOT / ".github" / "workflows" / "update_official_factors_v112.yml").read_text(encoding="utf-8-sig")
    assert "git checkout -B runtime-data origin/runtime-data" in workflow_src
    assert "git push origin runtime-data" in workflow_src
    assert "git push\n" not in workflow_src


def main():
    test_business_date_beats_reboot_timestamp_for_official_factor()
    test_new_legacy_local_beats_old_valid_remote()
    test_new_runtime_remote_restores_old_packaged_local()
    test_runtime_branch_default_and_wiring()
    print("PASS V186 reboot authority｜official-factor business date monotonic, runtime-data restore, no July rollback")


if __name__ == "__main__":
    main()
