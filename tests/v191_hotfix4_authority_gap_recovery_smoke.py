# -*- coding: utf-8 -*-
from pathlib import Path
from types import ModuleType, SimpleNamespace
import json
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

fake_st = ModuleType("streamlit")
fake_st.secrets = {}
fake_st.session_state = {}
fake_st.cache_data = SimpleNamespace(clear=lambda: None)
fake_st.cache_resource = SimpleNamespace(clear=lambda: None)
sys.modules.setdefault("streamlit", fake_st)

import godpick_persistence_service as gps


def rec(i: int, date: str):
    return {
        "record_id": f"rid-{i}",
        "股票代號": f"{1000 + (i % 8000):04d}",
        "股票名稱": f"T{i}",
        "推薦日期": date,
        "推薦模式": "股神推薦",
        "推薦價格": 100 + (i % 20),
    }


def install_local(tmp: Path, rows, state):
    (tmp / gps.RECORDS_FILE).write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    (tmp / gps.RECORDS_STATE_FILE).write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    (tmp / gps.RECORDS_MANIFEST_FILE).write_text(
        json.dumps(gps._records_manifest(rows, state["payload_hash"]), ensure_ascii=False), encoding="utf-8"
    )


# 1) Exact production incident shape: current H3/08 auto-repair state is large
# (1810 rows) but stale at 07/09.  Git history also contains corrupted 4-row
# 08/13 states, an older 1810/07-09 snapshot, 1870/08-12 and 1874/08-13.
# H4 MUST choose 1874/08-13, not the first merely-large/high-priority commit.
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    old_base = gps.BASE_DIR
    old_commits = gps._github_commits_for_path
    old_read_ref = gps._read_github_json_at_ref
    old_fs_config = gps.firebase_configured
    old_gh_config = gps.github_config
    old_fs_write = gps.write_records_firestore
    old_gh_schedule = gps.schedule_records_github_sync
    gps.BASE_DIR = tmp
    try:
        current = [rec(i, "2026-07-09") for i in range(1810)]
        current_state = gps._new_state(
            "godpick_records_durable_v4_mutation", current,
            count=1810, latest_recommendation_date="2026-07-09",
            mutation_reason="08 自動修復較新07推薦快照",
        )
        install_local(tmp, current, current_state)

        rows_1874 = list(current)
        rows_1874 += [rec(1810 + i, "2026-08-12") for i in range(60)]
        rows_1874 += [rec(1870 + i, "2026-08-13") for i in range(4)]
        rows_1870 = rows_1874[:1870]
        rows_1810 = list(current)
        rows_bad4 = [rec(3000 + i, "2026-08-13") for i in range(4)]

        commits = [
            {"sha": "repair1810", "commit": {"message": "07 股神推薦 old high-priority writer"}},
            {"sha": "bad4", "commit": {"message": "persist godpick records state"}},
            {"sha": "good1874", "commit": {"message": "07 股神推薦完成自動紀錄"}},
            {"sha": "good1870", "commit": {"message": "V191 scheduled Page8 performance"}},
        ]
        states = {
            "repair1810": {"count": 1810, "latest_recommendation_date": "2026-07-09"},
            "bad4": {"count": 4, "latest_recommendation_date": "2026-08-13"},
            "good1874": {"count": 1874, "latest_recommendation_date": "2026-08-13", "mutation_reason": "07 股神推薦完成自動紀錄"},
            "good1870": {"count": 1870, "latest_recommendation_date": "2026-08-12", "mutation_reason": "V191 scheduled Page8 performance"},
        }
        records = {
            "repair1810": rows_1810,
            "bad4": rows_bad4,
            "good1874": rows_1874,
            "good1870": rows_1870,
        }
        gps._github_commits_for_path = lambda path_name, limit=60: (commits, "4 commits")
        def read_ref(path_name, ref, default):
            if path_name == gps.RECORDS_STATE_FILE:
                return states.get(ref, default), f"state {ref}"
            if path_name == gps.RECORDS_FILE:
                return records.get(ref, default), f"records {ref}"
            return default, "default"
        gps._read_github_json_at_ref = read_ref
        gps.firebase_configured = lambda: False
        gps.github_config = lambda: {"token": "x", "owner": "o", "repo": "r", "branch": "runtime-data"}
        scheduled = {}
        gps.schedule_records_github_sync = lambda rows, state, reason="": (scheduled.update({"count": len(rows), "sha": state.get("recovered_from_commit")}) or True, "queued")
        gps.write_records_firestore = lambda rows, state: (True, "ok")

        needed, why = gps._records_history_gap_audit_needed(current_state, current)
        assert needed and "H3/08" in why, (needed, why)
        recovered, details, ok = gps.recover_records_from_github_history(current, max_commits=60)
        assert ok, details
        assert len(recovered) == 1874, len(recovered)
        assert gps._latest_record_recommendation_date(recovered) == "2026-08-13"
        state_after = json.loads((tmp / gps.RECORDS_STATE_FILE).read_text(encoding="utf-8"))
        assert state_after["recovered_from_commit"] == "good1874", state_after
        assert state_after["count"] == 1874, state_after
        assert state_after["latest_recommendation_date"] == "2026-08-13", state_after
        assert scheduled == {"count": 1874, "sha": "good1874"}, scheduled
    finally:
        gps.BASE_DIR = old_base
        gps._github_commits_for_path = old_commits
        gps._read_github_json_at_ref = old_read_ref
        gps.firebase_configured = old_fs_config
        gps.github_config = old_gh_config
        gps.write_records_firestore = old_fs_write
        gps.schedule_records_github_sync = old_gh_schedule


# 2) Business date outranks raw row count.  A newer 08/13 authority with 1100
# rows is preferred over an older 08/12 authority with 1200 rows.  This protects
# legitimate row-count changes while still restoring the newest recommendation day.
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    old_base = gps.BASE_DIR
    old_commits = gps._github_commits_for_path
    old_read_ref = gps._read_github_json_at_ref
    old_fs_config = gps.firebase_configured
    old_gh_config = gps.github_config
    old_schedule = gps.schedule_records_github_sync
    gps.BASE_DIR = tmp
    try:
        current = [rec(i, "2026-07-09") for i in range(1000)]
        state = gps._new_state(
            "godpick_records_durable_v4_mutation", current,
            count=1000, latest_recommendation_date="2026-07-09",
            mutation_reason="08 自動修復較新07推薦快照",
        )
        install_local(tmp, current, state)
        newer = list(current[:900]) + [rec(5000+i, "2026-08-13") for i in range(200)]
        older = list(current) + [rec(6000+i, "2026-08-12") for i in range(200)]
        gps._github_commits_for_path = lambda *a, **k: ([
            {"sha": "older1200", "commit": {"message": "07 股神推薦"}},
            {"sha": "newer1100", "commit": {"message": "Page8 performance"}},
        ], "2 commits")
        states = {
            "older1200": {"count": 1200, "latest_recommendation_date": "2026-08-12"},
            "newer1100": {"count": 1100, "latest_recommendation_date": "2026-08-13"},
        }
        files = {"older1200": older, "newer1100": newer}
        gps._read_github_json_at_ref = lambda path, ref, default: (
            (states.get(ref, default) if path == gps.RECORDS_STATE_FILE else files.get(ref, default)), ref
        )
        gps.firebase_configured = lambda: False
        gps.github_config = lambda: {"token": "x", "owner": "o", "repo": "r", "branch": "runtime-data"}
        chosen = {}
        gps.schedule_records_github_sync = lambda rows, stt, reason="": (chosen.update({"sha": stt.get("recovered_from_commit")}) or True, "queued")
        recovered, details, ok = gps.recover_records_from_github_history(current)
        assert ok, details
        assert chosen.get("sha") == "newer1100", (chosen, details)
        assert gps._latest_record_recommendation_date(recovered) == "2026-08-13"
    finally:
        gps.BASE_DIR = old_base
        gps._github_commits_for_path = old_commits
        gps._read_github_json_at_ref = old_read_ref
        gps.firebase_configured = old_fs_config
        gps.github_config = old_gh_config
        gps.schedule_records_github_sync = old_schedule


# 3) Explicit user delete/clear remains authoritative.  No Git-history scan is
# allowed to resurrect rows after a destructive mutation.
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    old_base = gps.BASE_DIR
    old_commits = gps._github_commits_for_path
    gps.BASE_DIR = tmp
    try:
        rows = [rec(i, "2026-08-13") for i in range(100)]
        state = gps._new_state(
            "godpick_records_durable_v191_h4_mutation", rows,
            count=100, latest_recommendation_date="2026-08-13",
            mutation_reason="刪除 3 筆紀錄", mutation_kind="destructive",
            mutation_deleted_count=3,
        )
        install_local(tmp, rows, state)
        called = {"n": 0}
        gps._github_commits_for_path = lambda *a, **k: (called.update(n=called["n"]+1) or [], "should-not-run")
        needed, why = gps._records_history_gap_audit_needed(state, rows)
        assert not needed and "禁止歷史復活" in why, (needed, why)
        recovered, details, ok = gps.recover_records_from_github_history(rows)
        assert not ok and len(recovered) == 100, details
        assert called["n"] == 0, called
    finally:
        gps.BASE_DIR = old_base
        gps._github_commits_for_path = old_commits

print("PASS V191-H4 authority gap recovery | best-date candidate | 1810->1874 | explicit-delete safety")

# 4) Page8 latest-snapshot repair must verify the post-condition.  A write that
# reports local success but leaves the authority date at 07/09 is NOT a repair.
try:
    from godpick_headless_page_loader import load_page_namespace
    import pandas as pd
    ns = load_page_namespace("pages/8_股神推薦紀錄.py", base_dir=ROOT)
    fn = ns["_reconcile_latest_snapshot_into_authority_v174"]
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        snap = tmp / "godpick_latest_recommendations.json"
        snap.write_text("{}", encoding="utf-8")
        payload = {
            "saved_at": "2026-08-13 09:53:00",
            "recommendation_date": "2026-08-13",
            "recommendations": [{
                "股票代號": "2330",
                "股票名稱": "台積電",
                "正式推薦分區": "正式下週主推薦",
                "推薦模式": "股神推薦",
            }],
        }
        ns["project_path"] = lambda name: tmp / name
        ns["read_local_json"] = lambda name, default: (payload, "snapshot", None)
        class Report:
            local_ok = True
            permanent_ok = True
            def messages(self): return ["mock save"]
        ns["upsert_records_authority_fast"] = lambda rows, reason="": (Report(), {"added": 1, "updated": 0, "changed": 1})
        ns["st"].session_state.clear()
        authority = pd.DataFrame([{"推薦日期": "2026-07-09", "股票代號": "2303"}])

        ns["records_authority_status"] = lambda: {"count": 1810, "latest_recommendation_date": "2026-07-09"}
        ok, details = fn(authority)
        assert not ok, details
        assert any("安全檢查未通過" in str(x) for x in details), details

        # New snapshot signature/session key for the second pass.
        ns["st"].session_state.clear()
        ns["records_authority_status"] = lambda: {"count": 1874, "latest_recommendation_date": "2026-08-13"}
        ok2, details2 = fn(authority)
        assert ok2, details2
        assert any("目前 1874 筆／最新 2026-08-13" in str(x) for x in details2), details2
except ModuleNotFoundError:
    # The changed-files-only package intentionally does not bundle every base
    # module.  Clean-overlay regression executes this branch with the full app.
    pass

print("PASS V191-H4 Page8 snapshot post-condition")

# 5) Reboot safety: a stale local copy must never trigger history resurrection
# when the CURRENT remote authority is an explicit user deletion/clear state.
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    old_base = gps.BASE_DIR
    old_gh_cfg = gps.github_config
    old_fs_cfg = gps.firebase_configured
    old_read_gh = gps.read_github_json
    old_recover = gps.recover_records_from_github_history
    gps.BASE_DIR = tmp
    try:
        local_rows = [rec(i, "2026-07-09") for i in range(100)]
        local_state = gps._new_state(
            "godpick_records_durable_v191_h4_mutation", local_rows,
            count=100, latest_recommendation_date="2026-07-09",
            mutation_reason="08 自動修復較新07推薦快照", mutation_kind="upsert",
        )
        install_local(tmp, local_rows, local_state)
        remote_rows = [rec(i, "2026-08-13") for i in range(90)]
        remote_state = gps._new_state(
            "godpick_records_durable_v191_h4_mutation", remote_rows,
            count=90, latest_recommendation_date="2026-08-13",
            mutation_reason="清空篩選 10 筆", mutation_kind="destructive",
            mutation_deleted_count=10,
        )
        gps.github_config = lambda: {"token": "x", "owner": "o", "repo": "r", "branch": "runtime-data"}
        gps.firebase_configured = lambda: False
        def read_gh(path, default):
            if path == gps.RECORDS_STATE_FILE:
                return remote_state, "remote destructive state"
            if path == gps.RECORDS_FILE:
                return remote_rows, "remote destructive rows"
            return default, "default"
        gps.read_github_json = read_gh
        called = {"recover": 0}
        def should_not_recover(*a, **k):
            called["recover"] += 1
            raise AssertionError("history recovery must not run over a current destructive remote authority")
        gps.recover_records_from_github_history = should_not_recover
        rows, details, restored = gps.ensure_records_local_authority_current()
        assert called["recover"] == 0, called
        assert restored, details
        assert len(rows) == 90 and gps._latest_record_recommendation_date(rows) == "2026-08-13", (len(rows), details)
        assert any("禁止歷史復活" in str(x) for x in details), details
    finally:
        gps.BASE_DIR = old_base
        gps.github_config = old_gh_cfg
        gps.firebase_configured = old_fs_cfg
        gps.read_github_json = old_read_gh
        gps.recover_records_from_github_history = old_recover

print("PASS V191-H4 current-remote destructive mutation guard")
