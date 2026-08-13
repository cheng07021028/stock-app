# -*- coding: utf-8 -*-
from pathlib import Path
from types import ModuleType, SimpleNamespace
import json
import sys
import tempfile
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Persistence service imports Streamlit in production.  CI intentionally runs
# without it, so install a tiny module shim before importing the service.
fake_st = ModuleType("streamlit")
fake_st.secrets = {}
fake_st.session_state = {}
fake_st.cache_data = SimpleNamespace(clear=lambda: None)
fake_st.cache_resource = SimpleNamespace(clear=lambda: None)
sys.modules.setdefault("streamlit", fake_st)

import godpick_persistence_service as gps
from godpick_headless_page_loader import load_page_namespace
import godpick_auto_update_tasks as tasks


def rec(i: int, date: str = "2026-08-12"):
    return {
        "record_id": f"rid-{i}",
        "股票代號": f"{1000+i:04d}",
        "股票名稱": f"T{i}",
        "推薦日期": date,
        "推薦模式": "股神推薦",
        "推薦價格": 100 + i,
    }

# 1) Full-snapshot anti-shrink: 120 -> 4 / 0 must be refused; explicit mutation
# remains legal so real Page8 deletions are not resurrected.
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    old_base = gps.BASE_DIR
    gps.BASE_DIR = tmp
    try:
        original = [rec(i) for i in range(120)]
        (tmp / gps.RECORDS_FILE).write_text(json.dumps(original, ensure_ascii=False), encoding="utf-8")
        report = gps.save_records_sync_fast(original[:4], reason="simulate Page10 subset overwrite")
        assert not report.local_ok, report.to_dict()
        assert "防歸零/防縮水" in report.local_message, report.local_message
        assert len(json.loads((tmp / gps.RECORDS_FILE).read_text(encoding="utf-8"))) == 120
        report0 = gps.save_records_permanent([], reason="simulate empty list overwrite")
        assert not report0.local_ok, report0.to_dict()
        assert len(json.loads((tmp / gps.RECORDS_FILE).read_text(encoding="utf-8"))) == 120

        # Legitimate explicit delete path is still allowed.
        kept = original[:-3]
        mut = gps.save_records_mutation_fast(
            kept,
            deleted_ids=[x["record_id"] for x in original[-3:]],
            previous_count=120,
            reason="Page8 explicit delete 3 records",
        )
        assert mut.local_ok, mut.to_dict()
        assert len(json.loads((tmp / gps.RECORDS_FILE).read_text(encoding="utf-8"))) == 117
    finally:
        gps.BASE_DIR = old_base


# 1b) If historical rescue is still unresolved, a new Page07/Page10 upsert must
# NOT turn the suspicious 0-row collapse into a legitimate mutation state.
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    old_base = gps.BASE_DIR
    gps.BASE_DIR = tmp
    try:
        (tmp / gps.RECORDS_FILE).write_text("[]", encoding="utf-8")
        collapse_state = gps._new_state(
            "godpick_records_durable_v3", [], count=0, latest_recommendation_date=""
        )
        (tmp / gps.RECORDS_STATE_FILE).write_text(json.dumps(collapse_state, ensure_ascii=False), encoding="utf-8")
        blocked, stats = gps.upsert_records_authority_fast([rec(1, "2026-08-13")], reason="new Page07 after unresolved collapse")
        assert not blocked.local_ok, blocked.to_dict()
        assert "歷史救援尚未完成" in blocked.local_message, blocked.local_message
        assert stats["after"] == 0
        state_after = json.loads((tmp / gps.RECORDS_STATE_FILE).read_text(encoding="utf-8"))
        assert state_after["version"] == "godpick_records_durable_v3", state_after

        # A genuine explicit-clear mutation is different: after the user truly
        # cleared history, new future recommendations are allowed to start again.
        legit_state = gps._new_state(
            "godpick_records_durable_v7_mutation", [], count=0,
            mutation_reason="Page8 使用者清空全部紀錄"
        )
        (tmp / gps.RECORDS_STATE_FILE).write_text(json.dumps(legit_state, ensure_ascii=False), encoding="utf-8")
        allowed, stats2 = gps.upsert_records_authority_fast([rec(2, "2026-08-13")], reason="new Page07 after explicit clear")
        assert allowed.local_ok, allowed.to_dict()
        assert stats2["after"] == 1, stats2
    finally:
        gps.BASE_DIR = old_base

# 2) Page10 must only upsert current-list metrics into Page08 history and must
# treat an empty list as a no-op.  It must not own save_records_permanent.
ns = load_page_namespace("pages/10_推薦清單.py", base_dir=ROOT)
assert callable(ns.get("upsert_records_authority_fast"))
assert ns.get("save_records_permanent") is None
calls = {"upsert": 0, "named": []}

class R:
    local_ok = True
    permanent_ok = True
    def messages(self):
        return ["ok"]

def fake_upsert(rows, reason=""):
    calls["upsert"] += 1
    calls["upsert_rows"] = len(list(rows))
    return R(), {"before": 1874, "after": 1874, "added": 0, "updated": 4, "changed": 4}

def fake_named(path, payload, *args, **kwargs):
    calls["named"].append((path, payload))
    return R()

ns["upsert_records_authority_fast"] = fake_upsert
ns["save_named_json_permanent"] = fake_named
ns["load_named_json_permanent"] = lambda path, default: ({
    "saved_at": "2026-08-13 09:53:54",
    "recommendation_date": "2026-08-13",
    "execution_trigger": "V191中央自動排程",
    "candidate_diagnosis": [{"股票代號": "9999"}],
    "recommendations": [],
}, ["load ok"])

current = pd.DataFrame([
    {**rec(i, "2026-08-13"), "推薦後1日%": float(i)} for i in range(4)
])
ok, msgs = ns["_sync_records"](current)
assert ok, msgs
assert calls["upsert"] == 1 and calls["upsert_rows"] == 4, calls
latest_writes = [payload for path, payload in calls["named"] if path == "godpick_latest_recommendations.json"]
assert latest_writes, calls
assert latest_writes[-1]["saved_at"] == "2026-08-13 09:53:54", latest_writes[-1]
assert latest_writes[-1]["performance_update_owner"] == "10_推薦清單"

before_calls = dict(calls)
ok0, msgs0 = ns["_sync_records"](pd.DataFrame())
assert ok0, msgs0
assert calls["upsert"] == before_calls["upsert"], calls
assert len(calls["named"]) == len(before_calls["named"]), calls
assert any("防歸零" in str(x) for x in msgs0), msgs0


# 2b) Integration-style Page10 sync against a 1,874-row authority: updating a
# 4-row current recommendation list must keep all 1,874 historical rows.
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    old_base = gps.BASE_DIR
    gps.BASE_DIR = tmp
    try:
        history = [rec(i, "2026-08-12") for i in range(1874)]
        # Make the first four match the current list's business keys/date.
        for i in range(4):
            history[i]["推薦日期"] = "2026-08-13"
        state = gps._new_state(
            "godpick_records_durable_v191_h3_full_sync", history,
            count=len(history), latest_recommendation_date="2026-08-13"
        )
        (tmp / gps.RECORDS_FILE).write_text(json.dumps(history, ensure_ascii=False), encoding="utf-8")
        (tmp / gps.RECORDS_STATE_FILE).write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
        (tmp / gps.RECORDS_MANIFEST_FILE).write_text(json.dumps(gps._records_manifest(history, state["payload_hash"]), ensure_ascii=False), encoding="utf-8")

        ns2 = load_page_namespace("pages/10_推薦清單.py", base_dir=ROOT)
        ns2["upsert_records_authority_fast"] = gps.upsert_records_authority_fast
        ns2["save_named_json_permanent"] = lambda *a, **kw: R()
        ns2["load_named_json_permanent"] = lambda path, default: (default, ["no latest snapshot"])
        current4 = pd.DataFrame([
            {**history[i], "推薦後1日%": 1.5 + i} for i in range(4)
        ])
        ok2, msg2 = ns2["_sync_records"](current4)
        assert ok2, msg2
        after = json.loads((tmp / gps.RECORDS_FILE).read_text(encoding="utf-8"))
        assert len(after) == 1874, len(after)
        by_id = {x["record_id"]: x for x in after}
        for i in range(4):
            assert float(by_id[history[i]["record_id"]].get("推薦後1日%")) == 1.5 + i
    finally:
        gps.BASE_DIR = old_base

# 3) Emergency GitHub-history recovery: inspect state history, download only the
# selected full snapshot once, merge surviving current rows, and restore locally.
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    old_base = gps.BASE_DIR
    old_commits = gps._github_commits_for_path
    old_read_ref = gps._read_github_json_at_ref
    old_fs_config = gps.firebase_configured
    old_write_fs = gps.write_records_firestore
    old_schedule_gh = gps.schedule_records_github_sync
    old_gh_config = gps.github_config
    gps.BASE_DIR = tmp
    try:
        current_small = [rec(999, "2026-08-13")]
        (tmp / gps.RECORDS_FILE).write_text(json.dumps(current_small, ensure_ascii=False), encoding="utf-8")
        gps._github_commits_for_path = lambda path_name, limit=12: ([{"sha": "newbad"}, {"sha": "goodsha"}], "2 commits")
        historical = [rec(i, "2026-08-12") for i in range(120)]
        def read_ref(path_name, ref, default):
            if path_name == gps.RECORDS_STATE_FILE and ref == "newbad":
                return {"count": 1, "latest_recommendation_date": "2026-08-13"}, "bad state"
            if path_name == gps.RECORDS_STATE_FILE and ref == "goodsha":
                return {"count": 120, "latest_recommendation_date": "2026-08-12"}, "good state"
            if path_name == gps.RECORDS_FILE and ref == "goodsha":
                return historical, "good records"
            return default, "default"
        gps._read_github_json_at_ref = read_ref
        remote_calls = {"fs": 0, "gh": 0}
        gps.firebase_configured = lambda: True
        gps.github_config = lambda: {"token":"x","owner":"o","repo":"r","branch":"runtime-data"}
        def fake_write_fs(rows, state):
            remote_calls["fs"] = len(rows)
            return True, f"full fs {len(rows)}"
        def fake_schedule_gh(rows, state, reason=""):
            remote_calls["gh"] = len(rows)
            return True, f"queued gh {len(rows)}"
        gps.write_records_firestore = fake_write_fs
        gps.schedule_records_github_sync = fake_schedule_gh
        recovered, details, okr = gps.recover_records_from_github_history(current_small, max_commits=4)
        assert okr, details
        assert len(recovered) == 121, len(recovered)
        assert any(x.get("股票代號") == current_small[0]["股票代號"] for x in recovered)
        local = json.loads((tmp / gps.RECORDS_FILE).read_text(encoding="utf-8"))
        assert len(local) == 121
        state = json.loads((tmp / gps.RECORDS_STATE_FILE).read_text(encoding="utf-8"))
        assert state["version"] == "godpick_records_durable_v191_h3_history_recovery"
        assert remote_calls == {"fs": 121, "gh": 121}, remote_calls
    finally:
        gps.BASE_DIR = old_base
        gps._github_commits_for_path = old_commits
        gps._read_github_json_at_ref = old_read_ref
        gps.firebase_configured = old_fs_config
        gps.write_records_firestore = old_write_fs
        gps.schedule_records_github_sync = old_schedule_gh
        gps.github_config = old_gh_config

# 4) AI learning fail-closed: empty authority must never overwrite learning state.
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    old_task_base = tasks.BASE_DIR
    old_gps_base = gps.BASE_DIR
    tasks.BASE_DIR = tmp
    gps.BASE_DIR = tmp
    try:
        (tmp / "godpick_records.json").write_text("[]", encoding="utf-8")
        out = tasks.task_feedback_learning({})
        assert not out["ok"], out
        assert "拒絕用空資料" in out["message"], out
    finally:
        tasks.BASE_DIR = old_task_base
        gps.BASE_DIR = old_gps_base

print("PASS V191-H3 history integrity | anti-shrink | Page10 incremental | Git history rescue | AI zero guard")
