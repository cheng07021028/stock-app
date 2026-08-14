# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
import importlib.util
import json
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import godpick_auto_scheduler as sched


def ok(name: str, cond: bool):
    if not cond:
        raise AssertionError(name)
    print(f"PASS: {name}")


# 1) GitHub wake delayed beyond grace: latest same-day missed slot must catch up.
now = datetime(2026, 8, 14, 8, 6, tzinfo=sched.TZ)
slots = sched._due_slots({"times": ["04:00"]}, now, 60, catch_up_missed_same_day=True)
ok("04:00 missed wake catches up at 08:06", len(slots) == 1 and slots[0].strftime("%H:%M") == "04:00")
slots_off = sched._due_slots({"times": ["04:00"]}, now, 60, catch_up_missed_same_day=False)
ok("catch-up remains configurable", slots_off == [])

# 2) Page17 remote refresh must replace a stale long-lived local scheduler status.
old_base = sched.BASE_DIR
old_mod = sys.modules.get("godpick_persistence_service")
try:
    with tempfile.TemporaryDirectory() as td:
        sched.BASE_DIR = Path(td)
        local = {"updated_at": "2026-08-13 21:47:33", "last_wakeup_at": "2026-08-13 21:47:33"}
        remote = {"updated_at": "2026-08-14 08:09:02", "last_wakeup_at": "2026-08-14 08:06:14"}
        (sched.BASE_DIR / sched.STATUS_FILE).write_text(json.dumps(local), encoding="utf-8")
        fake = ModuleType("godpick_persistence_service")
        fake.read_github_json = lambda path, default: (dict(remote), "mock remote")
        def _write(path, payload):
            p = sched.BASE_DIR / path
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            return True, "ok"
        fake.write_local_json_atomic = _write
        fake.load_named_json_permanent = lambda path, default: (default, [])
        sys.modules["godpick_persistence_service"] = fake
        refreshed = sched.load_status(refresh_remote=True)
        ok("Page17 adopts newer GitHub scheduler heartbeat", refreshed.get("last_wakeup_at") == "2026-08-14 08:06:14")
        disk = json.loads((sched.BASE_DIR / sched.STATUS_FILE).read_text(encoding="utf-8"))
        ok("new remote heartbeat is cached locally", disk.get("updated_at") == "2026-08-14 08:09:02")
finally:
    sched.BASE_DIR = old_base
    if old_mod is not None:
        sys.modules["godpick_persistence_service"] = old_mod
    else:
        sys.modules.pop("godpick_persistence_service", None)

# 3) A stale failed manual force-all must not hijack a production scheduled wake.
orig = {name: getattr(sched, name) for name in ["load_settings", "load_status", "load_history", "_acquire_lock", "_release_lock", "_checkpoint_runtime", "_execute_one", "_touch_lock"]}
try:
    cfg = {
        "enabled": True, "weekdays_only": True, "catch_up_missed_same_day": True,
        "grace_minutes": 60, "retry_count": 0, "retry_delay_seconds": 1, "history_keep": 400,
        "jobs": {"stock_master": {"enabled": True, "times": ["04:00"], "options": {}}},
    }
    status = {
        "jobs": {}, "completed_run_keys": [],
        "active_run": {"mode": "force_all", "started_at": "2026-08-13 16:50:06", "pending_jobs": ["godpick_recommendation"]},
    }
    sched.load_settings = lambda: cfg
    sched.load_status = lambda: status
    sched.load_history = lambda: []
    sched._acquire_lock = lambda: (True, "")
    sched._release_lock = lambda: None
    sched._checkpoint_runtime = lambda *a, **k: None
    sched._touch_lock = lambda *a, **k: None
    sched._execute_one = lambda job, jc, gc: {"ok": True, "message": f"{job} ran", "finished_at": "2026-08-14 08:06:01"}
    res = sched.run_due_jobs(now=now)
    ok("production wake executes due stock_master instead of force retry", [x.get("job") for x in res.get("executed", [])] == ["stock_master"])
    ok("old force pending kept only as diagnostics", res.get("status", {}).get("last_force_pending_jobs") == ["godpick_recommendation"])
    ok("unattended wake is not flagged resumed_force", not res.get("resumed_force_batch"))
finally:
    for name, value in orig.items():
        setattr(sched, name, value)

# 4) Screenshot-level simulation: at 08:51, a missed 04:00 stock master must
# catch up first, then 08:25 Page07 can pass dependencies, then failed 07:00
# latest-price can retry. Previously stock_master was lost after its 60-min grace.
orig = {name: getattr(sched, name) for name in ["load_settings", "load_status", "load_history", "_acquire_lock", "_release_lock", "_checkpoint_runtime", "_execute_one", "_touch_lock"]}
try:
    real_cfg = sched.normalize_settings({
        "enabled": True, "weekdays_only": True, "catch_up_missed_same_day": True, "grace_minutes": 60, "retry_count": 0,
        "jobs": {
            "stock_master": {"enabled": True, "times": ["04:00"]},
            "macro_full": {"enabled": True, "times": ["05:00", "20:00"]},
            "official_factors": {"enabled": True, "times": ["05:10", "20:05"]},
            "super_ai_context": {"enabled": True, "times": ["05:25", "20:25"]},
            "watchlist_runtime": {"enabled": True, "times": ["05:35", "20:30"]},
            "godpick_recommendation": {
                "enabled": True, "times": ["08:25", "22:10"], "require_dependencies": True,
                "dependencies": ["stock_master", "macro_full", "official_factors", "super_ai_context", "watchlist_runtime"]
            },
            "record_latest_price": {"enabled": True, "times": ["07:00", "20:35"]},
            "record_performance": {"enabled": True, "times": ["07:25", "20:45"]},
            "recommend_list_performance": {"enabled": True, "times": ["07:05", "21:10"]},
            "recommend_list_n_day": {"enabled": True, "times": ["07:15", "21:20"]},
            "recommend_list_hits": {"enabled": True, "times": ["07:25", "21:30"]},
            "t1_truth": {"enabled": True, "times": ["07:35", "21:40"]},
            "feedback_learning": {"enabled": True, "times": ["07:45", "21:50"]},
            "durability_retry": {"enabled": True, "times": ["22:00"]},
        },
    })
    done_jobs = {
        "macro_full": ("05:00", "2026-08-14 05:08:31"),
        "official_factors": ("05:10", "2026-08-14 05:59:02"),
        "super_ai_context": ("05:25", "2026-08-14 05:59:23"),
        "watchlist_runtime": ("05:35", "2026-08-14 05:59:37"),
        "record_performance": ("07:25", "2026-08-14 08:07:18"),
        "recommend_list_performance": ("07:05", "2026-08-14 07:39:14"),
        "recommend_list_n_day": ("07:15", "2026-08-14 07:39:43"),
        "recommend_list_hits": ("07:25", "2026-08-14 08:07:53"),
        "t1_truth": ("07:35", "2026-08-14 08:08:20"),
        "feedback_learning": ("07:45", "2026-08-14 08:08:45"),
    }
    screen_status = {"jobs": {}, "completed_run_keys": []}
    for job, (slot, success_at) in done_jobs.items():
        screen_status["completed_run_keys"].append(f"{job}|2026-08-14 {slot}")
        screen_status["jobs"][job] = {"last_status": "SUCCESS", "last_success_at": success_at}
    # Reflect the screenshot: stock master has no 8/14 success; Page07 was
    # previously BLOCKED; latest price previously FAILED.
    screen_status["jobs"]["godpick_recommendation"] = {"last_status": "BLOCKED", "last_failed_at": "2026-08-14 04:19:11"}
    screen_status["jobs"]["record_latest_price"] = {"last_status": "FAILED", "last_failed_at": "2026-08-14 07:38:42"}
    sched.load_settings = lambda: real_cfg
    sched.load_status = lambda: screen_status
    sched.load_history = lambda: []
    sched._acquire_lock = lambda: (True, "")
    sched._release_lock = lambda: None
    sched._checkpoint_runtime = lambda *a, **k: None
    sched._touch_lock = lambda *a, **k: None
    sched._execute_one = lambda job, jc, gc: {"ok": True, "message": f"{job} recovered", "finished_at": "2026-08-14 08:52:00"}
    screenshot_run = sched.run_due_jobs(now=datetime(2026, 8, 14, 8, 51, tzinfo=sched.TZ))
    got = [x.get("job") for x in screenshot_run.get("executed", [])]
    ok("08:51 screenshot recovery executes missed stock -> Page07 -> latest price", got == ["stock_master", "godpick_recommendation", "record_latest_price"])
    ok("Page07 dependency passes after same-run stock catch-up", next(x for x in screenshot_run["executed"] if x["job"] == "godpick_recommendation")["status"] == "SUCCESS")
finally:
    for name, value in orig.items():
        setattr(sched, name, value)

# 5) UI next-run table must expose a current overdue/due slot, not jump to tonight.
cfg2 = sched.normalize_settings({
    "enabled": True, "weekdays_only": True, "catch_up_missed_same_day": True, "grace_minutes": 60,
    "jobs": {"godpick_recommendation": {"enabled": True, "times": ["08:25", "22:10"]}},
})
rows = sched.next_run_rows(cfg2, {"jobs": {}, "completed_run_keys": []}, datetime(2026, 8, 14, 8, 51, tzinfo=sched.TZ))
gp = next(x for x in rows if x["工作ID"] == "godpick_recommendation")
ok("08:25 due slot remains visible at 08:51", "08:25" in gp["下次預計"] and "到期" in gp["下次預計"])
ok("due state is explicit", gp["到期狀態"] == "到期待執行")

# 6) Reproduce the exact old Actions branch-checkout failure locally.
with tempfile.TemporaryDirectory() as td:
    repo = Path(td) / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "test"], cwd=repo, check=True)
    (repo / "app.py").write_text("print('main')\n", encoding="utf-8")
    subprocess.run(["git", "add", "app.py"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "main"], cwd=repo, check=True)
    subprocess.run(["git", "checkout", "-qb", "runtime-data"], cwd=repo, check=True)
    (repo / "godpick_auto_scheduler_status.json").write_text("{\"remote\":1}", encoding="utf-8")
    subprocess.run(["git", "add", "godpick_auto_scheduler_status.json"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "runtime"], cwd=repo, check=True)
    subprocess.run(["git", "checkout", "-q", "main"], cwd=repo, check=True)
    (repo / "godpick_auto_scheduler_status.json").write_text("{\"local\":2}", encoding="utf-8")
    failed = subprocess.run(["git", "checkout", "runtime-data"], cwd=repo, text=True, capture_output=True)
    ok("old bulk branch publish reproduces untracked overwrite checkout failure", failed.returncode != 0 and "would be overwritten by checkout" in (failed.stderr + failed.stdout))

# 7) New workflow must not perform the dangerous second branch publish.
workflow = (ROOT / ".github/workflows/godpick_auto_scheduler_v191.yml").read_text(encoding="utf-8")
ok("workflow keeps 10-minute off-peak fallback wake", 'cron: "2-52/10 * * * *"' in workflow)
ok("workflow uses H10 heartbeat verifier", "verify_godpick_scheduler_remote_v191.py" in workflow)
ok("workflow removed runtime-data checkout bulk publish", "git checkout -B runtime-data" not in workflow and "git push origin runtime-data" not in workflow)

# 8) Heartbeat verifier bounded repair: older remote becomes local/latest without branch checkout.
spec = importlib.util.spec_from_file_location("verify_h10", ROOT / "tools/verify_godpick_scheduler_remote_v191.py")
verify = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(verify)
with tempfile.TemporaryDirectory() as td:
    old_root = verify.ROOT
    verify.ROOT = Path(td)
    local_status = {"last_wakeup_at": "2026-08-14 08:06:14", "updated_at": "2026-08-14 08:09:02"}
    local_hist = {"version": "v", "records": [{"run_key": "t1_truth|2026-08-14 07:35", "finished_at": "2026-08-14 08:08:20"}]}
    (verify.ROOT / verify.STATUS_FILE).write_text(json.dumps(local_status), encoding="utf-8")
    (verify.ROOT / verify.HISTORY_FILE).write_text(json.dumps(local_hist), encoding="utf-8")
    remote_store = {
        verify.STATUS_FILE: {"last_wakeup_at": "2026-08-13 21:47:33", "updated_at": "2026-08-13 21:47:33"},
        verify.HISTORY_FILE: {"version": "v", "records": []},
    }
    verify._read_remote = lambda path, default: (json.loads(json.dumps(remote_store.get(path, default))), "mock")
    def _save(path, payload):
        remote_store[path] = json.loads(json.dumps(payload))
        return SimpleNamespace(github_message="mock saved")
    verify._save_remote = _save
    s_ok, _ = verify.verify_status()
    h_ok, _ = verify.verify_history()
    ok("heartbeat verifier repairs stale remote scheduler status", s_ok and remote_store[verify.STATUS_FILE]["last_wakeup_at"] == "2026-08-14 08:06:14")
    ok("heartbeat verifier merges/repairs scheduler history", h_ok and remote_store[verify.HISTORY_FILE]["records"][-1]["run_key"] == "t1_truth|2026-08-14 07:35")
    verify.ROOT = old_root

print("H10 unattended scheduler smoke: ALL PASS")
