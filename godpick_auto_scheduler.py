# -*- coding: utf-8 -*-
"""V191 central automatic scheduler for the GodPick data/recommendation pipeline.

The scheduler is a *due checker*, not a background thread.  It is safe for
Streamlit and GitHub Actions: an external wake-up (GitHub Actions cron) invokes
``run_due_jobs`` and only jobs whose Taiwan-time slots are due are executed.
All settings/status/history are permanently persisted to runtime-data/Firestore
through the existing durability service.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
import copy
import json
import os
import socket
import threading
import time

VERSION = "godpick_auto_scheduler_v191_20260814_hotfix10_unattended_reliability"
BASE_DIR = Path(__file__).resolve().parent
TZ = ZoneInfo("Asia/Taipei")
SETTINGS_FILE = "data/config/godpick_auto_scheduler_settings.json"
STATUS_FILE = "godpick_auto_scheduler_status.json"
HISTORY_FILE = "godpick_auto_scheduler_history.json"
LOCK_FILE = BASE_DIR / ".godpick_auto_scheduler_v191.lock"

JOB_LABELS = {
    "stock_master": "09｜股票主檔更新",
    "macro_full": "00｜大盤走勢全部必要資料＋股神橋接",
    "official_factors": "16｜官方因子快取",
    "super_ai_context": "17｜SuperAI 融資券/期貨/PCR/ETF情境",
    "watchlist_runtime": "04｜自選股 runtime/正規化同步",
    "godpick_recommendation": "07｜股神推薦（前置成功後）",
    "record_latest_price": "08｜股神推薦紀錄－最新價",
    "record_performance": "08｜股神推薦紀錄－推薦後績效＋儲存同步",
    "recommend_list_performance": "10｜推薦清單－更新推薦後績效",
    "recommend_list_n_day": "10｜推薦清單－正式N日績效回補",
    "recommend_list_hits": "10｜推薦清單－隔日命中追蹤",
    "t1_truth": "V188｜T+1實戰真相＋機率校準",
    "feedback_learning": "AI｜績效回饋＋每日學習狀態重建",
    "durability_retry": "17｜永久化失敗/待同步重試",
}

DEFAULT_JOB_OPTIONS = {
    "stock_master": {},
    "macro_full": {},
    "official_factors": {},
    "super_ai_context": {"fetch_etf": True},
    "watchlist_runtime": {},
    "godpick_recommendation": {"force_full_market": False},
    "record_latest_price": {"only_active": False, "batch_size": 120},
    "record_performance": {"process_all": True, "max_records": 5000, "batch_limit": 120, "max_workers": 12, "stale_minutes": 30},
    "recommend_list_performance": {"process_all": True, "max_records": 5000, "batch_limit": 120, "max_workers": 12, "stale_minutes": 30},
    "recommend_list_n_day": {"max_rows": 300},
    "recommend_list_hits": {"max_rows": 300},
    "t1_truth": {"max_records": 500, "max_workers": 8},
    "feedback_learning": {},
    "durability_retry": {"max_sync_per_run": 6, "time_budget_seconds": 35},
}

# Default is deliberately disabled until the user explicitly saves/enables it.
# Suggested times are production-safe Taiwan-time presets and remain editable.
DEFAULT_SETTINGS: dict[str, Any] = {
    "version": VERSION,
    "timezone": "Asia/Taipei",
    "enabled": False,
    "weekdays_only": True,
    "grace_minutes": 35,
    "catch_up_missed_same_day": True,
    "retry_count": 2,
    "retry_delay_seconds": 20,
    "history_keep": 400,
    "updated_at": "",
    "jobs": {
        "stock_master": {"enabled": True, "times": ["07:30"], "options": DEFAULT_JOB_OPTIONS["stock_master"]},
        "macro_full": {"enabled": True, "times": ["14:20", "20:40"], "options": DEFAULT_JOB_OPTIONS["macro_full"]},
        "official_factors": {"enabled": True, "times": ["20:25"], "options": DEFAULT_JOB_OPTIONS["official_factors"]},
        "super_ai_context": {"enabled": True, "times": ["20:35"], "options": DEFAULT_JOB_OPTIONS["super_ai_context"]},
        "watchlist_runtime": {"enabled": True, "times": ["20:38"], "options": DEFAULT_JOB_OPTIONS["watchlist_runtime"]},
        "godpick_recommendation": {
            "enabled": True, "times": ["20:55"], "options": DEFAULT_JOB_OPTIONS["godpick_recommendation"],
            "require_dependencies": True,
            "dependencies": ["stock_master", "macro_full", "official_factors", "super_ai_context", "watchlist_runtime"],
        },
        "record_latest_price": {"enabled": True, "times": ["14:30", "21:05"], "options": DEFAULT_JOB_OPTIONS["record_latest_price"]},
        "record_performance": {"enabled": True, "times": ["21:10"], "options": DEFAULT_JOB_OPTIONS["record_performance"]},
        "recommend_list_performance": {"enabled": True, "times": ["21:15"], "options": DEFAULT_JOB_OPTIONS["recommend_list_performance"]},
        "recommend_list_n_day": {"enabled": True, "times": ["21:20"], "options": DEFAULT_JOB_OPTIONS["recommend_list_n_day"]},
        "recommend_list_hits": {"enabled": True, "times": ["21:25"], "options": DEFAULT_JOB_OPTIONS["recommend_list_hits"]},
        "t1_truth": {"enabled": True, "times": ["21:30"], "options": DEFAULT_JOB_OPTIONS["t1_truth"]},
        "feedback_learning": {"enabled": True, "times": ["21:35"], "options": DEFAULT_JOB_OPTIONS["feedback_learning"]},
        "durability_retry": {"enabled": True, "times": ["21:40"], "options": DEFAULT_JOB_OPTIONS["durability_retry"]},
    },
}


def now_tw() -> datetime:
    return datetime.now(TZ)


def now_text(dt: datetime | None = None) -> str:
    return (dt or now_tw()).strftime("%Y-%m-%d %H:%M:%S")


def _read_local(path_name: str, default: Any):
    p=BASE_DIR/path_name
    try:
        if p.exists(): return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception: pass
    return copy.deepcopy(default)


def _normalize_times(values: Any) -> list[str]:
    if isinstance(values, str): values=[x.strip() for x in values.replace("，",",").split(",") if x.strip()]
    if not isinstance(values, list): values=[]
    out=[]
    for raw in values:
        text=str(raw or "").strip()
        try:
            hh,mm=[int(x) for x in text.split(":",1)]
            if 0<=hh<=23 and 0<=mm<=59:
                val=f"{hh:02d}:{mm:02d}"
                if val not in out: out.append(val)
        except Exception: pass
    return sorted(out)


def normalize_settings(raw: Any) -> dict[str, Any]:
    out=copy.deepcopy(DEFAULT_SETTINGS)
    if isinstance(raw,dict):
        for k in ["enabled","weekdays_only","catch_up_missed_same_day","grace_minutes","retry_count","retry_delay_seconds","history_keep","updated_at"]:
            if k in raw: out[k]=raw[k]
        rjobs=raw.get("jobs") if isinstance(raw.get("jobs"),dict) else {}
        for key,default in out["jobs"].items():
            src=rjobs.get(key) if isinstance(rjobs.get(key),dict) else {}
            merged=copy.deepcopy(default); merged.update(src)
            merged["enabled"]=bool(merged.get("enabled",False))
            merged["times"]=_normalize_times(merged.get("times")) or list(default.get("times",[]))
            opts=copy.deepcopy(DEFAULT_JOB_OPTIONS.get(key,{})); opts.update(merged.get("options") if isinstance(merged.get("options"),dict) else {})
            merged["options"]=opts
            if key=="godpick_recommendation":
                merged["require_dependencies"]=bool(merged.get("require_dependencies",True))
                deps=merged.get("dependencies") if isinstance(merged.get("dependencies"),list) else default.get("dependencies",[])
                merged["dependencies"]=[str(x) for x in deps if str(x) in JOB_LABELS and str(x)!=key]
            out["jobs"][key]=merged
    out["catch_up_missed_same_day"]=bool(out.get("catch_up_missed_same_day", True))
    out["grace_minutes"]=max(5,min(int(out.get("grace_minutes",35) or 35),180))
    out["retry_count"]=max(0,min(int(out.get("retry_count",2) or 0),5))
    out["retry_delay_seconds"]=max(1,min(int(out.get("retry_delay_seconds",20) or 20),300))
    out["history_keep"]=max(50,min(int(out.get("history_keep",400) or 400),2000))
    out["version"]=VERSION; out["timezone"]="Asia/Taipei"
    return out


def _payload_clock(payload: Any, *fields: str) -> str:
    if not isinstance(payload, dict):
        return ""
    for field in fields:
        value = str(payload.get(field) or "").strip()
        if value:
            return value
    return ""


def _refresh_small_json_from_github(path_name: str, local_payload: Any, *, clock_fields: tuple[str, ...]) -> Any:
    """Refresh small scheduler control files without a heavy full authority election.

    Streamlit is a long-lived process while GitHub Actions is a separate process.
    H10 therefore cannot trust a non-empty local scheduler status forever.  For
    these small control files only, compare the remote business/update clock and
    adopt the newer GitHub payload.  Network failure is fail-soft and keeps the
    current local payload.
    """
    try:
        from godpick_persistence_service import read_github_json, write_local_json_atomic
        remote, _ = read_github_json(path_name, {})
        if not isinstance(remote, dict) or not remote:
            return local_payload
        local_clock = _payload_clock(local_payload, *clock_fields)
        remote_clock = _payload_clock(remote, *clock_fields)
        if (not isinstance(local_payload, dict) or not local_payload) or (remote_clock and remote_clock > local_clock):
            write_local_json_atomic(path_name, remote)
            return remote
    except Exception:
        pass
    return local_payload


def load_settings(*, refresh_remote: bool = False) -> dict[str, Any]:
    # Actions restores runtime-data before headless execution.  Page17 may stay
    # alive for days, so an explicit remote refresh is available for the UI.
    local=_read_local(SETTINGS_FILE,{})
    if refresh_remote:
        local = _refresh_small_json_from_github(SETTINGS_FILE, local, clock_fields=("updated_at",))
    if isinstance(local,dict) and local:
        return normalize_settings(local)
    try:
        from godpick_persistence_service import load_named_json_permanent
        payload,_=load_named_json_permanent(SETTINGS_FILE, {})
        if isinstance(payload,dict) and payload: return normalize_settings(payload)
    except Exception: pass
    return normalize_settings(local)


def save_settings(settings: dict[str, Any]) -> tuple[bool,str]:
    payload=normalize_settings(settings); payload["updated_at"]=now_text()
    try:
        from godpick_durability_service import persist_json_permanent
        return persist_json_permanent(SETTINGS_FILE,payload,reason="V191 central auto scheduler settings")
    except Exception as exc:
        return False,f"排程永久保存失敗：{exc}"


def load_status(*, refresh_remote: bool = False) -> dict[str,Any]:
    raw=_read_local(STATUS_FILE,{})
    if refresh_remote:
        raw = _refresh_small_json_from_github(STATUS_FILE, raw, clock_fields=("updated_at", "last_wakeup_at"))
    if not (isinstance(raw,dict) and raw):
        try:
            from godpick_persistence_service import load_named_json_permanent
            raw,_=load_named_json_permanent(STATUS_FILE,{})
        except Exception:
            raw={}
    return raw if isinstance(raw,dict) else {}


def load_history(*, refresh_remote: bool = False) -> list[dict[str,Any]]:
    raw=_read_local(HISTORY_FILE,[])
    if refresh_remote:
        try:
            from godpick_persistence_service import read_github_json, write_local_json_atomic
            remote,_ = read_github_json(HISTORY_FILE,{})
            local_records = raw.get("records",[]) if isinstance(raw,dict) else (raw if isinstance(raw,list) else [])
            remote_records = remote.get("records",[]) if isinstance(remote,dict) else (remote if isinstance(remote,list) else [])
            def _last_clock(rows):
                if not isinstance(rows,list) or not rows: return ""
                row=rows[-1] if isinstance(rows[-1],dict) else {}
                return str(row.get("finished_at") or row.get("started_at") or "")
            if isinstance(remote_records,list) and remote_records and (_last_clock(remote_records) > _last_clock(local_records) or len(remote_records) > len(local_records)):
                raw=remote
                write_local_json_atomic(HISTORY_FILE, remote)
        except Exception:
            pass
    empty = raw in (None, {}, [])
    if empty:
        try:
            from godpick_persistence_service import load_named_json_permanent
            raw,_=load_named_json_permanent(HISTORY_FILE,[])
        except Exception:
            raw=[]
    if isinstance(raw,dict): raw=raw.get("records",[])
    return [x for x in raw if isinstance(x,dict)] if isinstance(raw,list) else []


def _persist_runtime(path_name:str,payload:Any,reason:str)->tuple[bool,str]:
    try:
        from godpick_durability_service import persist_json_permanent
        return persist_json_permanent(path_name,payload,reason=reason)
    except Exception as exc:
        p=BASE_DIR/path_name; p.parent.mkdir(parents=True,exist_ok=True)
        p.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
        return False,f"僅本機保存：{exc}"


def _slot_datetime(day: datetime, hhmm: str) -> datetime:
    hh,mm=[int(x) for x in hhmm.split(":")]
    return day.replace(hour=hh,minute=mm,second=0,microsecond=0)


def _run_key(job:str, slot_dt:datetime, *, force:bool=False)->str:
    if force:
        # A manual force-all is deployment/diagnostic execution, not a scheduled
        # slot.  Never consume a future production slot such as 20:25.
        return f"{job}|FORCE|{slot_dt.strftime('%Y-%m-%d %H:%M')}"
    return f"{job}|{slot_dt.strftime('%Y-%m-%d %H:%M')}"


def _already_done(status:dict[str,Any],run_key:str)->bool:
    return run_key in set(status.get("completed_run_keys",[]) or [])


def _due_slots(job_cfg:dict[str,Any], now:datetime, grace_minutes:int, *, force:bool=False, catch_up_missed_same_day: bool = True)->list[datetime]:
    if force:
        # Use the actual force execution minute.  Previous V191 code used the
        # day's last configured schedule and could mark a FUTURE slot complete.
        return [now.replace(second=0,microsecond=0)]
    past=[]; within_grace=[]
    for t in _normalize_times(job_cfg.get("times")):
        slot=_slot_datetime(now,t)
        if slot <= now:
            past.append(slot)
            if now <= slot + timedelta(minutes=grace_minutes):
                within_grace.append(slot)
    if within_grace:
        # Only the latest due slot matters for refresh-style jobs; an older slot
        # is superseded by a newer same-day refresh.
        return [max(within_grace)]
    if catch_up_missed_same_day and past:
        # GitHub scheduled workflows are a wake-up hint, not a hard real-time
        # clock.  H10 catches up the latest missed slot on the same trading day
        # instead of silently losing the job after grace_minutes expires.
        return [max(past)]
    return []


def _dependency_check(job_cfg:dict[str,Any],status:dict[str,Any],now:datetime)->tuple[bool,str]:
    if not bool(job_cfg.get("require_dependencies",False)): return True,""
    jobs=status.get("jobs") if isinstance(status.get("jobs"),dict) else {}
    missing=[]
    for dep in job_cfg.get("dependencies",[]) or []:
        row=jobs.get(dep) if isinstance(jobs.get(dep),dict) else {}
        if row.get("last_status")!="SUCCESS" or str(row.get("last_success_at",""))[:10]!=now.strftime("%Y-%m-%d"):
            missing.append(JOB_LABELS.get(dep,dep))
    if missing: return False,"前置尚未於今日成功："+"、".join(missing)
    return True,""


def _pid_alive(pid: Any, owner_host: str = "") -> bool | None:
    """Return True/False for a same-host PID, None when another host owns it."""
    try:
        pid_i = int(pid or 0)
    except Exception:
        return False
    if pid_i <= 0:
        return False
    current_host = socket.gethostname()
    if owner_host and owner_host != current_host:
        return None
    if pid_i == os.getpid():
        return True
    if os.name == "nt":
        try:
            import ctypes
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid_i)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return False
        except Exception:
            return None
    try:
        os.kill(pid_i, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return None


def _read_lock_meta() -> dict[str, Any]:
    try:
        raw = json.loads(LOCK_FILE.read_text(encoding="utf-8-sig")) if LOCK_FILE.exists() else {}
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _acquire_lock(max_age_minutes:int=240)->tuple[bool,str]:
    """Acquire scheduler lock and immediately reclaim locks left by a dead PID."""
    try:
        if LOCK_FILE.exists():
            age=max(0.0, time.time()-LOCK_FILE.stat().st_mtime)
            meta=_read_lock_meta()
            alive=_pid_alive(meta.get("pid"), str(meta.get("host") or ""))
            if alive is True:
                return False,f"中央排程已有執行中鎖定（PID {meta.get('pid')}，{age/60:.1f}分鐘）"
            if alive is None and age<max_age_minutes*60:
                return False,f"中央排程可能由其他主機執行中（{age/60:.1f}分鐘）"
            # Same-host dead PID or genuinely stale cross-host lock: reclaim now.
            LOCK_FILE.unlink(missing_ok=True)
        payload={"pid":os.getpid(),"host":socket.gethostname(),"started_at":now_text(),"updated_at":now_text(),"current_job":""}
        LOCK_FILE.write_text(json.dumps(payload,ensure_ascii=False),encoding="utf-8")
        return True,""
    except Exception as exc:
        return False,f"排程鎖建立失敗：{exc}"


def _touch_lock(current_job: str = "") -> None:
    try:
        if not LOCK_FILE.exists():
            return
        meta=_read_lock_meta()
        if int(meta.get("pid") or 0) != os.getpid():
            return
        meta["updated_at"]=now_text()
        meta["current_job"]=str(current_job or "")
        LOCK_FILE.write_text(json.dumps(meta,ensure_ascii=False),encoding="utf-8")
    except Exception:
        pass


def _release_lock():
    try:
        meta=_read_lock_meta()
        if not meta or int(meta.get("pid") or 0)==os.getpid():
            LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass


def _repair_failed_completed_keys(status: dict[str, Any]) -> int:
    """V191 hotfix: FAILED/BLOCKED slots must remain retryable within grace window."""
    completed=list(status.get("completed_run_keys",[]) or [])
    if not completed:
        return 0
    bad=set()
    jobs=status.get("jobs") if isinstance(status.get("jobs"),dict) else {}
    for job, row in jobs.items():
        if not isinstance(row,dict) or row.get("last_status") in {"SUCCESS","WARNING"}:
            continue
        slot=str(row.get("last_slot") or "").strip()
        if slot:
            bad.add(f"{job}|{slot}")
    if not bad:
        return 0
    repaired=[k for k in completed if k not in bad]
    removed=len(completed)-len(repaired)
    status["completed_run_keys"]=repaired[-1200:]
    return removed


def _repair_future_completed_keys(status: dict[str, Any], now: datetime) -> int:
    """Remove legacy non-FORCE completion keys that point to future slots.

    H2 stopped creating these keys, but old bad keys can survive in runtime-data
    and suppress tonight's real 20:25/20:55 jobs.  FORCE keys are diagnostic
    history and are never touched.
    """
    completed=list(status.get("completed_run_keys",[]) or [])
    if not completed:
        return 0
    repaired=[]; removed=0
    for key in completed:
        text=str(key or "")
        if "|FORCE|" in text:
            repaired.append(text); continue
        try:
            job, slot_text = text.split("|",1)
            slot_dt=datetime.strptime(slot_text,"%Y-%m-%d %H:%M").replace(tzinfo=TZ)
        except Exception:
            repaired.append(text); continue
        if slot_dt > now + timedelta(minutes=1):
            removed += 1
            continue
        repaired.append(text)
    status["completed_run_keys"]=repaired[-1200:]
    return removed


def _checkpoint_runtime(status: dict[str, Any], history: list[dict[str, Any]], cfg: dict[str, Any], reason: str) -> None:
    """Persist after every job so a Streamlit rerun/process restart cannot erase progress."""
    history[:] = history[-int(cfg.get("history_keep",400)):]
    status["updated_at"]=now_text()
    try:
        _persist_runtime(STATUS_FILE,status,f"V191 scheduler checkpoint｜{reason}")
    except Exception:
        pass
    try:
        _persist_runtime(HISTORY_FILE,{"version":VERSION,"updated_at":now_text(),"records":history},f"V191 scheduler history checkpoint｜{reason}")
    except Exception:
        pass


def _execute_one(job:str, job_cfg:dict[str,Any], global_cfg:dict[str,Any]) -> dict[str,Any]:
    from godpick_auto_update_tasks import TASK_HANDLERS
    handler=TASK_HANDLERS.get(job)
    if not callable(handler): return {"ok":False,"message":f"找不到工作處理器：{job}"}
    attempts=max(1,int(global_cfg.get("retry_count",2) or 0)+1); delay=int(global_cfg.get("retry_delay_seconds",20) or 20)
    last={}
    for n in range(1,attempts+1):
        _touch_lock(job)
        try: last=handler(dict(job_cfg.get("options") or {})) or {}
        except Exception as exc: last={"ok":False,"message":f"{type(exc).__name__}: {exc}"}
        last["attempt"]=n; last["max_attempts"]=attempts
        if last.get("ok"): break
        if n<attempts:
            _touch_lock(f"{job}｜等待重試 {n+1}/{attempts}")
            time.sleep(delay)
    _touch_lock(job)
    return last


def _emit_progress(progress_callback, event: dict[str, Any]) -> None:
    """Best-effort live progress callback; UI failures must never stop scheduler work."""
    if not callable(progress_callback):
        return
    try:
        progress_callback(dict(event or {}))
    except Exception:
        # Progress rendering is observational only.  Never let a Streamlit/UI
        # callback failure change durable scheduler execution semantics.
        return


def run_due_jobs(*, now:datetime|None=None, force_all_enabled:bool=False, simulate:bool=False, selected_jobs:list[str]|None=None, progress_callback=None) -> dict[str,Any]:
    now=(now or now_tw()).astimezone(TZ); cfg=load_settings(); status=load_status(); history=load_history()
    status.setdefault("version",VERSION); status.setdefault("jobs",{}); status.setdefault("completed_run_keys",[])
    repaired_count=_repair_failed_completed_keys(status)
    repaired_future_count=_repair_future_completed_keys(status, now)
    status["version"]=VERSION; status["last_wakeup_at"]=now_text(now); status["scheduler_enabled"]=bool(cfg.get("enabled"))

    # H10: a manual force-all is deployment validation and must never hijack
    # unattended production slots.  Older H5 behavior converted the next cron
    # wake into force-all retry-only mode; two stubborn failed jobs could then
    # suppress every real due job for up to 12 hours.  Preserve the diagnostic
    # evidence, but let the normal scheduled due-check proceed independently.
    resumed_force=False
    prior_active=status.get("active_run") if isinstance(status.get("active_run"),dict) else {}
    if not force_all_enabled and not selected_jobs and prior_active.get("mode")=="force_all":
        pending=[str(x) for x in (prior_active.get("pending_jobs") or []) if str(x) in JOB_LABELS]
        if pending:
            snapshot={**prior_active}
            snapshot["deferred_at"]=now_text(now)
            snapshot["reason"]="H10：未完成強制驗證不再攔截正式無人值守排程；正式時段優先，必要時可由Page17再次手動強制驗證。"
            status["last_incomplete_run"]=snapshot
            status["last_force_pending_jobs"]=pending
        status.pop("active_run",None)

    if not cfg.get("enabled") and not force_all_enabled:
        return {"ok":True,"skipped":True,"message":"V191中央自動排程目前未啟用；未執行任何工作。","executed":[],"settings":cfg,"status":status}
    if cfg.get("weekdays_only",True) and now.weekday()>=5 and not force_all_enabled:
        return {"ok":True,"skipped":True,"message":"今日為週末，依設定不執行自動交易資料工作。","executed":[],"settings":cfg,"status":status}
    lock_ok,lock_msg=(True,"") if simulate else _acquire_lock()
    if not lock_ok: return {"ok":False,"skipped":True,"message":lock_msg,"executed":[],"settings":cfg,"status":status}

    enabled_order=[job for job,jc in cfg.get("jobs",{}).items() if bool(jc.get("enabled",False)) and (not selected_jobs or job in selected_jobs)]
    _progress_total=len(enabled_order)
    _progress_counts={"success":0,"warning":0,"failed":0,"blocked":0}
    _emit_progress(progress_callback,{
        "event":"run_start","force_all":bool(force_all_enabled),"simulate":bool(simulate),
        "resumed_force_batch":bool(resumed_force),"total_jobs":_progress_total,"completed_jobs":0,
        "pending_jobs":list(enabled_order),"started_at":now_text(now),**_progress_counts,
    })
    if not simulate:
        if not resumed_force:
            status["active_run"]={
                "mode":"force_all" if force_all_enabled else "scheduled",
                "started_at":now_text(now),"updated_at":now_text(now),"pid":os.getpid(),"host":socket.gethostname(),
                "pending_jobs":list(enabled_order),"completed_jobs":[],"failed_jobs":[],"blocked_jobs":[],
            }
        else:
            status["active_run"]["resumed_at"]=now_text(now)
            status["active_run"]["pid"]=os.getpid(); status["active_run"]["host"]=socket.gethostname()
        repair_msgs=[]
        if repaired_count:
            repair_msgs.append(f"清除 {repaired_count} 個舊版誤標失敗完成鍵")
        if repaired_future_count:
            repair_msgs.append(f"清除 {repaired_future_count} 個尚未到時卻誤標完成的未來正式slot")
        if repair_msgs:
            status["last_repair_message"]="；".join(repair_msgs)+"，恢復正確排程。"
        _checkpoint_runtime(status,history,cfg,"run-start")

    executed=[]; overall=True
    try:
        for job,job_cfg in cfg.get("jobs",{}).items():
            if selected_jobs and job not in selected_jobs: continue
            if not bool(job_cfg.get("enabled",False)): continue
            due=_due_slots(job_cfg,now,int(cfg.get("grace_minutes",35)),force=force_all_enabled,catch_up_missed_same_day=bool(cfg.get("catch_up_missed_same_day",True)))
            job_had_slot=False
            for slot in due:
                key=_run_key(job,slot,force=force_all_enabled)
                if _already_done(status,key) and not force_all_enabled: continue
                job_had_slot=True
                _touch_lock(job)
                try:
                    _progress_index=enabled_order.index(job)+1
                except Exception:
                    _progress_index=len(executed)+1
                _emit_progress(progress_callback,{
                    "event":"job_start","job":job,"job_label":JOB_LABELS.get(job,job),
                    "index":_progress_index,"total_jobs":_progress_total,"completed_jobs":len(executed),
                    "slot":slot.strftime("%Y-%m-%d %H:%M"),"started_at":now_text(now) if simulate else now_text(),
                    **_progress_counts,
                })
                deps_ok,deps_msg=_dependency_check(job_cfg,status,now)
                started=now_text(now) if simulate else now_text()
                if not deps_ok:
                    result={"ok":False,"blocked":True,"message":deps_msg,"finished_at":now_text(now) if simulate else now_text()}
                elif simulate:
                    result={"ok":True,"simulated":True,"message":"模擬：到期且前置條件已通過，未實際執行。","finished_at":now_text(now)}
                else:
                    result=_execute_one(job,job_cfg,cfg)
                row={
                    "run_key":key,"job":job,"job_label":JOB_LABELS.get(job,job),"slot":slot.strftime("%Y-%m-%d %H:%M"),
                    "started_at":started,"finished_at":result.get("finished_at") or now_text(),"status":("WARNING" if result.get("ok") and result.get("warning") else ("SUCCESS" if result.get("ok") else ("BLOCKED" if result.get("blocked") else "FAILED"))),
                    "message":str(result.get("message") or ""),"duration_seconds":result.get("duration_seconds"),"attempt":result.get("attempt"),"details":result.get("details",{}),
                }
                executed.append(row); history.append({k:v for k,v in row.items() if k != "details"})
                js=status["jobs"].setdefault(job,{})
                js.update({"label":JOB_LABELS.get(job,job),"last_status":row["status"],"last_run_at":row["finished_at"],"last_message":row["message"],"last_duration_seconds":row.get("duration_seconds"),"last_slot":row["slot"]})
                completed=list(status.get("completed_run_keys",[]) or [])
                if row["status"] in {"SUCCESS","WARNING"}:
                    if row["status"]=="SUCCESS":
                        js["last_success_at"]=row["finished_at"]
                    else:
                        js["last_warning_at"]=row["finished_at"]
                    js["last_completed_at"]=row["finished_at"]
                    if key not in completed: completed.append(key)
                else:
                    overall=False
                    # Failed/blocked slots remain retryable; warnings are complete
                    # degraded runs and must not loop forever.
                    completed=[x for x in completed if x != key]
                    js["last_failed_at"]=row["finished_at"]
                status["completed_run_keys"]=completed[-1200:]
                if not simulate:
                    active=status.get("active_run") if isinstance(status.get("active_run"),dict) else {}
                    if row["status"]=="FAILED" and job not in active.get("failed_jobs",[]): active.setdefault("failed_jobs",[]).append(job)
                    if row["status"]=="BLOCKED" and job not in active.get("blocked_jobs",[]): active.setdefault("blocked_jobs",[]).append(job)
                    if row["status"]=="WARNING" and job not in active.get("warning_jobs",[]): active.setdefault("warning_jobs",[]).append(job)
                    active["last_job"]=job; active["last_status"]=row["status"]; active["updated_at"]=now_text()
                    status["last_summary"]={"executed":len(executed),"success":sum(1 for x in executed if x["status"]=="SUCCESS"),"warning":sum(1 for x in executed if x["status"]=="WARNING"),"failed":sum(1 for x in executed if x["status"]=="FAILED"),"blocked":sum(1 for x in executed if x["status"]=="BLOCKED"),"in_progress":True}
                    _checkpoint_runtime(status,history,cfg,f"after-{job}-{row['status'].lower()}")
                _progress_counts={
                    "success":sum(1 for x in executed if x["status"]=="SUCCESS"),
                    "warning":sum(1 for x in executed if x["status"]=="WARNING"),
                    "failed":sum(1 for x in executed if x["status"]=="FAILED"),
                    "blocked":sum(1 for x in executed if x["status"]=="BLOCKED"),
                }
                _emit_progress(progress_callback,{
                    "event":"job_complete","job":job,"job_label":JOB_LABELS.get(job,job),
                    "index":_progress_index,"total_jobs":_progress_total,"completed_jobs":len(executed),
                    "status":row["status"],"message":row["message"],"finished_at":row["finished_at"],
                    "duration_seconds":row.get("duration_seconds"),**_progress_counts,
                })
            if not simulate and job_had_slot:
                active=status.get("active_run") if isinstance(status.get("active_run"),dict) else {}
                pending=active.get("pending_jobs") if isinstance(active.get("pending_jobs"),list) else []
                _last_job_status = str((status.get("jobs",{}).get(job,{}) or {}).get("last_status") or "")
                if _last_job_status in {"SUCCESS","WARNING"}:
                    active["pending_jobs"]=[x for x in pending if x != job]
                    if job not in active.get("completed_jobs",[]): active.setdefault("completed_jobs",[]).append(job)
                else:
                    # Keep failed/blocked force-all evidence pending in this manual
                    # validation batch.  H10 no longer lets an unattended cron wake
                    # switch into force-only retry mode; production slots stay first.
                    active["pending_jobs"]=list(pending)
                    active["resume_reason"]="存在FAILED/BLOCKED工作；H10保留診斷但不攔截正式排程"
                active["updated_at"]=now_text()
                _checkpoint_runtime(status,history,cfg,f"job-finished-{job}-{_last_job_status.lower() or 'unknown'}")

        history=history[-int(cfg.get("history_keep",400)):]
        summary={"executed":len(executed),"success":sum(1 for x in executed if x["status"]=="SUCCESS"),"warning":sum(1 for x in executed if x["status"]=="WARNING"),"failed":sum(1 for x in executed if x["status"]=="FAILED"),"blocked":sum(1 for x in executed if x["status"]=="BLOCKED"),"in_progress":False}
        status["updated_at"]=now_text(); status["last_summary"]=summary
        if not simulate:
            active=status.get("active_run") if isinstance(status.get("active_run"),dict) else {}
            remaining=[str(x) for x in (active.get("pending_jobs") or []) if str(x)] if isinstance(active,dict) else []
            if force_all_enabled and remaining:
                # Do not erase manual force validation evidence when work failed,
                # but H10 will not auto-convert the next production wake into this
                # force batch.
                active["paused_at"]=now_text(); active["summary"]=summary
                active["resume_reason"]="本輪仍有未完成強制驗證工作；正式排程不受影響，可人工再次強制驗證"
                status["active_run"]=active
                status["last_incomplete_run"]={**active}
                _checkpoint_runtime(status,history,cfg,"run-paused-pending-retry")
            else:
                active=status.pop("active_run",None)
                if isinstance(active,dict):
                    active["finished_at"]=now_text(); active["summary"]=summary
                    status["last_completed_run"]=active
                _checkpoint_runtime(status,history,cfg,"run-finished")
        resume_text="（續跑上次中斷的強制批次）" if resumed_force else ""
        _emit_progress(progress_callback,{
            "event":"run_complete","force_all":bool(force_all_enabled),"simulate":bool(simulate),
            "resumed_force_batch":bool(resumed_force),"total_jobs":_progress_total,"completed_jobs":len(executed),
            "pending_jobs":list(((status.get("active_run") or {}).get("pending_jobs") or [])) if isinstance(status,dict) else [],
            **{k:summary.get(k,0) for k in ("success","warning","failed","blocked")},
            "finished_at":now_text(now) if simulate else now_text(),
        })
        return {"ok":overall,"message":f"V191排程{resume_text}本輪執行 {len(executed)} 項：成功 {summary['success']}／警示 {summary['warning']}／失敗 {summary['failed']}／前置阻擋 {summary['blocked']}","executed":executed,"settings":cfg,"status":status,"resumed_force_batch":resumed_force,"repaired_failed_run_keys":repaired_count,"repaired_future_run_keys":repaired_future_count}
    finally:
        if not simulate: _release_lock()


def job_status_rows(job_ids: list[str] | None = None) -> list[dict[str, Any]]:
    cfg=load_settings(); status=load_status(); rows=next_run_rows(cfg,status)
    wanted=set(job_ids or [])
    return [r for r in rows if not wanted or r.get("工作ID") in wanted]


def next_run_rows(settings:dict[str,Any]|None=None,status:dict[str,Any]|None=None,now:datetime|None=None)->list[dict[str,Any]]:
    cfg=normalize_settings(settings or load_settings()); now=(now or now_tw()).astimezone(TZ); status=status or load_status(); rows=[]
    completed=set(status.get("completed_run_keys",[]) or []) if isinstance(status,dict) else set()
    catch_up=bool(cfg.get("catch_up_missed_same_day",True))
    grace=int(cfg.get("grace_minutes",35) or 35)
    for job,jc in cfg["jobs"].items():
        times=_normalize_times(jc.get("times"))
        today_slots=[_slot_datetime(now,t) for t in times]
        past=[dt for dt in today_slots if dt<=now]
        latest_past=max(past) if past else None
        overdue=False
        overdue_slot=None
        if bool(jc.get("enabled")) and latest_past is not None:
            key=_run_key(job,latest_past)
            if key not in completed and (catch_up or now<=latest_past+timedelta(minutes=grace)):
                overdue=True; overdue_slot=latest_past
        candidates=[]
        for add in range(0,8):
            day=now+timedelta(days=add)
            if cfg.get("weekdays_only",True) and day.weekday()>=5: continue
            for t in times:
                dt=_slot_datetime(day,t)
                if dt>now: candidates.append(dt)
        nxt=min(candidates) if candidates else None
        js=(status.get("jobs",{}) or {}).get(job,{})
        next_text=now_text(overdue_slot)+"（到期待執行/補跑）" if overdue and overdue_slot else (now_text(nxt) if nxt else "—")
        due_state=("到期待執行" if overdue_slot and now<=overdue_slot+timedelta(minutes=grace) else "漏點待補跑") if overdue else "等待下次時段"
        rows.append({
            "工作ID":job,"自動更新項目":JOB_LABELS.get(job,job),"啟用":bool(jc.get("enabled")),
            "每日時間":",".join(times),"到期狀態":due_state,"下次預計":next_text,
            "最後狀態":js.get("last_status","尚未執行"),"最後成功":js.get("last_success_at",""),"最後訊息":js.get("last_message","")
        })
    return rows


RUNTIME_ARTIFACTS=[
    SETTINGS_FILE,STATUS_FILE,HISTORY_FILE,
    "market_snapshot.json","macro_mode_bridge.json","macro_trend_records.json","market_nextday_forecast_records.json",
    "macro_market_close_cache.json","macro_institutional_cache.json","macro_taifex_cache.json","macro_us_market_cache.json","macro_otc_cache.json","overnight_global_market_cache.json","macro_news_event_cache.json","macro_v70_one_click_status.json",
    "official_factors_cache.json","official_factors_update_log.json","official_factor_institutional_history.json",
    "stock_master_cache.json","godpick_records.json","godpick_latest_recommendations.json","godpick_latest_run_anchor.json","godpick_recommend_list.json",
    "godpick_rotation_history.json","godpick_learning_state.json","godpick_calibration_samples.json","godpick_performance_profile.json",
    "super_ai_market_context.json","super_ai_experience_index.json","super_ai_experience_profile.json","godpick_t1_trade_truth.json","godpick_probability_calibration.json",
    "godpick_durability_outbox.json","godpick_durability_audit.json","godpick_recommendation_readiness.json",
]

__all__=["VERSION","SETTINGS_FILE","STATUS_FILE","HISTORY_FILE","JOB_LABELS","DEFAULT_SETTINGS","load_settings","save_settings","load_status","load_history","run_due_jobs","next_run_rows","job_status_rows","normalize_settings","RUNTIME_ARTIFACTS"]
