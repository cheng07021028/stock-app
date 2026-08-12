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
import threading
import time

VERSION = "godpick_auto_scheduler_v191_20260812"
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
    "durability_retry": {},
}

# Default is deliberately disabled until the user explicitly saves/enables it.
# Suggested times are production-safe Taiwan-time presets and remain editable.
DEFAULT_SETTINGS: dict[str, Any] = {
    "version": VERSION,
    "timezone": "Asia/Taipei",
    "enabled": False,
    "weekdays_only": True,
    "grace_minutes": 35,
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
        for k in ["enabled","weekdays_only","grace_minutes","retry_count","retry_delay_seconds","history_keep","updated_at"]:
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
    out["grace_minutes"]=max(5,min(int(out.get("grace_minutes",35) or 35),180))
    out["retry_count"]=max(0,min(int(out.get("retry_count",2) or 0),5))
    out["retry_delay_seconds"]=max(1,min(int(out.get("retry_delay_seconds",20) or 20),300))
    out["history_keep"]=max(50,min(int(out.get("history_keep",400) or 400),2000))
    out["version"]=VERSION; out["timezone"]="Asia/Taipei"
    return out


def load_settings() -> dict[str, Any]:
    # Local-first after the first authority restore keeps Page17 fast.  V191
    # delivery packages intentionally exclude this runtime file, so a cold
    # reboot with no local copy falls through to runtime-data/Firestore once.
    local=_read_local(SETTINGS_FILE,{})
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


def load_status() -> dict[str,Any]:
    raw=_read_local(STATUS_FILE,{})
    if not (isinstance(raw,dict) and raw):
        try:
            from godpick_persistence_service import load_named_json_permanent
            raw,_=load_named_json_permanent(STATUS_FILE,{})
        except Exception:
            raw={}
    return raw if isinstance(raw,dict) else {}


def load_history() -> list[dict[str,Any]]:
    raw=_read_local(HISTORY_FILE,[])
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


def _run_key(job:str, slot_dt:datetime)->str:
    return f"{job}|{slot_dt.strftime('%Y-%m-%d %H:%M')}"


def _already_done(status:dict[str,Any],run_key:str)->bool:
    return run_key in set(status.get("completed_run_keys",[]) or [])


def _due_slots(job_cfg:dict[str,Any], now:datetime, grace_minutes:int, *, force:bool=False)->list[datetime]:
    if force:
        times=_normalize_times(job_cfg.get("times"))
        return [_slot_datetime(now,times[-1] if times else now.strftime("%H:%M"))]
    out=[]
    for t in _normalize_times(job_cfg.get("times")):
        slot=_slot_datetime(now,t)
        if slot<=now<=slot+timedelta(minutes=grace_minutes): out.append(slot)
    return out


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


def _acquire_lock(max_age_minutes:int=240)->tuple[bool,str]:
    try:
        if LOCK_FILE.exists():
            age=time.time()-LOCK_FILE.stat().st_mtime
            if age<max_age_minutes*60: return False,f"中央排程已有執行中鎖定（{age/60:.1f}分鐘）"
            LOCK_FILE.unlink(missing_ok=True)
        LOCK_FILE.write_text(json.dumps({"pid":os.getpid(),"started_at":now_text()},ensure_ascii=False),encoding="utf-8")
        return True,""
    except Exception as exc:
        return False,f"排程鎖建立失敗：{exc}"


def _release_lock():
    try: LOCK_FILE.unlink(missing_ok=True)
    except Exception: pass


def _execute_one(job:str, job_cfg:dict[str,Any], global_cfg:dict[str,Any]) -> dict[str,Any]:
    from godpick_auto_update_tasks import TASK_HANDLERS
    handler=TASK_HANDLERS.get(job)
    if not callable(handler): return {"ok":False,"message":f"找不到工作處理器：{job}"}
    attempts=max(1,int(global_cfg.get("retry_count",2) or 0)+1); delay=int(global_cfg.get("retry_delay_seconds",20) or 20)
    last={}
    for n in range(1,attempts+1):
        try: last=handler(dict(job_cfg.get("options") or {})) or {}
        except Exception as exc: last={"ok":False,"message":f"{type(exc).__name__}: {exc}"}
        last["attempt"]=n; last["max_attempts"]=attempts
        if last.get("ok"): break
        if n<attempts: time.sleep(delay)
    return last


def run_due_jobs(*, now:datetime|None=None, force_all_enabled:bool=False, simulate:bool=False, selected_jobs:list[str]|None=None) -> dict[str,Any]:
    now=(now or now_tw()).astimezone(TZ); cfg=load_settings(); status=load_status(); history=load_history()
    status.setdefault("version",VERSION); status.setdefault("jobs",{}); status.setdefault("completed_run_keys",[])
    status["last_wakeup_at"]=now_text(now); status["scheduler_enabled"]=bool(cfg.get("enabled"))
    if not cfg.get("enabled") and not force_all_enabled:
        return {"ok":True,"skipped":True,"message":"V191中央自動排程目前未啟用；未執行任何工作。","executed":[],"settings":cfg,"status":status}
    if cfg.get("weekdays_only",True) and now.weekday()>=5 and not force_all_enabled:
        return {"ok":True,"skipped":True,"message":"今日為週末，依設定不執行自動交易資料工作。","executed":[],"settings":cfg,"status":status}
    lock_ok,lock_msg=(True,"") if simulate else _acquire_lock()
    if not lock_ok: return {"ok":False,"skipped":True,"message":lock_msg,"executed":[],"settings":cfg,"status":status}
    executed=[]; overall=True
    try:
        for job,job_cfg in cfg.get("jobs",{}).items():
            if selected_jobs and job not in selected_jobs: continue
            if not bool(job_cfg.get("enabled",False)): continue
            due=_due_slots(job_cfg,now,int(cfg.get("grace_minutes",35)),force=force_all_enabled)
            for slot in due:
                key=_run_key(job,slot)
                if _already_done(status,key) and not force_all_enabled: continue
                deps_ok,deps_msg=_dependency_check(job_cfg,status,now)
                started=now_text()
                if not deps_ok:
                    result={"ok":False,"blocked":True,"message":deps_msg,"finished_at":now_text()}
                elif simulate:
                    result={"ok":True,"simulated":True,"message":"模擬：到期且前置條件已通過，未實際執行。","finished_at":now_text()}
                else:
                    result=_execute_one(job,job_cfg,cfg)
                row={
                    "run_key":key,"job":job,"job_label":JOB_LABELS.get(job,job),"slot":slot.strftime("%Y-%m-%d %H:%M"),
                    "started_at":started,"finished_at":result.get("finished_at") or now_text(),"status":"SUCCESS" if result.get("ok") else ("BLOCKED" if result.get("blocked") else "FAILED"),
                    "message":str(result.get("message") or ""),"duration_seconds":result.get("duration_seconds"),"attempt":result.get("attempt"),"details":result.get("details",{}),
                }
                executed.append(row); history.append({k:v for k,v in row.items() if k != "details"})
                js=status["jobs"].setdefault(job,{})
                js.update({"label":JOB_LABELS.get(job,job),"last_status":row["status"],"last_run_at":row["finished_at"],"last_message":row["message"],"last_duration_seconds":row.get("duration_seconds"),"last_slot":row["slot"]})
                if row["status"]=="SUCCESS": js["last_success_at"]=row["finished_at"]
                else: overall=False
                if not simulate:
                    status["completed_run_keys"]=(status.get("completed_run_keys",[])+[key])[-1200:]
        history=history[-int(cfg.get("history_keep",400)):]
        status["updated_at"]=now_text(); status["last_summary"]={"executed":len(executed),"success":sum(1 for x in executed if x["status"]=="SUCCESS"),"failed":sum(1 for x in executed if x["status"]=="FAILED"),"blocked":sum(1 for x in executed if x["status"]=="BLOCKED")}
        if not simulate:
            _persist_runtime(STATUS_FILE,status,"V191 scheduler execution status")
            _persist_runtime(HISTORY_FILE,{"version":VERSION,"updated_at":now_text(),"records":history},"V191 scheduler execution history")
        return {"ok":overall,"message":f"V191排程本輪執行 {len(executed)} 項：成功 {sum(1 for x in executed if x['status']=='SUCCESS')}／失敗 {sum(1 for x in executed if x['status']=='FAILED')}／前置阻擋 {sum(1 for x in executed if x['status']=='BLOCKED')}","executed":executed,"settings":cfg,"status":status}
    finally:
        if not simulate: _release_lock()


def job_status_rows(job_ids: list[str] | None = None) -> list[dict[str, Any]]:
    cfg=load_settings(); status=load_status(); rows=next_run_rows(cfg,status)
    wanted=set(job_ids or [])
    return [r for r in rows if not wanted or r.get("工作ID") in wanted]


def next_run_rows(settings:dict[str,Any]|None=None,status:dict[str,Any]|None=None,now:datetime|None=None)->list[dict[str,Any]]:
    cfg=normalize_settings(settings or load_settings()); now=(now or now_tw()).astimezone(TZ); status=status or load_status(); rows=[]
    for job,jc in cfg["jobs"].items():
        times=_normalize_times(jc.get("times")); candidates=[]
        for add in range(0,8):
            day=now+timedelta(days=add)
            if cfg.get("weekdays_only",True) and day.weekday()>=5: continue
            for t in times:
                dt=_slot_datetime(day,t)
                if dt>now: candidates.append(dt)
        nxt=min(candidates) if candidates else None; js=(status.get("jobs",{}) or {}).get(job,{})
        rows.append({"工作ID":job,"自動更新項目":JOB_LABELS.get(job,job),"啟用":bool(jc.get("enabled")),"每日時間":",".join(times),"下次預計":now_text(nxt) if nxt else "—","最後狀態":js.get("last_status","尚未執行"),"最後成功":js.get("last_success_at",""),"最後訊息":js.get("last_message","")})
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
