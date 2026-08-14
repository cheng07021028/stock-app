# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def patch_scheduler() -> None:
    path = ROOT / "godpick_auto_scheduler.py"
    src = path.read_text(encoding="utf-8-sig")
    src = src.replace(
        'VERSION = "godpick_auto_scheduler_v191_20260814_hotfix19_page17_autocatchup"',
        'VERSION = "godpick_auto_scheduler_v191_20260815_hotfix27_wakeup_heartbeat_truth"',
        1,
    )

    anchor = '''    status["last_wakeup_source"] = _wake_source[:80]\n\n    # H10: a manual force-all is deployment validation and must never hijack\n'''
    replacement = '''    status["last_wakeup_source"] = _wake_source[:80]\n    # H27: bind every persisted heartbeat to the concrete external wake.  GitHub\n    # exposes GITHUB_RUN_ID/GITHUB_RUN_ATTEMPT automatically; Windows/manual wakes\n    # may leave them blank but still retain source/time.\n    status["last_wakeup_run_id"] = str(os.environ.get("GITHUB_RUN_ID") or os.environ.get("GODPICK_WAKEUP_RUN_ID") or "").strip()\n    status["last_wakeup_attempt"] = str(os.environ.get("GITHUB_RUN_ATTEMPT") or "").strip()\n    status["last_wakeup_event"] = str(os.environ.get("GODPICK_WAKEUP_EVENT") or "").strip()\n\n    # H10: a manual force-all is deployment validation and must never hijack\n'''
    if anchor not in src:
        raise SystemExit("H27 scheduler wake metadata anchor not found")
    src = src.replace(anchor, replacement, 1)

    old = '''    if not cfg.get("enabled") and not force_all_enabled:\n        return {"ok":True,"skipped":True,"message":"V191中央自動排程目前未啟用；未執行任何工作。","executed":[],"settings":cfg,"status":status}\n    if cfg.get("weekdays_only",True) and now.weekday()>=5 and not force_all_enabled:\n        return {"ok":True,"skipped":True,"message":"今日為週末，依設定不執行自動交易資料工作。","executed":[],"settings":cfg,"status":status}\n    lock_ok,lock_msg=(True,"") if simulate else _acquire_lock()\n    if not lock_ok: return {"ok":False,"skipped":True,"message":lock_msg,"executed":[],"settings":cfg,"status":status}\n'''
    new = '''    if not cfg.get("enabled") and not force_all_enabled:\n        _skip_msg="V191中央自動排程目前未啟用；未執行任何工作。"\n        status["last_wakeup_result"]="SKIPPED_DISABLED"\n        status["last_wakeup_message"]=_skip_msg\n        status["updated_at"]=now_text(now)\n        if not simulate:\n            _persist_runtime(STATUS_FILE,status,"V191-H27 wake heartbeat｜scheduler disabled")\n        return {"ok":True,"skipped":True,"message":_skip_msg,"executed":[],"settings":cfg,"status":status}\n    if cfg.get("weekdays_only",True) and now.weekday()>=5 and not force_all_enabled:\n        _skip_msg="今日為週末，依設定不執行自動交易資料工作。"\n        status["last_wakeup_result"]="SKIPPED_WEEKEND"\n        status["last_wakeup_message"]=_skip_msg\n        status["updated_at"]=now_text(now)\n        if not simulate:\n            _persist_runtime(STATUS_FILE,status,"V191-H27 wake heartbeat｜weekend skip")\n        return {"ok":True,"skipped":True,"message":_skip_msg,"executed":[],"settings":cfg,"status":status}\n    lock_ok,lock_msg=(True,"") if simulate else _acquire_lock()\n    if not lock_ok:\n        status["last_wakeup_result"]="SKIPPED_ACTIVE_LOCK"\n        status["last_wakeup_message"]=str(lock_msg or "中央排程已有執行中鎖定")\n        status["updated_at"]=now_text(now)\n        if not simulate:\n            _persist_runtime(STATUS_FILE,status,"V191-H27 wake heartbeat｜active lock")\n        return {"ok":False,"skipped":True,"message":lock_msg,"executed":[],"settings":cfg,"status":status}\n'''
    if old not in src:
        raise SystemExit("H27 scheduler early-return anchor not found")
    src = src.replace(old, new, 1)

    start_anchor = '''        _checkpoint_runtime(status,history,cfg,"run-start")\n\n    executed=[]; overall=True\n'''
    start_new = '''        status["last_wakeup_result"]="RUNNING"\n        status["last_wakeup_message"]="中央排程已喚醒並進入到期工作檢查/執行。"\n        _checkpoint_runtime(status,history,cfg,"run-start")\n\n    executed=[]; overall=True\n'''
    if start_anchor not in src:
        raise SystemExit("H27 run-start anchor not found")
    src = src.replace(start_anchor, start_new, 1)

    finish_anchor = '''        status["updated_at"]=now_text(); status["last_progress_at"]=status["updated_at"]; status["last_summary"]=summary\n        if not simulate:\n'''
    finish_new = '''        status["updated_at"]=now_text(); status["last_progress_at"]=status["updated_at"]; status["last_summary"]=summary\n        status["last_wakeup_result"]="COMPLETED" if overall else "COMPLETED_WITH_FAILURE"\n        status["last_wakeup_message"]=(\n            f"本輪完成：成功 {summary['success']}／警示 {summary['warning']}／失敗 {summary['failed']}／前置阻擋 {summary['blocked']}"\n        )\n        if not simulate:\n'''
    if finish_anchor not in src:
        raise SystemExit("H27 run-finish anchor not found")
    src = src.replace(finish_anchor, finish_new, 1)

    path.write_text(src, encoding="utf-8")


def patch_verifier() -> None:
    path = ROOT / "tools" / "verify_godpick_scheduler_remote_v191.py"
    src = path.read_text(encoding="utf-8-sig")
    if "import os\n" not in src:
        src = src.replace("import json\nimport sys\n", "import json\nimport os\nimport sys\n", 1)

    old = '''    local_wake = _clock(local, "last_wakeup_at")\n    remote_wake = _clock(remote, "last_wakeup_at")\n    local_updated = _clock(local, "updated_at", "last_wakeup_at")\n    remote_updated = _clock(remote, "updated_at", "last_wakeup_at")\n    if isinstance(remote, dict) and remote and remote_wake >= local_wake and remote_updated >= local_updated:\n        return True, f"status remote confirmed｜wake={remote_wake}"\n    report = _save_remote(STATUS_FILE, local)\n    remote2, _ = _read_remote(STATUS_FILE, {})\n    ok = bool(\n        isinstance(remote2, dict)\n        and _clock(remote2, "last_wakeup_at") >= local_wake\n        and _clock(remote2, "updated_at", "last_wakeup_at") >= local_updated\n    )\n    return ok, f"status repaired={ok}｜{getattr(report, 'github_message', '')}｜probe={msg}"\n'''
    new = '''    local_wake = _clock(local, "last_wakeup_at")\n    remote_wake = _clock(remote, "last_wakeup_at")\n    local_updated = _clock(local, "updated_at", "last_wakeup_at")\n    remote_updated = _clock(remote, "updated_at", "last_wakeup_at")\n    expected_run_id = str(os.environ.get("GITHUB_RUN_ID") or os.environ.get("GODPICK_WAKEUP_RUN_ID") or "").strip()\n    local_run_id = str(local.get("last_wakeup_run_id") or "").strip()\n    remote_run_id = str((remote or {}).get("last_wakeup_run_id") or "").strip() if isinstance(remote, dict) else ""\n    # H27: a verifier running inside GitHub Actions must confirm THIS workflow's\n    # heartbeat, not merely accept an older remote status that happens to be >= an\n    # equally stale restored local file.\n    if expected_run_id and local_run_id != expected_run_id:\n        return False, f"local scheduler heartbeat is not current workflow｜expected_run={expected_run_id} local_run={local_run_id or '-'} wake={local_wake or '-'}"\n    if (\n        isinstance(remote, dict) and remote\n        and remote_wake >= local_wake and remote_updated >= local_updated\n        and (not expected_run_id or remote_run_id == expected_run_id)\n    ):\n        return True, f"status remote confirmed｜wake={remote_wake}｜run={remote_run_id or '-'}"\n    report = _save_remote(STATUS_FILE, local)\n    remote2, _ = _read_remote(STATUS_FILE, {})\n    remote2_run_id = str((remote2 or {}).get("last_wakeup_run_id") or "").strip() if isinstance(remote2, dict) else ""\n    ok = bool(\n        isinstance(remote2, dict)\n        and _clock(remote2, "last_wakeup_at") >= local_wake\n        and _clock(remote2, "updated_at", "last_wakeup_at") >= local_updated\n        and (not expected_run_id or remote2_run_id == expected_run_id)\n    )\n    return ok, f"status repaired={ok}｜run={remote2_run_id or '-'}｜{getattr(report, 'github_message', '')}｜probe={msg}"\n'''
    if old not in src:
        raise SystemExit("H27 verifier anchor not found")
    src = src.replace(old, new, 1)
    path.write_text(src, encoding="utf-8")


def patch_page17() -> None:
    path = ROOT / "pages" / "17_系統健康檢查.py"
    src = path.read_text(encoding="utf-8-sig")
    src = src.replace(
        'st.caption("V191-H19｜中央自動排程＋永久化監控：H19 加入進頁自動補送喚醒、8秒即時狀態刷新與逐列執行中/排隊顯示；H18 長工作防重跑與永久同步確認完整保留。")',
        'st.caption("V191-H27｜中央自動排程＋永久化監控：每次外部喚醒皆永久記錄 heartbeat/run_id/result；週末正常喚醒會明確顯示為依設定跳過，不再誤報排程失效。H18/H19 長工作防重跑與自動補送喚醒完整保留。")',
        1,
    )

    metric_anchor = '''        _m5.metric("喚醒來源", str((_auto_status or {}).get("last_wakeup_source") or "未標記"))\n        try:\n'''
    metric_new = '''        _m5.metric("喚醒來源", str((_auto_status or {}).get("last_wakeup_source") or "未標記"))\n        _last_wakeup_result = str((_auto_status or {}).get("last_wakeup_result") or "").strip()\n        _last_wakeup_message = str((_auto_status or {}).get("last_wakeup_message") or "").strip()\n        _last_wakeup_run_id = str((_auto_status or {}).get("last_wakeup_run_id") or "").strip()\n        if _last_wakeup_result:\n            st.caption(\n                f"最後喚醒結果：{_last_wakeup_result}"\n                + (f"｜{_last_wakeup_message}" if _last_wakeup_message else "")\n                + (f"｜run_id={_last_wakeup_run_id}" if _last_wakeup_run_id else "")\n            )\n        try:\n'''
    if metric_anchor not in src:
        raise SystemExit("H27 Page17 metric anchor not found")
    src = src.replace(metric_anchor, metric_new, 1)

    old = '''            elif _wake_age_min is not None:\n                if _wake_age_min <= 35:\n                    st.success(f"GitHub/中央排程 heartbeat 正常：最後可驗證喚醒約 {_wake_age_min:.0f} 分鐘前。")\n                else:\n                    st.warning(\n                        f"目前沒有執行中批次，且中央排程 heartbeat 已約 {_wake_age_min:.0f} 分鐘未更新。"\n                        "此時才需要檢查 Windows 嚴格10分鐘工作排程、網路或 GitHub 備援喚醒。"\n                    )\n'''
    new = '''            elif _wake_age_min is not None:\n                if _wake_age_min <= 35 and _last_wakeup_result == "SKIPPED_WEEKEND":\n                    st.success(\n                        f"GitHub/中央排程喚醒正常：最後可驗證喚醒約 {_wake_age_min:.0f} 分鐘前；"\n                        "今日為週末，依『僅交易日週一～週五執行』設定不執行交易資料工作。這不是排程故障。"\n                    )\n                elif _wake_age_min <= 35 and _last_wakeup_result == "SKIPPED_DISABLED":\n                    st.info(f"外部喚醒器正常：最後喚醒約 {_wake_age_min:.0f} 分鐘前，但中央自動排程總開關目前停用。")\n                elif _wake_age_min <= 35:\n                    st.success(f"GitHub/中央排程 heartbeat 正常：最後可驗證喚醒約 {_wake_age_min:.0f} 分鐘前。")\n                else:\n                    st.warning(\n                        f"目前沒有執行中批次，且中央排程 heartbeat 已約 {_wake_age_min:.0f} 分鐘未更新。"\n                        "此時才需要檢查 Windows 嚴格10分鐘工作排程、網路或 GitHub 備援喚醒。"\n                    )\n'''
    if old not in src:
        raise SystemExit("H27 Page17 heartbeat display anchor not found")
    src = src.replace(old, new, 1)
    path.write_text(src, encoding="utf-8")


if __name__ == "__main__":
    patch_scheduler()
    patch_verifier()
    patch_page17()
    print("V191-H27 scheduler heartbeat truth patch applied/verified")
