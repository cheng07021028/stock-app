# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def patch_scheduler() -> None:
    path = ROOT / "godpick_auto_scheduler.py"
    src = path.read_text(encoding="utf-8-sig")

    src = src.replace(
        'VERSION = "godpick_auto_scheduler_v191_20260815_hotfix27_wakeup_heartbeat_truth"',
        'VERSION = "godpick_auto_scheduler_v191_20260815_hotfix28_persistent_execution_order"',
        1,
    )

    anchor = '''    "durability_retry": "17｜永久化失敗/待同步重試",\n}\n\nDEFAULT_JOB_OPTIONS = {'''
    replacement = '''    "durability_retry": "17｜永久化失敗/待同步重試",\n}\n\n# H28: canonical AI-first order.  Existing installations without an explicit\n# order are migrated to this sequence; Page17 can permanently customize it.\n# Recommendation is deliberately last by default so performance truth, T+1\n# calibration and daily learning are rebuilt before the next decision.\nDEFAULT_EXECUTION_ORDER = [\n    "stock_master",\n    "macro_full",\n    "official_factors",\n    "super_ai_context",\n    "watchlist_runtime",\n    "record_latest_price",\n    "record_performance",\n    "recommend_list_performance",\n    "recommend_list_n_day",\n    "recommend_list_hits",\n    "t1_truth",\n    "feedback_learning",\n    "durability_retry",\n    "godpick_recommendation",\n]\n\nDEFAULT_JOB_OPTIONS = {'''
    if anchor not in src:
        raise SystemExit("H28 JOB_LABELS anchor not found")
    src = src.replace(anchor, replacement, 1)

    anchor = '''    "history_keep": 400,\n    "updated_at": "",\n    "jobs": {'''
    replacement = '''    "history_keep": 400,\n    "updated_at": "",\n    "execution_order": list(DEFAULT_EXECUTION_ORDER),\n    "recommendation_run_last_after_all_enabled": True,\n    "jobs": {'''
    if anchor not in src:
        raise SystemExit("H28 DEFAULT_SETTINGS anchor not found")
    src = src.replace(anchor, replacement, 1)

    anchor = '''    return sorted(out)\n\n\ndef normalize_settings(raw: Any) -> dict[str, Any]:'''
    replacement = '''    return sorted(out)\n\n\ndef _normalize_execution_order(values: Any) -> list[str]:\n    raw = values if isinstance(values, list) else []\n    out: list[str] = []\n    for value in raw:\n        key = str(value or "").strip()\n        if key in JOB_LABELS and key not in out:\n            out.append(key)\n    for key in DEFAULT_EXECUTION_ORDER:\n        if key in JOB_LABELS and key not in out:\n            out.append(key)\n    for key in JOB_LABELS:\n        if key not in out:\n            out.append(key)\n    return out\n\n\ndef _ordered_job_keys(cfg: dict[str, Any]) -> list[str]:\n    order = _normalize_execution_order((cfg or {}).get("execution_order"))\n    if bool((cfg or {}).get("recommendation_run_last_after_all_enabled", True)):\n        order = [x for x in order if x != "godpick_recommendation"] + ["godpick_recommendation"]\n    return order\n\n\ndef normalize_settings(raw: Any) -> dict[str, Any]:'''
    if anchor not in src:
        raise SystemExit("H28 normalize helper anchor not found")
    src = src.replace(anchor, replacement, 1)

    old = '''        for k in ["enabled","weekdays_only","catch_up_missed_same_day","grace_minutes","retry_count","retry_delay_seconds","history_keep","updated_at"]:\n            if k in raw: out[k]=raw[k]'''
    new = '''        for k in ["enabled","weekdays_only","catch_up_missed_same_day","grace_minutes","retry_count","retry_delay_seconds","history_keep","updated_at","recommendation_run_last_after_all_enabled"]:\n            if k in raw: out[k]=raw[k]'''
    if old not in src:
        raise SystemExit("H28 normalize scalar anchor not found")
    src = src.replace(old, new, 1)

    anchor = '''            out["jobs"][key]=merged\n    out["catch_up_missed_same_day"]=bool(out.get("catch_up_missed_same_day", True))'''
    replacement = '''            out["jobs"][key]=merged\n    _raw_order = raw.get("execution_order") if isinstance(raw, dict) else None\n    out["execution_order"] = _normalize_execution_order(_raw_order or out.get("execution_order"))\n    out["recommendation_run_last_after_all_enabled"] = bool(out.get("recommendation_run_last_after_all_enabled", True))\n    if out["recommendation_run_last_after_all_enabled"]:\n        out["execution_order"] = [x for x in out["execution_order"] if x != "godpick_recommendation"] + ["godpick_recommendation"]\n    out["catch_up_missed_same_day"]=bool(out.get("catch_up_missed_same_day", True))'''
    if anchor not in src:
        raise SystemExit("H28 normalize order anchor not found")
    src = src.replace(anchor, replacement, 1)

    anchor = '''def _pid_alive(pid: Any, owner_host: str = "") -> bool | None:'''
    gate = '''def _recommendation_final_gate(global_cfg: dict[str, Any], status: dict[str, Any], now: datetime, job: str, *, force: bool = False) -> tuple[bool, str]:\n    """H28: recommendation waits for every enabled predecessor's final daily slot.\n\n    Merely sorting the loop is insufficient when recommendation is scheduled at\n    20:55 but learning/performance jobs are configured for 21:05~21:40.  With\n    the guard enabled, Page07 stays retryable and the next external wake runs it\n    only after all earlier enabled jobs have completed their latest configured\n    slot for the same Taiwan date.  SUCCESS and WARNING both create completion\n    keys; FAILED/BLOCKED never do, so they naturally keep Page07 waiting.\n    """\n    if job != "godpick_recommendation" or force:\n        return True, ""\n    if not bool((global_cfg or {}).get("recommendation_run_last_after_all_enabled", True)):\n        return True, ""\n    order = _ordered_job_keys(global_cfg)\n    try:\n        rec_index = order.index("godpick_recommendation")\n    except ValueError:\n        return True, ""\n    waiting: list[str] = []\n    jobs_cfg = (global_cfg or {}).get("jobs") if isinstance((global_cfg or {}).get("jobs"), dict) else {}\n    status_jobs = status.get("jobs") if isinstance(status.get("jobs"), dict) else {}\n    for dep in order[:rec_index]:\n        dep_cfg = jobs_cfg.get(dep) if isinstance(jobs_cfg.get(dep), dict) else {}\n        if not bool(dep_cfg.get("enabled", False)):\n            continue\n        times = _normalize_times(dep_cfg.get("times"))\n        if not times:\n            continue\n        latest_slot = _slot_datetime(now, max(times))\n        label = JOB_LABELS.get(dep, dep)\n        if now < latest_slot:\n            waiting.append(f"{label}（最後時段 {latest_slot.strftime('%H:%M')} 尚未到）")\n            continue\n        key = _run_key(dep, latest_slot)\n        if _already_done(status, key):\n            continue\n        dep_state = status_jobs.get(dep) if isinstance(status_jobs.get(dep), dict) else {}\n        last_status = str(dep_state.get("last_status") or "尚未完成")\n        last_slot = str(dep_state.get("last_slot") or "")\n        suffix = f"{last_status} {last_slot}".strip()\n        waiting.append(f"{label}（{latest_slot.strftime('%H:%M')} 最終時段未完成；{suffix}）")\n    if waiting:\n        return False, "H28最終推薦閘門等待：" + "、".join(waiting)\n    return True, ""\n\n\n'''
    if anchor not in src:
        raise SystemExit("H28 final gate anchor not found")
    src = src.replace(anchor, gate + anchor, 1)

    old = '''    enabled_order=[job for job,jc in cfg.get("jobs",{}).items() if bool(jc.get("enabled",False)) and (not selected_jobs or job in selected_jobs)]'''
    new = '''    enabled_order=[job for job in _ordered_job_keys(cfg) if bool(((cfg.get("jobs") or {}).get(job) or {}).get("enabled",False)) and (not selected_jobs or job in selected_jobs)]'''
    if old not in src:
        raise SystemExit("H28 enabled order anchor not found")
    src = src.replace(old, new, 1)

    old = '''        for job,job_cfg in cfg.get("jobs",{}).items():\n            if selected_jobs and job not in selected_jobs: continue'''
    new = '''        for job in _ordered_job_keys(cfg):\n            job_cfg=(cfg.get("jobs") or {}).get(job) or {}\n            if selected_jobs and job not in selected_jobs: continue'''
    if old not in src:
        raise SystemExit("H28 execution loop anchor not found")
    src = src.replace(old, new, 1)

    old = '''                deps_ok,deps_msg=_dependency_check(job_cfg,status,now)\n                started=now_text(now) if simulate else now_text()'''
    new = '''                deps_ok,deps_msg=_dependency_check(job_cfg,status,now)\n                if deps_ok:\n                    deps_ok,deps_msg=_recommendation_final_gate(cfg,status,now,job,force=force_all_enabled)\n                started=now_text(now) if simulate else now_text()'''
    if old not in src:
        raise SystemExit("H28 final gate call anchor not found")
    src = src.replace(old, new, 1)

    old = '''        for job,jc in cfg.get("jobs",{}).items():\n            if not bool((jc or {}).get("enabled",False)):\n                continue'''
    new = '''        for job in _ordered_job_keys(cfg):\n            jc=(cfg.get("jobs") or {}).get(job) or {}\n            if not bool((jc or {}).get("enabled",False)):\n                continue'''
    if old not in src:
        raise SystemExit("H28 wakeup decision order anchor not found")
    src = src.replace(old, new, 1)

    old = '''    live_active=bool(active_pending) and (active_age is None or active_age<=70)\n\n    for job,jc in cfg["jobs"].items():'''
    new = '''    live_active=bool(active_pending) and (active_age is None or active_age<=70)\n    configured_order=_ordered_job_keys(cfg)\n\n    for job in configured_order:\n        jc=(cfg.get("jobs") or {}).get(job) or {}'''
    if old not in src:
        raise SystemExit("H28 next rows order anchor not found")
    src = src.replace(old, new, 1)

    old = '''        rows.append({\n            "工作ID":job,"自動更新項目":JOB_LABELS.get(job,job),"啟用":bool(jc.get("enabled")),'''
    new = '''        rows.append({\n            "設定順位": configured_order.index(job)+1,\n            "工作ID":job,"自動更新項目":JOB_LABELS.get(job,job),"啟用":bool(jc.get("enabled")),'''
    if old not in src:
        raise SystemExit("H28 next rows rank anchor not found")
    src = src.replace(old, new, 1)

    path.write_text(src, encoding="utf-8")


def patch_page17() -> None:
    path = ROOT / "pages" / "17_系統健康檢查.py"
    src = path.read_text(encoding="utf-8-sig")

    src = src.replace(
        'st.caption("V191-H27｜中央自動排程＋永久化監控：每次外部喚醒皆永久記錄 heartbeat/run_id/result；週末正常喚醒會明確顯示為依設定跳過，不再誤報排程失效。H18/H19 長工作防重跑與自動補送喚醒完整保留。")',
        'st.caption("V191-H28｜中央自動排程＋永久化監控：新增可永久保存的工作執行順位；07 股神推薦可固定等待所有前置已啟用工作的當日最終時段完成後才執行。H27 heartbeat 真實性與 H18/H19 防重跑/補送喚醒完整保留。")',
        1,
    )

    anchor = '''            _rec_force_full = st.checkbox(\n                "自動股神推薦固定使用全市場完整掃描",\n                value=bool((((_auto_cfg.get("jobs") or {}).get("godpick_recommendation") or {}).get("options") or {}).get("force_full_market", False)),\n                help="未勾選時，沿用第7頁已永久保存的掃描範圍/群組/市場/門檻；勾選時只覆寫自動排程的掃描範圍為全市場，不修改第7頁人工設定。",\n            )\n            st.caption("每日時間可填一個或多個，例如 14:20,20:40。Windows 10分鐘嚴格喚醒＋GitHub 排程備援；真正是否到期仍由本設定判斷。")\n            _edited_jobs = {}\n            for _job, _label in AUTO_JOB_LABELS.items():\n                _jc = ((_auto_cfg.get("jobs") or {}).get(_job) or {})\n                _c1, _c2, _c3 = st.columns([0.7, 4.0, 2.2])\n                with _c1:\n                    _jen = st.checkbox("啟用", value=bool(_jc.get("enabled", False)), key=f"v191_en_{_job}")\n                with _c2:\n                    st.markdown(f"**{_label}**")\n                    if _job == "godpick_recommendation":\n                        st.caption("只有股票主檔、大盤、官方因子『內容日期驗證』、SuperAI市場情境、自選股runtime於今日前置成功，才允許自動推薦；真正選股由第7頁模組執行。若本輪0檔通過可操作底線，狀態會是WARNING並保存候選診斷，不會硬塞弱股。")\n                with _c3:\n                    _jtimes = st.text_input("台灣時間", value=",".join(_jc.get("times") or []), key=f"v191_times_{_job}", label_visibility="collapsed")\n                _newj = dict(_jc)\n                _newj["enabled"] = bool(_jen)\n                _newj["times"] = [x.strip() for x in str(_jtimes).replace("，", ",").split(",") if x.strip()]\n                if _job == "godpick_recommendation":\n                    _opts = dict(_newj.get("options") or {})\n                    _opts["force_full_market"] = bool(_rec_force_full)\n                    _newj["options"] = _opts\n                _edited_jobs[_job] = _newj'''
    replacement = '''            _rec_force_full = st.checkbox(\n                "自動股神推薦固定使用全市場完整掃描",\n                value=bool((((_auto_cfg.get("jobs") or {}).get("godpick_recommendation") or {}).get("options") or {}).get("force_full_market", False)),\n                help="未勾選時，沿用第7頁已永久保存的掃描範圍/群組/市場/門檻；勾選時只覆寫自動排程的掃描範圍為全市場，不修改第7頁人工設定。",\n            )\n            _run_last = st.checkbox(\n                "07｜股神推薦固定等待所有排在前面的已啟用工作『當日最後時段完成』後才執行（建議）",\n                value=bool(_auto_cfg.get("recommendation_run_last_after_all_enabled", True)),\n                help="不是只改畫面順序。若前置工作今天還有較晚的排程時段、尚未完成或失敗，07 會保持待補跑，等下一次中央喚醒再判斷；避免用部分新、部分舊資料推薦。",\n            )\n            _auto_order_raw = _auto_cfg.get("execution_order") if isinstance(_auto_cfg.get("execution_order"), list) else []\n            _auto_order = []\n            for _key in [*_auto_order_raw, *AUTO_JOB_LABELS.keys()]:\n                _key = str(_key or "")\n                if _key in AUTO_JOB_LABELS and _key not in _auto_order:\n                    _auto_order.append(_key)\n            if _run_last and "godpick_recommendation" in _auto_order:\n                _auto_order = [x for x in _auto_order if x != "godpick_recommendation"] + ["godpick_recommendation"]\n            st.caption("每日時間可填一個或多個，例如 14:20,20:40。『順位』會永久保存；重複順位會依目前列順序穩定排序。若勾選推薦最終閘門，07 的順位會固定在最後，且會等待前面工作的當日最後時段真正完成。")\n            _h1, _h2, _h3, _h4 = st.columns([0.7, 0.8, 4.0, 2.2])\n            _h1.caption("啟用")\n            _h2.caption("順位")\n            _h3.caption("自動更新項目")\n            _h4.caption("台灣時間")\n            _edited_jobs = {}\n            _edited_rank = {}\n            for _job in _auto_order:\n                _label = AUTO_JOB_LABELS.get(_job, _job)\n                _jc = ((_auto_cfg.get("jobs") or {}).get(_job) or {})\n                _c1, _c2, _c3, _c4 = st.columns([0.7, 0.8, 4.0, 2.2])\n                with _c1:\n                    _jen = st.checkbox("啟用", value=bool(_jc.get("enabled", False)), key=f"v191_en_{_job}", label_visibility="collapsed")\n                with _c2:\n                    _rank = st.number_input("順位", min_value=1, max_value=max(1, len(AUTO_JOB_LABELS)), value=_auto_order.index(_job)+1, step=1, key=f"v191_rank_{_job}", label_visibility="collapsed")\n                with _c3:\n                    st.markdown(f"**{_label}**")\n                    if _job == "godpick_recommendation":\n                        st.caption("H28：建議維持最後順位。除原有股票主檔/大盤/官方因子/SuperAI/自選股前置外，最終閘門還會等待排在 07 前面的績效、T+1 校準、AI 每日學習與永久化工作完成，才使用最新資訊推薦。")\n                with _c4:\n                    _jtimes = st.text_input("台灣時間", value=",".join(_jc.get("times") or []), key=f"v191_times_{_job}", label_visibility="collapsed")\n                _newj = dict(_jc)\n                _newj["enabled"] = bool(_jen)\n                _newj["times"] = [x.strip() for x in str(_jtimes).replace("，", ",").split(",") if x.strip()]\n                if _job == "godpick_recommendation":\n                    _opts = dict(_newj.get("options") or {})\n                    _opts["force_full_market"] = bool(_rec_force_full)\n                    _newj["options"] = _opts\n                _edited_jobs[_job] = _newj\n                _edited_rank[_job] = int(_rank)'''
    if anchor not in src:
        raise SystemExit("H28 Page17 scheduler form anchor not found")
    src = src.replace(anchor, replacement, 1)

    old = '''        if _save_auto:\n            _new_cfg = dict(_auto_cfg)\n            _new_cfg.update({"enabled": bool(_enable_all), "weekdays_only": bool(_weekdays), "catch_up_missed_same_day": bool(_catch_up), "grace_minutes": int(_grace), "retry_count": int(_retry), "retry_delay_seconds": int(_delay), "jobs": _edited_jobs})\n            _ok, _msg = save_auto_scheduler_settings(_new_cfg)'''
    new = '''        if _save_auto:\n            _new_order = sorted(_edited_rank.keys(), key=lambda _j: (_edited_rank[_j], _auto_order.index(_j)))\n            if _run_last and "godpick_recommendation" in _new_order:\n                _new_order = [x for x in _new_order if x != "godpick_recommendation"] + ["godpick_recommendation"]\n            _new_cfg = dict(_auto_cfg)\n            _new_cfg.update({\n                "enabled": bool(_enable_all), "weekdays_only": bool(_weekdays),\n                "catch_up_missed_same_day": bool(_catch_up), "grace_minutes": int(_grace),\n                "retry_count": int(_retry), "retry_delay_seconds": int(_delay),\n                "jobs": _edited_jobs, "execution_order": _new_order,\n                "recommendation_run_last_after_all_enabled": bool(_run_last),\n            })\n            _ok, _msg = save_auto_scheduler_settings(_new_cfg)'''
    if old not in src:
        raise SystemExit("H28 Page17 save anchor not found")
    src = src.replace(old, new, 1)

    src = src.replace(
        'st.success("V191 中央排程設定已永久保存；下一次中央喚醒後會依新時間執行。")',
        'st.success("V191-H28 中央排程設定已永久保存；下一次中央喚醒會依新順位、時間與推薦最終閘門執行。")',
        1,
    )

    path.write_text(src, encoding="utf-8")


if __name__ == "__main__":
    patch_scheduler()
    patch_page17()
    print("V191-H28 persistent scheduler execution order patch applied/verified")
