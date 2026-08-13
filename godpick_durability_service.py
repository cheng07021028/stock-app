# -*- coding: utf-8 -*-
"""V183 durable data registry and non-blocking persistence coordinator.

Design goals
------------
1. Every critical AI / market state is written atomically to local storage first.
2. The same payload is then sent through ``godpick_persistence_service`` so a
   configured GitHub runtime-data branch and/or Firestore can become the remote
   durable copy.
3. UI-heavy actions never need to wait for a full GitHub round trip.  The local
   outbox records whether a remote sync is pending/success/failed and can be
   retried on the next health/update pass.
4. "permanent" is never inferred from file existence alone: audit output
   distinguishes local durability from remote-confirmed durability.

No secrets are read or persisted by this module.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any
import copy
import hashlib
import json
import threading

DURABILITY_VERSION = "godpick_durability_v191_20260813_hotfix3"
OUTBOX_FILE = "godpick_durability_outbox.json"
AUDIT_FILE = "godpick_durability_audit.json"

# Critical business/AI states.  Derived display caches are listed separately so
# the audit can show that losing them is recoverable while losing the business
# state is not.
CORE_DURABLE_FILES: dict[str, dict[str, Any]] = {
    "market_snapshot.json": {"critical": True, "purpose": "大盤權威快照"},
    "macro_mode_bridge.json": {"critical": True, "purpose": "大盤→股神風控橋接"},
    "macro_trend_records.json": {"critical": True, "purpose": "大盤歷史/趨勢經驗"},
    "market_nextday_forecast_records.json": {"critical": True, "purpose": "大盤隔日預測與命中校準"},
    "official_factors_cache.json": {"critical": True, "purpose": "法人/營收/估值官方因子"},
    "official_factor_institutional_history.json": {"critical": True, "purpose": "法人逐日歷史/連買統計"},
    "godpick_records.json": {"critical": True, "purpose": "推薦永久紀錄與績效", "managed_by": "specialized record authority", "skip_generic_migration": True},
    "godpick_latest_recommendations.json": {"critical": True, "purpose": "最新正式推薦快照"},
    "godpick_latest_run_anchor.json": {"critical": True, "purpose": "V185最新推薦永久錨點／防部署回退"},
    "godpick_recommend_list.json": {"critical": True, "purpose": "推薦清單"},
    "godpick_rotation_history.json": {"critical": True, "purpose": "推薦輪動/重複記憶"},
    "godpick_learning_state.json": {"critical": True, "purpose": "AI學習狀態"},
    "godpick_calibration_samples.json": {"critical": True, "purpose": "漏選/近門檻校正樣本"},
    "super_ai_market_context.json": {"critical": True, "purpose": "融資券/期貨/PCR/ETF市場情境"},
    "super_ai_experience_index.json": {"critical": True, "purpose": "SuperAI逐日經驗索引"},
    "super_ai_experience_profile.json": {"critical": True, "purpose": "SuperAI校準/績效摘要"},
    "godpick_t1_trade_truth.json": {"critical": True, "purpose": "V188 T+1選股/進場/風控實戰真相"},
    "godpick_probability_calibration.json": {"critical": True, "purpose": "V188隔日機率校準與Brier統計"},
    "stock_master_cache.json": {"critical": True, "purpose": "股票主檔"},
    "stock_category_overrides.json": {"critical": True, "purpose": "人工類股覆寫"},
    "watchlist.json": {"critical": True, "purpose": "自選股權威檔"},
    # User/system configuration is also business state: a restart must not reset
    # weights, table views, schedules or UI governance back to defaults.
    "godpick_user_settings.json": {"critical": True, "purpose": "股神權重/模型套用設定", "managed_by": "weight calibration + durability"},
    "godpick_record_ui_config.json": {"critical": True, "purpose": "推薦紀錄欄位/顯示設定", "managed_by": "page8 GitHub + durability migration"},
    "godpick_management_ui_config.json": {"critical": True, "purpose": "管理中心欄位/表格設定", "managed_by": "column manager GitHub + durability migration"},
    "godpick_ui_font_settings.json": {"critical": True, "purpose": "全域字體設定", "managed_by": "font manager GitHub + durability migration"},
    "dashboard_table_settings.json": {"critical": True, "purpose": "儀表板排序/篩選設定", "managed_by": "V183 durability"},
    "hk_chart_settings.json": {"critical": True, "purpose": "歷史K線圖表設定", "managed_by": "V183 durability"},
    "data/config/official_factor_schedule_settings.json": {"critical": True, "purpose": "官方因子排程設定", "managed_by": "health service GitHub + durability migration"},
    "data/config/godpick_auto_scheduler_settings.json": {"critical": True, "purpose": "V191中央自動排程設定", "managed_by": "V191 scheduler + durability"},
    # Sensitive authentication material is tracked by the inventory but MUST NOT
    # be copied through the generic Firestore layer. app_auth_core owns its GitHub
    # persistence and access controls.
    "auth_config.json": {"critical": False, "purpose": "帳號/權限設定（敏感）", "managed_by": "app_auth_core GitHub", "skip_generic_migration": True},
    # Operational logs/status are useful for diagnosis but are rebuildable.
    "official_factors_update_log.json": {"critical": False, "purpose": "官方因子更新履歷"},
    "godpick_global_update_status.json": {"critical": False, "purpose": "一鍵更新執行狀態"},
    "godpick_auto_scheduler_status.json": {"critical": False, "purpose": "V191中央排程最後執行狀態"},
    "godpick_auto_scheduler_history.json": {"critical": False, "purpose": "V191中央排程成功/失敗履歷"},
    "macro_startup_status.json": {"critical": False, "purpose": "大盤啟動更新狀態"},
    "last_query_state.json": {"critical": False, "purpose": "最近查詢狀態"},
    # Derived / rebuildable files: still useful to audit, but not business truth.
    "watchlist_normalized.json": {"critical": False, "purpose": "自選股衍生正規化快照"},
    "watchlist_runtime_snapshot.json": {"critical": False, "purpose": "自選股執行期快照"},
    "macro_market_close_cache.json": {"critical": False, "purpose": "大盤行情快取"},
    "macro_institutional_cache.json": {"critical": False, "purpose": "大盤法人快取"},
    "macro_taifex_cache.json": {"critical": False, "purpose": "期貨快取"},
    "macro_us_market_cache.json": {"critical": False, "purpose": "美股快取"},
    "macro_otc_cache.json": {"critical": False, "purpose": "櫃買快取"},
    "overnight_global_market_cache.json": {"critical": False, "purpose": "隔夜國際盤/美盤風控快取"},
    "macro_news_event_cache.json": {"critical": False, "purpose": "大盤事件安全快取"},
    "macro_v70_one_click_status.json": {"critical": False, "purpose": "大盤V70一鍵更新完成狀態"},
}

_BASE = Path(__file__).resolve().parent
_LOCK = threading.Lock()
_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="godpick-durable-v188")
_PENDING: dict[str, dict[str, Any]] = {}
_RUNNING = False


def _now() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    try:
        import pandas as pd
        if isinstance(value, pd.DataFrame):
            return [_json_safe(x) for x in value.to_dict(orient="records")]
        if isinstance(value, pd.Series):
            return _json_safe(value.to_dict())
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return value.item()
    except Exception:
        return str(value)


def _hash(payload: Any) -> str:
    raw = json.dumps(_json_safe(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _path(path_name: str, base_dir: str | Path | None = None) -> Path:
    p = Path(path_name)
    if p.is_absolute():
        return p
    return Path(base_dir or _BASE) / p


def _state_file_for(path_name: str) -> str:
    try:
        from godpick_persistence_service import _state_file_for as fn
        return fn(path_name)
    except Exception:
        p = Path(path_name)
        return f"{p.stem.replace(' ', '_')}_sync_state.json"


def _atomic_write(path_name: str, payload: Any, *, base_dir: str | Path | None = None) -> tuple[bool, str]:
    try:
        target = _path(path_name, base_dir)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".tmp_v183")
        tmp.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp.replace(target)
        return True, f"local:{target.name}"
    except Exception as exc:
        return False, f"local write failed:{exc}"


def _read_json(path_name: str, default: Any, *, base_dir: str | Path | None = None) -> Any:
    try:
        p = _path(path_name, base_dir)
        if not p.exists():
            return copy.deepcopy(default)
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return copy.deepcopy(default)


def _write_outbox(payload: dict[str, Any], *, base_dir: str | Path | None = None) -> None:
    _atomic_write(OUTBOX_FILE, payload, base_dir=base_dir)


def load_durability_outbox(*, base_dir: str | Path | None = None) -> dict[str, Any]:
    data = _read_json(OUTBOX_FILE, {}, base_dir=base_dir)
    return data if isinstance(data, dict) else {}


def _set_outbox(path_name: str, item: dict[str, Any], *, base_dir: str | Path | None = None) -> None:
    with _LOCK:
        box = load_durability_outbox(base_dir=base_dir)
        box[path_name] = item
        box["_version"] = DURABILITY_VERSION
        box["_updated_at"] = _now()
        _write_outbox(box, base_dir=base_dir)


def _remote_worker() -> None:
    global _RUNNING
    while True:
        with _LOCK:
            if not _PENDING:
                _RUNNING = False
                return
            # Coalesce repeated writes to the same file: only newest payload matters.
            path_name, job = next(iter(_PENDING.items()))
            _PENDING.pop(path_name, None)
        payload = job["payload"]
        github_path = job.get("github_path")
        firestore_doc = job.get("firestore_doc")
        base_dir = job.get("base_dir")
        started = _now()
        _set_outbox(path_name, {
            "status": "running", "payload_hash": job["payload_hash"],
            "queued_at": job.get("queued_at"), "started_at": started,
            "reason": job.get("reason", ""),
        }, base_dir=base_dir)
        ok = False
        message = ""
        try:
            from godpick_persistence_service import save_named_json_permanent
            report = save_named_json_permanent(
                path_name, payload, github_path=github_path, firestore_doc=firestore_doc
            )
            ok = bool(getattr(report, "permanent_ok", False))
            message = "｜".join(x for x in [
                getattr(report, "local_message", ""),
                getattr(report, "github_message", ""),
                getattr(report, "firestore_message", ""),
            ] if x)
        except Exception as exc:
            message = f"durable remote sync exception: {exc}"
        _set_outbox(path_name, {
            "status": "success" if ok else "failed",
            "payload_hash": job["payload_hash"],
            "queued_at": job.get("queued_at"), "started_at": started,
            "finished_at": _now(), "reason": job.get("reason", ""),
            "message": message[:1500],
        }, base_dir=base_dir)


def persist_json_async(
    path_name: str,
    payload: Any,
    *,
    github_path: str | None = None,
    firestore_doc: str | None = None,
    reason: str = "",
    base_dir: str | Path | None = None,
) -> tuple[bool, str]:
    """Atomically save local data and queue remote durable sync.

    Return value means the local authority file is safe.  Remote durability is
    reported separately through the outbox and must not be called successful
    until the worker confirms it.
    """
    global _RUNNING
    safe = _json_safe(payload)
    if Path(path_name).name == "godpick_records.json" and isinstance(safe, list) and not safe:
        existing = _read_json(path_name, [], base_dir=base_dir)
        if isinstance(existing, list) and existing:
            return False, f"V191-H3防歸零：拒絕以0筆覆蓋既有推薦歷史 {len(existing)} 筆"
    local_ok, local_msg = _atomic_write(path_name, safe, base_dir=base_dir)
    if not local_ok:
        return False, local_msg
    payload_hash = _hash(safe)
    queued_at = _now()
    _set_outbox(path_name, {
        "status": "pending", "payload_hash": payload_hash,
        "queued_at": queued_at, "reason": reason,
        "message": "本機已原子保存；遠端永久化排隊中",
    }, base_dir=base_dir)
    with _LOCK:
        _PENDING[path_name] = {
            "payload": safe,
            "payload_hash": payload_hash,
            "github_path": github_path,
            "firestore_doc": firestore_doc,
            "reason": reason,
            "base_dir": str(base_dir) if base_dir else None,
            "queued_at": queued_at,
        }
        if not _RUNNING:
            _RUNNING = True
            _EXECUTOR.submit(_remote_worker)
    return True, f"{local_msg}｜remote pending"


def persist_json_permanent(
    path_name: str,
    payload: Any,
    *,
    github_path: str | None = None,
    firestore_doc: str | None = None,
    reason: str = "",
) -> tuple[bool, str]:
    """Blocking durability for small critical metadata/checkpoints.

    This is intentionally separate from ``persist_json_async`` so hot paths can
    decide whether waiting for remote confirmation is worth the latency.
    """
    try:
        from godpick_persistence_service import save_named_json_permanent
        report = save_named_json_permanent(path_name, _json_safe(payload), github_path=github_path, firestore_doc=firestore_doc)
        ok = bool(getattr(report, "permanent_ok", False))
        msg = "｜".join(x for x in [
            getattr(report, "local_message", ""), getattr(report, "github_message", ""), getattr(report, "firestore_message", "")
        ] if x)
        _set_outbox(path_name, {
            "status": "success" if ok else "failed", "payload_hash": _hash(payload),
            "finished_at": _now(), "reason": reason, "message": msg[:1500],
        })
        return ok, msg
    except Exception as exc:
        return False, f"permanent save exception:{exc}"


def queue_existing_critical_for_migration(
    *, base_dir: str | Path | None = None, critical_only: bool = True
) -> list[str]:
    """Queue existing legacy/local authority files for verified remote durability.

    V183 deliberately does not auto-upload every large historical JSON at app
    startup.  The health center can invoke this migration once; each existing
    file is atomically kept locally and queued through the same outbox.  Files
    already remote-confirmed with the identical hash are skipped.
    """
    base = Path(base_dir or _BASE)
    current = audit_core_durability(base_dir=base, write_audit=False)
    rows = current.get("rows", []) if isinstance(current, dict) else []
    messages: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if critical_only and not bool(row.get("critical")):
            continue
        path_name = str(row.get("file") or "")
        meta = CORE_DURABLE_FILES.get(path_name, {})
        if bool(meta.get("skip_generic_migration")):
            messages.append(f"{path_name}: sensitive/external manager，略過generic migration")
            continue
        if not path_name or not row.get("exists"):
            messages.append(f"{path_name or '?'}: missing，略過")
            continue
        if bool(row.get("remote_confirmed")):
            messages.append(f"{path_name}: same-hash remote confirmed，略過")
            continue
        payload = _read_json(path_name, None, base_dir=base)
        if payload is None:
            messages.append(f"{path_name}: JSON讀取失敗")
            continue
        ok, msg = persist_json_async(
            path_name, payload,
            reason="V188 migrate existing authority to durable remote",
            base_dir=base,
        )
        messages.append(f"{path_name}: {'queued' if ok else 'failed'}｜{msg}")
    return messages


def retry_failed_durability(*, base_dir: str | Path | None = None) -> list[str]:
    messages: list[str] = []
    box = load_durability_outbox(base_dir=base_dir)
    for path_name, item in list(box.items()):
        if path_name.startswith("_") or not isinstance(item, dict):
            continue
        if item.get("status") not in {"failed", "pending", "running"}:
            continue
        payload = _read_json(path_name, None, base_dir=base_dir)
        if payload is None:
            messages.append(f"{path_name}: local missing")
            continue
        ok, msg = persist_json_async(path_name, payload, reason="V188 durability retry", base_dir=base_dir)
        messages.append(f"{path_name}: {'queued' if ok else 'failed'}｜{msg}")
    return messages


def audit_core_durability(*, base_dir: str | Path | None = None, write_audit: bool = True) -> dict[str, Any]:
    base = Path(base_dir or _BASE)
    outbox = load_durability_outbox(base_dir=base)
    rows: list[dict[str, Any]] = []
    critical_total = 0
    critical_local = 0
    critical_remote_confirmed = 0
    for path_name, meta in CORE_DURABLE_FILES.items():
        critical = bool(meta.get("critical"))
        if critical:
            critical_total += 1
        p = base / path_name
        exists = p.exists()
        payload = _read_json(path_name, None, base_dir=base) if exists else None
        payload_hash = _hash(payload) if exists and payload is not None else ""
        state_name = _state_file_for(path_name)
        state = _read_json(state_name, {}, base_dir=base)
        state_hash = str(state.get("payload_hash") or "") if isinstance(state, dict) else ""
        state_match = bool(payload_hash and state_hash and payload_hash == state_hash)
        ob = outbox.get(path_name, {}) if isinstance(outbox, dict) else {}
        remote_status = str(ob.get("status") or "unknown") if isinstance(ob, dict) else "unknown"
        remote_hash = str(ob.get("payload_hash") or "") if isinstance(ob, dict) else ""
        remote_confirmed = bool(remote_status == "success" and payload_hash and remote_hash == payload_hash)
        # Recommendation history uses a specialized collection + large-file
        # GitHub sync, not the generic one-document durability worker.  Read its
        # verified hashes so the audit does not stay WARNING forever or attempt
        # an unsafe generic migration of a 20MB record file.
        if path_name == "godpick_records.json" and payload_hash:
            try:
                from godpick_persistence_service import (
                    load_records_github_sync_status, _read_records_firestore_summary
                )
                gh_status, _ = load_records_github_sync_status()
                fs_summary, _ = _read_records_firestore_summary()
                gh_ok = bool(isinstance(gh_status, dict) and str(gh_status.get("status") or "") == "success" and str(gh_status.get("payload_hash") or "") == payload_hash)
                fs_ok = bool(isinstance(fs_summary, dict) and str(fs_summary.get("payload_hash") or "") == payload_hash and int(fs_summary.get("count") or -1) == len(payload if isinstance(payload, list) else []))
                if gh_ok or fs_ok:
                    remote_confirmed = True
                    remote_status = "success-specialized"
                    remote_hash = payload_hash
                elif str(gh_status.get("status") or "") in {"pending", "running"}:
                    remote_status = "pending-specialized"
            except Exception:
                pass
        if critical and exists:
            critical_local += 1
        if critical and remote_confirmed:
            critical_remote_confirmed += 1
        if not exists:
            status = "MISSING"
        elif remote_confirmed:
            status = "REMOTE_CONFIRMED"
        elif state_match:
            status = "LOCAL_STATE_OK_REMOTE_UNCONFIRMED"
        elif remote_status in {"pending", "running"}:
            status = "REMOTE_PENDING"
        else:
            status = "LOCAL_ONLY_OR_LEGACY"
        rows.append({
            "file": path_name,
            "critical": critical,
            "purpose": meta.get("purpose", ""),
            "exists": exists,
            "bytes": p.stat().st_size if exists else 0,
            "payload_hash": payload_hash,
            "sync_state_exists": (base / state_name).exists(),
            "sync_state_match": state_match,
            "remote_status": remote_status,
            "remote_confirmed": remote_confirmed,
            "status": status,
        })
    audit = {
        "version": DURABILITY_VERSION,
        "generated_at": _now(),
        "critical_total": critical_total,
        "critical_local": critical_local,
        "critical_remote_confirmed": critical_remote_confirmed,
        "critical_local_rate_pct": round(100.0 * critical_local / max(1, critical_total), 1),
        "critical_remote_confirmed_rate_pct": round(100.0 * critical_remote_confirmed / max(1, critical_total), 1),
        "rows": rows,
        "note": "remote_confirmed 只在本機 payload hash 與成功 outbox hash 一致時成立；不以檔案存在冒充永久化。",
    }
    if write_audit:
        _atomic_write(AUDIT_FILE, audit, base_dir=base)
    return audit


__all__ = [
    "DURABILITY_VERSION", "CORE_DURABLE_FILES", "persist_json_async", "persist_json_permanent",
    "load_durability_outbox", "queue_existing_critical_for_migration",
    "retry_failed_durability", "audit_core_durability",
]
