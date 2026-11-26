# -*- coding: utf-8 -*-
"""Durable persistence and cross-module synchronization helpers.

This module centralizes local/GitHub/Firestore persistence used by the
watchlist, recommendation records, recommendation list and export settings.
It intentionally uses project-root absolute paths and atomic local writes so a
Streamlit rerun or process reboot cannot silently lose data because the current
working directory changed.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import math
import os
import tempfile
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests
import streamlit as st
from godpick_h163_firestore_quota_guard import firestore_read_guard as _H163_FS_GUARD


def get_firestore_read_guard_status() -> dict[str, Any]:
    """Safe, non-credential Page08 diagnostics; does not touch the network."""
    return _H163_FS_GUARD.status()

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:  # pragma: no cover - optional dependency
    firebase_admin = None
    credentials = None
    firestore = None


BASE_DIR = Path(__file__).resolve().parent
WATCHLIST_FILE = "watchlist.json"
WATCHLIST_STATE_FILE = "watchlist_sync_state.json"
RECORDS_FILE = "godpick_records.json"
RECORDS_STATE_FILE = "godpick_records_sync_state.json"
EXPORT_SETTINGS_FILE = "godpick_export_sync_settings.json"
EXPORT_HISTORY_FILE = "godpick_export_history.json"
MODULE_SYNC_STATE_FILE = "godpick_module_sync_state.json"
RECORDS_GITHUB_SYNC_STATUS_FILE = "godpick_records_github_sync_status.json"
RECORDS_MANIFEST_FILE = "godpick_records_manifest.json"

_RECORDS_GITHUB_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="godpick-records-github")
_RECORDS_GITHUB_LOCK = threading.Lock()
# H8: queue coordination and actual network upload are different locks.  The
# queue lock must stay short-lived; the upload lock serializes every in-process
# writer of the 20-30MB records snapshot + its state file.
_RECORDS_GITHUB_UPLOAD_LOCK = threading.RLock()
_GITHUB_PATH_LOCKS_GUARD = threading.Lock()
_GITHUB_PATH_LOCKS: dict[str, threading.RLock] = {}
_RECORDS_GITHUB_PENDING: tuple[list[dict[str, Any]], dict[str, Any], str] | None = None
_RECORDS_GITHUB_RUNNING = False
_RECORDS_LOCAL_LOCK = threading.RLock()

# V191-H4: history-gap audit is intentionally bounded.  It is only used when
# the current recommendation authority looks stale/suspicious, and the same
# authority signature is not re-audited on every Streamlit rerun.
_RECORDS_HISTORY_AUDIT_LOCK = threading.Lock()
_RECORDS_HISTORY_LAST_SIGNATURE = ""
_RECORDS_HISTORY_LAST_AT = 0.0
_RECORDS_HISTORY_AUDIT_COOLDOWN_SECONDS = 300.0

NAMED_FIRESTORE_DOCS = {
    "godpick_recommend_list.json": "godpick_recommend_list",
    "godpick_latest_recommendations.json": "godpick_latest_recommendations",
    "godpick_latest_run_anchor.json": "godpick_latest_run_anchor",
    "official_factors_cache.json": "official_factors_cache",
    "godpick_export_sync_settings.json": "godpick_export_sync_settings",
    "godpick_export_history.json": "godpick_export_history",
    "godpick_module_sync_state.json": "godpick_module_sync_state",
    "godpick_calibration_samples.json": "godpick_calibration_samples",
}


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _json_safe_value(value: Any) -> Any:
    """Convert pandas/numpy values into JSON/Firestore-safe native values.

    Recommendation records contain many pandas scalar values.  Local JSON
    serialization previously hid those types through ``default=str``, while
    Firestore rejected the same rows and made the whole permanent sync fail.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (pd.Timestamp, datetime)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, pd.Timedelta):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(v) for v in value]
    # numpy scalar support without requiring numpy as a hard dependency.
    if hasattr(value, "item") and callable(getattr(value, "item", None)):
        try:
            native = value.item()
            if native is not value:
                return _json_safe_value(native)
        except Exception:
            pass
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    return str(value)


_TW_TZ = ZoneInfo("Asia/Taipei")


def _now_text() -> str:
    return datetime.now(_TW_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _parse_time(value: Any) -> datetime:
    text = _safe_str(value)
    if not text:
        return datetime.min
    text = text.replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(text[:26])
    except Exception:
        try:
            return datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.min


def _now_epoch() -> float:
    return float(time.time())


def _now_utc_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _state_epoch(state: Any, fallback: datetime | None = None) -> float:
    if isinstance(state, dict):
        for key in ("updated_at_epoch", "epoch", "timestamp"):
            try:
                value = float(state.get(key) or 0)
                if value > 0:
                    return value
            except Exception:
                pass
        parsed = _parse_time(state.get("updated_at_utc") or state.get("updated_at"))
        if parsed > datetime.min:
            try:
                # Legacy strings are interpreted as UTC to avoid mixing server local time
                # with Firestore UTC timestamps. New writes always include epoch.
                return parsed.replace(tzinfo=timezone.utc).timestamp()
            except Exception:
                pass
    if isinstance(fallback, datetime) and fallback > datetime.min:
        try:
            return fallback.timestamp()
        except Exception:
            pass
    return 0.0


def _recommendation_date_text(value: Any) -> str:
    """Normalize a recommendation date for semantic authority comparison."""
    text = _safe_str(value)
    if not text:
        return ""
    try:
        parsed = pd.to_datetime(text, errors="coerce")
        if pd.notna(parsed):
            return parsed.strftime("%Y-%m-%d")
    except Exception:
        pass
    return text[:10] if len(text) >= 10 else ""


def _authority_freshness_key(source: str, state: Any, *, fallback: datetime | None = None, rows: Any = None) -> tuple[Any, ...]:
    """Compare authorities by business-data freshness before file/revision time.

    Streamlit Cloud recreates repository files during reboot.  Their filesystem
    mtime and repaired local state can therefore look newer than Firestore even
    when the actual latest recommendation is weeks older.  The newest business
    date is the primary key; count/revision/epoch only break ties.
    """
    state_dict = state if isinstance(state, dict) else {}
    latest = _recommendation_date_text(state_dict.get("latest_recommendation_date"))
    if not latest and rows is not None:
        try:
            latest = _latest_record_recommendation_date(records_as_rows_exact(rows))
        except Exception:
            latest = ""
    try:
        count = int(state_dict.get("count") or (len(rows) if rows is not None else 0) or 0)
    except Exception:
        count = 0
    try:
        revision = int(state_dict.get("revision") or 0)
    except Exception:
        revision = 0
    epoch = _state_epoch(state_dict, fallback)
    # New revisions are time_ns; normalize them to seconds so they can be
    # compared with legacy epoch metadata without one scale dominating forever.
    revision_epoch = (revision / 1_000_000_000.0) if revision > 10_000_000_000 else float(revision or 0)
    activity = max(epoch, revision_epoch)
    priority = {"local": 0, "github": 1, "firestore": 2}.get(source, 0)
    return (latest, activity, count, priority)


def _new_state(version: str, payload: Any, **extra: Any) -> dict[str, Any]:
    epoch = _now_epoch()
    state = {
        "version": version,
        "updated_at": _now_text(),
        "updated_at_utc": _now_utc_text(),
        "updated_at_epoch": epoch,
        "revision": time.time_ns(),
        "payload_hash": _json_hash(payload),
    }
    state.update(extra)
    return state


def _state_is_valid(payload: Any, state: Any) -> bool:
    if not isinstance(state, dict):
        return False
    expected = _safe_str(state.get("payload_hash"))
    return bool(expected and expected == _json_hash(payload))


def project_path(path_name: str | os.PathLike[str]) -> Path:
    path = Path(path_name).expanduser()
    return path if path.is_absolute() else BASE_DIR / path


def _json_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def read_local_json(path_name: str, default: Any) -> tuple[Any, str, datetime]:
    path = project_path(path_name)
    if not path.exists():
        return copy.deepcopy(default), f"本機不存在：{path.as_posix()}", datetime.min
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        modified = datetime.fromtimestamp(path.stat().st_mtime)
        return payload, f"已讀取本機：{path.as_posix()}", modified
    except Exception as exc:
        return copy.deepcopy(default), f"本機讀取失敗：{path.as_posix()}｜{exc}", datetime.min


def write_local_json_atomic(path_name: str, payload: Any) -> tuple[bool, str]:
    path = project_path(path_name)
    with _RECORDS_LOCAL_LOCK:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Large recommendation-record snapshots are written compactly.  This
            # reduces disk I/O and JSON verification time without changing data.
            if isinstance(payload, list) and len(payload) >= 500:
                text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
            else:
                text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
            fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
            try:
                with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                    handle.write(text)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_name, path)
            finally:
                if os.path.exists(temp_name):
                    os.unlink(temp_name)
            verify = json.loads(path.read_text(encoding="utf-8-sig"))
            if _json_hash(verify) != _json_hash(payload):
                return False, f"本機回讀驗證失敗：{path.as_posix()}"
            return True, f"本機原子寫入並驗證：{path.as_posix()}"
        except Exception as exc:
            return False, f"本機寫入失敗：{path.as_posix()}｜{exc}"

def _normalize_record_code(value: Any) -> str:
    text = _safe_str(value)
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits if 4 <= len(digits) <= 6 else text


def _record_business_key_authority(row: dict[str, Any]) -> str:
    return "|".join([
        _normalize_record_code(row.get("股票代號") or row.get("code")),
        _safe_str(row.get("推薦日期") or row.get("date"))[:10],
        _safe_str(row.get("推薦模式") or row.get("mode") or "股神推薦"),
    ])


def _latest_record_recommendation_date(rows: Iterable[dict[str, Any]]) -> str:
    dates: list[str] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        text = _safe_str(row.get("推薦日期") or row.get("date"))[:10]
        if len(text) == 10 and text[4:5] == "-" and text[7:8] == "-":
            dates.append(text)
    return max(dates) if dates else ""


def _guard_records_full_replace(
    incoming: Iterable[dict[str, Any]],
    *,
    allow_destructive_replace: bool = False,
    reason: str = "",
) -> tuple[bool, str, int, int]:
    """V191-H3 hard guard against accidental history truncation.

    Recommendation history is append/update authority.  Legitimate deletions use
    ``save_records_mutation_fast`` with explicit deleted ids.  A generic full
    snapshot writer must therefore never reduce the canonical row count unless
    the caller explicitly opts into a destructive restore.  This blocks the
    Page10 subset/empty-list bug that could turn ~1800 historical rows into 4/0.
    """
    new_rows = records_as_rows_exact(list(incoming or []))
    current_payload, _, _ = read_local_json(RECORDS_FILE, [])
    current_rows = records_as_rows_exact(current_payload)
    before = len(current_rows)
    after = len(new_rows)
    if allow_destructive_replace or before <= 0:
        return True, "", before, after
    before_ids = {_safe_str(row.get("record_id")) for row in current_rows}
    after_ids = {_safe_str(row.get("record_id")) for row in new_rows}
    if after >= before and before_ids.issubset(after_ids) and _latest_record_recommendation_date(new_rows) >= _latest_record_recommendation_date(current_rows):
        return True, "", before, after
    msg = (
        f"V191-H3 防歸零/防縮水已阻止完整覆蓋：目前權威 {before} 筆，"
        f"傳入僅 {after} 筆；reason={_safe_str(reason) or 'unspecified'}。"
        "合法刪除必須走 mutation/deleted_ids，不得用子集合整檔覆蓋歷史。"
    )
    return False, msg, before, after


def _records_state_is_destructive_mutation(state: Any) -> bool:
    """Return True only for explicit user/data-removal mutations.

    H3 treated every ``*_mutation`` state as destructive.  That was too broad:
    Page07/Page08 algorithmic upserts also use the mutation writer, including
    ``08 自動修復較新07推薦快照``.  Consequently a stale 1810-row/2026-07-09
    repair state was incorrectly exempt from history-gap recovery.

    H4 records mutation_deleted_count on new writes and keeps reason-token
    compatibility for older states.  Explicit deletes/clears remain protected
    from resurrection forever.
    """
    stt = state if isinstance(state, dict) else {}
    try:
        if int(stt.get("mutation_deleted_count") or 0) > 0:
            return True
    except Exception:
        pass
    kind = _safe_str(stt.get("mutation_kind")).lower()
    if kind in {"destructive", "delete", "clear"}:
        return True
    reason = _safe_str(stt.get("mutation_reason") or stt.get("sync_reason") or stt.get("recovery_reason")).lower()
    destructive_tokens = (
        "刪除", "清空", "使用者清除", "使用者刪", "delete", "deleted",
        "clear filtered", "clear all", "remove record", "purge",
    )
    return any(token in reason for token in destructive_tokens)


def _records_state_has_known_recovery_gap_risk(state: Any) -> bool:
    """Identify H3/auto-repair states that must be audited before new upserts."""
    stt = state if isinstance(state, dict) else {}
    if _records_state_is_destructive_mutation(stt):
        return False
    version = _safe_str(stt.get("version")).lower()
    reason = _safe_str(stt.get("mutation_reason") or stt.get("recovery_reason") or stt.get("sync_reason")).lower()
    return bool(
        "h3_history_recovery" in version
        or "h3 automatic recovery" in reason
        or "自動修復較新07推薦快照" in reason
        or "history rescue" in reason
        or "歷史救援" in reason
    )


def _records_state_looks_like_accidental_collapse(state: Any, row_count: int) -> bool:
    """Detect the known full-replace failure shape without undoing user deletes."""
    stt = state if isinstance(state, dict) else {}
    if _records_state_is_destructive_mutation(stt):
        return False
    version = _safe_str(stt.get("version")).lower()
    # H3 originally rejected every mutation state here.  H4 only rejects actual
    # destructive mutations; an algorithmic 0/4-row mutation is still unsafe.
    return bool(
        "godpick_records_durable_v3" in version
        or (row_count <= 50 and any(token in version for token in [
            "fast_sync", "reboot_restore", "nochange", "durable_v4_mutation",
            "durable_v5", "h3_history_recovery",
        ]))
    )


def _records_history_gap_audit_needed(state: Any, rows: Iterable[dict[str, Any]] | None = None) -> tuple[bool, str]:
    """Decide whether Git history should be checked for a missing date range.

    This is deliberately conservative.  We always audit the known collapse /
    H3 auto-repair signatures.  Otherwise a non-destructive authority that has
    not advanced for 14 days is audited once, which catches a reboot rollback
    without turning normal daily reruns into GitHub-history scans.
    """
    current = records_as_rows_exact(list(rows or []))
    stt = state if isinstance(state, dict) else {}
    if _records_state_is_destructive_mutation(stt):
        return False, "明確刪除/清空 mutation，禁止歷史復活"
    if _records_state_looks_like_accidental_collapse(stt, len(current)):
        return True, "偵測到意外縮水特徵"
    if _records_state_has_known_recovery_gap_risk(stt):
        return True, "偵測到 H3/08 自動修復狀態，需驗證是否漏掉後續日期"
    latest = _recommendation_date_text(stt.get("latest_recommendation_date")) or _latest_record_recommendation_date(current)
    if latest:
        try:
            age_days = (datetime.now(ZoneInfo("Asia/Taipei")).date() - pd.Timestamp(latest).date()).days
        except Exception:
            age_days = 0
        if age_days >= 14 and len(current) >= 50:
            return True, f"權威最新推薦日已落後 {age_days} 天"
    return False, "目前權威無需歷史缺口稽核"


def records_authority_signature() -> str:
    """Cheap high-resolution signature without parsing the 20+ MB authority JSON."""
    path = project_path(RECORDS_FILE)
    state_path = project_path(RECORDS_STATE_FILE)
    try:
        stat = path.stat()
        file_part = f"{path}:{int(stat.st_mtime_ns)}:{int(stat.st_size)}"
    except Exception:
        file_part = f"{path}:missing"
    state, _, _ = read_local_json(RECORDS_STATE_FILE, {})
    if isinstance(state, dict):
        state_part = "|".join([
            _safe_str(state.get("payload_hash")),
            str(int(state.get("revision") or 0)),
            str(int(state.get("count") or 0)),
            _safe_str(state.get("updated_at")),
            _safe_str(state.get("latest_recommendation_date")),
        ])
    else:
        state_part = "no-state"
    try:
        stt = state_path.stat()
        state_stat = f"{int(stt.st_mtime_ns)}:{int(stt.st_size)}"
    except Exception:
        state_stat = "missing"
    return f"{file_part}|{state_stat}|{state_part}"


def records_authority_status() -> dict[str, Any]:
    """Return authority metadata without reparsing the 20+ MB JSON when state is valid."""
    path = project_path(RECORDS_FILE)
    state, state_msg, _ = read_local_json(RECORDS_STATE_FILE, {})
    manifest, manifest_msg, _ = read_local_json(RECORDS_MANIFEST_FILE, {})
    try:
        stat = path.stat()
        exists = True
        size = int(stat.st_size)
    except Exception:
        exists = False
        size = 0
    state_hash = _safe_str(state.get("payload_hash")) if isinstance(state, dict) else ""
    manifest_hash = _safe_str(manifest.get("payload_hash")) if isinstance(manifest, dict) else ""
    count = int((state or {}).get("count") or 0) if isinstance(state, dict) else 0
    latest_date = _safe_str((state or {}).get("latest_recommendation_date")) if isinstance(state, dict) else ""
    valid = bool(exists and state_hash and manifest_hash and state_hash == manifest_hash)
    payload_message = f"本機權威檔存在：{path.as_posix()}｜{size} bytes" if exists else f"本機不存在：{path.as_posix()}"

    # Legacy deployments may not have state/manifest yet. Parse only once for repair/diagnosis.
    if exists and (not state_hash or count <= 0 or not latest_date):
        payload, payload_message, _ = read_local_json(RECORDS_FILE, [])
        rows = records_as_rows_exact(payload)
        count = len(rows)
        latest_date = _latest_record_recommendation_date(rows)
        payload_hash = _json_hash(rows)
        if not state_hash:
            state_hash = payload_hash
        valid = bool(state_hash == payload_hash)
    return {
        "path": str(path),
        "count": count,
        "latest_recommendation_date": latest_date,
        "payload_hash": state_hash,
        "state_hash": state_hash,
        "revision": int((state or {}).get("revision") or 0) if isinstance(state, dict) else 0,
        "updated_at": _safe_str((state or {}).get("updated_at")) if isinstance(state, dict) else "",
        "valid": valid,
        "signature": records_authority_signature(),
        "payload_message": payload_message,
        "state_message": f"{state_msg}｜{manifest_msg}",
    }


# H240: current immutable publication takes precedence over historical rescue.
# Process-local diagnostics only; never invent a durable revision on read failure.
_H240_CHECKS: dict[str, dict[str, Any]] = {}


def _h240_status(rows, *, status, source="", head=None, reason=""):
    head = head if isinstance(head, dict) else {}
    result = {"status": status, "source": source, "count": len(rows),
              "latest": _latest_record_recommendation_date(rows),
              "remote_count": int(head.get("count") or 0),
              "remote_latest": _recommendation_date_text(head.get("latest_recommendation_date")),
              "reason": reason, "checked_at": _now_text(), "local_signature": records_authority_signature()}
    _H240_CHECKS[str(BASE_DIR)] = result
    return result


def records_authority_verification_h240():
    result = dict(_H240_CHECKS.get(str(BASE_DIR), {"status": "UNCHECKED"}))
    if result.get("status") == "VERIFIED" and result.get("local_signature") != records_authority_signature():
        result.update(status="UNCHECKED", reason="本機權威已變動，須重新核對遠端")
    return result


def _h240_read_current_shards(head):
    """Read exactly the manifest designated by the observed head, not legacy JSON."""
    from godpick_h127_sharded_records import load_snapshot
    raw, msg = load_snapshot(head, get_json=read_github_json, record_hash_func=_json_hash, workers=4)
    if raw is None:
        return None, msg
    rows = records_as_rows_exact(raw)
    if (len(rows) != int(head.get("count", -1)) or
            _json_hash(rows) != head.get("payload_hash") or
            _latest_record_recommendation_date(rows) != _recommendation_date_text(head.get("latest_recommendation_date"))):
        return None, "H240 分片實際日期/筆數/完整內容與權威摘要不一致"
    current, msg2 = read_github_json(RECORDS_STATE_FILE, {})
    if not isinstance(current, dict) or any(current.get(k) != head.get(k) for k in (
            "payload_hash", "revision", "count", "latest_recommendation_date", "shard_manifest_sha256")):
        return None, "H240 還原期間遠端權威已變更或無法讀取；下次重試｜" + str(msg2)
    return rows, msg


def _h240_current_github_restore(local_rows, local_state, local_valid, gh_state, fs_state, details, *, fallback=False):
    """Prefer a verified current H127 head; never publish or merge during recovery.

    A competing newer Firestore state is not automatically erased: GitHub can
    provide a verified view, but mutations remain blocked until reconciliation.
    """
    if gh_state.get("storage_mode") != "h127-gzip-shards-v1":
        return None
    gh_count = int(gh_state.get("count", -1))
    gh_date = _recommendation_date_text(gh_state.get("latest_recommendation_date"))
    if gh_count < 0 or (gh_count and not gh_date):
        return None
    local_date = _latest_record_recommendation_date(local_rows)
    local_hash = _json_hash(local_rows)
    gh_hash = _safe_str(gh_state.get("payload_hash"))
    gh_rev = _state_revision_v191_h8(gh_state)
    fs_rev = _state_revision_v191_h8(fs_state)
    local_rev = _state_revision_v191_h8(local_state) if local_valid else 0
    newer_fs = bool(fs_state and fs_state.get("payload_hash") != gh_hash and fs_rev > gh_rev)
    newer_delete = bool(_records_state_is_destructive_mutation(gh_state) and gh_rev > local_rev)
    if local_valid and local_rev > gh_rev and _records_state_is_destructive_mutation(local_state):
        return None
    if newer_fs and _records_state_is_destructive_mutation(fs_state):
        return None  # let explicit newer deletion be validated in the normal path
    if (gh_count < len(local_rows) or gh_date < local_date) and not newer_delete:
        return None
    if gh_hash == local_hash and gh_count == len(local_rows) and gh_date == local_date and local_valid:
        if newer_fs:
            return None
        _h240_status(local_rows, status="VERIFIED", source="GitHub H127 同Hash", head=gh_state)
        return local_rows, details + ["H240 GitHub現行H127權威同Hash；不掃歷史、不重抓分片"], False
    # Do not override newer same-date local edits, except a count-proven gap.
    if local_valid and local_rev > gh_rev and gh_count == len(local_rows) and gh_date == local_date:
        return None
    if newer_fs and not fallback and (int(fs_state.get("count") or 0) > gh_count or
                     _recommendation_date_text(fs_state.get("latest_recommendation_date")) > gh_date):
        return None
    rows, msg = _h240_read_current_shards(gh_state)
    details.append("H240 現行GitHub完整分片：" + str(msg))
    if rows is None:
        return None  # the existing verified Firestore twin fallback can still succeed
    # A current verified head is enough for recovery; the older checkout is
    # diagnostic only and must never be merged back into the remote authority.
    if local_rows and local_hash != gh_hash:
        ok, backup = write_local_json_atomic(f".cache/h240_pre_restore/{local_hash}.json", local_rows)
        if not ok:
            _h240_status(local_rows, status="BLOCKED", head=gh_state, reason="舊本機快照備份失敗")
            return local_rows, details + [backup], False
    state = dict(gh_state)
    state.update(restored_from="github_h240", restored_at=_now_text())
    ok1, msg1 = write_local_json_atomic(RECORDS_FILE, rows)
    ok2, msg2 = write_local_json_atomic(RECORDS_STATE_FILE, state) if ok1 else (False, "未更新state")
    ok3, msg3 = write_local_json_atomic(RECORDS_MANIFEST_FILE, _records_manifest(rows, gh_hash)) if ok2 else (False, "未更新manifest")
    details.extend([msg1, msg2, msg3])
    if not (ok1 and ok2 and ok3):
        _h240_status(rows if ok1 else local_rows, status="BLOCKED", head=gh_state, reason="還原未完整提交")
        return rows if ok1 else local_rows, details, False
    status = "CONFLICT" if newer_fs else "VERIFIED"
    reason = "Firestore有較新且不同Hash摘要，須對帳，不允許覆寫" if newer_fs else "現行H127完整分片、筆數、日期及Hash均核實"
    _h240_status(rows, status=status, source="GitHub H127", head=gh_state, reason=reason)
    return rows, details + [f"H240 {reason}｜{len(rows)}筆｜最新{gh_date}"], not newer_fs


def ensure_records_local_authority_current() -> tuple[list[dict[str, Any]], list[str], bool]:
    from godpick_h241_github_records import ensure
    return ensure()


def upsert_records_authority_fast(
    upsert_rows: Iterable[dict[str, Any]],
    *,
    reason: str = "record authority upsert",
    require_remote_confirm: bool = False,
) -> tuple[PersistenceReport, dict[str, int]]:
    from godpick_h241_github_records import upsert
    return upsert(upsert_rows, reason=reason, require_remote_confirm=require_remote_confirm)

def _secret(name: str, default: str = "") -> str:
    # H162: in the Streamlit app, the UI dispatcher and Page08 persistence MUST
    # use the same effective token after Secrets rotation. An older exported
    # environment variable may still exist and must not shadow st.secrets.
    # GitHub Actions/headless workers retain the environment-only fallback.
    if name == "GITHUB_TOKEN":
        try:
            secret_token = _safe_str(st.secrets.get(name, ""))
            if secret_token:
                return secret_token
        except Exception:
            pass
    # V186: the runtime-data branch safety guard still wins for branch keys;
    # only the token uses Secrets-first precedence, never a code branch change.
    env_value = _safe_str(os.getenv(name, ""))
    if env_value:
        return env_value
    try:
        return _safe_str(st.secrets.get(name, default))
    except Exception:
        return _safe_str(default)


def github_config() -> dict[str, str]:
    explicit_runtime = _secret("GITHUB_RUNTIME_DATA_BRANCH", "")
    legacy_branch = _secret("GITHUB_REPO_BRANCH", "runtime-data")
    runtime_branch = explicit_runtime or (legacy_branch if legacy_branch not in {"", "main", "master"} else "runtime-data")
    return {
        "token": _secret("GITHUB_TOKEN"),
        "owner": _secret("GITHUB_REPO_OWNER", "cheng07021028"),
        "repo": _secret("GITHUB_REPO_NAME", "stock-app"),
        "branch": runtime_branch,
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_url(path_name: str) -> str:
    cfg = github_config()
    clean_path = str(path_name).replace("\\", "/").lstrip("/")
    return f"https://api.github.com/repos/{cfg['owner']}/{cfg['repo']}/contents/{clean_path}"


def _github_raw_bytes(path_name: str, timeout: int = 120) -> tuple[bytes | None, str]:
    """Read a GitHub file in raw mode, including files larger than 1 MB.

    GitHub's contents API returns ``encoding: none`` and an empty ``content``
    field for 1-100 MB files.  The previous implementation treated that as an
    empty file, so a successful 20+ MB record upload was always reported as a
    failed verification.
    """
    cfg = github_config()
    headers = _github_headers(cfg["token"])
    headers["Accept"] = "application/vnd.github.raw+json"
    try:
        response = requests.get(
            _github_url(path_name),
            headers=headers,
            params={"ref": cfg["branch"], "_": int(time.time() * 1000)},
            timeout=(15, timeout),
        )
        if response.status_code == 404:
            return None, f"GitHub 尚未建立：{path_name}"
        if response.status_code != 200:
            return None, f"GitHub raw 讀取失敗：{path_name}｜HTTP {response.status_code}｜{response.text[:300]}"
        return response.content, f"已使用 GitHub raw 模式讀取：{path_name}"
    except Exception as exc:
        return None, f"GitHub raw 讀取例外：{path_name}｜{exc}"


def read_github_json(path_name: str, default: Any) -> tuple[Any, str]:
    cfg = github_config()
    if not cfg["token"]:
        return copy.deepcopy(default), "未設定 GITHUB_TOKEN"
    # >>> V191_H127_RECORDS_SHARD_AUTHORITY
    # The remote state is the publication pointer; a failed/incomplete shard
    # set must not be replaced with an older legacy JSON as if it were current.
    if (Path(path_name).name == RECORDS_FILE
            or path_name == (_secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE)):
        state_h127, _state_msg_h127 = read_github_json(RECORDS_STATE_FILE, {})
        if isinstance(state_h127, dict) and state_h127.get("storage_mode") == "h127-gzip-shards-v1":
            from godpick_h127_sharded_records import load_snapshot
            rows_h127, msg_h127 = load_snapshot(
                state_h127, get_json=read_github_json, record_hash_func=_json_hash
            )
            return (rows_h127 if rows_h127 is not None else copy.deepcopy(default)), msg_h127
        return copy.deepcopy(default), "H241 未取得現行 H127 權威，不回退舊版 GitHub JSON"
    # <<< V191_H127_RECORDS_SHARD_AUTHORITY
    try:
        response = requests.get(
            _github_url(path_name),
            headers=_github_headers(cfg["token"]),
            params={"ref": cfg["branch"]},
            timeout=(15, 45),
        )
        if response.status_code == 404:
            return copy.deepcopy(default), f"GitHub 尚未建立：{path_name}"
        if response.status_code != 200:
            return copy.deepcopy(default), f"GitHub 讀取失敗：{path_name}｜HTTP {response.status_code}｜{response.text[:300]}"

        meta = response.json() if response.content else {}
        content = meta.get("content", "") if isinstance(meta, dict) else ""
        encoding = _safe_str(meta.get("encoding")) if isinstance(meta, dict) else ""
        if content:
            decoded = base64.b64decode(content).decode("utf-8-sig")
            return json.loads(decoded), f"已讀取 GitHub：{path_name}"

        # Files larger than 1 MB are returned with encoding=none/content empty.
        # Re-read the same path with GitHub's raw media type instead of falsely
        # treating the file as blank.
        raw, raw_msg = _github_raw_bytes(path_name, timeout=150)
        if raw is None:
            return copy.deepcopy(default), f"GitHub 內容無法取得：{path_name}｜encoding={encoding or 'unknown'}｜{raw_msg}"
        try:
            return json.loads(raw.decode("utf-8-sig")), raw_msg
        except Exception as exc:
            return copy.deepcopy(default), f"GitHub raw JSON 解析失敗：{path_name}｜{exc}"
    except Exception as exc:
        return copy.deepcopy(default), f"GitHub 讀取例外：{path_name}｜{exc}"


def _read_github_json_at_ref(path_name: str, ref: str, default: Any) -> tuple[Any, str]:
    """Read one runtime-data file at an explicit historical Git commit/ref."""
    cfg = github_config()
    if not cfg["token"]:
        return copy.deepcopy(default), "未設定 GITHUB_TOKEN"
    try:
        response = requests.get(
            _github_url(path_name),
            headers=_github_headers(cfg["token"]),
            params={"ref": _safe_str(ref)},
            timeout=(15, 60),
        )
        if response.status_code != 200:
            return copy.deepcopy(default), f"GitHub歷史讀取失敗：{path_name}@{ref[:10]}｜HTTP {response.status_code}"
        meta = response.json() if response.content else {}
        content = meta.get("content", "") if isinstance(meta, dict) else ""
        if content:
            return json.loads(base64.b64decode(content).decode("utf-8-sig")), f"已讀取GitHub歷史：{path_name}@{ref[:10]}"
        # Large files need raw media mode with the same historical ref.
        headers = _github_headers(cfg["token"])
        headers["Accept"] = "application/vnd.github.raw+json"
        raw = requests.get(
            _github_url(path_name), headers=headers, params={"ref": _safe_str(ref)}, timeout=(15, 180)
        )
        if raw.status_code != 200:
            return copy.deepcopy(default), f"GitHub歷史raw讀取失敗：{path_name}@{ref[:10]}｜HTTP {raw.status_code}"
        return json.loads(raw.content.decode("utf-8-sig")), f"已使用GitHub raw讀取歷史：{path_name}@{ref[:10]}"
    except Exception as exc:
        return copy.deepcopy(default), f"GitHub歷史讀取例外：{path_name}@{ref[:10]}｜{exc}"


def _github_commits_for_path(path_name: str, *, limit: int = 60) -> tuple[list[dict[str, Any]], str]:
    cfg = github_config()
    if not cfg["token"]:
        return [], "未設定 GITHUB_TOKEN"
    url = f"https://api.github.com/repos/{cfg['owner']}/{cfg['repo']}/commits"
    try:
        resp = requests.get(
            url, headers=_github_headers(cfg["token"]),
            params={"sha": cfg["branch"], "path": str(path_name).replace("\\", "/"), "per_page": max(1, min(int(limit), 100))},
            timeout=(15, 45),
        )
        if resp.status_code != 200:
            return [], f"GitHub歷史清單讀取失敗：HTTP {resp.status_code}｜{resp.text[:220]}"
        rows = resp.json() if resp.content else []
        return ([x for x in rows if isinstance(x, dict)] if isinstance(rows, list) else []), f"GitHub歷史清單 {len(rows) if isinstance(rows, list) else 0} 筆"
    except Exception as exc:
        return [], f"GitHub歷史清單例外：{exc}"


def recover_records_from_github_history(
    current_rows: Iterable[dict[str, Any]] | None = None,
    *,
    max_commits: int = 60,
    reason: str = "V191-H4 authority gap recovery",
) -> tuple[list[dict[str, Any]], list[str], bool]:
    """Recover the *best* verified cumulative recommendation authority in Git history.

    H3 stopped at the first historical state that was merely "large enough".
    Because high-priority Page07 commits were inspected before chronological
    completeness, the live system could choose 1810 rows ending 2026-07-09 even
    though later verified states contained 1870 rows through 2026-08-12 and
    1874 rows through 2026-08-13.

    H4 first scans only the small state files, ranks plausible candidates by
    business-data freshness (latest recommendation date first, then row count),
    and downloads the large records JSON only for the best-ranked candidates.
    Explicit destructive mutations are never resurrected.
    """
    current = records_as_rows_exact(list(current_rows or []))
    current_count = len(current)
    current_state, _, _ = read_local_json(RECORDS_STATE_FILE, {})
    current_state = dict(current_state or {}) if isinstance(current_state, dict) else {}
    current_latest = (
        _recommendation_date_text(current_state.get("latest_recommendation_date"))
        or _latest_record_recommendation_date(current)
    )
    details: list[str] = [
        f"V191-H4權威缺口救援：目前 {current_count} 筆｜最新{current_latest or '未取得'}"
    ]

    if _records_state_is_destructive_mutation(current_state):
        details.append("目前 state 是明確刪除/清空 mutation；依資料保護規則禁止從歷史復活已刪紀錄。")
        return current, details, False

    commits, commits_msg = _github_commits_for_path(RECORDS_STATE_FILE, limit=max_commits)
    details.append(commits_msg)
    if not commits:
        return current, details, False

    collapse_mode = bool(
        _records_state_looks_like_accidental_collapse(current_state, current_count)
        or current_count <= 50
    )
    known_gap_mode = _records_state_has_known_recovery_gap_risk(current_state)
    # Tiny 0/4-row incidents need a genuinely cumulative snapshot; a large stale
    # authority may recover from a snapshot with roughly the same population as
    # long as the business date is newer.  This filters the corrupted 4-row
    # 2026-08-13 commits while admitting 1874/2026-08-13 over 1810/2026-07-09.
    if current_count <= 50:
        min_candidate_count = max(50, current_count + 1)
    else:
        min_candidate_count = max(50, int(math.floor(current_count * 0.80)))

    def _message_priority(item: dict[str, Any]) -> int:
        try:
            msg = _safe_str(((item.get("commit") or {}).get("message"))).lower()
        except Exception:
            msg = ""
        high = (
            "07 股神推薦", "股神推薦完成", "page8 v18", "page8 performance",
            "scheduled page8", "推薦後績效",
        )
        return 1 if any(token in msg for token in high) else 0

    # Fetch state JSONs concurrently; they are tiny.  The large authority file is
    # intentionally NOT downloaded here.
    indexed = [(idx, item) for idx, item in enumerate(commits) if _safe_str(item.get("sha"))]

    def _load_state(pair: tuple[int, dict[str, Any]]) -> tuple[int, dict[str, Any], dict[str, Any], str]:
        idx, item = pair
        sha = _safe_str(item.get("sha"))
        payload, msg = _read_github_json_at_ref(RECORDS_STATE_FILE, sha, {})
        return idx, item, (dict(payload) if isinstance(payload, dict) else {}), msg

    state_results: list[tuple[int, dict[str, Any], dict[str, Any], str]] = []
    if indexed:
        try:
            with ThreadPoolExecutor(max_workers=min(8, len(indexed))) as pool:
                state_results = list(pool.map(_load_state, indexed))
        except Exception:
            state_results = [_load_state(pair) for pair in indexed]

    ranked: list[tuple[tuple[Any, ...], str, dict[str, Any], dict[str, Any]]] = []
    for idx, item, state_payload, _state_msg in state_results:
        if not state_payload:
            continue
        try:
            count = int(state_payload.get("count") or 0)
        except Exception:
            count = 0
        latest = _recommendation_date_text(state_payload.get("latest_recommendation_date"))
        sha = _safe_str(item.get("sha"))
        if count < min_candidate_count:
            continue
        if _records_state_is_destructive_mutation(state_payload):
            # A historical destructive state can be valid as an authority at its
            # time, but it is not safe as an automatic resurrection source.
            continue

        if collapse_mode:
            plausible = count >= max(50, current_count + max(10, int(max(current_count, 1) * 0.05)))
        else:
            plausible = bool(latest and (not current_latest or latest > current_latest))
            if not plausible and known_gap_mode and latest == current_latest:
                plausible = count > current_count
        if not plausible:
            continue

        # Rank by business truth, not writer type: newest recommendation date is
        # primary; row count is secondary.  Writer signal and commit recency only
        # break ties.  This is the exact invariant H3 violated.
        score = (latest, count, _message_priority(item), -idx)
        ranked.append((score, sha, state_payload, item))

    if not ranked:
        details.append(
            f"Git歷史中沒有安全候選可補足目前缺口｜最低候選筆數 {min_candidate_count}。"
        )
        return current, details, False

    ranked.sort(key=lambda x: x[0], reverse=True)
    details.append(
        "候選排序前3：" + "；".join(
            f"{sha[:8]}={int(stt.get('count') or 0)}筆/{_recommendation_date_text(stt.get('latest_recommendation_date')) or '無日期'}"
            for _, sha, stt, _ in ranked[:3]
        )
    )

    gh_path = _secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE
    chosen_sha = ""
    chosen_state: dict[str, Any] = {}
    historical: list[dict[str, Any]] = []
    # Normally only the first candidate is downloaded.  If its large file is
    # missing/inconsistent, try the next ranked state rather than falling back to
    # a known stale snapshot.
    for _score, sha, state_payload, _item in ranked[:5]:
        if state_payload.get("storage_mode") == "h127-gzip-shards-v1":
            from godpick_h127_sharded_records import load_snapshot
            historical_payload, hist_msg = load_snapshot(state_payload,
                get_json=lambda path, default: _read_github_json_at_ref(path, sha, default),
                record_hash_func=_json_hash, workers=4)
        else:
            historical_payload, hist_msg = _read_github_json_at_ref(gh_path, sha, [])
        details.append(hist_msg)
        rows = records_as_rows_exact(historical_payload)
        hist_latest = _latest_record_recommendation_date(rows)
        try:
            state_count = int(state_payload.get("count") or 0)
        except Exception:
            state_count = 0
        state_latest = _recommendation_date_text(state_payload.get("latest_recommendation_date"))
        if len(rows) < min_candidate_count:
            details.append(f"候選 {sha[:8]} 大檔僅 {len(rows)} 筆，低於安全門檻，跳過。")
            continue
        # Require the full file to substantiate its state metadata.  Count may be
        # slightly larger after legacy normalization, but it must not be a tiny
        # subset and its newest business date cannot trail the advertised state.
        if state_count and len(rows) < max(min_candidate_count, int(state_count * 0.95)):
            details.append(f"候選 {sha[:8]} 大檔筆數與state不一致（{len(rows)}/{state_count}），跳過。")
            continue
        if state_latest and hist_latest and hist_latest < state_latest:
            details.append(f"候選 {sha[:8]} 大檔最新日{hist_latest}落後state {state_latest}，跳過。")
            continue
        chosen_sha = sha
        chosen_state = dict(state_payload)
        historical = rows
        break

    if not chosen_sha or not historical:
        details.append("所有高分歷史候選的大檔驗證皆未通過；保留目前權威，不做猜測式復原。")
        return current, details, False

    merged = _merge_record_sources_v178([historical, current])
    merged_latest = _latest_record_recommendation_date(merged)
    if len(merged) <= current_count and (not merged_latest or merged_latest <= current_latest):
        details.append("最佳歷史快照合併後沒有增加筆數或日期；不覆蓋目前權威。")
        return current, details, False

    recovery_state = _new_state(
        "godpick_records_durable_v191_h4_authority_gap_recovery", merged,
        count=len(merged),
        latest_recommendation_date=merged_latest,
        recovered_from_commit=chosen_sha,
        recovered_from_count=int(chosen_state.get("count") or len(historical)),
        recovered_from_latest_recommendation_date=_recommendation_date_text(chosen_state.get("latest_recommendation_date")),
        recovery_reason=_safe_str(reason),
        mutation_kind="recovery_non_destructive",
    )
    ok1, msg1 = write_local_json_atomic(RECORDS_FILE, merged)
    ok2, msg2 = write_local_json_atomic(RECORDS_STATE_FILE, recovery_state) if ok1 else (False, "records寫入失敗")
    ok3, msg3 = write_local_json_atomic(RECORDS_MANIFEST_FILE, _records_manifest(merged, recovery_state["payload_hash"])) if ok1 and ok2 else (False, "state寫入失敗")
    details.extend([msg1, msg2, msg3])
    if not (ok1 and ok2 and ok3):
        return current, details, False

    try:
        if firebase_configured():
            fs_ok, fs_msg = write_records_firestore(merged, recovery_state)
            details.append(f"救援後Firestore完整重建｜{fs_msg}")
        else:
            fs_ok, fs_msg = False, "Firebase 未設定"
            details.append(f"救援後Firestore完整重建｜{fs_msg}")
        if github_config().get("token"):
            gh_queued, gh_msg = schedule_records_github_sync(
                merged, recovery_state, f"{reason} | V191-H4 GitHub full rebuild after authority-gap rescue"
            )
            details.append(f"救援後GitHub完整快照｜{gh_msg}")
        else:
            gh_queued, gh_msg = False, "GitHub 未設定"
            details.append(f"救援後GitHub完整快照｜{gh_msg}")
        if _configured_remote_exists() and not (fs_ok or gh_queued):
            details.append("V191-H4警告：本機缺口已補回，但遠端完整重建尚未接受；永久化重試會繼續處理。")
    except Exception as exc:
        details.append(f"救援已完成本機恢復，但遠端完整重建例外：{exc}")
    details.append(
        f"V191-H4已由最佳Git歷史權威補回缺口：{current_count} → {len(merged)} 筆｜"
        f"最新{current_latest or '未取得'} → {merged_latest or '未取得'}｜來源 {chosen_sha[:10]}。"
    )
    return merged, details, True


def _github_path_write_lock(path_name: str) -> threading.RLock:
    """Return a process-wide lock for one GitHub contents path.

    Streamlit sessions share a process.  Without a per-path lock, Page7/Page8
    and the durability worker can fetch the same SHA and race the following PUT.
    GitHub correctly returns HTTP 409 in that case.  H8 serializes same-process
    writers while CAS retries still protect cross-process/GitHub-Actions races.
    """
    key = str(path_name or "").replace("\\", "/")
    with _GITHUB_PATH_LOCKS_GUARD:
        lock = _GITHUB_PATH_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _GITHUB_PATH_LOCKS[key] = lock
        return lock


def _github_meta_sha(path_name: str, cfg: dict[str, str]) -> tuple[str, int, str]:
    try:
        current = requests.get(
            _github_url(path_name),
            headers=_github_headers(cfg["token"]),
            params={"ref": cfg["branch"], "_": int(time.time() * 1000)},
            timeout=(15, 60),
        )
        if current.status_code == 404:
            return "", 404, "not found"
        if current.status_code != 200:
            return "", int(current.status_code), f"取得 SHA 失敗 HTTP {current.status_code}｜{current.text[:250]}"
        meta = current.json() if current.content else {}
        return (_safe_str(meta.get("sha")) if isinstance(meta, dict) else ""), 200, "ok"
    except Exception as exc:
        return "", 0, f"取得 SHA 例外：{exc}"


def write_github_json(path_name: str, payload: Any, message: str = "update durable data") -> tuple[bool, str]:
    """Conflict-safe GitHub contents write.

    H8 fixes the observed 29.4MB recommendation-history HTTP 409.  Each attempt
    re-fetches the current blob SHA (CAS), same-process writers are serialized,
    and a 409/422 is treated as a retryable version race rather than a terminal
    data error.  Verification still compares the actual remote JSON hash.
    """
    if Path(path_name).name in {RECORDS_FILE, RECORDS_STATE_FILE} or path_name == (_secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE) or str(path_name).startswith("data/godpick_records_h127/"):
        return False, "H241 推薦紀錄僅允許固定父版本CAS提交；禁止一般JSON覆寫"
    cfg = github_config()
    if not cfg["token"]:
        return False, "未設定 GITHUB_TOKEN"

    safe_payload = _json_safe_value(payload)
    raw = json.dumps(safe_payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii")
    size_mb = len(raw) / (1024 * 1024)
    upload_timeout = max(90, min(360, int(90 + size_mb * 9)))
    # Large payload conflicts are expensive, but two retries were insufficient
    # in production. Four CAS rounds handle concurrent Page8/scheduler writers
    # without turning a permanent network outage into an unbounded wait.
    attempts = 4 if size_mb >= 5 else 5
    wanted_hash = _json_hash(safe_payload)
    last_error = ""
    conflict_count = 0

    with _github_path_write_lock(path_name):
        for attempt in range(attempts):
            sha, meta_status, meta_msg = _github_meta_sha(path_name, cfg)
            if meta_status not in (200, 404):
                last_error = meta_msg
                if meta_status in (401, 403):
                    return False, f"H161 GitHub權限不足 HTTP {meta_status}：需確認 Token 所選 stock-app 儲存庫與 Contents: Read and write；未重試大型備份。{path_name}"
                time.sleep(min(3.0, 0.7 * (attempt + 1)))
                continue
            try:
                if path_name == RECORDS_STATE_FILE:
                    existing_state, _ = read_github_json(RECORDS_STATE_FILE, {})
                    if isinstance(existing_state, dict) and isinstance(safe_payload, dict):
                        try:
                            remote_rev = int(existing_state.get("revision") or 0)
                            incoming_rev = int(safe_payload.get("revision") or 0)
                        except Exception:
                            remote_rev = incoming_rev = 0
                        if remote_rev and incoming_rev and remote_rev > incoming_rev:
                            return False, (
                                "GitHub records state 已有較新revision；拒絕舊快照覆寫"
                            )
                        if existing_state.get("payload_hash") and not _records_state_is_destructive_mutation(safe_payload):
                            if (int(safe_payload.get("count", -1)) < int(existing_state.get("count", -1)) or
                                    _recommendation_date_text(safe_payload.get("latest_recommendation_date")) < _recommendation_date_text(existing_state.get("latest_recommendation_date"))):
                                return False, "H240 GitHub CAS拒絕日期或筆數回退"
                body: dict[str, Any] = {
                    "message": f"{message} @ {_now_text()}",
                    "content": encoded,
                    "branch": cfg["branch"],
                }
                if sha:
                    body["sha"] = sha
                response = requests.put(
                    _github_url(path_name),
                    headers=_github_headers(cfg["token"]),
                    json=body,
                    timeout=(20, upload_timeout),
                )
                if response.status_code in (200, 201):
                    # For large files, avoid immediately downloading another
                    # 20-30MB just to verify the PUT.  The Contents response
                    # returns the new blob SHA; re-read only metadata and confirm
                    # that SHA is still current.  The records gateway then writes
                    # and verifies the small payload-hash state file.
                    resp_meta = response.json() if response.content else {}
                    content_meta = resp_meta.get("content", {}) if isinstance(resp_meta, dict) else {}
                    accepted_sha = _safe_str(content_meta.get("sha")) if isinstance(content_meta, dict) else ""
                    if size_mb >= 5 and accepted_sha:
                        current_sha, verify_status, verify_msg = _github_meta_sha(path_name, cfg)
                        if verify_status == 200 and current_sha == accepted_sha:
                            suffix = f"｜CAS衝突自動重試 {conflict_count} 次" if conflict_count else ""
                            return True, f"GitHub 大型檔寫入完成並以blob SHA驗證：{path_name}｜{size_mb:.1f} MB{suffix}"
                        last_error = f"GitHub 大型檔寫入後版本已被接續：accepted={accepted_sha[:12]} current={current_sha[:12]}｜{verify_msg}"
                    else:
                        verify, verify_msg = read_github_json(path_name, None)
                        if verify is not None and _json_hash(verify) == wanted_hash:
                            suffix = f"｜CAS衝突自動重試 {conflict_count} 次" if conflict_count else ""
                            return True, f"GitHub 寫入並回讀驗證：{path_name}｜{size_mb:.1f} MB{suffix}"
                        last_error = f"GitHub 寫入成功但回讀不一致：{verify_msg}"
                elif response.status_code == 422 and any(
                    phrase in response.text.lower() for phrase in
                    ("too large", "file is too large", "too big", "exceeds the size")
                ):
                    # H127: validation error is NOT a CAS conflict. Retrying
                    # the identical 54MB body cannot succeed.
                    return False, (
                        f"GitHub 文件過大 HTTP 422：{path_name}｜{size_mb:.1f} MB｜"
                        "需使用 H127 壓縮分片，不再做無效 CAS 重傳"
                    )
                elif response.status_code in (401, 403):
                    # Permission is not a CAS race; retrying a 500KB shard 5x
                    # cannot grant the missing Contents permission.
                    return False, (
                        f"H161 GitHub寫入權限不足 HTTP {response.status_code}：{path_name}｜"
                        "請更新 Streamlit Secrets 使用的 Token：stock-app → Contents: Read and write；"
                        "並確認 GitHub runtime-data 分支。資料未完成遠端保存。"
                    )
                elif response.status_code in (409, 422):
                    conflict_count += 1
                    last_error = f"GitHub 版本衝突 HTTP {response.status_code}｜{response.text[:220]}"
                    # The competing writer may have written exactly our payload.
                    # For small files this verification is cheap; for large files
                    # the records-specific state check below avoids re-downloading
                    # 30MB and this loop simply rebases to the newest SHA.
                    if size_mb < 5:
                        try:
                            verify, _ = read_github_json(path_name, None)
                            if verify is not None and _json_hash(verify) == wanted_hash:
                                return True, f"GitHub 版本衝突後確認遠端已是同內容：{path_name}"
                        except Exception:
                            pass
                else:
                    last_error = f"GitHub 寫入失敗 HTTP {response.status_code}｜{response.text[:350]}"
            except Exception as exc:
                last_error = f"GitHub 寫入例外：{exc}"
            # deterministic bounded backoff; no hidden infinite retry
            time.sleep(min(5.0, 1.0 + attempt * 1.25))
    return False, f"{path_name}｜{last_error or '未知錯誤'}｜資料大小 {size_mb:.1f} MB｜CAS重試 {conflict_count} 次"


def _firebase_config() -> dict[str, str]:
    return {
        "project_id": _secret("FIREBASE_PROJECT_ID"),
        "client_email": _secret("FIREBASE_CLIENT_EMAIL"),
        "private_key": _secret("FIREBASE_PRIVATE_KEY").replace("\\n", "\n"),
    }


def firebase_configured() -> bool:
    cfg = _firebase_config()
    return bool(cfg["project_id"] and cfg["client_email"] and cfg["private_key"])


def _init_firebase_app():
    # H163: one exhausted Cloud quota must not turn each Streamlit rerun into
    # another failed Firestore request; callers safely fall back to GitHub.
    _quota_note = _H163_FS_GUARD.quota_reason()
    if _quota_note:
        raise RuntimeError(_quota_note)
    if firebase_admin is None or credentials is None or firestore is None:
        raise RuntimeError("firebase-admin 未安裝或不可用")
    try:
        return firebase_admin.get_app()
    except ValueError:
        pass
    cfg = _firebase_config()
    if not firebase_configured():
        raise RuntimeError("Firebase secrets 不完整")
    cred = credentials.Certificate(
        {
            "type": "service_account",
            "project_id": cfg["project_id"],
            "private_key": cfg["private_key"],
            "client_email": cfg["client_email"],
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    )
    return firebase_admin.initialize_app(cred, {"projectId": cfg["project_id"]})


def _firestore_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    return _parse_time(value)


def _commit_operations(db, operations: list[tuple[str, Any, Any]], chunk_size: int = 400) -> None:
    for offset in range(0, len(operations), chunk_size):
        batch = db.batch()
        for op, ref, payload in operations[offset : offset + chunk_size]:
            if op == "set":
                batch.set(ref, payload, merge=True)
            elif op == "delete":
                batch.delete(ref)
        batch.commit()


def normalize_watchlist(payload: Any) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = {}
    if not isinstance(payload, dict):
        return out
    for group_name, items in payload.items():
        group = _safe_str(group_name)
        if not group or group.startswith("__"):
            continue
        seen: set[str] = set()
        rows: list[dict[str, str]] = []
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                code_raw = _safe_str(item.get("code") or item.get("股票代號"))
                digits = "".join(ch for ch in code_raw if ch.isdigit())
                code = digits if 4 <= len(digits) <= 6 else code_raw
                if not code or code in seen:
                    continue
                seen.add(code)
                row = {
                    "code": code,
                    "name": _safe_str(item.get("name") or item.get("股票名稱")) or code,
                    "market": _safe_str(item.get("market") or item.get("市場別")) or "上市",
                }
                category = _safe_str(item.get("category") or item.get("類別") or item.get("產業"))
                if category:
                    row["category"] = category
                rows.append(row)
        out[group] = sorted(rows, key=lambda r: (r.get("code", ""), r.get("name", "")))
    return out


def _watchlist_doc_id(group_name: str) -> str:
    return "g_" + hashlib.sha1(group_name.encode("utf-8")).hexdigest()[:32]


def _read_watchlist_firestore_full() -> tuple[dict[str, list[dict[str, str]]], str, datetime, dict[str, Any]]:
    if not firebase_configured():
        return {}, "Firebase 未設定", datetime.min, {}
    try:
        _init_firebase_app()
        db = firestore.client()
        snapshot = db.collection("system").document("watchlist_snapshot").get()
        if snapshot.exists:
            data = snapshot.to_dict() or {}
            payload = normalize_watchlist(data.get("payload", {}))
            state = {
                "payload_hash": data.get("payload_hash"),
                "updated_at": data.get("updated_at_text"),
                "updated_at_utc": data.get("updated_at_utc"),
                "updated_at_epoch": data.get("updated_at_epoch"),
                "revision": data.get("revision"),
            }
            if _state_is_valid(payload, state):
                return payload, "已讀取 Firestore watchlist_snapshot", _firestore_time(data.get("updated_at")), state

        rows: dict[str, list[dict[str, str]]] = {}
        latest = datetime.min
        for doc in db.collection("watchlists").stream():
            data = doc.to_dict() or {}
            group = _safe_str(data.get("group_name")) or doc.id
            items = data.get("items", [])
            rows[group] = items if isinstance(items, list) else []
            latest = max(latest, _firestore_time(data.get("updated_at")))
        normalized = normalize_watchlist(rows)
        summary = db.collection("system").document("watchlist_summary").get()
        state: dict[str, Any] = {}
        if summary.exists:
            summary_data = summary.to_dict() or {}
            latest = max(latest, _firestore_time(summary_data.get("updated_at")))
            state = {
                "payload_hash": summary_data.get("payload_hash"),
                "updated_at": summary_data.get("updated_at_text"),
                "updated_at_utc": summary_data.get("updated_at_utc"),
                "updated_at_epoch": summary_data.get("updated_at_epoch"),
                "revision": summary_data.get("revision"),
            }
        return normalized, "已讀取 Firestore watchlists（相容模式）", latest, state
    except Exception as exc:
        _H163_FS_GUARD.note_error(exc, "watchlist_read")
        return {}, f"Firestore watchlist 讀取失敗：{exc}", datetime.min, {}


def read_watchlist_firestore() -> tuple[dict[str, list[dict[str, str]]], str, datetime]:
    payload, message, latest, _ = _read_watchlist_firestore_full()
    return payload, message, latest


def write_watchlist_firestore(payload: dict[str, list[dict[str, str]]], state: dict[str, Any]) -> tuple[bool, str]:
    if not firebase_configured():
        return False, "Firebase 未設定"
    try:
        _init_firebase_app()
        db = firestore.client()
        normalized = normalize_watchlist(payload)
        now = firestore.SERVER_TIMESTAMP
        operations: list[tuple[str, Any, Any]] = []
        existing_ids = {doc.id for doc in db.collection("watchlists").stream()}
        new_ids: set[str] = set()
        for group, items in normalized.items():
            doc_id = _watchlist_doc_id(group)
            new_ids.add(doc_id)
            ref = db.collection("watchlists").document(doc_id)
            operations.append((
                "set",
                ref,
                {
                    "group_name": group,
                    "items": items,
                    "count": len(items),
                    "payload_hash": state["payload_hash"],
                    "revision": state.get("revision"),
                    "updated_at_epoch": state.get("updated_at_epoch"),
                    "updated_at_utc": state.get("updated_at_utc"),
                    "updated_at": now,
                    "source": "godpick_persistence_service",
                },
            ))
        for doc_id in existing_ids - new_ids:
            operations.append(("delete", db.collection("watchlists").document(doc_id), None))

        common = {
            "group_count": len(normalized),
            "stock_count": sum(len(v) for v in normalized.values()),
            "payload_hash": state["payload_hash"],
            "revision": state.get("revision"),
            "updated_at_epoch": state.get("updated_at_epoch"),
            "updated_at_utc": state.get("updated_at_utc"),
            "updated_at": now,
            "updated_at_text": state["updated_at"],
            "source": "godpick_persistence_service",
        }
        operations.append(("set", db.collection("system").document("watchlist_summary"), common))
        snapshot_payload = dict(common)
        snapshot_payload["payload"] = normalized
        operations.append(("set", db.collection("system").document("watchlist_snapshot"), snapshot_payload))
        _commit_operations(db, operations)

        verify, _, _, verify_state = _read_watchlist_firestore_full()
        if _json_hash(verify) != state["payload_hash"] or not _state_is_valid(verify, verify_state):
            return False, "Firestore watchlist 快照回讀驗證不一致"
        return True, "Firestore watchlist 完整快照與群組明細寫入並回讀驗證"
    except Exception as exc:
        _H163_FS_GUARD.note_error(exc, "watchlist_write")
        return False, f"Firestore watchlist 寫入失敗：{exc}"

@dataclass
class PersistenceReport:
    local_ok: bool = False
    github_ok: bool = False
    firestore_ok: bool = False
    github_pending: bool = False
    permanent_ok: bool = False
    local_message: str = ""
    github_message: str = ""
    firestore_message: str = ""
    payload_hash: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def messages(self) -> list[str]:
        return [
            f"本機：{'成功' if self.local_ok else '失敗'}｜{self.local_message}",
            f"GitHub：{'成功' if self.github_ok else ('同步中' if self.github_pending else '失敗')}｜{self.github_message}",
            f"Firestore：{'已停用（推薦紀錄）' if self.firestore_message.startswith('H241') else ('成功' if self.firestore_ok else '失敗')}｜{self.firestore_message}",
        ]


def _configured_remote_exists() -> bool:
    return bool(github_config()["token"] or firebase_configured())


def save_watchlist_permanent(payload: Any) -> PersistenceReport:
    normalized = normalize_watchlist(payload)
    state = _new_state(
        "watchlist_durable_v2",
        normalized,
        group_count=len(normalized),
        stock_count=sum(len(v) for v in normalized.values()),
    )
    report = PersistenceReport(payload_hash=state["payload_hash"], updated_at=state["updated_at"])
    ok1, msg1 = write_local_json_atomic(WATCHLIST_FILE, normalized)
    ok2, msg2 = write_local_json_atomic(WATCHLIST_STATE_FILE, state)
    report.local_ok = bool(ok1 and ok2)
    report.local_message = f"{msg1}｜{msg2}"

    gh_path = _secret("WATCHLIST_GITHUB_PATH", WATCHLIST_FILE) or WATCHLIST_FILE
    gh1, ghm1 = write_github_json(gh_path, normalized, "persist watchlist")
    gh2, ghm2 = write_github_json(WATCHLIST_STATE_FILE, state, "persist watchlist state") if gh1 else (False, "watchlist 未寫入，略過狀態")
    report.github_ok = bool(gh1 and gh2)
    report.github_message = f"{ghm1}｜{ghm2}"

    fs_ok, fs_msg = write_watchlist_firestore(normalized, state)
    report.firestore_ok = fs_ok
    report.firestore_message = fs_msg

    if _configured_remote_exists():
        report.permanent_ok = bool(report.local_ok and (report.github_ok or report.firestore_ok))
    else:
        report.permanent_ok = False  # H141: ephemeral local file is NEVER Reboot-safe.
    return report


def load_watchlist_permanent() -> tuple[dict[str, list[dict[str, str]]], list[str]]:
    local_payload, local_msg, local_mtime = read_local_json(WATCHLIST_FILE, {})
    local_state, local_state_msg, _ = read_local_json(WATCHLIST_STATE_FILE, {})

    gh_path = _secret("WATCHLIST_GITHUB_PATH", WATCHLIST_FILE) or WATCHLIST_FILE
    gh_payload, gh_msg = read_github_json(gh_path, {})
    gh_state, gh_state_msg = read_github_json(WATCHLIST_STATE_FILE, {})

    fs_payload, fs_msg, fs_mtime, fs_state = _read_watchlist_firestore_full()

    local_norm = normalize_watchlist(local_payload)
    gh_norm = normalize_watchlist(gh_payload)
    fs_norm = normalize_watchlist(fs_payload)
    candidates = [
        ("local", local_norm, local_state, local_mtime),
        ("github", gh_norm, gh_state, datetime.min),
        ("firestore", fs_norm, fs_state, fs_mtime),
    ]
    valid = []
    for source, payload, state, fallback in candidates:
        if _state_is_valid(payload, state):
            valid.append((source, payload, state, _state_epoch(state, fallback)))
        elif source == "local" and payload and not isinstance(state, dict):
            valid.append((source, payload, {}, _state_epoch({}, fallback)))

    if valid:
        source, chosen, chosen_state, _ = max(
            valid,
            key=lambda item: (item[3], int((item[2] or {}).get("revision") or 0), {"local": 0, "github": 1, "firestore": 2}.get(item[0], 0)),
        )
    else:
        # Legacy installation without state metadata: prefer remote data, then local.
        source, chosen = next(
            ((src, data) for src, data in [("firestore", fs_norm), ("github", gh_norm), ("local", local_norm)] if data),
            ("local", local_norm),
        )
        chosen_state = _new_state(
            "watchlist_durable_v2",
            chosen,
            group_count=len(chosen),
            stock_count=sum(len(v) for v in chosen.values()),
            migrated_from=source,
        )

    restored_state = dict(chosen_state or {})
    restored_state.update({
        "version": restored_state.get("version") or "watchlist_durable_v2",
        "payload_hash": _json_hash(chosen),
        "group_count": len(chosen),
        "stock_count": sum(len(v) for v in chosen.values()),
        "restored_from": source,
        "restored_at": _now_text(),
    })
    # Loading must never advance the authority timestamp; otherwise a stale local copy
    # can look newer than a later remote update from another user.
    restored_state.setdefault("updated_at", _now_text())
    restored_state.setdefault("updated_at_utc", _now_utc_text())
    restored_state.setdefault("updated_at_epoch", _now_epoch())
    restored_state.setdefault("revision", time.time_ns())
    write_local_json_atomic(WATCHLIST_FILE, chosen)
    write_local_json_atomic(WATCHLIST_STATE_FILE, restored_state)
    details = [
        f"權威來源：{source}｜群組 {len(chosen)}｜股票 {sum(len(v) for v in chosen.values())}",
        f"本機：{local_msg}｜{local_state_msg}",
        f"GitHub：{gh_msg}｜{gh_state_msg}",
        f"Firestore：{fs_msg}",
    ]
    return chosen, details


def _record_id(row: dict[str, Any]) -> str:
    rid = _safe_str(row.get("record_id") or row.get("rec_id") or row.get("id"))
    if rid:
        return rid
    return "|".join(
        [
            _safe_str(row.get("股票代號") or row.get("code")),
            _safe_str(row.get("推薦日期") or row.get("date")),
            _safe_str(row.get("推薦時間") or row.get("time")),
            _safe_str(row.get("推薦模式") or row.get("mode")),
        ]
    )


def _repair_record_ids_rows_v178(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Preserve every record while repairing legacy duplicate/missing record IDs.

    Older files reused one record_id across multiple recommendation dates. A dict
    keyed by record_id silently merged those rows and Firestore document IDs also
    collided. V178 creates deterministic per-event IDs and keeps the legacy ID for
    audit/migration.
    """
    clean = [dict(r) for r in rows if isinstance(r, dict)]
    counts: dict[str, int] = {}
    for row in clean:
        rid = _safe_str(row.get("record_id"))
        if rid:
            counts[rid] = counts.get(rid, 0) + 1
    used = {rid for rid, n in counts.items() if n == 1}
    for row in clean:
        old_id = _safe_str(row.get("record_id"))
        if old_id and counts.get(old_id, 0) == 1:
            continue
        fields = [
            old_id,
            _safe_str(row.get("股票代號") or row.get("code")),
            _safe_str(row.get("推薦日期") or row.get("date")),
            _safe_str(row.get("推薦時間") or row.get("time")),
            _safe_str(row.get("推薦模式") or row.get("mode")),
            _safe_str(row.get("建立時間") or row.get("created_at")),
            _safe_str(row.get("推薦價格") or row.get("推薦日價格")),
            _safe_str(row.get("推薦理由摘要") or row.get("備註")),
        ]
        seed = "|".join(fields)
        new_id = hashlib.md5(seed.encode("utf-8")).hexdigest()
        ordinal = 1
        while new_id in used:
            ordinal += 1
            new_id = hashlib.md5(f"{seed}|{ordinal}".encode("utf-8")).hexdigest()
        used.add(new_id)
        if old_id:
            row["原始record_id"] = _safe_str(row.get("原始record_id")) or old_id
            row["record_id修復狀態"] = _safe_str(row.get("record_id修復狀態")) or "V178重建｜原ID重複"
        else:
            row["record_id修復狀態"] = _safe_str(row.get("record_id修復狀態")) or "V178建立｜原ID缺失"
        row["record_id"] = new_id
    return clean



def records_as_rows_exact(payload: Any) -> list[dict[str, Any]]:
    """Convert records to JSON-safe rows without business-key de-duplication.

    Mutation actions must delete only the selected record_ids.  The historical
    full-sync normalizer merges duplicate business keys; using it during a
    single-row delete could silently remove unrelated legacy rows.  This helper
    preserves row count and order while still sanitizing pandas/numpy values.
    """
    if isinstance(payload, pd.DataFrame):
        rows = payload.loc[:, ~payload.columns.duplicated()].to_dict(orient="records")
    elif isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = next(
            (payload.get(k) for k in ["records", "data", "items", "recommendations", "rows"] if isinstance(payload.get(k), list)),
            [],
        )
    else:
        rows = []
    out: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = {str(k): _json_safe_value(v) for k, v in raw.items()}
        rid = _record_id(row)
        if not rid.strip("|"):
            continue
        if not _safe_str(row.get("record_id")):
            row["record_id"] = rid
        out.append(row)
    return _repair_record_ids_rows_v178(out)


def normalize_records(payload: Any) -> list[dict[str, Any]]:
    """JSON-safe normalization that never drops rows because record_id collides."""
    if isinstance(payload, pd.DataFrame):
        rows = payload.loc[:, ~payload.columns.duplicated()].to_dict(orient="records")
    elif isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = next((payload.get(k) for k in ["records", "data", "items", "recommendations", "rows"] if isinstance(payload.get(k), list)), [])
    else:
        rows = []
    clean: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = {str(k): _json_safe_value(v) for k, v in raw.items()}
        # Keep business events exact. Missing/duplicate record IDs are repaired below.
        if not _record_id(row).strip("|"):
            continue
        clean.append(row)
    clean = _repair_record_ids_rows_v178(clean)
    return sorted(
        clean,
        key=lambda row: (
            _safe_str(row.get("推薦日期") or row.get("date")),
            _safe_str(row.get("推薦時間") or row.get("time")),
            _safe_str(row.get("股票代號") or row.get("code")),
            _safe_str(row.get("record_id")),
        ),
    )


def _read_records_firestore_full(*, snapshot_key: str = "") -> tuple[list[dict[str, Any]], str, datetime, dict[str, Any]]:
    return [], "H241 推薦紀錄 Firestore 已停用", datetime.min, {}


def read_records_firestore() -> tuple[list[dict[str, Any]], str, datetime]:
    rows, message, latest, _ = _read_records_firestore_full()
    return rows, message, latest


def write_records_firestore(records: list[dict[str, Any]], state: dict[str, Any]) -> tuple[bool, str]:
    return False, "H241 推薦紀錄 Firestore 已停用"




def _state_revision_v191_h8(state: Any) -> int:
    try:
        return int((state or {}).get("revision") or 0) if isinstance(state, dict) else 0
    except Exception:
        return 0


def _records_remote_state_relation_v191_h8(state: dict[str, Any]) -> tuple[str, str]:
    """Return same/newer/older/unknown using the tiny remote state file."""
    try:
        remote, msg = read_github_json(RECORDS_STATE_FILE, {})
    except Exception as exc:
        return "unknown", f"讀取遠端records state例外：{exc}"
    if not isinstance(remote, dict) or not remote:
        return "unknown", msg
    local_hash = _safe_str(state.get("payload_hash"))
    remote_hash = _safe_str(remote.get("payload_hash"))
    if local_hash and remote_hash == local_hash:
        return "same", f"遠端state已同Hash｜{remote_hash[:12]}"
    local_rev = _state_revision_v191_h8(state)
    remote_rev = _state_revision_v191_h8(remote)
    if not _records_state_is_destructive_mutation(state):
        try:
            if (int(state.get("count", -1)) < int(remote.get("count", -1)) or
                    _recommendation_date_text(state.get("latest_recommendation_date")) < _recommendation_date_text(remote.get("latest_recommendation_date"))):
                return "newer", "H240 遠端實際筆數或推薦日期較完整，拒絕舊快照回寫"
        except (TypeError, ValueError):
            return "unknown", "H240 權威摘要格式異常"
    if remote_rev and local_rev and remote_rev > local_rev:
        return "newer", f"遠端已有較新revision {remote_rev}>{local_rev}｜hash {remote_hash[:12]}"
    if remote_rev and local_rev and remote_rev < local_rev:
        return "older", f"遠端revision較舊 {remote_rev}<{local_rev}"
    return "unknown", f"遠端state hash不同但revision無法判定｜{msg}"


def _sync_records_github_snapshot_v191_h8(records: list[dict[str, Any]], state: dict[str, Any], reason: str) -> tuple[bool, str]:
    # Legacy background snapshots lack an exact verified parent: reject publication.
    rows, _, _ = ensure_records_local_authority_current()
    ok = records_authority_verification_h240().get("status") == "VERIFIED" and _json_hash(rows) == _json_hash(records)
    return ok, "H241 舊背景快照僅可驗證同Hash，不允許覆蓋現行GitHub權威"


def _records_github_sync_worker() -> None:
    """Coalescing H8 uploader for the large records authority.

    New snapshots replace pending old ones.  409/422 races are handled inside
    the CAS gateway; a stale snapshot can also safely yield to a newer remote
    revision.  The UI therefore no longer asks the user to manually press save
    for a transient version conflict.
    """
    global _RECORDS_GITHUB_PENDING, _RECORDS_GITHUB_RUNNING
    while True:
        with _RECORDS_GITHUB_LOCK:
            job = _RECORDS_GITHUB_PENDING
            _RECORDS_GITHUB_PENDING = None
            if job is None:
                _RECORDS_GITHUB_RUNNING = False
                return
        records, state, reason = job
        write_local_json_atomic(
            RECORDS_GITHUB_SYNC_STATUS_FILE,
            {
                "status": "running",
                "reason": reason,
                "count": len(records),
                "payload_hash": state.get("payload_hash"),
                "started_at": _now_text(),
                "auto_retry": True,
                "version": "V191-H8-CAS",
            },
        )
        ok, msg = _sync_records_github_snapshot_v191_h8(records, state, reason)
        retry_count = 0
        # One bounded worker-level retry complements the per-PUT CAS loop.  It
        # handles the case where another process completes an entire records+
        # state transaction while this worker was uploading.  Never infinite.
        if not ok and ("版本衝突 HTTP 409" in msg or "CAS衝突" in msg or "版本衝突 HTTP 422" in msg):
            retry_count = 1
            time.sleep(1.5)
            ok, retry_msg = _sync_records_github_snapshot_v191_h8(records, state, f"{reason} | H8 retry")
            msg = f"{msg}｜H8自動重試：{retry_msg}"
        # 401/403 is a permanent credential/access failure, not a transient
        # shard/CAS race. Keep it visible until the operator fixes Secrets.
        permission_denied = bool(not ok and any(mark in msg for mark in ("HTTP 401", "HTTP 403")))
        write_local_json_atomic(
            RECORDS_GITHUB_SYNC_STATUS_FILE,
            {
                "status": "success" if ok else ("blocked_permission" if permission_denied else "retry_pending"),
                "reason": reason,
                "count": len(records),
                "payload_hash": state.get("payload_hash"),
                "finished_at": _now_text(),
                "message": msg,
                "auto_retry": not ok and not permission_denied,
                "auto_retry_count": retry_count,
                "version": "V191-H8-CAS",
            },
        )


def schedule_records_github_sync(records: Any, state: dict[str, Any], reason: str = "record mutation") -> tuple[bool, str]:
    return False, "H241 已停止推薦紀錄背景整檔覆寫，請使用同步權威提交"




def _record_payload_hash(row: dict[str, Any]) -> str:
    """Stable per-record hash used by explicit sync to avoid 1,800+ full rewrites."""
    return hashlib.sha256(
        json.dumps(_json_safe_value(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _records_manifest(records: list[dict[str, Any]], payload_hash: str = "") -> dict[str, Any]:
    rows: dict[str, str] = {}
    for row in records:
        rec_id = _safe_str(row.get("record_id"))
        if not rec_id:
            continue
        rows[rec_id] = _record_payload_hash(row)
    return {
        "version": "godpick_records_manifest_v1",
        "count": len(records),
        "payload_hash": payload_hash or _json_hash(records),
        "row_hashes": rows,
        "updated_at": _now_text(),
    }


def _read_records_firestore_summary() -> tuple[dict[str, Any], str]:
    return {}, "H241 推薦紀錄 Firestore 已停用"

def load_records_github_sync_status() -> tuple[dict[str, Any], str]:
    payload, message, _ = read_local_json(RECORDS_GITHUB_SYNC_STATUS_FILE, {})
    return (payload if isinstance(payload, dict) else {}), message


def write_records_firestore_mutation(
    records: list[dict[str, Any]],
    state: dict[str, Any],
    *,
    deleted_ids: Iterable[str] | None = None,
    upsert_rows: Iterable[dict[str, Any]] | None = None,
    expected_previous_count: int | None = None,
) -> tuple[bool, str]:
    return False, "H241 推薦紀錄 Firestore 已停用"


def _h176_repair_firestore_gap_for_verified_delete(
    before: list[dict[str, Any]], *, expected_local_hash: str,
) -> tuple[bool, str, dict[str, Any]]:
    return False, "H241 推薦紀錄 Firestore 已停用，不執行差異修復", {}


def _h141_prevent_remote_rollback(
    records: list[dict[str, Any]], *, deleted_ids: Iterable[str] | None = None,
    previous_rows: list[dict[str, Any]] | None = None,
    previous_count: int | None = None,
) -> tuple[bool, str]:
    """H161: fail-closed compare-and-delete against the complete authority.

    The pre-delete snapshot, not the already-shortened result, must match the
    remote payload hash.  This permits deleting the last/newest date but NEVER
    assumes that 100 selected IDs justify silently omitting 74 unrelated rows.
    No GitHub/Firestore permission => no local destructive mutation either.
    """
    from godpick_h141_integrity_guard import record_snapshot_guard
    cfg = github_config()
    has_firestore = firebase_configured()
    if not cfg.get("token") and not has_firestore:
        return False, (
            "H141保護：未設定 GITHUB_TOKEN/Firestore，只有暫存本機歷史。"
            "已阻止正式紀錄修改及遠端回寫；請先設 Streamlit Secrets，恢復遠端權威。"
        )
    deleted = {_safe_str(x) for x in (deleted_ids or []) if _safe_str(x)}
    states: list[tuple[str, dict[str, Any]]] = []
    if cfg.get("token"):
        gh_state, gh_msg = read_github_json(RECORDS_STATE_FILE, {})
        if isinstance(gh_state, dict) and gh_state.get("payload_hash"):
            states.append(("GitHub runtime-data", gh_state))
        else:
            return False, (
                f"H240/H161 GitHub遠端權威無法驗證（{gh_msg}）；禁止刪除或以本機舊快照回寫。"
                "若HTTP 403，請確認 Fine-grained Token 已選 stock-app 並授予 Contents: Read and write。"
            )
    if has_firestore:
        fs_state, fs_msg = _read_records_firestore_summary()
        if isinstance(fs_state, dict) and fs_state.get("payload_hash"):
            states.append(("Firestore", fs_state))
        elif deleted:
            # H177: a configured-but-unreachable Firestore MUST NOT silently
            # disappear from the authority set during a destructive operation.
            # GitHub-only deletion would otherwise be revived by a later
            # Firestore restoration on the next Streamlit reboot.
            return False, (
                "H177刪除未執行：Firestore已設定但摘要無法核實（" + str(fs_msg)[:260] +
                "）；即使GitHub筆數相同，仍禁止只刪GitHub後由Firestore還原。"
                "請先修正Firestore連線/配額，再重新載入。"
            )
        elif not states:
            return False, f"H141保護：Firestore 無法驗證推薦摘要（{fs_msg}）；禁止寫入。"
    if deleted:
        # An explicit delete must start with the *complete exact* same identity
        # set as the persisted authority; shrinking row count is not consent to
        # remove unseen rows. A human-friendly record count alone is insufficient.
        if previous_rows is None:
            return False, "H161刪除驗證未取得刪除前完整權威，已停止；請重新載入。"
        before = records_as_rows_exact(previous_rows)
        before_ids = [_safe_str(x.get("record_id")) for x in before]
        after_ids = [_safe_str(x.get("record_id")) for x in records]
        if (not before or len(before_ids) != len(set(before_ids)) or
                len(after_ids) != len(set(after_ids)) or
                not deleted.issubset(set(before_ids)) or
                set(after_ids) != set(before_ids) - deleted):
            return False, "H161刪除身分對帳失敗：只允許精確 record_id 刪除，其他歷史筆數/ID 不可變動。"
        if previous_count is not None and int(previous_count) != len(before):
            return False, f"H161刪除前筆數已變動：原畫面 {previous_count}，權威 {len(before)}；請重新載入。"
        if not states:
            return False, "H161刪除前沒有可驗證的遠端權威（不允許只更新本機）。"
        before_hash = _json_hash(before)
        h176_repair_notes: list[str] = []
        # H176: a stale Firestore summary must not lock out deletions forever,
        # but it also must not be waived. First prove full GitHub H127 authority,
        # reconcile only absent Firestore document IDs, then re-run H161 guards.
        github_complete_state = any(
            source == "GitHub runtime-data"
            and _safe_str(state.get("payload_hash")) == before_hash
            and int(state.get("count") or -1) == len(before)
            for source, state in states
        )
        for index, (source, state) in enumerate(states):
            if source != "Firestore":
                continue
            if (int(state.get("count") or -1) == len(before)
                    and _safe_str(state.get("payload_hash")) == before_hash):
                continue
            if not github_complete_state:
                return False, (
                    "H176 Firestore資料不一致且GitHub未證明完整本機權威；"
                    "未自動刪除，請先恢復/確認遠端完整紀錄。"
                )
            repaired, why, repaired_state = _h176_repair_firestore_gap_for_verified_delete(
                before, expected_local_hash=before_hash,
            )
            if not repaired:
                return False, why
            h176_repair_notes.append(why)
            states[index] = (source, repaired_state)
        for source, state in states:
            good, why = record_snapshot_guard(before, state, destructive=False)
            if not good:
                return False, f"H161刪除前資料不完整：{source}｜{why}；請先重新載入完整權威。"
            remote_count = int(state.get("count") or 0)
            if remote_count != len(before):
                return False, (
                    f"H161刪除前資料不完整：畫面 {len(before)} 筆，{source} {remote_count} 筆，"
                    f"差 {remote_count-len(before):+d} 筆；先重新載入完整權威再重新勾選。"
                )
            remote_hash = _safe_str(state.get("payload_hash"))
            if remote_hash != before_hash:
                return False, (
                    f"H161刪除前版本不一致：{source} 的內容Hash與本機不同；"
                    "為避免覆蓋其他人的新資料，請重新載入完整權威。"
                )
        return True, (f"H161精確刪除預檢成功：原權威 {len(before)} 筆，僅刪 {len(deleted)} 個明確record_id"
                      + ("｜" + "｜".join(h176_repair_notes) if h176_repair_notes else ""))
    for source, state in states:
        good, why = record_snapshot_guard(records, state, destructive=False)
        if not good:
            return False, f"{source}｜{why}"
    # H240: compare the *base* as well as the submitted result. Appending a
    # current-date record to an obsolete checkout must not hide the missing history.
    base_payload, _, _ = read_local_json(RECORDS_FILE, [])
    base_rows = records_as_rows_exact(base_payload)
    base_hash = _json_hash(base_rows)
    base_state, _, _ = read_local_json(RECORDS_STATE_FILE, {})
    base_rev = _state_revision_v191_h8(base_state) if _state_is_valid(base_rows, base_state) else 0
    if not {_safe_str(r.get("record_id")) for r in base_rows}.issubset({_safe_str(r.get("record_id")) for r in records}):
        return False, "H240 非刪除操作不得遺漏既有record_id；請由明確刪除流程執行。"
    for source, state in states:
        good, why = record_snapshot_guard(base_rows, state, destructive=False)
        if not good:
            return False, f"H240 舊基底禁止回寫：{source}｜{why}"
        if (state.get("payload_hash") != base_hash and _state_revision_v191_h8(state) > base_rev):
            return False, f"H240 {source}有較新不同Hash權威；先重新載入完整歷史再修改。"
    return True, "已核對遠端權威，禁止較舊資料回寫"


def save_records_mutation_fast(
    payload: Any,
    *,
    expected_authority_signature: str = "",
    deleted_ids: Iterable[str] | None = None,
    upsert_rows: Iterable[dict[str, Any]] | None = None,
    previous_count: int | None = None,
    reason: str = "record mutation",
    require_remote_confirm: bool = False,
) -> PersistenceReport:
    from godpick_h241_github_records import mutate
    return mutate(payload, expected_authority_signature=expected_authority_signature, deleted_ids=deleted_ids, upsert_rows=upsert_rows, previous_count=previous_count, reason=reason, require_remote_confirm=require_remote_confirm)


def save_records_sync_fast(
    payload: Any,
    reason: str = "explicit record sync",
    expected_authority_signature: str = "",
    *,
    allow_destructive_replace: bool = False,
    require_remote_confirm: bool = False,
) -> PersistenceReport:
    from godpick_h241_github_records import sync
    return sync(payload, reason, expected_authority_signature, allow_destructive_replace=allow_destructive_replace, require_remote_confirm=require_remote_confirm)


def save_records_permanent(
    payload: Any,
    *,
    reason: str = "full recommendation authority sync",
    allow_destructive_replace: bool = False,
) -> PersistenceReport:
    return save_records_sync_fast(payload, reason=reason, allow_destructive_replace=allow_destructive_replace, require_remote_confirm=True)


def _merge_record_sources_v178(source_rows: Iterable[Iterable[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Merge replicated authority sources without tripling identical records.

    This helper is only for recovery when no state file verifies. Normal saves do
    not business-dedupe records. After V178 repair, record_id is unique per event
    and stable across identical source copies.
    """
    merged: dict[str, dict[str, Any]] = {}
    for rows in source_rows:
        for row in normalize_records(list(rows or [])):
            rid = _safe_str(row.get("record_id"))
            if not rid:
                continue
            old = merged.get(rid, {})
            combined = dict(old)
            for k, v in row.items():
                if v not in (None, "") or k not in combined:
                    combined[k] = v
            merged[rid] = combined
    return sorted(merged.values(), key=lambda row: (
        _safe_str(row.get("推薦日期")), _safe_str(row.get("推薦時間")),
        _safe_str(row.get("股票代號")), _safe_str(row.get("record_id")),
    ))


def restore_records_snapshot(payload: Any, *, source: str = "remote") -> tuple[bool, str]:
    # H241 no arbitrary historical or Firestore snapshots may replace GitHub authority.
    rows, details, _ = ensure_records_local_authority_current()
    ok = records_authority_verification_h240().get("status") == "VERIFIED" and _json_hash(records_as_rows_exact(payload)) == _json_hash(rows)
    return ok, "H241 只接受現行GitHub權威；" + "｜".join(details)


def recover_records_authority() -> tuple[list[dict[str, Any]], list[str], bool]:
    """V181 backward-compatible authority recovery entry point.

    Older Page 8 / persistence revisions call this name.  Route them to the
    current verified authority resolver so a rolling/mixed Streamlit deployment
    cannot fail only because one module was refreshed before another.
    """
    try:
        return ensure_records_local_authority_current()
    except Exception as exc:
        local_payload, local_msg, _ = read_local_json(RECORDS_FILE, [])
        rows = normalize_records(local_payload)
        return rows, [f"權威恢復相容層失敗：{exc}", f"本機 fallback：{local_msg}"], False


def load_records_permanent() -> tuple[list[dict[str, Any]], list[str]]:
    # V191-H3: all ordinary readers use the same monotonic authority resolver.
    # This prevents the legacy loader from independently accepting a newer 0/4
    # row replica and writing it back locally before the history-recovery guard
    # gets a chance to inspect the collapse signature.
    rows, details, _ = ensure_records_local_authority_current()
    return rows, details


def _default_firestore_doc(path_name: str, explicit: str | None = None) -> str | None:
    if explicit:
        return explicit
    return NAMED_FIRESTORE_DOCS.get(Path(path_name).name)


def _read_named_firestore(doc_name: str, default: Any) -> tuple[Any, str, datetime, dict[str, Any]]:
    if not doc_name or not firebase_configured():
        return copy.deepcopy(default), "未使用 Firestore", datetime.min, {}
    try:
        _init_firebase_app()
        db = firestore.client()
        doc = db.collection("system").document(doc_name).get()
        if not doc.exists:
            return copy.deepcopy(default), f"Firestore 尚未建立 system/{doc_name}", datetime.min, {}
        data = doc.to_dict() or {}
        state = {
            "payload_hash": data.get("payload_hash"),
            "updated_at": data.get("updated_at_text"),
            "updated_at_utc": data.get("updated_at_utc"),
            "updated_at_epoch": data.get("updated_at_epoch"),
            "revision": data.get("revision"),
        }
        if data.get("storage_mode") == "chunks":
            chunks = []
            for cdoc in db.collection("durable_json").document(doc_name).collection("chunks").stream():
                cdata = cdoc.to_dict() or {}
                chunks.append((int(cdata.get("index") or 0), _safe_str(cdata.get("data"))))
            encoded = "".join(part for _, part in sorted(chunks))
            payload = json.loads(base64.b64decode(encoded.encode("ascii")).decode("utf-8-sig")) if encoded else copy.deepcopy(default)
        else:
            payload = data.get("payload", copy.deepcopy(default))
        if _safe_str(state.get("payload_hash")) and not _state_is_valid(payload, state):
            return copy.deepcopy(default), f"Firestore system/{doc_name} 雜湊驗證失敗", datetime.min, {}
        return payload, f"已讀取 Firestore system/{doc_name}", _firestore_time(data.get("updated_at")), state
    except Exception as exc:
        _H163_FS_GUARD.note_error(exc, "named_json_read")
        return copy.deepcopy(default), f"Firestore 讀取失敗：{exc}", datetime.min, {}


def _write_named_firestore(doc_name: str, payload: Any, state: dict[str, Any]) -> tuple[bool, str]:
    if not doc_name or not firebase_configured():
        return False, "未設定 Firestore 文件或 Firebase"
    try:
        _init_firebase_app()
        db = firestore.client()
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
        manifest = {
            "payload_hash": state["payload_hash"],
            "revision": state.get("revision"),
            "updated_at_epoch": state.get("updated_at_epoch"),
            "updated_at_utc": state.get("updated_at_utc"),
            "updated_at": firestore.SERVER_TIMESTAMP,
            "updated_at_text": state["updated_at"],
            "source": "godpick_persistence_service",
        }
        chunks_ref = db.collection("durable_json").document(doc_name).collection("chunks")
        existing = {doc.id for doc in chunks_ref.stream()}
        operations: list[tuple[str, Any, Any]] = []
        if len(raw) <= 650_000:
            manifest.update({"storage_mode": "inline", "payload": payload, "chunk_count": 0})
            for doc_id in existing:
                operations.append(("delete", chunks_ref.document(doc_id), None))
        else:
            encoded = base64.b64encode(raw).decode("ascii")
            parts = [encoded[i:i + 450_000] for i in range(0, len(encoded), 450_000)]
            manifest.update({"storage_mode": "chunks", "chunk_count": len(parts)})
            for idx, part in enumerate(parts):
                doc_id = f"{idx:05d}"
                operations.append(("set", chunks_ref.document(doc_id), {"index": idx, "data": part, "payload_hash": state["payload_hash"]}))
            for doc_id in existing - {f"{i:05d}" for i in range(len(parts))}:
                operations.append(("delete", chunks_ref.document(doc_id), None))
        operations.append(("set", db.collection("system").document(doc_name), manifest))
        _commit_operations(db, operations)
        verify, _, _, verify_state = _read_named_firestore(doc_name, None)
        if verify is None or not _state_is_valid(verify, verify_state):
            return False, f"Firestore system/{doc_name} 回讀驗證不一致"
        return True, f"Firestore system/{doc_name} 寫入並回讀驗證"
    except Exception as exc:
        _H163_FS_GUARD.note_error(exc, "named_json_write")
        return False, f"Firestore 系統設定寫入失敗：{exc}"


def _state_file_for(path_name: str) -> str:
    path = Path(path_name)
    safe_stem = path.stem.replace(" ", "_")
    return f"{safe_stem}_sync_state.json"



def _named_json_authority_key(
    path_name: str,
    source: str,
    payload: Any,
    state: Any,
    fallback: datetime | None = None,
) -> tuple[Any, ...]:
    """Choose named JSON authority by business time for recommendation snapshots.

    Repository files can be redeployed with an old recommendation payload.  Their
    filesystem/state time may then look new even though the business snapshot is
    weeks old.  For latest recommendation artifacts the saved/recommendation date
    must therefore outrank technical file timestamps.
    """
    priority = {"local": 0, "github": 1, "firestore": 2}.get(source, 0)
    base = Path(path_name).name
    state_dict = state if isinstance(state, dict) else {}
    activity = _state_epoch(state_dict, fallback)
    try:
        revision = int(state_dict.get("revision") or 0)
    except Exception:
        revision = 0
    revision_epoch = (revision / 1_000_000_000.0) if revision > 10_000_000_000 else float(revision or 0)
    activity = max(activity, revision_epoch)

    if base in {"godpick_latest_recommendations.json", "godpick_latest_run_anchor.json"}:
        data = payload if isinstance(payload, dict) else {}
        business_date = _recommendation_date_text(
            data.get("recommendation_date") or data.get("saved_at") or data.get("kline_date")
        )
        saved_at = _safe_str(data.get("saved_at")).replace("T", " ")[:26]
        # Business date/time is authoritative. State/revision only breaks ties.
        return (business_date, saved_at, activity, priority)

    if base == "official_factors_cache.json":
        data = payload if isinstance(payload, dict) else {}
        business_date = _official_factor_business_date(data)
        updated_at = _safe_str(data.get("updated_at")).replace("T", " ")[:26]
        # V186 monotonic authority: data_date first, never deployment mtime first.
        return (business_date, updated_at, activity, priority)

    return (activity, revision, priority)


def _official_factor_business_date(payload: Any) -> str:
    """Return the newest real business-data date inside the factor cache.

    File/redeploy time is never allowed to outrank this date.  This is the key
    V186 reboot guard: a packaged 2026-07 cache cannot beat an 08-11 runtime
    cache just because the old file was recreated during app startup.
    """
    data = payload if isinstance(payload, dict) else {}
    direct = _recommendation_date_text(data.get("data_date"))
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    meta_date = _recommendation_date_text(meta.get("data_date"))
    candidates = [x for x in (direct, meta_date) if x]
    rows = data.get("records") if isinstance(data.get("records"), list) else []
    fields = (
        "官方因子資料日期", "官方資料日期", "三大法人資料日期",
        "法人資料日期", "估值資料日期", "FinMind資料日期",
    )
    # Sampling is not sufficient here: the cache may contain mixed dates.
    for row in rows:
        if not isinstance(row, dict):
            continue
        for field in fields:
            value = _recommendation_date_text(row.get(field))
            if value:
                candidates.append(value)
                break
    return max(candidates) if candidates else ""


def _named_json_nonempty(value: Any) -> bool:
    return value not in (None, {}, [])


def save_named_json_permanent(
    path_name: str,
    payload: Any,
    *,
    github_path: str | None = None,
    firestore_doc: str | None = None,
    require_remote: bool = False,
) -> PersistenceReport:
    if Path(path_name).name in {RECORDS_FILE, RECORDS_STATE_FILE} or (github_path and Path(github_path).name in {RECORDS_FILE, RECORDS_STATE_FILE}) or firestore_doc in {"godpick_records", "godpick_records_summary"}:
        return PersistenceReport(local_message="H241 推薦紀錄必須使用專用權威提交", github_message="未提交", firestore_message="H241 推薦紀錄 Firestore 已停用")
    state = _new_state("named_json_durable_v2", payload, path=path_name)
    state_file = _state_file_for(path_name)
    report = PersistenceReport(payload_hash=state["payload_hash"], updated_at=state["updated_at"])
    ok1, msg1 = write_local_json_atomic(path_name, payload)
    ok2, msg2 = write_local_json_atomic(state_file, state)
    report.local_ok = bool(ok1 and ok2)
    report.local_message = f"{msg1}｜{msg2}"

    remote_path = github_path or path_name
    gh1, ghm1 = write_github_json(remote_path, payload, f"persist {path_name}")
    gh2, ghm2 = write_github_json(state_file, state, f"persist {path_name} state") if gh1 else (False, "資料未寫入，略過狀態")
    report.github_ok = bool(gh1 and gh2)
    report.github_message = f"{ghm1}｜{ghm2}"

    doc_name = _default_firestore_doc(path_name, firestore_doc)
    fs_ok, fs_msg = _write_named_firestore(doc_name, payload, state) if doc_name else (False, "此檔案未設定 Firestore 文件")
    report.firestore_ok = fs_ok
    report.firestore_message = fs_msg

    remote_ok = bool(report.github_ok or report.firestore_ok)
    if require_remote:
        # H98: APP/Streamlit reboot safety requires an authority outside the
        # ephemeral application filesystem. Local-only success must never be
        # reported as permanent for critical business-date artifacts.
        report.permanent_ok = bool(report.local_ok and remote_ok)
        if report.local_ok and not remote_ok:
            suffix = "H98嚴格永久化：本機已保存，但GitHub runtime-data/Firestore皆未回讀確認；不可視為Reboot-safe。"
            report.local_message = f"{report.local_message}｜{suffix}"
    elif _configured_remote_exists():
        report.permanent_ok = bool(report.local_ok and remote_ok)
    else:
        report.permanent_ok = False  # H141: ephemeral local file is NEVER Reboot-safe.
    return report


def load_named_json_permanent(
    path_name: str,
    default: Any,
    *,
    github_path: str | None = None,
    firestore_doc: str | None = None,
) -> tuple[Any, list[str]]:
    if Path(path_name).name == RECORDS_FILE:
        return load_records_permanent()
    if Path(path_name).name == RECORDS_STATE_FILE or (github_path and Path(github_path).name in {RECORDS_FILE, RECORDS_STATE_FILE}) or firestore_doc in {"godpick_records", "godpick_records_summary"}:
        return copy.deepcopy(default), ["H241 請使用推薦紀錄專用權威入口"]
    state_file = _state_file_for(path_name)
    local_payload, local_msg, local_mtime = read_local_json(path_name, default)
    local_state, local_state_msg, _ = read_local_json(state_file, {})

    remote_path = github_path or path_name
    gh_payload, gh_msg = read_github_json(remote_path, default)
    gh_state, gh_state_msg = read_github_json(state_file, {})

    doc_name = _default_firestore_doc(path_name, firestore_doc)
    fs_payload, fs_msg, fs_mtime, fs_state = _read_named_firestore(doc_name, default) if doc_name else (copy.deepcopy(default), "此檔案未設定 Firestore 文件", datetime.min, {})

    candidates = [
        ("local", local_payload, local_state, local_mtime),
        ("github", gh_payload, gh_state, datetime.min),
        ("firestore", fs_payload, fs_state, fs_mtime),
    ]
    semantic_business_files = {
        "godpick_latest_recommendations.json",
        "godpick_latest_run_anchor.json",
        "official_factors_cache.json",
    }
    base_name = Path(path_name).name

    if base_name in semantic_business_files:
        # V186: business-date artifacts must participate in authority election
        # even when a legacy/new local payload does not yet have a sync-state
        # sidecar.  Otherwise one old remote payload with a valid state can
        # incorrectly suppress a newer local payload after reboot/rerun.
        semantic_candidates = [x for x in candidates if _named_json_nonempty(x[1])]
        if semantic_candidates:
            source, chosen, chosen_state, chosen_fallback = max(
                semantic_candidates,
                key=lambda item: _named_json_authority_key(path_name, item[0], item[1], item[2], item[3]),
            )
            if not _state_is_valid(chosen, chosen_state):
                chosen_state = _new_state(
                    "named_json_durable_v186", chosen, path=path_name, migrated_from=source
                )
        else:
            source, chosen = "default", copy.deepcopy(default)
            chosen_state = _new_state("named_json_durable_v186", chosen, path=path_name, migrated_from=source)
    else:
        valid = []
        for source, data, state, fallback in candidates:
            if _state_is_valid(data, state):
                valid.append((source, data, state, _state_epoch(state, fallback)))
        if valid:
            source, chosen, chosen_state, chosen_fallback = max(
                valid,
                key=lambda item: _named_json_authority_key(path_name, item[0], item[1], item[2], item[3]),
            )
        else:
            legacy_candidates = [
                ("local", local_payload, {}, local_mtime),
                ("github", gh_payload, {}, datetime.min),
                ("firestore", fs_payload, {}, fs_mtime),
            ]
            legacy_candidates = [x for x in legacy_candidates if _named_json_nonempty(x[1])]
            if legacy_candidates:
                source, chosen, _, _ = max(
                    legacy_candidates,
                    key=lambda item: _named_json_authority_key(path_name, item[0], item[1], item[2], item[3]),
                )
            else:
                source, chosen = "default", copy.deepcopy(default)
            chosen_state = _new_state("named_json_durable_v186", chosen, path=path_name, migrated_from=source)

    restored_state = dict(chosen_state or {})
    restored_state.update({
        "version": restored_state.get("version") or "named_json_durable_v2",
        "path": path_name,
        "payload_hash": _json_hash(chosen),
        "restored_from": source,
        "restored_at": _now_text(),
    })
    restored_state.setdefault("updated_at", _now_text())
    restored_state.setdefault("updated_at_utc", _now_utc_text())
    restored_state.setdefault("updated_at_epoch", _now_epoch())
    restored_state.setdefault("revision", time.time_ns())
    write_local_json_atomic(path_name, chosen)
    write_local_json_atomic(state_file, restored_state)
    return chosen, [
        f"權威來源：{source}",
        f"本機：{local_msg}｜{local_state_msg}",
        f"GitHub：{gh_msg}｜{gh_state_msg}",
        f"Firestore：{fs_msg}",
    ]


def default_export_sync_settings() -> dict[str, Any]:
    return {
        "version": "godpick_export_sync_settings_v1",
        "export_folder": "exports/godpick",
        "target_group": "股神推薦",
        "auto_export_excel": True,
        "sync_latest_only": True,
        "updated_at": "",
    }


def load_export_sync_settings() -> tuple[dict[str, Any], list[str]]:
    payload, details = load_named_json_permanent(
        EXPORT_SETTINGS_FILE,
        default_export_sync_settings(),
        firestore_doc="godpick_export_sync_settings",
    )
    base = default_export_sync_settings()
    if isinstance(payload, dict):
        base.update(payload)
    base["export_folder"] = _safe_str(base.get("export_folder")) or default_export_sync_settings()["export_folder"]
    base["target_group"] = _safe_str(base.get("target_group")) or "股神推薦"
    base["auto_export_excel"] = bool(base.get("auto_export_excel", True))
    base["sync_latest_only"] = bool(base.get("sync_latest_only", True))
    return base, details


def save_export_sync_settings(settings: dict[str, Any]) -> PersistenceReport:
    payload = default_export_sync_settings()
    payload.update(settings if isinstance(settings, dict) else {})
    payload["updated_at"] = _now_text()
    return save_named_json_permanent(
        EXPORT_SETTINGS_FILE,
        payload,
        firestore_doc="godpick_export_sync_settings",
    )


def resolve_export_folder(folder_text: str) -> Path:
    text = _safe_str(folder_text) or default_export_sync_settings()["export_folder"]
    folder = Path(text).expanduser()
    return folder if folder.is_absolute() else BASE_DIR / folder


def write_export_file(folder_text: str, filename: str, data: bytes) -> tuple[bool, str, str]:
    folder = resolve_export_folder(folder_text)
    try:
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / Path(filename).name
        fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(folder))
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, target)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
        if not target.exists() or target.stat().st_size != len(data):
            return False, f"Excel 匯出回讀驗證失敗：{target.as_posix()}", target.as_posix()
        return True, f"Excel 已永久匯出：{target.as_posix()}", target.as_posix()
    except Exception as exc:
        return False, f"Excel 匯出失敗：{folder.as_posix()}｜{exc}", ""


def load_export_history() -> tuple[list[dict[str, Any]], list[str]]:
    history, details = load_named_json_permanent(
        EXPORT_HISTORY_FILE,
        [],
        firestore_doc="godpick_export_history",
    )
    return (history if isinstance(history, list) else []), details


def append_export_history(item: dict[str, Any], limit: int = 500) -> PersistenceReport:
    history, _ = load_export_history()
    entry = dict(item)
    entry.setdefault("created_at", _now_text())
    entry.setdefault("created_at_utc", _now_utc_text())
    entry.setdefault("event_id", hashlib.sha1(
        f"{entry.get('created_at_utc')}|{entry.get('file_name')}|{entry.get('source')}|{entry.get('path')}".encode("utf-8")
    ).hexdigest()[:24])
    rows = [row for row in history if _safe_str((row or {}).get("event_id")) != entry["event_id"]]
    rows.insert(0, entry)
    rows = rows[: max(1, int(limit))]
    return save_named_json_permanent(
        EXPORT_HISTORY_FILE,
        rows,
        firestore_doc="godpick_export_history",
    )


def save_module_sync_state(state: dict[str, Any]) -> PersistenceReport:
    payload = dict(state if isinstance(state, dict) else {})
    payload.setdefault("updated_at", _now_text())
    payload.setdefault("version", "godpick_module_sync_state_v1")
    return save_named_json_permanent(
        MODULE_SYNC_STATE_FILE,
        payload,
        firestore_doc="godpick_module_sync_state",
    )


def load_module_sync_state() -> tuple[dict[str, Any], list[str]]:
    payload, details = load_named_json_permanent(
        MODULE_SYNC_STATE_FILE,
        {},
        firestore_doc="godpick_module_sync_state",
    )
    return payload if isinstance(payload, dict) else {}, details


def report_is_reboot_durable(report: PersistenceReport) -> bool:
    return bool(report.permanent_ok)


def h182_readonly_recommendation_replica_audit(*, selected_ids: Iterable[str] | None = None) -> tuple[dict[str, Any], str]:
    return {"valid": False, "reason": "H241 推薦紀錄 Firestore 已停用"}, "不讀取、不對帳、不寫入 Firestore"
