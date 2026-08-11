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
_RECORDS_GITHUB_PENDING: tuple[list[dict[str, Any]], dict[str, Any], str] | None = None
_RECORDS_GITHUB_RUNNING = False
_RECORDS_LOCAL_LOCK = threading.RLock()

NAMED_FIRESTORE_DOCS = {
    "godpick_recommend_list.json": "godpick_recommend_list",
    "godpick_latest_recommendations.json": "godpick_latest_recommendations",
    "godpick_latest_run_anchor.json": "godpick_latest_run_anchor",
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


def ensure_records_local_authority_current() -> tuple[list[dict[str, Any]], list[str], bool]:
    """Restore the local canonical JSON from the freshest verified authority.

    V175 fixes Streamlit Cloud reboot rollback.  Repository files recreated on
    boot may have a new mtime even though their business data stops at an old
    date.  We therefore compare latest recommendation date first, then count,
    revision and epoch.  Legacy Firestore summaries without payload_hash are
    still eligible; the full collection is downloaded and hashed before use.
    """
    with _RECORDS_LOCAL_LOCK:
        local_payload, local_msg, local_mtime = read_local_json(RECORDS_FILE, [])
        local_rows = records_as_rows_exact(local_payload)
        local_state, local_state_msg, _ = read_local_json(RECORDS_STATE_FILE, {})
        local_hash = _json_hash(local_rows)
        local_state = dict(local_state or {}) if isinstance(local_state, dict) else {}
        local_valid = bool(_safe_str(local_state.get("payload_hash")) == local_hash)
        local_state.setdefault("count", len(local_rows))
        local_state.setdefault("latest_recommendation_date", _latest_record_recommendation_date(local_rows))
        if not local_valid:
            # Use file mtime only as a tie-breaker.  Never stamp a stale rebooted
            # repository copy with "now" before checking remote authorities.
            local_state["payload_hash"] = local_hash

        details = [
            f"本機：{local_msg}｜{local_state_msg}｜{len(local_rows)}筆｜"
            f"最新{local_state.get('latest_recommendation_date') or '未取得'}"
        ]
        candidates: list[tuple[str, dict[str, Any], datetime | None, Any]] = [
            ("local", local_state, local_mtime if local_valid else datetime.min, local_rows)
        ]

        fs_summary: dict[str, Any] = {}
        if firebase_configured():
            fs_summary, fs_msg = _read_records_firestore_summary()
            details.append(f"Firestore摘要：{fs_msg}")
            if isinstance(fs_summary, dict) and (
                _safe_str(fs_summary.get("payload_hash"))
                or _recommendation_date_text(fs_summary.get("latest_recommendation_date"))
                or int(fs_summary.get("count") or 0) > 0
                or _state_epoch(fs_summary) > 0
            ):
                candidates.append(("firestore", dict(fs_summary), None, None))

        gh_state: dict[str, Any] = {}
        if github_config().get("token"):
            raw_state, gh_state_msg = read_github_json(RECORDS_STATE_FILE, {})
            details.append(f"GitHub狀態：{gh_state_msg}")
            if isinstance(raw_state, dict):
                gh_state = dict(raw_state)
                if (
                    _safe_str(gh_state.get("payload_hash"))
                    or _recommendation_date_text(gh_state.get("latest_recommendation_date"))
                    or int(gh_state.get("count") or 0) > 0
                    or _state_epoch(gh_state) > 0
                ):
                    candidates.append(("github", gh_state, None, None))

        source, newest_state, newest_fallback, newest_rows_hint = max(
            candidates,
            key=lambda item: _authority_freshness_key(
                item[0], item[1], fallback=item[2], rows=item[3]
            ),
        )
        local_key = _authority_freshness_key("local", local_state, fallback=(local_mtime if local_valid else datetime.min), rows=local_rows)
        newest_key = _authority_freshness_key(source, newest_state, fallback=newest_fallback, rows=newest_rows_hint)
        details.append(f"新鮮度比較：local={local_key[:4]}｜{source}={newest_key[:4]}")

        if source == "local":
            if not local_valid and local_rows:
                repaired_state = _new_state(
                    "godpick_records_durable_v7_local_repair",
                    local_rows,
                    count=len(local_rows),
                    latest_recommendation_date=_latest_record_recommendation_date(local_rows),
                )
                write_local_json_atomic(RECORDS_STATE_FILE, repaired_state)
                write_local_json_atomic(RECORDS_MANIFEST_FILE, _records_manifest(local_rows, repaired_state["payload_hash"]))
                details.append("本機為最新來源；已補建權威state，但未改變資料日期。")
            return local_rows, details + [f"權威來源：local｜{len(local_rows)}筆"], False

        remote_rows: list[dict[str, Any]] = []
        remote_state: dict[str, Any] = dict(newest_state or {})
        if source == "firestore":
            fs_rows, fs_full_msg, _, fs_full_state = _read_records_firestore_full()
            details.append(f"Firestore完整資料：{fs_full_msg}")
            remote_rows = records_as_rows_exact(fs_rows)
            if isinstance(fs_full_state, dict) and fs_full_state:
                remote_state.update(fs_full_state)
        elif source == "github":
            gh_path = _secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE
            gh_payload, gh_msg = read_github_json(gh_path, [])
            details.append(f"GitHub完整資料：{gh_msg}")
            remote_rows = records_as_rows_exact(gh_payload)

        if not remote_rows:
            details.append(f"遠端 {source} 摘要較新，但完整資料為空；保留本機，不做回退。")
            return local_rows, details, False

        remote_hash = _json_hash(remote_rows)
        summary_hash = _safe_str(remote_state.get("payload_hash"))
        if summary_hash and summary_hash != remote_hash:
            # Legacy/partial summaries can lag behind the collection.  A full
            # reload already proves later rows exist, so compare semantic dates
            # before rejecting the data outright.
            remote_latest = _latest_record_recommendation_date(remote_rows)
            local_latest = _latest_record_recommendation_date(local_rows)
            if remote_latest <= local_latest:
                details.append(f"遠端 {source} hash不一致且資料日期未較新；保留本機。")
                return local_rows, details, False
            details.append(f"遠端 {source} 摘要hash落後，但完整資料最新至{remote_latest}；採完整資料修復摘要。")

        remote_state.update({
            "version": "godpick_records_durable_v7_reboot_restore",
            "payload_hash": remote_hash,
            "count": len(remote_rows),
            "latest_recommendation_date": _latest_record_recommendation_date(remote_rows),
            "restored_from": source,
            "restored_at": _now_text(),
            "updated_at": _safe_str(remote_state.get("updated_at_text") or remote_state.get("updated_at")) or _now_text(),
            "updated_at_utc": _safe_str(remote_state.get("updated_at_utc")) or _now_utc_text(),
            "updated_at_epoch": _state_epoch(remote_state) or _now_epoch(),
            "revision": int(remote_state.get("revision") or time.time_ns()),
        })
        ok1, msg1 = write_local_json_atomic(RECORDS_FILE, remote_rows)
        ok2, msg2 = write_local_json_atomic(RECORDS_STATE_FILE, remote_state)
        if ok1 and ok2:
            write_local_json_atomic(RECORDS_MANIFEST_FILE, _records_manifest(remote_rows, remote_hash))
            details.extend([
                f"已在Reboot後自動還原較新權威來源：{source}｜{len(remote_rows)}筆｜"
                f"最新{remote_state.get('latest_recommendation_date') or '未取得'}",
                msg1, msg2,
            ])
            return remote_rows, details, True
        details.append(f"遠端較新但本機還原失敗：{msg1}｜{msg2}")
        return local_rows, details, False


def upsert_records_authority_fast(
    upsert_rows: Iterable[dict[str, Any]],
    *,
    reason: str = "record authority upsert",
) -> tuple[PersistenceReport, dict[str, int]]:
    """Atomically upsert recommendation rows into the current authority file.

    This never starts from a page's stale full DataFrame.  It re-reads the latest
    canonical JSON under a process lock, merges by business key
    (code+recommendation date+mode), preserves the existing record_id, then uses
    the durable mutation path.  It prevents page 7/page 8 concurrent reruns from
    rolling the authority file back to an older date.
    """
    # Rebooted Streamlit Cloud may start from the old repository JSON.  Always
    # restore the freshest remote authority before merging a new page-7 record,
    # otherwise the new write can rebuild a summary from an incomplete local set.
    if _configured_remote_exists():
        try:
            ensure_records_local_authority_current()
        except Exception:
            pass
    incoming = records_as_rows_exact(list(upsert_rows or []))
    empty_report = PersistenceReport()
    if not incoming:
        empty_report.local_message = "沒有可寫入的推薦紀錄"
        return empty_report, {"before": 0, "after": 0, "added": 0, "updated": 0, "changed": 0}

    with _RECORDS_LOCAL_LOCK:
        current_payload, _, _ = read_local_json(RECORDS_FILE, [])
        current = records_as_rows_exact(current_payload)
        index_by_key: dict[str, int] = {}
        for idx, row in enumerate(current):
            key = _record_business_key_authority(row)
            if key.strip("|"):
                index_by_key[key] = idx

        added = 0
        updated = 0
        changed_rows: list[dict[str, Any]] = []
        for raw in incoming:
            row = {str(k): _json_safe_value(v) for k, v in raw.items()}
            key = _record_business_key_authority(row)
            if not key.strip("|"):
                continue
            if key in index_by_key:
                idx = index_by_key[key]
                old = dict(current[idx])
                merged = dict(old)
                for k, v in row.items():
                    if v not in (None, "") or k not in merged:
                        merged[k] = v
                if _safe_str(old.get("record_id")):
                    merged["record_id"] = old["record_id"]
                if _json_hash(old) != _json_hash(merged):
                    current[idx] = merged
                    changed_rows.append(merged)
                    updated += 1
            else:
                if not _safe_str(row.get("record_id")):
                    row["record_id"] = hashlib.sha1(key.encode("utf-8")).hexdigest()[:24]
                current.append(row)
                index_by_key[key] = len(current) - 1
                changed_rows.append(row)
                added += 1

        if not changed_rows:
            state = _new_state("godpick_records_durable_v7_nochange", current, count=len(current), latest_recommendation_date=_latest_record_recommendation_date(current), mutation_reason=_safe_str(reason))
            fs_verified = False
            fs_message = "Firebase 未設定"
            if firebase_configured():
                fs_summary, fs_message = _read_records_firestore_summary()
                try:
                    fs_verified = bool(
                        _safe_str(fs_summary.get("payload_hash")) == state["payload_hash"]
                        and int(fs_summary.get("count") or -1) == len(current)
                    )
                except Exception:
                    fs_verified = False
            gh_status, _, _ = read_local_json(RECORDS_GITHUB_SYNC_STATUS_FILE, {})
            gh_verified = bool(
                isinstance(gh_status, dict)
                and _safe_str(gh_status.get("status")) == "success"
                and _safe_str(gh_status.get("payload_hash")) == state["payload_hash"]
            )
            remote_ok = bool(fs_verified or gh_verified) if _configured_remote_exists() else True
            report = PersistenceReport(
                local_ok=True,
                firestore_ok=fs_verified,
                github_ok=gh_verified,
                permanent_ok=remote_ok,
                local_message=f"權威檔內容未變更；目前 {len(current)} 筆",
                firestore_message=("Firestore已驗證一致" if fs_verified else fs_message),
                github_message=("GitHub已驗證一致" if gh_verified else "GitHub尚未完成同hash回讀驗證"),
                payload_hash=state["payload_hash"],
                updated_at=state["updated_at"],
            )
            return report, {"before": len(current), "after": len(current), "added": 0, "updated": 0, "changed": 0}

        report = save_records_mutation_fast(
            current,
            deleted_ids=[],
            upsert_rows=changed_rows,
            previous_count=len(current) - added,
            reason=reason,
        )
        return report, {
            "before": len(current) - added,
            "after": len(current),
            "added": added,
            "updated": updated,
            "changed": added + updated,
        }

def _secret(name: str, default: str = "") -> str:
    # V186: runtime_branch_bootstrap writes the runtime-data branch to the
    # environment before page modules load.  Prefer that process-level guard,
    # then Streamlit Secrets.  This keeps durable business data off the code
    # branch even if a page imports this service outside the home-page wrapper.
    env_value = _safe_str(os.getenv(name, ""))
    if env_value:
        return env_value
    try:
        return _safe_str(st.secrets.get(name, default))
    except Exception:
        return _safe_str(default)


def github_config() -> dict[str, str]:
    runtime_branch = (
        _secret("GITHUB_RUNTIME_DATA_BRANCH", "")
        or _secret("GITHUB_REPO_BRANCH", "runtime-data")
        or "runtime-data"
    )
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


def write_github_json(path_name: str, payload: Any, message: str = "update durable data") -> tuple[bool, str]:
    cfg = github_config()
    if not cfg["token"]:
        return False, "未設定 GITHUB_TOKEN"

    safe_payload = _json_safe_value(payload)
    # Compact JSON substantially reduces the 20+ MB recommendation-record file
    # and therefore upload time, without changing its data structure.
    raw = json.dumps(safe_payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii")
    size_mb = len(raw) / (1024 * 1024)
    upload_timeout = max(90, min(300, int(75 + size_mb * 8)))
    attempts = 2 if size_mb >= 5 else 3

    last_error = ""
    for attempt in range(attempts):
        try:
            current = requests.get(
                _github_url(path_name),
                headers=_github_headers(cfg["token"]),
                params={"ref": cfg["branch"], "_": int(time.time() * 1000)},
                timeout=(15, 60),
            )
            sha = ""
            if current.status_code == 200:
                meta = current.json() if current.content else {}
                sha = _safe_str(meta.get("sha")) if isinstance(meta, dict) else ""
            elif current.status_code != 404:
                last_error = f"取得 SHA 失敗 HTTP {current.status_code}｜{current.text[:250]}"
                time.sleep(0.6 * (attempt + 1))
                continue

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
                verify, verify_msg = read_github_json(path_name, None)
                if verify is not None and _json_hash(verify) == _json_hash(safe_payload):
                    return True, f"GitHub 寫入並以 raw 模式回讀驗證：{path_name}｜{size_mb:.1f} MB"
                last_error = f"GitHub 寫入成功但回讀不一致：{verify_msg}"
            elif response.status_code in (409, 422):
                last_error = f"GitHub 版本衝突 HTTP {response.status_code}｜{response.text[:220]}"
            else:
                last_error = f"GitHub 寫入失敗 HTTP {response.status_code}｜{response.text[:350]}"
        except Exception as exc:
            last_error = f"GitHub 寫入例外：{exc}"
        time.sleep(0.8 * (attempt + 1))
    return False, f"{path_name}｜{last_error or '未知錯誤'}｜資料大小 {size_mb:.1f} MB"


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
            f"Firestore：{'成功' if self.firestore_ok else '失敗'}｜{self.firestore_message}",
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
        report.permanent_ok = report.local_ok
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


def _read_records_firestore_full() -> tuple[list[dict[str, Any]], str, datetime, dict[str, Any]]:
    if not firebase_configured():
        return [], "Firebase 未設定", datetime.min, {}
    try:
        _init_firebase_app()
        db = firestore.client()
        rows = []
        latest = datetime.min
        for doc in db.collection("godpick_records").stream():
            data = doc.to_dict() or {}
            data.setdefault("record_id", doc.id)
            latest = max(latest, _firestore_time(data.get("updated_at")))
            rows.append(data)
        summary = db.collection("system").document("godpick_records_summary").get()
        state: dict[str, Any] = {}
        if summary.exists:
            summary_data = summary.to_dict() or {}
            latest = max(latest, _firestore_time(summary_data.get("updated_at")))
            state = {
                "payload_hash": summary_data.get("payload_hash"),
                "count": summary_data.get("count"),
                "latest_recommendation_date": summary_data.get("latest_recommendation_date"),
                "updated_at": summary_data.get("updated_at_text") or summary_data.get("updated_at"),
                "updated_at_utc": summary_data.get("updated_at_utc"),
                "updated_at_epoch": summary_data.get("updated_at_epoch"),
                "revision": summary_data.get("revision"),
            }
        normalized_rows = normalize_records(rows)
        if state.get("payload_hash"):
            state["summary_payload_hash"] = state.get("payload_hash")
        # The fully streamed collection is authoritative for its own hash/count/date;
        # a legacy or partially updated summary must not invalidate a successful reload.
        state["count"] = len(normalized_rows)
        state["latest_recommendation_date"] = _latest_record_recommendation_date(normalized_rows)
        state["payload_hash"] = _json_hash(normalized_rows)
        return normalized_rows, "已讀取 Firestore godpick_records", latest, state
    except Exception as exc:
        return [], f"Firestore records 讀取失敗：{exc}", datetime.min, {}


def read_records_firestore() -> tuple[list[dict[str, Any]], str, datetime]:
    rows, message, latest, _ = _read_records_firestore_full()
    return rows, message, latest


def write_records_firestore(records: list[dict[str, Any]], state: dict[str, Any]) -> tuple[bool, str]:
    if not firebase_configured():
        return False, "Firebase 未設定"
    try:
        _init_firebase_app()
        db = firestore.client()
        normalized = normalize_records(records)
        records_ref = db.collection("godpick_records")
        existing_ids = {doc.id for doc in records_ref.stream()}
        new_ids: set[str] = set()
        operations: list[tuple[str, Any, Any]] = []
        now = firestore.SERVER_TIMESTAMP
        for row in normalized:
            rec_id = _safe_str(row.get("record_id"))
            if not rec_id:
                continue
            new_ids.add(rec_id)
            payload = dict(row)
            payload["updated_at"] = now
            operations.append(("set", records_ref.document(rec_id), payload))
        for rec_id in existing_ids - new_ids:
            operations.append(("delete", records_ref.document(rec_id), None))
        operations.append(
            (
                "set",
                db.collection("system").document("godpick_records_summary"),
                {
                    "count": len(normalized),
                    "payload_hash": state["payload_hash"],
                    "latest_recommendation_date": state.get("latest_recommendation_date") or _latest_record_recommendation_date(normalized),
                    "revision": state.get("revision"),
                    "updated_at_epoch": state.get("updated_at_epoch"),
                    "updated_at_utc": state.get("updated_at_utc"),
                    "updated_at": now,
                    "updated_at_text": state["updated_at"],
                    "source": "godpick_persistence_service",
                },
            )
        )
        _commit_operations(db, operations)
        # Avoid downloading all records again when the collection is large; verify the summary hash.
        summary = db.collection("system").document("godpick_records_summary").get()
        summary_data = summary.to_dict() or {}
        if _safe_str(summary_data.get("payload_hash")) != state["payload_hash"]:
            return False, "Firestore records 摘要回讀驗證不一致"
        return True, f"Firestore records 分批寫入 {len(normalized)} 筆並驗證"
    except Exception as exc:
        return False, f"Firestore records 寫入失敗：{exc}"




def _records_github_sync_worker() -> None:
    """Coalescing background uploader for the large records JSON.

    Streamlit reruns do not terminate the Python process, so the single worker
    can finish the GitHub backup after the UI has returned.  New mutations
    replace the pending snapshot; after the current upload completes, the
    worker immediately uploads the newest snapshot instead of every
    intermediate version.
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
            },
        )
        gh_path = _secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE
        gh1, ghm1 = write_github_json(gh_path, records, f"{reason}: persist godpick records")
        gh2, ghm2 = write_github_json(RECORDS_STATE_FILE, state, f"{reason}: persist godpick records state") if gh1 else (False, "records 未寫入，略過狀態")
        ok = bool(gh1 and gh2)
        write_local_json_atomic(
            RECORDS_GITHUB_SYNC_STATUS_FILE,
            {
                "status": "success" if ok else "failed",
                "reason": reason,
                "count": len(records),
                "payload_hash": state.get("payload_hash"),
                "finished_at": _now_text(),
                "message": f"{ghm1}｜{ghm2}",
            },
        )


def schedule_records_github_sync(records: Any, state: dict[str, Any], reason: str = "record mutation") -> tuple[bool, str]:
    """Queue the newest full records snapshot for one background GitHub upload."""
    global _RECORDS_GITHUB_PENDING, _RECORDS_GITHUB_RUNNING
    cfg = github_config()
    if not cfg["token"]:
        return False, "GitHub 未設定"
    # V162：save_records_mutation_fast 已產生 JSON-safe list；不要在按鈕 callback 內再次掃描/清理 20MB 全檔。
    # 非 list 輸入才走相容正規化。
    normalized = records if isinstance(records, list) else records_as_rows_exact(records)
    safe_state = dict(state)
    with _RECORDS_GITHUB_LOCK:
        _RECORDS_GITHUB_PENDING = (normalized, safe_state, _safe_str(reason) or "record mutation")
        if not _RECORDS_GITHUB_RUNNING:
            _RECORDS_GITHUB_RUNNING = True
            _RECORDS_GITHUB_EXECUTOR.submit(_records_github_sync_worker)
    return True, f"GitHub 大型備份已排入背景同步：{len(normalized)} 筆"




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
    """Read the small summary and repair legacy freshness metadata in memory."""
    if not firebase_configured():
        return {}, "Firebase 未設定"
    try:
        _init_firebase_app()
        db = firestore.client()
        snap = db.collection("system").document("godpick_records_summary").get()
        data = (snap.to_dict() or {}) if snap.exists else {}
        messages = ["已讀取 Firestore 推薦紀錄摘要"]

        # Legacy writers stored only count/updated_at.  Query one newest record so
        # startup can still recognize that Firestore is newer than the 7/9 repo file.
        if not _recommendation_date_text(data.get("latest_recommendation_date")):
            try:
                query_direction = getattr(getattr(firestore, "Query", None), "DESCENDING", "DESCENDING")
                query = (
                    db.collection("godpick_records")
                    .order_by("推薦日期", direction=query_direction)
                    .limit(1)
                )
                newest_docs = list(query.stream())
                if newest_docs:
                    newest_data = newest_docs[0].to_dict() or {}
                    latest_date = _recommendation_date_text(newest_data.get("推薦日期") or newest_data.get("date"))
                    if latest_date:
                        data["latest_recommendation_date"] = latest_date
                        messages.append(f"已探測遠端最新推薦日期 {latest_date}")
            except Exception as probe_exc:
                messages.append(f"最新日期探測略過：{probe_exc}")

        # Firestore Timestamp is not JSON text; preserve it as a fallback epoch.
        if not data.get("updated_at_epoch"):
            updated = data.get("updated_at")
            try:
                if hasattr(updated, "timestamp"):
                    data["updated_at_epoch"] = float(updated.timestamp())
            except Exception:
                pass
        return data, "｜".join(messages)
    except Exception as exc:
        return {}, f"Firestore 摘要讀取失敗：{exc}"

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
    """Persist a small record mutation without rewriting the whole collection.

    The recommendation record file can exceed 20 MB.  A delete/edit used to
    rewrite every Firestore document and then upload the entire GitHub JSON in
    the Streamlit button callback, which made the page appear to run forever.
    This helper applies only the affected document operations and refreshes the
    summary hash.  A stale/missing remote summary is repaired in place; it never
    falls back to rewriting all recommendation documents inside a delete/edit
    button callback.
    """
    if not firebase_configured():
        return False, "Firebase 未設定"
    try:
        _init_firebase_app()
        db = firestore.client()
        # V162：增量刪除不再因遠端摘要筆數不同而回退成「重寫全部 1800+ 文件」。
        # 這個回退是刪除按鈕長時間運算的主因。即使摘要落後，仍只執行指定 delete/upsert，
        # 然後以目前本機筆數修復摘要；完整一致性可由上方「儲存同步」另行驗證。
        try:
            normalized_count = len(records)
        except Exception:
            normalized_count = len(records_as_rows_exact(records))
        summary_ref = db.collection("system").document("godpick_records_summary")
        summary = summary_ref.get()
        summary_data = summary.to_dict() or {} if summary.exists else {}
        count_mismatch = False
        old_count = -1
        if expected_previous_count is not None:
            try:
                old_count = int(summary_data.get("count"))
            except Exception:
                old_count = -1
            count_mismatch = old_count != int(expected_previous_count)

        records_ref = db.collection("godpick_records")
        delete_set = {_safe_str(x) for x in (deleted_ids or []) if _safe_str(x)}
        normalized_upserts = records_as_rows_exact(list(upsert_rows or []))
        operations: list[tuple[str, Any, Any]] = []
        now = firestore.SERVER_TIMESTAMP

        for rec_id in sorted(delete_set):
            operations.append(("delete", records_ref.document(rec_id), None))
        for row in normalized_upserts:
            rec_id = _safe_str(row.get("record_id"))
            if not rec_id:
                continue
            payload = dict(row)
            payload["updated_at"] = now
            operations.append(("set", records_ref.document(rec_id), payload))

        operations.append(
            (
                "set",
                summary_ref,
                {
                    "count": normalized_count,
                    "payload_hash": state["payload_hash"],
                    "latest_recommendation_date": state.get("latest_recommendation_date") or _latest_record_recommendation_date(records),
                    "revision": state.get("revision"),
                    "updated_at_epoch": state.get("updated_at_epoch"),
                    "updated_at_utc": state.get("updated_at_utc"),
                    "updated_at": now,
                    "updated_at_text": state["updated_at"],
                    "source": "godpick_persistence_service_mutation",
                },
            )
        )
        _commit_operations(db, operations)

        verify = summary_ref.get()
        verify_data = verify.to_dict() or {}
        if _safe_str(verify_data.get("payload_hash")) != state["payload_hash"]:
            return False, "Firestore mutation 摘要回讀驗證不一致"
        if int(verify_data.get("count") or -1) != normalized_count:
            return False, "Firestore mutation 筆數回讀驗證不一致"

        # Verify the actual removed documents for delete operations.  This is a
        # tiny bounded read and avoids downloading the whole collection.
        if delete_set:
            refs = [records_ref.document(x) for x in sorted(delete_set)]
            remaining = [snap.id for snap in db.get_all(refs) if snap.exists]
            if remaining:
                return False, f"Firestore mutation 刪除回讀仍存在：{','.join(remaining[:10])}"

        mismatch_note = "｜已修復遠端摘要筆數落差" if count_mismatch else ""
        return True, (
            f"Firestore 增量同步完成：刪除 {len(delete_set)} 筆、"
            f"更新 {len(normalized_upserts)} 筆、目前 {normalized_count} 筆{mismatch_note}"
        )
    except Exception as exc:
        return False, f"Firestore records 增量寫入失敗：{exc}"


def save_records_mutation_fast(
    payload: Any,
    *,
    expected_authority_signature: str = "",
    deleted_ids: Iterable[str] | None = None,
    upsert_rows: Iterable[dict[str, Any]] | None = None,
    previous_count: int | None = None,
    reason: str = "record mutation",
) -> PersistenceReport:
    """Fast durable save for delete/edit/add actions.

    Local JSON is committed atomically first.  When Firestore is configured we
    apply a verified incremental mutation and defer the very large GitHub file
    to the explicit ``儲存同步`` button.  If Firestore is unavailable but
    GitHub is configured, the function falls back to a full GitHub write so a
    cloud reboot cannot silently restore deleted rows.
    """
    if _safe_str(expected_authority_signature):
        current_signature = records_authority_signature()
        if current_signature != _safe_str(expected_authority_signature):
            report = PersistenceReport()
            report.local_message = "權威檔已被其他頁面更新；已阻止舊畫面覆蓋，請自動重新載入後再操作。"
            report.updated_at = _now_text()
            return report
    records = records_as_rows_exact(payload)
    state = _new_state(
        "godpick_records_durable_v4_mutation",
        records,
        count=len(records),
        latest_recommendation_date=_latest_record_recommendation_date(records),
        github_pending=True,
        mutation_reason=_safe_str(reason),
    )
    report = PersistenceReport(payload_hash=state["payload_hash"], updated_at=state["updated_at"])

    ok1, msg1 = write_local_json_atomic(RECORDS_FILE, records)
    ok2, msg2 = write_local_json_atomic(RECORDS_STATE_FILE, state)
    ok3, msg3 = write_local_json_atomic(RECORDS_MANIFEST_FILE, _records_manifest(records, state["payload_hash"])) if ok1 and ok2 else (False, "前置寫入失敗，未建立manifest")
    report.local_ok = bool(ok1 and ok2 and ok3)
    report.local_message = f"{msg1}｜{msg2}｜{msg3}"
    if not report.local_ok:
        report.github_message = "本機原子寫入失敗，未執行遠端 mutation"
        report.firestore_message = "本機原子寫入失敗，未執行遠端 mutation"
        report.permanent_ok = False
        return report

    # V178 legacy-ID migration: if this snapshot contains repaired IDs, make the
    # Firestore mutation self-contained. Delete collided legacy doc IDs and upsert
    # every surviving repaired row so a user can safely edit/delete before an
    # explicit full sync.
    effective_deleted_ids = {_safe_str(x) for x in (deleted_ids or []) if _safe_str(x)}
    effective_upsert_rows = [dict(x) for x in (upsert_rows or []) if isinstance(x, dict)]
    repaired_rows = [row for row in records if _safe_str(row.get("record_id修復狀態"))]
    for row in repaired_rows:
        legacy_id = _safe_str(row.get("原始record_id"))
        if legacy_id:
            effective_deleted_ids.add(legacy_id)
        effective_upsert_rows.append(dict(row))
    # de-duplicate upserts by the now-unique record_id
    _upsert_map = {_safe_str(r.get("record_id")): r for r in effective_upsert_rows if _safe_str(r.get("record_id"))}
    effective_upsert_rows = list(_upsert_map.values())

    fs_ok = False
    fs_msg = "Firebase 未設定"
    if firebase_configured():
        fs_ok, fs_msg = write_records_firestore_mutation(
            records,
            state,
            deleted_ids=effective_deleted_ids,
            upsert_rows=effective_upsert_rows,
            expected_previous_count=previous_count,
        )
    report.firestore_ok = bool(fs_ok)
    report.firestore_message = fs_msg

    cfg = github_config()
    if cfg["token"]:
        queued, queue_msg = schedule_records_github_sync(records, state, reason)
        report.github_ok = False
        report.github_pending = bool(queued)
        report.github_message = queue_msg
    else:
        report.github_ok = False
        report.github_pending = False
        report.github_message = "GitHub 未設定；沒有待上傳的遠端備份"

    if _configured_remote_exists():
        # Firestore is already verified, or GitHub has accepted the newest full
        # snapshot into the coalescing background queue.  The report explicitly
        # exposes github_pending so the UI never labels it as a completed upload.
        # A queued GitHub upload is not a verified permanent write.  Reboot-safe
        # success is reported only after Firestore or GitHub has confirmed it.
        report.permanent_ok = bool(report.local_ok and (report.firestore_ok or report.github_ok))
    else:
        report.permanent_ok = report.local_ok
    return report


def save_records_sync_fast(payload: Any, reason: str = "explicit record sync", expected_authority_signature: str = "") -> PersistenceReport:
    """Content-aware explicit sync for the large recommendation record file.

    The old ``儲存同步`` rewrote every Firestore document and synchronously
    uploaded a 20+ MB GitHub JSON even when not one cell changed.  This version
    writes local data atomically, applies only changed/deleted Firestore rows,
    and coalesces the large GitHub snapshot in the background.  Repeated syncs
    with identical content return after a small summary verification.
    """
    if _safe_str(expected_authority_signature):
        current_signature = records_authority_signature()
        if current_signature != _safe_str(expected_authority_signature):
            report = PersistenceReport()
            report.local_message = "權威檔已被其他頁面更新；已阻止舊資料整檔回寫。"
            report.updated_at = _now_text()
            return report
    records = normalize_records(payload)
    state = _new_state("godpick_records_durable_v5_fast_sync", records, count=len(records), latest_recommendation_date=_latest_record_recommendation_date(records), sync_reason=_safe_str(reason))
    report = PersistenceReport(payload_hash=state["payload_hash"], updated_at=state["updated_at"])

    old_state, _, _ = read_local_json(RECORDS_STATE_FILE, {})
    old_manifest, _, _ = read_local_json(RECORDS_MANIFEST_FILE, {})
    old_hash = _safe_str((old_state or {}).get("payload_hash")) if isinstance(old_state, dict) else ""
    unchanged = bool(old_hash and old_hash == state["payload_hash"] and (BASE_DIR / RECORDS_FILE).exists())

    if unchanged:
        report.local_ok = True
        report.local_message = f"內容未變更，略過 {len(records)} 筆本機大型 JSON 重寫"
    else:
        ok1, msg1 = write_local_json_atomic(RECORDS_FILE, records)
        ok2, msg2 = write_local_json_atomic(RECORDS_STATE_FILE, state)
        report.local_ok = bool(ok1 and ok2)
        report.local_message = f"{msg1}｜{msg2}"
        if not report.local_ok:
            report.github_message = "本機原子寫入失敗，未排入 GitHub"
            report.firestore_message = "本機原子寫入失敗，未執行 Firestore"
            return report

    new_manifest = _records_manifest(records, state["payload_hash"])
    old_rows = (old_manifest or {}).get("row_hashes", {}) if isinstance(old_manifest, dict) else {}
    if not isinstance(old_rows, dict):
        old_rows = {}
    new_rows = new_manifest["row_hashes"]
    by_id = {_safe_str(row.get("record_id")): row for row in records if _safe_str(row.get("record_id"))}
    changed_ids = [rid for rid, row_hash in new_rows.items() if _safe_str(old_rows.get(rid)) != row_hash]
    deleted_ids = [rid for rid in old_rows if rid not in new_rows]

    fs_summary, fs_summary_msg = _read_records_firestore_summary()
    remote_hash = _safe_str(fs_summary.get("payload_hash")) if isinstance(fs_summary, dict) else ""
    
    try:
        remote_count = int((fs_summary or {}).get("count") if isinstance(fs_summary, dict) else -1)
    except Exception:
        remote_count = -1
    if firebase_configured() and remote_hash == state["payload_hash"] and remote_count == len(records):
        report.firestore_ok = True
        report.firestore_message = f"Firestore 摘要一致，略過 {len(records)} 筆全量回寫"
    elif firebase_configured():
        # If a legacy install has no manifest, only the first explicit sync is
        # allowed to perform a full write.  Thereafter all syncs are incremental.
        if not old_rows and not unchanged:
            fs_ok, fs_msg = write_records_firestore(records, state)
        else:
            fs_ok, fs_msg = write_records_firestore_mutation(
                records, state,
                deleted_ids=deleted_ids,
                upsert_rows=[by_id[rid] for rid in changed_ids if rid in by_id],
                expected_previous_count=(int((old_manifest or {}).get("count") or 0) if isinstance(old_manifest, dict) else None),
            )
        report.firestore_ok = bool(fs_ok)
        report.firestore_message = fs_msg
    else:
        report.firestore_ok = False
        report.firestore_message = fs_summary_msg

    # Persist the manifest only after the local snapshot exists.  It is a local
    # acceleration index, not a new source of truth.
    manifest_ok, manifest_msg = write_local_json_atomic(RECORDS_MANIFEST_FILE, new_manifest)
    if manifest_ok:
        report.local_message += f"｜增量索引：{manifest_msg}"
    else:
        report.local_message += f"｜增量索引失敗：{manifest_msg}"

    gh_status, _, _ = read_local_json(RECORDS_GITHUB_SYNC_STATUS_FILE, {})
    gh_same = isinstance(gh_status, dict) and _safe_str(gh_status.get("status")) == "success" and _safe_str(gh_status.get("payload_hash")) == state["payload_hash"]
    if gh_same:
        report.github_ok = True
        report.github_pending = False
        report.github_message = "GitHub 背景備份內容已一致，略過重傳"
    elif github_config()["token"]:
        queued, queue_msg = schedule_records_github_sync(records, state, reason)
        report.github_ok = False
        report.github_pending = bool(queued)
        report.github_message = queue_msg
    else:
        report.github_ok = False
        report.github_pending = False
        report.github_message = "GitHub 未設定"

    if _configured_remote_exists():
        # A queued GitHub upload is not a verified permanent write.  Reboot-safe
        # success is reported only after Firestore or GitHub has confirmed it.
        report.permanent_ok = bool(report.local_ok and (report.firestore_ok or report.github_ok))
    else:
        report.permanent_ok = report.local_ok
    return report


def save_records_permanent(payload: Any) -> PersistenceReport:
    records = normalize_records(payload)
    state = _new_state("godpick_records_durable_v3", records, count=len(records), latest_recommendation_date=_latest_record_recommendation_date(records))
    report = PersistenceReport(payload_hash=state["payload_hash"], updated_at=state["updated_at"])

    ok1, msg1 = write_local_json_atomic(RECORDS_FILE, records)
    ok2, msg2 = write_local_json_atomic(RECORDS_STATE_FILE, state)
    ok3, msg3 = write_local_json_atomic(RECORDS_MANIFEST_FILE, _records_manifest(records, state["payload_hash"])) if ok1 and ok2 else (False, "前置寫入失敗，未建立manifest")
    report.local_ok = bool(ok1 and ok2 and ok3)
    report.local_message = f"{msg1}｜{msg2}｜{msg3}"

    # Firestore is written before the large GitHub file so one slow GitHub
    # request cannot prevent the primary remote backup from completing.
    fs_ok, fs_msg = write_records_firestore(records, state)
    report.firestore_ok = fs_ok
    report.firestore_message = fs_msg

    gh_path = _secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE
    gh1, ghm1 = write_github_json(gh_path, records, "persist godpick records")
    gh2, ghm2 = write_github_json(RECORDS_STATE_FILE, state, "persist godpick records state") if gh1 else (False, "records 未寫入，略過狀態")
    report.github_ok = bool(gh1 and gh2)
    report.github_message = f"{ghm1}｜{ghm2}"

    if _configured_remote_exists():
        report.permanent_ok = bool(report.local_ok and (report.github_ok or report.firestore_ok))
    else:
        report.permanent_ok = report.local_ok
    return report


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
    """V181 compatibility restore for mixed/legacy Streamlit deployments.

    Some deployed V178/V179 revisions call ``restore_records_snapshot`` from
    ``recover_records_authority``.  A mixed deployment could contain the caller
    without this helper and crash Page 8 with NameError.  Keep this public helper
    permanently: restore the exact authority rows locally, repair legacy duplicate
    record_ids, and atomically refresh state + manifest.  No remote write occurs.
    """
    try:
        rows = normalize_records(payload)
        if not rows:
            return False, f"{source} 快照沒有有效推薦紀錄，未覆蓋本機權威檔。"
        state = _new_state(
            "godpick_records_durable_v181_restore_compat",
            rows,
            count=len(rows),
            latest_recommendation_date=_latest_record_recommendation_date(rows),
            restored_from=_safe_str(source) or "remote",
            restored_at=_now_text(),
        )
        manifest = _records_manifest(rows, state["payload_hash"])
        with _RECORDS_LOCAL_LOCK:
            ok1, msg1 = write_local_json_atomic(RECORDS_FILE, rows)
            ok2, msg2 = write_local_json_atomic(RECORDS_STATE_FILE, state) if ok1 else (False, "records 寫入失敗，未更新 state")
            ok3, msg3 = write_local_json_atomic(RECORDS_MANIFEST_FILE, manifest) if ok1 and ok2 else (False, "records/state 寫入失敗，未更新 manifest")
        ok = bool(ok1 and ok2 and ok3)
        if ok:
            return True, f"已由 {source} 還原推薦紀錄 {len(rows)} 筆並重建 authority state/manifest。"
        return False, f"{source} 還原未完成｜{msg1}｜{msg2}｜{msg3}"
    except Exception as exc:
        return False, f"{source} 還原推薦紀錄例外：{exc}"


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
    local_payload, local_msg, local_mtime = read_local_json(RECORDS_FILE, [])
    local_state, local_state_msg, _ = read_local_json(RECORDS_STATE_FILE, {})

    gh_path = _secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE
    gh_payload, gh_msg = read_github_json(gh_path, [])
    gh_state, gh_state_msg = read_github_json(RECORDS_STATE_FILE, {})

    fs_payload, fs_msg, fs_mtime, fs_state = _read_records_firestore_full()
    raw_candidates = [
        ("local", local_payload, local_state, local_mtime),
        ("github", gh_payload, gh_state, datetime.min),
        ("firestore", fs_payload, fs_state, fs_mtime),
    ]
    candidates = []
    valid = []
    for source, raw_rows, state, fallback in raw_candidates:
        normalized = normalize_records(raw_rows)
        candidates.append((source, normalized, state, fallback))
        # Legacy V177 state hashes were computed before duplicate-ID repair.
        # Accept either the raw payload hash or the V178 normalized hash, then
        # rewrite the selected local state to the normalized V178 hash below.
        raw_valid = _state_is_valid(raw_rows, state)
        normalized_valid = _state_is_valid(normalized, state)
        if raw_valid or normalized_valid:
            valid.append((source, normalized, state, _state_epoch(state, fallback)))

    if valid:
        source, chosen, chosen_state, _ = max(
            valid,
            key=lambda item: _authority_freshness_key(item[0], item[2], fallback=item[3], rows=item[1]),
        )
    else:
        chosen = _merge_record_sources_v178([rows for _, rows, _, _ in candidates])
        source = "legacy_merge_v178"
        chosen_state = _new_state("godpick_records_durable_v178_recovery", chosen, count=len(chosen), migrated_from=source)

    restored_state = dict(chosen_state or {})
    restored_state.update({
        "version": restored_state.get("version") or "godpick_records_durable_v2",
        "payload_hash": _json_hash(chosen),
        "count": len(chosen),
        "restored_from": source,
        "restored_at": _now_text(),
    })
    restored_state.setdefault("updated_at", _now_text())
    restored_state.setdefault("updated_at_utc", _now_utc_text())
    restored_state.setdefault("updated_at_epoch", _now_epoch())
    restored_state.setdefault("revision", time.time_ns())
    write_local_json_atomic(RECORDS_FILE, chosen)
    write_local_json_atomic(RECORDS_STATE_FILE, restored_state)
    details = [
        f"權威來源：{source}｜推薦紀錄 {len(chosen)} 筆",
        f"本機：{local_msg}｜{local_state_msg}",
        f"GitHub：{gh_msg}｜{gh_state_msg}",
        f"Firestore：{fs_msg}",
    ]
    return chosen, details


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
) -> PersistenceReport:
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

    if _configured_remote_exists():
        report.permanent_ok = bool(report.local_ok and (report.github_ok or report.firestore_ok))
    else:
        report.permanent_ok = report.local_ok
    return report


def load_named_json_permanent(
    path_name: str,
    default: Any,
    *,
    github_path: str | None = None,
    firestore_doc: str | None = None,
) -> tuple[Any, list[str]]:
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
