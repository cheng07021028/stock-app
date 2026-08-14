# -*- coding: utf-8 -*-
"""V191-H22 monotonic macro-data authority and reboot rollback guard.

Problem fixed
-------------
Streamlit/GitHub Actions processes start from the deployment checkout. Runtime
market JSON is intentionally stored on the ``runtime-data`` branch, so a fresh
process can initially see an old packaged ``market_snapshot.json`` and an old
``macro_market_close_cache.json``. Page0 used that cache to rebuild the snapshot,
which could turn a successfully updated August market state back into 2026-07-09.

H22 makes business date, not deployment/file mtime, the authority key. On process
startup it restores newer runtime-data payloads before any page can use the old
checkout. It also reconstructs the primary market cache from a newer snapshot,
so Page0 cannot immediately rebuild the snapshot from an older cache.

A second hard guard wraps the existing durability gateways in the running process.
Even if Page0 temporarily builds an older snapshot, an older/stale macro payload
is not allowed to overwrite runtime-data. If a newer safe payload is already in
memory, it is immediately restored locally as well.

No runtime JSON is committed to ``main``. Newer local runtime data is queued to
the existing durability layer, which writes to the configured runtime-data branch
/ Firestore without triggering a Streamlit redeploy.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
import copy
import json
import re
import threading


VERSION = "v191_h22_macro_monotonic_authority_20260814"
TAIPEI = timezone(timedelta(hours=8))
BASE_DIR = Path(__file__).resolve().parent
STATUS_FILE = "macro_runtime_authority_status.json"
MAX_PUBLISH_STALE_DAYS = 14

AUTHORITY_FILES: tuple[str, ...] = (
    "market_snapshot.json",
    "macro_mode_bridge.json",
    "macro_trend_records.json",
    "macro_market_close_cache.json",
    "macro_otc_cache.json",
    "macro_taifex_cache.json",
    "overnight_global_market_cache.json",
)

_LOCK = threading.RLock()
_DONE = False
_LAST_REPORT: dict[str, Any] = {}
_SAFE_PAYLOADS: dict[str, Any] = {}
_SAFE_DATES: dict[str, str] = {}
_GUARD_INSTALLED = False


def _now() -> datetime:
    return datetime.now(TAIPEI)


def _text(value: Any) -> str:
    try:
        return "" if value is None else str(value).strip()
    except Exception:
        return ""


def _date_text(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    if re.fullmatch(r"\d{8}", text[:8]):
        try:
            return datetime.strptime(text[:8], "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            pass
    text = text.replace("/", "-")
    try:
        dt = datetime.fromisoformat(text[:19])
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    m = re.search(r"(20\d{2})[-/]?(\d{2})[-/]?(\d{2})", text)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).strftime("%Y-%m-%d")
        except Exception:
            pass
    return ""


def _time_text(value: Any) -> str:
    text = _text(value).replace("T", " ")
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text[:19]).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return text[:19]


def _read_local(path_name: str, default: Any) -> Any:
    try:
        path = BASE_DIR / path_name
        if not path.exists():
            return copy.deepcopy(default)
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return copy.deepcopy(default)


def _business_dates(payload: Any) -> list[str]:
    dates: list[str] = []

    def add(value: Any) -> None:
        d = _date_text(value)
        if d:
            dates.append(d)

    if isinstance(payload, dict):
        for key in (
            "market_date", "data_date", "twse_data_date", "otc_data_date",
            "futures_data_date", "institutional_date", "date", "used_date",
            "recommendation_date", "forecast_date",
        ):
            add(payload.get(key))

        for key, value in payload.items():
            if re.fullmatch(r"20\d{6}", str(key)):
                add(key)
            if isinstance(value, dict):
                for sub in ("date", "used_date", "data_date", "market_date", "data_date_text"):
                    add(value.get(sub))

        items = payload.get("items") if isinstance(payload.get("items"), dict) else {}
        for item in items.values():
            if isinstance(item, dict):
                add(item.get("data_date") or item.get("date"))

        if not dates:
            add(payload.get("updated_at") or payload.get("saved_at"))

    elif isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            for key in (
                "market_date", "data_date", "twse_data_date", "date", "used_date",
                "updated_at",
            ):
                add(item.get(key))

    return dates


def business_date(payload: Any) -> str:
    dates = _business_dates(payload)
    return max(dates) if dates else ""


def activity_time(payload: Any) -> str:
    candidates: list[str] = []
    if isinstance(payload, dict):
        for key in ("updated_at", "saved_at", "finished_at", "snapshot_time", "time"):
            value = _time_text(payload.get(key))
            if value:
                candidates.append(value)
        for value in payload.values():
            if isinstance(value, dict):
                t = _time_text(value.get("updated_at"))
                if t:
                    candidates.append(t)
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                t = _time_text(item.get("updated_at"))
                if t:
                    candidates.append(t)
    return max(candidates) if candidates else ""


def authority_key(payload: Any, source: str) -> tuple[str, str, int]:
    source_priority = 1 if source == "runtime-data" else 0
    return (business_date(payload), activity_time(payload), source_priority)


def _fresh_enough_to_publish(payload: Any) -> bool:
    d = business_date(payload)
    if not d:
        return False
    try:
        age = (_now().date() - datetime.strptime(d, "%Y-%m-%d").date()).days
        return -1 <= age <= MAX_PUBLISH_STALE_DAYS
    except Exception:
        return False


def _write_local(path_name: str, payload: Any) -> tuple[bool, str]:
    try:
        from godpick_persistence_service import write_local_json_atomic
        return write_local_json_atomic(path_name, payload)
    except Exception as exc:
        return False, f"write_local_json_atomic unavailable: {exc}"


def _read_runtime_remote(path_name: str) -> tuple[Any, str]:
    try:
        from godpick_persistence_service import read_github_json
        payload, msg = read_github_json(path_name, None)
        return payload, str(msg)
    except Exception as exc:
        return None, f"runtime-data read exception: {exc}"


def _safe_number(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _bounded_pct(value: Any, limit: float = 20.0) -> float | None:
    x = _safe_number(value)
    return x if x is not None and abs(x) <= limit else None


def _bounded_change(value: Any, close: Any) -> float | None:
    x = _safe_number(value)
    c = _safe_number(close)
    if x is None:
        return None
    limit = max(50.0, abs(c or 0.0) * 0.20)
    return x if abs(x) <= limit else None


def _snapshot_to_market_cache(snapshot: dict[str, Any], current: Any) -> dict[str, Any]:
    cache = dict(current) if isinstance(current, dict) else {}
    d = _date_text(snapshot.get("twse_data_date") or snapshot.get("market_date") or snapshot.get("data_date"))
    close = snapshot.get("twse_index")
    if not d or close in (None, ""):
        return cache
    key = d.replace("-", "")
    row = dict(cache.get(key) if isinstance(cache.get(key), dict) else {})
    row.update({
        "ok": True, "date": d, "used_date": d, "close": close,
        "pct": _bounded_pct(snapshot.get("twse_change_pct")),
        "change_points": _bounded_change(snapshot.get("twse_change"), close),
        "source": snapshot.get("twse_source") or "H22 market_snapshot authority restore",
        "updated_at": snapshot.get("updated_at") or _now().strftime("%Y-%m-%d %H:%M:%S"),
        "authority_rebuilt_from": "market_snapshot.json", "authority_version": VERSION,
    })
    cache[key] = row
    return cache


def _snapshot_to_otc_cache(snapshot: dict[str, Any], current: Any) -> dict[str, Any]:
    cache = dict(current) if isinstance(current, dict) else {}
    d = _date_text(snapshot.get("otc_data_date"))
    close = snapshot.get("otc_index")
    if not d or close in (None, ""):
        return cache
    key = d.replace("-", "")
    row = dict(cache.get(key) if isinstance(cache.get(key), dict) else {})
    row.update({
        "ok": True, "date": d, "used_date": d, "close": close,
        "pct": _bounded_pct(snapshot.get("otc_change_pct")),
        "change_points": _bounded_change(snapshot.get("otc_change"), close),
        "source": snapshot.get("otc_source") or "H22 market_snapshot authority restore",
        "updated_at": snapshot.get("updated_at") or _now().strftime("%Y-%m-%d %H:%M:%S"),
        "authority_rebuilt_from": "market_snapshot.json", "authority_version": VERSION,
    })
    cache[key] = row
    return cache


def _snapshot_to_taifex_cache(snapshot: dict[str, Any], current: Any) -> dict[str, Any]:
    cache = dict(current) if isinstance(current, dict) else {}
    d = _date_text(snapshot.get("futures_data_date"))
    close = snapshot.get("futures_index")
    if not d or close in (None, ""):
        return cache
    key = d.replace("-", "")
    row = dict(cache.get(key) if isinstance(cache.get(key), dict) else {})
    row.update({
        "ok": True, "date": d, "tx_close": close,
        "tx_change": _bounded_change(snapshot.get("futures_change"), close),
        "tx_pct": _bounded_pct(snapshot.get("futures_change_pct")),
        "source": snapshot.get("futures_source") or "H22 market_snapshot authority restore",
        "updated_at": snapshot.get("updated_at") or _now().strftime("%Y-%m-%d %H:%M:%S"),
        "authority_rebuilt_from": "market_snapshot.json", "authority_version": VERSION,
    })
    cache[key] = row
    return cache


def _remember_safe_payload(path_name: str, payload: Any) -> None:
    base = Path(path_name).name
    if base not in AUTHORITY_FILES or payload in (None, {}, []):
        return
    incoming_date = business_date(payload)
    old_date = _SAFE_DATES.get(base, "")
    if incoming_date and (not old_date or incoming_date >= old_date):
        _SAFE_DATES[base] = incoming_date
        _SAFE_PAYLOADS[base] = copy.deepcopy(payload)


def _rollback_reason(path_name: str, payload: Any) -> str:
    base = Path(path_name).name
    if base not in AUTHORITY_FILES:
        return ""
    incoming_date = business_date(payload)
    safe_date = _SAFE_DATES.get(base, "")
    if safe_date and incoming_date and incoming_date < safe_date:
        return f"H22防回退：{base} 傳入交易日 {incoming_date} < 已確認權威 {safe_date}"
    if incoming_date and not _fresh_enough_to_publish(payload):
        return f"H22防陳舊永久化：{base} 交易日 {incoming_date} 已超過 {MAX_PUBLISH_STALE_DAYS} 天安全窗"
    return ""


def _restore_safe_payload(path_name: str) -> None:
    base = Path(path_name).name
    safe = _SAFE_PAYLOADS.get(base)
    if safe not in (None, {}, []):
        try:
            _write_local(base, safe)
        except Exception:
            pass


def install_macro_durability_monotonic_guard() -> bool:
    """Wrap generic durability gateways so an old macro payload cannot go remote."""
    global _GUARD_INSTALLED
    with _LOCK:
        if _GUARD_INSTALLED:
            return True
        try:
            import godpick_durability_service as ds
            import godpick_persistence_service as gps

            original_async = ds.persist_json_async
            original_permanent = ds.persist_json_permanent
            original_named = gps.save_named_json_permanent

            if not getattr(original_async, "_v191_h22_macro_guard", False):
                def guarded_async(path_name: str, payload: Any, *args, **kwargs):
                    reason = _rollback_reason(path_name, payload)
                    if reason:
                        _restore_safe_payload(path_name)
                        return False, reason
                    _remember_safe_payload(path_name, payload)
                    return original_async(path_name, payload, *args, **kwargs)
                guarded_async._v191_h22_macro_guard = True  # type: ignore[attr-defined]
                ds.persist_json_async = guarded_async

            if not getattr(original_permanent, "_v191_h22_macro_guard", False):
                def guarded_permanent(path_name: str, payload: Any, *args, **kwargs):
                    reason = _rollback_reason(path_name, payload)
                    if reason:
                        _restore_safe_payload(path_name)
                        return False, reason
                    _remember_safe_payload(path_name, payload)
                    return original_permanent(path_name, payload, *args, **kwargs)
                guarded_permanent._v191_h22_macro_guard = True  # type: ignore[attr-defined]
                ds.persist_json_permanent = guarded_permanent

            if not getattr(original_named, "_v191_h22_macro_guard", False):
                def guarded_named(path_name: str, payload: Any, *args, **kwargs):
                    reason = _rollback_reason(path_name, payload)
                    if reason:
                        _restore_safe_payload(path_name)
                        report = gps.PersistenceReport()
                        report.local_ok = False
                        report.github_ok = False
                        report.firestore_ok = False
                        report.permanent_ok = False
                        report.local_message = reason
                        report.github_message = "H22已阻止舊大盤資料覆蓋runtime-data"
                        report.firestore_message = "H22已阻止舊大盤資料覆蓋永久權威"
                        report.updated_at = _now().strftime("%Y-%m-%d %H:%M:%S")
                        return report
                    _remember_safe_payload(path_name, payload)
                    return original_named(path_name, payload, *args, **kwargs)
                guarded_named._v191_h22_macro_guard = True  # type: ignore[attr-defined]
                gps.save_named_json_permanent = guarded_named

            _GUARD_INSTALLED = True
            return True
        except Exception:
            return False


def ensure_macro_runtime_authority_current(*, force: bool = False, queue_newer_local: bool = True) -> dict[str, Any]:
    """Restore newest business-date macro authority once per process."""
    global _DONE, _LAST_REPORT
    with _LOCK:
        install_macro_durability_monotonic_guard()
        if _DONE and not force:
            return dict(_LAST_REPORT)

        local_payloads = {
            name: _read_local(name, {} if name != "macro_trend_records.json" else [])
            for name in AUTHORITY_FILES
        }
        for name, payload in local_payloads.items():
            _remember_safe_payload(name, payload)

        remote_payloads: dict[str, Any] = {}
        remote_messages: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=min(6, len(AUTHORITY_FILES))) as pool:
            futures = {pool.submit(_read_runtime_remote, name): name for name in AUTHORITY_FILES}
            for fut in as_completed(futures):
                name = futures[fut]
                try:
                    payload, msg = fut.result()
                except Exception as exc:
                    payload, msg = None, str(exc)
                remote_payloads[name] = payload
                remote_messages[name] = msg

        rows: list[dict[str, Any]] = []
        chosen_payloads: dict[str, Any] = {}
        for name in AUTHORITY_FILES:
            local = local_payloads.get(name)
            remote = remote_payloads.get(name)
            local_nonempty = local not in (None, {}, [])
            remote_nonempty = remote not in (None, {}, [])
            if remote_nonempty and (not local_nonempty or authority_key(remote, "runtime-data") > authority_key(local, "local")):
                chosen, source = remote, "runtime-data"
                ok, write_msg = _write_local(name, chosen)
            else:
                chosen, source = local, "local"
                ok, write_msg = True, "local retained"
            chosen_payloads[name] = chosen
            _remember_safe_payload(name, chosen)
            rows.append({
                "file": name,
                "local_business_date": business_date(local),
                "runtime_business_date": business_date(remote),
                "chosen_source": source,
                "chosen_business_date": business_date(chosen),
                "local_write_ok": bool(ok),
                "message": write_msg if source == "runtime-data" else remote_messages.get(name, ""),
            })

        snapshot = chosen_payloads.get("market_snapshot.json")
        if isinstance(snapshot, dict) and snapshot:
            repair_map = {
                "macro_market_close_cache.json": _snapshot_to_market_cache,
                "macro_otc_cache.json": _snapshot_to_otc_cache,
                "macro_taifex_cache.json": _snapshot_to_taifex_cache,
            }
            for name, builder in repair_map.items():
                before = chosen_payloads.get(name)
                repaired = builder(snapshot, before)
                if authority_key(repaired, "local") > authority_key(before, "local"):
                    ok, msg = _write_local(name, repaired)
                    chosen_payloads[name] = repaired
                    _remember_safe_payload(name, repaired)
                    rows.append({
                        "file": name,
                        "action": "rebuilt_from_newer_market_snapshot",
                        "before_business_date": business_date(before),
                        "after_business_date": business_date(repaired),
                        "local_write_ok": bool(ok),
                        "message": msg,
                    })

        queued: list[str] = []
        if queue_newer_local:
            try:
                from godpick_durability_service import persist_json_async
                for name in AUTHORITY_FILES:
                    current = chosen_payloads.get(name)
                    remote = remote_payloads.get(name)
                    if current in (None, {}, []) or not _fresh_enough_to_publish(current):
                        continue
                    if remote in (None, {}, []) or authority_key(current, "local") > authority_key(remote, "runtime-data"):
                        ok, _ = persist_json_async(name, current, reason=f"{VERSION} monotonic macro authority")
                        if ok:
                            queued.append(name)
            except Exception:
                pass

        report = {
            "version": VERSION,
            "ok": True,
            "updated_at": _now().strftime("%Y-%m-%d %H:%M:%S"),
            "snapshot_business_date": business_date(chosen_payloads.get("market_snapshot.json")),
            "market_cache_business_date": business_date(chosen_payloads.get("macro_market_close_cache.json")),
            "queued_newer_local": queued,
            "persistence_guard_installed": bool(_GUARD_INSTALLED),
            "rows": rows,
        }
        try:
            _write_local(STATUS_FILE, report)
        except Exception:
            pass
        _LAST_REPORT = dict(report)
        _DONE = True
        return report


def reset_macro_authority_process_guard() -> None:
    global _DONE, _LAST_REPORT
    with _LOCK:
        _DONE = False
        _LAST_REPORT = {}


__all__ = [
    "VERSION", "AUTHORITY_FILES", "business_date", "authority_key",
    "install_macro_durability_monotonic_guard",
    "ensure_macro_runtime_authority_current", "reset_macro_authority_process_guard",
]
