# -*- coding: utf-8 -*-
"""V191-H22 monotonic macro-data authority and reboot rollback guard.

Problem fixed
-------------
Streamlit/GitHub Actions processes start from the deployment checkout.  Runtime
market JSON is intentionally stored on the ``runtime-data`` branch, so a fresh
process can initially see an old packaged ``market_snapshot.json`` and an old
``macro_market_close_cache.json``.  Page0 used that cache to rebuild the snapshot,
which could turn a successfully updated August market state back into 2026-07-09.

H22 makes business date, not deployment/file mtime, the authority key.  On process
startup it restores newer runtime-data payloads before any page can use the old
checkout.  It also reconstructs the primary market cache from a newer snapshot,
so Page0 cannot immediately rebuild the snapshot from an older cache.

No runtime JSON is committed to ``main``.  Newer local runtime data may be queued
to the existing durability layer, which writes to the configured runtime-data
branch / Firestore without triggering a Streamlit redeploy.
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

# These are the files whose rollback can alter Page0/Page7 decisions.  Derived
# US/institutional caches can still be refreshed normally; the critical point is
# that the TWSE/OTC/futures business date driving Page0 must never move backward.
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

        # Dated cache dictionaries use YYYYMMDD keys.
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

        # updated_at is only a last-resort business-date hint; explicit exchange
        # dates above always coexist for normal market payloads.
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
    # runtime-data wins a true tie, but can never beat a newer business date.
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


def _snapshot_to_market_cache(snapshot: dict[str, Any], current: Any) -> dict[str, Any]:
    cache = dict(current) if isinstance(current, dict) else {}
    d = _date_text(snapshot.get("twse_data_date") or snapshot.get("market_date") or snapshot.get("data_date"))
    close = snapshot.get("twse_index")
    if not d or close in (None, ""):
        return cache
    key = d.replace("-", "")
    existing = cache.get(key) if isinstance(cache.get(key), dict) else {}
    row = dict(existing)
    row.update({
        "ok": True,
        "date": d,
        "used_date": d,
        "close": close,
        "pct": snapshot.get("twse_change_pct"),
        "change_points": snapshot.get("twse_change"),
        "source": snapshot.get("twse_source") or "H22 market_snapshot authority restore",
        "updated_at": snapshot.get("updated_at") or _now().strftime("%Y-%m-%d %H:%M:%S"),
        "authority_rebuilt_from": "market_snapshot.json",
        "authority_version": VERSION,
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
        "pct": snapshot.get("otc_change_pct"), "change_points": snapshot.get("otc_change"),
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
        "tx_change": snapshot.get("futures_change"), "tx_pct": snapshot.get("futures_change_pct"),
        "source": snapshot.get("futures_source") or "H22 market_snapshot authority restore",
        "updated_at": snapshot.get("updated_at") or _now().strftime("%Y-%m-%d %H:%M:%S"),
        "authority_rebuilt_from": "market_snapshot.json", "authority_version": VERSION,
    })
    cache[key] = row
    return cache


def ensure_macro_runtime_authority_current(*, force: bool = False, queue_newer_local: bool = True) -> dict[str, Any]:
    """Restore newest business-date macro authority once per process.

    The function intentionally does not trust deployment mtime.  It compares the
    semantic market date of packaged-local vs runtime-data for each file.
    """
    global _DONE, _LAST_REPORT
    with _LOCK:
        if _DONE and not force:
            return dict(_LAST_REPORT)

        local_payloads = {name: _read_local(name, {} if name != "macro_trend_records.json" else []) for name in AUTHORITY_FILES}

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
            rows.append({
                "file": name,
                "local_business_date": business_date(local),
                "runtime_business_date": business_date(remote),
                "chosen_source": source,
                "chosen_business_date": business_date(chosen),
                "local_write_ok": bool(ok),
                "message": write_msg if source == "runtime-data" else remote_messages.get(name, ""),
            })

        # Critical anti-rollback repair: Page0 derives its display/snapshot from
        # macro_market_close_cache.  If the checkout cache is older than the
        # elected snapshot, inject the snapshot's exchange row into the cache.
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
                    rows.append({
                        "file": name,
                        "action": "rebuilt_from_newer_market_snapshot",
                        "before_business_date": business_date(before),
                        "after_business_date": business_date(repaired),
                        "local_write_ok": bool(ok),
                        "message": msg,
                    })

        # If a manual update produced a newer local business date than runtime-data,
        # queue that newer payload to durability.  Packaged stale July files are
        # explicitly not published just because a process restarted today.
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
    "VERSION",
    "AUTHORITY_FILES",
    "business_date",
    "authority_key",
    "ensure_macro_runtime_authority_current",
    "reset_macro_authority_process_guard",
]
