# -*- coding: utf-8 -*-
"""H10: verify the unattended scheduler heartbeat/history reached runtime-data.

Scheduled tasks already persist their own authorities through the production
CAS/durability services.  The old workflow then copied every runtime file into
a second git branch commit, creating an untracked-file checkout failure and a
stale-overwrite race.  This verifier only confirms the scheduler control plane
and performs a bounded repair when the remote copy is older.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import os
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from godpick_auto_scheduler import STATUS_FILE, HISTORY_FILE


def _read_remote(path: str, default: Any):
    from godpick_persistence_service import read_github_json
    return read_github_json(path, default)


def _save_remote(path: str, payload: Any):
    from godpick_persistence_service import save_named_json_permanent
    return save_named_json_permanent(path, payload)


def _read_local(rel: str, default: Any) -> Any:
    p = ROOT / rel
    try:
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def _clock(payload: Any, *fields: str) -> str:
    if not isinstance(payload, dict):
        return ""
    for field in fields:
        value = str(payload.get(field) or "").strip()
        if value:
            return value
    return ""


def _history_records(payload: Any) -> list[dict[str, Any]]:
    rows = payload.get("records", []) if isinstance(payload, dict) else payload
    return [x for x in (rows or []) if isinstance(x, dict)] if isinstance(rows, list) else []


def _history_key(row: dict[str, Any]) -> str:
    return str(row.get("run_key") or "|").strip()


def _history_last_clock(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    row = rows[-1]
    return str(row.get("finished_at") or row.get("started_at") or "")


def _merge_history(local_payload: Any, remote_payload: Any) -> dict[str, Any]:
    merged: dict[str, dict[str, Any]] = {}
    extras: list[dict[str, Any]] = []
    for row in [*_history_records(remote_payload), *_history_records(local_payload)]:
        key = _history_key(row)
        if key and key != "|":
            merged[key] = dict(row)
        else:
            extras.append(dict(row))
    rows = list(merged.values()) + extras
    rows.sort(key=lambda r: str(r.get("finished_at") or r.get("started_at") or ""))
    version = ""
    for payload in (local_payload, remote_payload):
        if isinstance(payload, dict) and payload.get("version"):
            version = str(payload.get("version"))
            break
    return {
        "version": version or "godpick_auto_scheduler_v191_h10_history",
        "updated_at": _history_last_clock(rows),
        "records": rows[-400:],
    }


def verify_status() -> tuple[bool, str]:
    local = _read_local(STATUS_FILE, {})
    remote, msg = _read_remote(STATUS_FILE, {})
    if not isinstance(local, dict) or not local:
        return False, "local scheduler status missing"
    local_wake = _clock(local, "last_wakeup_at")
    remote_wake = _clock(remote, "last_wakeup_at")
    local_updated = _clock(local, "updated_at", "last_wakeup_at")
    remote_updated = _clock(remote, "updated_at", "last_wakeup_at")
    expected_run_id = str(os.environ.get("GODPICK_EXPECTED_WAKEUP_RUN_ID") or "").strip()
    local_run_id = str(local.get("last_wakeup_run_id") or "").strip()
    remote_run_id = str((remote or {}).get("last_wakeup_run_id") or "").strip() if isinstance(remote, dict) else ""
    # H27b: only the central scheduler workflow opts into strict current-run verification;
    # heartbeat, not merely accept an older remote status that happens to be >= an
    # equally stale restored local file.
    if expected_run_id and local_run_id != expected_run_id:
        return False, f"local scheduler heartbeat is not current workflow｜expected_run={expected_run_id} local_run={local_run_id or '-'} wake={local_wake or '-'}"
    if (
        isinstance(remote, dict) and remote
        and remote_wake >= local_wake and remote_updated >= local_updated
        and (not expected_run_id or remote_run_id == expected_run_id)
    ):
        return True, f"status remote confirmed｜wake={remote_wake}｜run={remote_run_id or '-'}"
    report = _save_remote(STATUS_FILE, local)
    remote2, _ = _read_remote(STATUS_FILE, {})
    remote2_run_id = str((remote2 or {}).get("last_wakeup_run_id") or "").strip() if isinstance(remote2, dict) else ""
    ok = bool(
        isinstance(remote2, dict)
        and _clock(remote2, "last_wakeup_at") >= local_wake
        and _clock(remote2, "updated_at", "last_wakeup_at") >= local_updated
        and (not expected_run_id or remote2_run_id == expected_run_id)
    )
    return ok, f"status repaired={ok}｜run={remote2_run_id or '-'}｜{getattr(report, 'github_message', '')}｜probe={msg}"


def verify_history() -> tuple[bool, str]:
    local = _read_local(HISTORY_FILE, {})
    remote, msg = _read_remote(HISTORY_FILE, {})
    lrows = _history_records(local)
    rrows = _history_records(remote)
    if not lrows:
        return True, "history local empty; nothing to verify"
    latest_key = _history_key(lrows[-1])
    remote_keys = {_history_key(x) for x in rrows[-500:]}
    if latest_key and latest_key in remote_keys:
        return True, f"history remote confirmed｜latest={latest_key}"
    # Never overwrite a newer concurrent history with a stale local snapshot.
    merged = _merge_history(local, remote)
    report = _save_remote(HISTORY_FILE, merged)
    remote2, _ = _read_remote(HISTORY_FILE, {})
    keys2 = {_history_key(x) for x in _history_records(remote2)[-500:]}
    ok = bool(latest_key and latest_key in keys2)
    return ok, f"history merged/repaired={ok}｜{getattr(report, 'github_message', '')}｜probe={msg}"


def main() -> int:
    s_ok, s_msg = verify_status()
    h_ok, h_msg = verify_history()
    print(json.dumps({"status_ok": s_ok, "status": s_msg, "history_ok": h_ok, "history": h_msg}, ensure_ascii=False, indent=2))
    return 0 if (s_ok and h_ok) else 2


if __name__ == "__main__":
    raise SystemExit(main())
