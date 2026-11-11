# -*- coding: utf-8 -*-
"""H153.8: read-only, business-date-aware runtime-data recovery.

The public GitHub `main` branch carries July demonstration snapshots.  After a
Streamlit reboot it is *not* a valid authority for current market/factor data.
This module reads only whitelisted paths from the configured `runtime-data`
branch, validates their business date and preserves the newer valid local data.
It never writes to GitHub, never changes trading permissions, and cannot turn
an old snapshot into a current one by using its filesystem/updated timestamp.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Any, Callable
from urllib.parse import quote
from zoneinfo import ZoneInfo
import json
import os
import re
import time

import requests

VERSION = "V191-H153.8-reboot-business-date-authority"
ALLOWED = {"market_snapshot.json", "macro_mode_bridge.json", "official_factors_cache.json"}
_TZ = ZoneInfo("Asia/Taipei")
_LOCK = RLock()
_REMOTE_CACHE: dict[tuple[str, str, str, str], tuple[float, dict[str, Any] | None, str]] = {}
_REPO_PUBLIC_CACHE: dict[tuple[str, str], tuple[float, bool]] = {}
_LAST_STATUS: dict[str, dict[str, Any]] = {}
_MAX_BYTES = 12 * 1024 * 1024


def _today() -> date:
    return datetime.now(_TZ).date()


def date_of(value: Any) -> date | None:
    """Accept only business dates, never filesystem mtime or fetched_at."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        try:
            value = str(int(value))
        except (ValueError, OverflowError):
            return None
    s = str(value or "").strip()
    if not s:
        return None
    try:
        if re.fullmatch(r"\d{8}", s):
            return datetime.strptime(s, "%Y%m%d").date()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return date.fromisoformat(s)
    except ValueError:
        return None
    return None


def business_date(kind: str, obj: Any) -> date | None:
    if not isinstance(obj, dict):
        return None
    if kind in ("market_snapshot.json", "macro_mode_bridge.json"):
        req = obj.get("required_by_godpick")
        req = req if isinstance(req, dict) else {}
        # A saved date is *not* a market data date.
        fields = (obj.get("twse_data_date"), obj.get("market_date"), obj.get("data_date"),
                  req.get("market_date"), req.get("data_date"))
        return next((d for v in fields if (d := date_of(v)) is not None), None)
    if kind == "official_factors_cache.json":
        records = obj.get("records")
        if not isinstance(records, list) or not records:
            return None
        meta = obj.get("meta") if isinstance(obj.get("meta"), dict) else {}
        return date_of(obj.get("data_date")) or date_of(meta.get("data_date"))
    return None


def _valid_date(kind: str, obj: Any, *, today: date) -> date | None:
    d = business_date(kind, obj)
    return d if d is not None and date(2020, 1, 1) <= d <= today else None


def _repo_config() -> tuple[str, str, str, str]:
    def setting(name: str, default: str) -> str:
        s = os.getenv(name, "").strip()
        if not s:
            try:
                import streamlit as st
                s = str(st.secrets.get(name, "") or "").strip()
            except Exception:
                pass
        return s or default
    owner = setting("GITHUB_REPO_OWNER", "cheng07021028")
    repo = setting("GITHUB_REPO_NAME", "stock-app")
    branch = setting("GITHUB_RUNTIME_DATA_BRANCH", "runtime-data")
    token = setting("GITHUB_TOKEN", "")
    if not all(re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", x) for x in (owner, repo)):
        raise ValueError("Invalid configured repository")
    if not re.fullmatch(r"[A-Za-z0-9_./-]{1,130}", branch) or ".." in branch:
        raise ValueError("Invalid configured runtime branch")
    return owner, repo, branch, token


def _repo_is_public(owner: str, repo: str) -> bool:
    k = (owner, repo)
    with _LOCK:
        prev = _REPO_PUBLIC_CACHE.get(k)
        if prev and prev[0] > time.monotonic():
            return prev[1]
    try:
        r = requests.get(f"https://api.github.com/repos/{owner}/{repo}", timeout=(2, 3),
                         headers={"Accept": "application/vnd.github+json"})
        ok = r.status_code == 200 and r.json().get("private") is False
    except Exception:
        ok = False
    with _LOCK:
        _REPO_PUBLIC_CACHE[k] = (time.monotonic() + (600 if ok else 90), ok)
    return ok


def fetch_runtime_json(kind: str, *, force: bool = False) -> tuple[dict[str, Any] | None, str]:
    """Bounded read from public GitHub or authenticated private repository.

    No token is necessary for READS of a confirmed public repo; it *is* needed
    for writing private/authoritative settings from the app.
    """
    if kind not in ALLOWED:
        return None, "UNSUPPORTED_FILE"
    try:
        owner, repo, branch, token = _repo_config()
    except Exception as exc:
        return None, f"CONFIG_ERROR:{type(exc).__name__}"
    key = (owner, repo, branch, kind)
    with _LOCK:
        entry = _REMOTE_CACHE.get(key)
        if entry and entry[0] > time.monotonic() and not force:
            return entry[1], entry[2]
    if not token and not _repo_is_public(owner, repo):
        out = (None, "PRIVATE_REPOSITORY_NEEDS_TOKEN_OR_NETWORK_CHECK")
    else:
        try:
            url = (f"https://raw.githubusercontent.com/{quote(owner)}/{quote(repo)}/"
                   f"{quote(branch, safe='/')}/{quote(kind)}")
            headers = {"Accept": "application/json", "User-Agent": "StockApp-H1538-readonly"}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            r = requests.get(url, timeout=(3, 8), headers=headers, stream=True)
            if r.status_code != 200:
                out = (None, f"RUNTIME_READ_HTTP_{r.status_code}")
            else:
                blocks: list[bytes] = []
                size = 0
                for chunk in r.iter_content(128 * 1024):
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > _MAX_BYTES:
                        raise ValueError("snapshot exceeds 12 MiB safety limit")
                    blocks.append(chunk)
                obj = json.loads(b"".join(blocks).decode("utf-8-sig"))
                out = (obj, "RUNTIME_REMOTE_OK") if isinstance(obj, dict) else (None, "INVALID_JSON_ROOT")
            r.close()
        except Exception as exc:
            out = (None, f"RUNTIME_READ_ERROR:{type(exc).__name__}")
    with _LOCK:
        _REMOTE_CACHE[key] = (time.monotonic() + (180 if out[0] else 45), *out)
    return out


def select_authority(kind: str, local: Any, remote: Any, *, today: date | None = None,
                     quarantine_old: bool = True) -> dict[str, Any]:
    """Pure selector: no regression to a smaller business date or fake success.

    A >10 calendar day packaged snapshot without durable proof is diagnostics,
    not tradable data.  Same-date data stay local unless local is empty.
    """
    if kind not in ALLOWED:
        raise ValueError(f"Unsupported authority file: {kind}")
    today = today or _today()
    ld = _valid_date(kind, local, today=today)
    rd = _valid_date(kind, remote, today=today)
    winner: dict[str, Any] = {}
    source = "unverified"
    if rd and (not ld or rd > ld):
        winner = remote
        source = "github-runtime-data-readonly"
    elif ld:
        winner = local
        source = "local"
    elif rd:
        winner = remote
        source = "github-runtime-data-readonly"
    stale = bool(business_date(kind, local) and not ld)
    if winner and quarantine_old and _valid_date(kind, winner, today=today) < today - timedelta(days=10):
        winner = {}
        source = "stale-quarantined"
        stale = True
    return {
        "payload": winner,
        "source": source,
        "business_date": (business_date(kind, winner).isoformat() if winner else ""),
        "local_date": ld.isoformat() if ld else "",
        "remote_date": rd.isoformat() if rd else "",
        "quarantined": stale or source == "stale-quarantined",
    }


def reconcile_json(kind: str, local_payload: Any, *, root: Path | None = None,
                   fetcher: Callable[[str], tuple[dict[str, Any] | None, str]] | None = None,
                   today: date | None = None, write_local: bool = False,
                   quarantine_old: bool = True) -> dict[str, Any]:
    """Select verified newest read-only authority, optionally atomic local copy.

    No write to the GitHub branch, records, settings, or secrets is performed.
    """
    if kind not in ALLOWED:
        raise ValueError("Only whitelisted business data can be reconciled")
    fetcher = fetcher or fetch_runtime_json
    remote, message = fetcher(kind)
    result = select_authority(kind, local_payload, remote, today=today,
                              quarantine_old=quarantine_old)
    result["remote_status"] = message
    if write_local and result["source"] == "github-runtime-data-readonly" and root:
        p = Path(root) / kind
        try:
            temp = p.with_name(p.name + ".h1538.tmp")
            temp.write_text(json.dumps(result["payload"], ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            temp.replace(p)
            result["local_restored"] = True
        except (OSError, TypeError, ValueError):
            result["local_restored"] = False
    with _LOCK:
        _LAST_STATUS[kind] = {k: v for k, v in result.items() if k != "payload"}
    return result


def last_status(kind: str) -> dict[str, Any]:
    with _LOCK:
        return dict(_LAST_STATUS.get(kind, {}))


def reset_test_cache() -> None:
    with _LOCK:
        _REMOTE_CACHE.clear()
        _REPO_PUBLIC_CACHE.clear()
        _LAST_STATUS.clear()
