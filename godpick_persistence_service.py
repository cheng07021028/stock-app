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
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
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

NAMED_FIRESTORE_DOCS = {
    "godpick_recommend_list.json": "godpick_recommend_list",
    "godpick_latest_recommendations.json": "godpick_latest_recommendations",
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


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
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


def _secret(name: str, default: str = "") -> str:
    try:
        return _safe_str(st.secrets.get(name, default))
    except Exception:
        return _safe_str(default)


def github_config() -> dict[str, str]:
    return {
        "token": _secret("GITHUB_TOKEN"),
        "owner": _secret("GITHUB_REPO_OWNER", "cheng07021028"),
        "repo": _secret("GITHUB_REPO_NAME", "stock-app"),
        "branch": _secret("GITHUB_REPO_BRANCH", "main") or "main",
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
            f"GitHub：{'成功' if self.github_ok else '失敗'}｜{self.github_message}",
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


def normalize_records(payload: Any) -> list[dict[str, Any]]:
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
    out: dict[str, dict[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        # Sanitize before Firestore/GitHub persistence.  NaN, pd.NA and numpy
        # scalars were the main reason a large record batch failed remotely.
        row = {str(k): _json_safe_value(v) for k, v in raw.items()}
        key = _record_id(row)
        if not key.strip("|"):
            continue
        old = out.get(key, {})
        merged = dict(old)
        for k, v in row.items():
            if _safe_str(v) or k not in merged:
                merged[k] = v
        if not _safe_str(merged.get("record_id")):
            merged["record_id"] = hashlib.sha1(key.encode("utf-8")).hexdigest()[:24]
        out[key] = _json_safe_value(merged)
    return sorted(
        out.values(),
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
                "updated_at": summary_data.get("updated_at_text"),
                "updated_at_utc": summary_data.get("updated_at_utc"),
                "updated_at_epoch": summary_data.get("updated_at_epoch"),
                "revision": summary_data.get("revision"),
            }
        return normalize_records(rows), "已讀取 Firestore godpick_records", latest, state
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


def save_records_permanent(payload: Any) -> PersistenceReport:
    records = normalize_records(payload)
    state = _new_state("godpick_records_durable_v3", records, count=len(records))
    report = PersistenceReport(payload_hash=state["payload_hash"], updated_at=state["updated_at"])

    ok1, msg1 = write_local_json_atomic(RECORDS_FILE, records)
    ok2, msg2 = write_local_json_atomic(RECORDS_STATE_FILE, state)
    report.local_ok = bool(ok1 and ok2)
    report.local_message = f"{msg1}｜{msg2}"

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


def load_records_permanent() -> tuple[list[dict[str, Any]], list[str]]:
    local_payload, local_msg, local_mtime = read_local_json(RECORDS_FILE, [])
    local_state, local_state_msg, _ = read_local_json(RECORDS_STATE_FILE, {})

    gh_path = _secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE
    gh_payload, gh_msg = read_github_json(gh_path, [])
    gh_state, gh_state_msg = read_github_json(RECORDS_STATE_FILE, {})

    fs_payload, fs_msg, fs_mtime, fs_state = _read_records_firestore_full()
    candidates = [
        ("local", normalize_records(local_payload), local_state, local_mtime),
        ("github", normalize_records(gh_payload), gh_state, datetime.min),
        ("firestore", normalize_records(fs_payload), fs_state, fs_mtime),
    ]
    valid = []
    for source, rows, state, fallback in candidates:
        if _state_is_valid(rows, state):
            valid.append((source, rows, state, _state_epoch(state, fallback)))

    if valid:
        source, chosen, chosen_state, _ = max(
            valid,
            key=lambda item: (item[3], int((item[2] or {}).get("revision") or 0), {"local": 0, "github": 1, "firestore": 2}.get(item[0], 0)),
        )
    else:
        merged: list[dict[str, Any]] = []
        for _, rows, _, _ in candidates:
            merged.extend(rows)
        chosen = normalize_records(merged)
        source = "legacy_merge"
        chosen_state = _new_state("godpick_records_durable_v2", chosen, count=len(chosen), migrated_from=source)

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
    valid = []
    for source, data, state, fallback in candidates:
        if _state_is_valid(data, state):
            valid.append((source, data, state, _state_epoch(state, fallback)))
    if valid:
        source, chosen, chosen_state, _ = max(
            valid,
            key=lambda item: (item[3], int((item[2] or {}).get("revision") or 0), {"local": 0, "github": 1, "firestore": 2}.get(item[0], 0)),
        )
    else:
        source, chosen = next(
            ((src, data) for src, data in [("firestore", fs_payload), ("github", gh_payload), ("local", local_payload)] if data not in (None, {}, [])),
            ("default", copy.deepcopy(default)),
        )
        chosen_state = _new_state("named_json_durable_v2", chosen, path=path_name, migrated_from=source)

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
