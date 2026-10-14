# -*- coding: utf-8 -*-
"""H111: synchronous, remotely verified scan-setting authority.

Only the Page 7 scan form uses this small separate JSON document. It never
writes the legacy weight/record JSON to GitHub, so independent background
writers cannot roll back the scan preferences after a reboot.
"""
from __future__ import annotations

import copy
import json
import os
import secrets as secure_random
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import requests

SCHEMA = "stockapp.h111.scan-settings.v1"
REMOTE_PATH = "data/godpick_scan_settings_authority_h111.json"
LOCAL_MIRROR = ".cache/godpick_scan_settings_authority_h111.json"


@dataclass
class AuthorityResult:
    ok: bool
    settings: dict[str, Any]
    source: str
    message: str
    revision: str = ""
    durable_verified: bool = False


def _config(provided_secrets: Mapping[str, Any] | None = None) -> dict[str, str]:
    data = provided_secrets
    if data is None:
        try:
            import streamlit as st
            data = st.secrets
        except Exception:
            data = {}
    def get(name: str, default: str = "") -> str:
        try:
            return str(data.get(name, os.getenv(name, default)) or default).strip()
        except Exception:
            return str(os.getenv(name, default)).strip()
    return {
        "token": get("GITHUB_TOKEN"),
        "owner": get("GITHUB_REPO_OWNER", "cheng07021028"),
        "repo": get("GITHUB_REPO_NAME", "stock-app"),
        "branch": get("GITHUB_REPO_BRANCH", "main"),
    }


def _url(cfg: Mapping[str, str]) -> str:
    repo_path = urllib.parse.quote(REMOTE_PATH, safe="/")
    return f"https://api.github.com/repos/{urllib.parse.quote(cfg['owner'])}/{urllib.parse.quote(cfg['repo'])}/contents/{repo_path}"


def _headers(cfg: Mapping[str, str]) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {cfg['token']}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _remote_get(cfg: dict[str, str], transport=requests) -> tuple[int, dict[str, Any], str, str]:
    """HTTP status, document, blob sha, error (never exposes credentials)."""
    if not cfg.get("token"):
        return 0, {}, "", "未設定 GITHUB_TOKEN，無法驗證跨 Reboot 永久保存"
    try:
        import base64
        r = transport.get(_url(cfg), headers=_headers(cfg), params={"ref": cfg["branch"]}, timeout=12)
        if r.status_code == 404:
            return 404, {}, "", "遠端尚未建立 H111 設定"
        if r.status_code != 200:
            return r.status_code, {}, "", f"GitHub 讀取失敗 HTTP {r.status_code}"
        raw = r.json()
        if not isinstance(raw, dict) or not raw.get("content"):
            return 422, {}, "", "GitHub 設定缺少檔案內容"
        document = json.loads(base64.b64decode(raw["content"]).decode("utf-8-sig"))
        if not isinstance(document, dict) or document.get("schema") != SCHEMA or not isinstance(document.get("settings"), dict):
            return 422, {}, "", "遠端設定格式不符，禁止覆蓋或假裝讀取成功"
        return 200, document, str(raw.get("sha", "")), ""
    except Exception as exc:
        return 0, {}, "", f"遠端設定讀取例外：{type(exc).__name__}"


def _local_path(project_root: str | Path) -> Path:
    return Path(project_root).resolve() / LOCAL_MIRROR


def _write_local(project_root: str | Path, document: dict[str, Any]) -> None:
    # Local is cache only; temporary Streamlit cloud storage is not a durable source.
    p = _local_path(project_root)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _read_local(project_root: str | Path) -> dict[str, Any]:
    try:
        p = _local_path(project_root)
        d = json.loads(p.read_text(encoding="utf-8-sig"))
        return d if isinstance(d, dict) and d.get("schema") == SCHEMA and isinstance(d.get("settings"), dict) else {}
    except Exception:
        return {}


def load_scan_settings(*, project_root: str | Path, legacy_settings: Any = None,
                       provided_secrets: Mapping[str, Any] | None = None,
                       transport=requests) -> AuthorityResult:
    cfg = _config(provided_secrets)
    code, doc, _sha, msg = _remote_get(cfg, transport)
    if code == 200:
        try:
            _write_local(project_root, doc)
        except Exception:
            pass
        return AuthorityResult(True, copy.deepcopy(doc["settings"]), "github_verified", "H111 遠端永久設定已載入", str(doc.get("revision", "")), True)
    local = _read_local(project_root)
    if code == 404:
        # First rollout: use existing last-known user preferences as migration
        # input, but never label them remotely backed up until Apply succeeds.
        if local:
            return AuthorityResult(True, copy.deepcopy(local["settings"]), "local_unverified", "尚未建立遠端權威；目前僅有本機快取，請按套用設定完成遷移", str(local.get("revision", "")))
        return AuthorityResult(True, copy.deepcopy(legacy_settings) if isinstance(legacy_settings, dict) else {}, "legacy_migration", "尚未建立 H111 遠端權威；請按套用設定完成首次保存")
    # Remote unreachable: do NOT silently load older baked-in repository JSON.
    if local:
        return AuthorityResult(False, copy.deepcopy(local["settings"]), "local_unverified", f"{msg}；暫顯本機快取，不能保證 Reboot 後保留", str(local.get("revision", "")))
    return AuthorityResult(False, copy.deepcopy(legacy_settings) if isinstance(legacy_settings, dict) else {}, "legacy_unverified", f"{msg}；暫顯舊設定，不能保證 Reboot 後保留")


def save_scan_settings(settings: dict[str, Any], *, project_root: str | Path,
                       provided_secrets: Mapping[str, Any] | None = None,
                       transport=requests) -> AuthorityResult:
    if not isinstance(settings, dict) or not settings:
        return AuthorityResult(False, {}, "none", "拒絕保存空白或格式錯誤的推薦設定")
    cfg = _config(provided_secrets)
    if not cfg["token"]:
        return AuthorityResult(False, copy.deepcopy(settings), "local_unverified", "永久保存失敗：未設定 GITHUB_TOKEN；沒有把暫存寫入當成成功")
    document = {
        "schema": SCHEMA,
        "revision": secure_random.token_hex(12),
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "settings": copy.deepcopy(settings),
    }
    import base64
    payload = base64.b64encode(json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).decode("ascii")
    for attempt in range(3):
        code, old, sha, msg = _remote_get(cfg, transport)
        if code not in (200, 404):
            return AuthorityResult(False, copy.deepcopy(settings), "remote_error", f"永久保存失敗：{msg}")
        body = {
            "message": "H111 save verified scan preferences",
            "content": payload,
            "branch": cfg["branch"],
        }
        if sha:
            body["sha"] = sha
        try:
            r = transport.put(_url(cfg), headers=_headers(cfg), json=body, timeout=18)
        except Exception as exc:
            return AuthorityResult(False, copy.deepcopy(settings), "remote_error", f"永久保存失敗：GitHub 寫入例外 {type(exc).__name__}")
        if r.status_code in (409, 422) and attempt < 2:
            continue  # Concurrent update; re-read SHA, never overwrite blindly.
        if r.status_code not in (200, 201):
            return AuthorityResult(False, copy.deepcopy(settings), "remote_error", f"永久保存失敗：GitHub HTTP {r.status_code}")
        # A queued task / a success HTTP code is not enough. Verify readback.
        for _ in range(3):
            code2, got, _sha2, _msg2 = _remote_get(cfg, transport)
            if code2 == 200 and got.get("revision") == document["revision"] and got.get("settings") == settings:
                try:
                    _write_local(project_root, document)
                except Exception:
                    pass  # Cache failure cannot negate verified remote persistence.
                return AuthorityResult(True, copy.deepcopy(settings), "github_verified", "GitHub 已同步寫入並回讀驗證；Reboot 可由遠端恢復", document["revision"], True)
            time.sleep(0.15)
        return AuthorityResult(False, copy.deepcopy(settings), "remote_unverified", "GitHub 已接受寫入，但回讀未證實一致；不可視為永久保存成功", document["revision"])
    return AuthorityResult(False, copy.deepcopy(settings), "remote_error", "GitHub 設定並行衝突；重試仍失敗")
