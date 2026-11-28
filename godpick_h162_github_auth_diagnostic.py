# -*- coding: utf-8 -*-
"""V191-H162: safe GitHub credential/branch diagnostic for Page 17.

Never prints, persists, hashes, logs, or returns credential text. A manual write
probe ONLY creates a random small JSON file in the runtime-data branch, verifies
readback, and attempts to delete it. It never touches stock or authority files.
"""
from __future__ import annotations

import base64
import json
import os
import re
import uuid
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

import requests

VERSION = "V191-H250-R11-DIAG"
_PROBE_PREFIX = "data/.h162_auth_probe_"
_TIMEOUT = (3, 12)


def _get(source: Any, key: str, default: str = "") -> str:
    try:
        return str((source or {}).get(key, default) or default).strip()
    except Exception:
        return default


def resolve_config(*, secrets: Mapping | None = None, environ: Mapping | None = None) -> dict[str, Any]:
    """Match Page08 H162: Secrets token wins; runtime branch env safety guard wins."""
    if secrets is None:
        try:
            import streamlit as st
            secrets = st.secrets
        except Exception:
            secrets = {}
    if environ is None:
        environ = os.environ
    secret_token = _get(secrets, "GITHUB_TOKEN")
    env_token = _get(environ, "GITHUB_TOKEN")
    token = secret_token or env_token
    source = "Streamlit Secrets" if secret_token else ("Environment" if env_token else "未設定")
    owner = _get(environ, "GITHUB_REPO_OWNER") or _get(secrets, "GITHUB_REPO_OWNER") or "cheng07021028"
    repo = _get(environ, "GITHUB_REPO_NAME") or _get(secrets, "GITHUB_REPO_NAME") or "stock-app"
    branch = (_get(environ, "GITHUB_RUNTIME_DATA_BRANCH")
              or _get(secrets, "GITHUB_RUNTIME_DATA_BRANCH")
              or _get(environ, "GITHUB_REPO_BRANCH")
              or _get(secrets, "GITHUB_REPO_BRANCH")
              or "runtime-data")
    return {
        "token": token,
        "source": source,
        "owner": owner,
        "repo": repo,
        "branch": branch,
        "secrets_present": bool(secret_token),
        "env_present": bool(env_token),
        "sources_match": bool(secret_token and env_token and secret_token == env_token)
                         if (secret_token and env_token) else None,
        "sources_conflict": bool(secret_token and env_token and secret_token != env_token),
    }


def public_config(config: Mapping) -> dict[str, Any]:
    """Only whitelisted, non-secret output; never return token or its fingerprint."""
    return {
        "憑證來源": str(config.get("source") or "未設定"),
        "Secrets有設定": bool(config.get("secrets_present")),
        "環境變數有設定": bool(config.get("env_present")),
        "兩處憑證不同": bool(config.get("sources_conflict")),
        "儲存庫": f"{config.get('owner', '')}/{config.get('repo', '')}",
        "資料分支": str(config.get("branch") or ""),
    }


def _valid(config: Mapping) -> tuple[bool, str]:
    token = str(config.get("token") or "")
    if not token or token in {"*", "****", "*****"}:
        return False, "找不到有效的 GITHUB_TOKEN；請確認 Secrets 內是實際 Token，不是遮罩符號。"
    if (not re.fullmatch(r"[a-zA-Z0-9_.-]+", str(config.get("owner") or "")) or
            not re.fullmatch(r"[a-zA-Z0-9_.-]+", str(config.get("repo") or ""))):
        return False, "儲存庫名稱格式錯誤，已停止請求。"
    if str(config.get("branch") or "") != "runtime-data":
        return False, "資料分支不是 runtime-data；為避免誤寫主程式分支，已停止授權測試。"
    return True, ""


def _headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "StockApp-H162-safe-auth-check",
    }


def _base(config: Mapping) -> str:
    return "https://api.github.com/repos/" + quote(str(config["owner"]), safe="") + "/" + quote(str(config["repo"]), safe="")


def _denial_detail(response: Any) -> str:
    """Only fixed classifications and numeric allowlisted headers leave this function.
    Never render a response body, arbitrary header, or credential. No automatic retry.
    """
    from datetime import datetime, timezone, timedelta
    code = int(getattr(response, "status_code", 0))
    try:
        obj = response.json()
        msg = str(obj.get("message", "")).lower() if isinstance(obj, dict) else ""
    except Exception:
        msg = ""
    try:
        h = {str(k).lower(): str(v) for k, v in response.headers.items()}
    except Exception:
        h = {}
    def number(name):
        v = h.get(name, "")
        return int(v) if re.fullmatch(r"[0-9]{1,12}", v) else None
    remain = number("x-ratelimit-remaining")
    reset = number("x-ratelimit-reset")
    retry = number("retry-after")
    if code == 401:
        reason = "AUTH_INVALID：認證未通過，核對生效 Token"
    elif code in (403, 429) and remain == 0:
        reason = "PRIMARY_RATE_LIMIT：主要 API 配額耗盡"
    elif code in (403, 429) and ("secondary rate limit" in msg or "abuse detection" in msg):
        reason = "SECONDARY_RATE_LIMIT：短時間請求／寫入限制"
    elif code in (403, 429) and (retry is not None or code == 429 or "rate limit" in msg):
        reason = "RATE_LIMIT：服務要求暫停請求"
    elif code == 403 and ("resource not accessible" in msg or "insufficient permission" in msg or "permission denied" in msg or "must have push access" in msg):
        reason = "PERMISSION_DENIED：生效憑證未獲此操作權限"
    elif code in (403, 409, 422) and any(x in msg for x in ("repository rule", "protected branch", "branch protection", "rule violation", "gh013")):
        reason = "REPOSITORY_RULE：儲存庫規則或分支限制"
    elif code == 403 and ("saml" in msg or "sso" in msg):
        reason = "SSO_REQUIRED：需核對組織授權"
    elif code == 403:
        reason = "FORBIDDEN_UNCLASSIFIED：拒絕原因未明，不可直接判定權限不足"
    elif code == 404:
        reason = "NOT_FOUND：路徑不可見或不存在"
    elif code == 409:
        reason = "CONFLICT：版本衝突"
    elif code == 422:
        reason = "VALIDATION_FAILED：請求驗證未通過"
    else:
        reason = "HTTP_ERROR：遠端操作未通過"
    parts = [reason]
    if remain is not None:
        parts.append(f"API剩餘 {remain}")
    if retry is not None:
        parts.append(f"Retry-After {retry} 秒")
    if reset is not None:
        try:
            parts.append("配額重置（台灣時間）" + datetime.fromtimestamp(reset, timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S"))
        except (ValueError, OSError, OverflowError):
            pass
    if reason.startswith(("PRIMARY_RATE_LIMIT", "SECONDARY_RATE_LIMIT", "RATE_LIMIT")):
        parts.append("暫停重試，依限流時間恢復；未提供時間時至少等待1分鐘")
    return "｜".join(parts)


def _result_item(label: str, status: int | str, detail: str) -> dict[str, str]:
    return {"檢查": label, "HTTP": str(status), "結果": detail}


def run_readonly_probe(*, secrets: Mapping | None = None, environ: Mapping | None = None,
                       transport: Any = requests) -> dict[str, Any]:
    """Read-only proof of the active credential/branch. No records are downloaded."""
    cfg = resolve_config(secrets=secrets, environ=environ)
    safe = public_config(cfg)
    ok, reason = _valid(cfg)
    if not ok:
        return {"ok": False, "config": safe, "steps": [], "message": reason}
    items = []
    url = _base(cfg)
    headers = _headers(cfg["token"])
    for label, endpoint in (
        ("儲存庫授權讀取", url),
        ("runtime-data 分支讀取", url + "/branches/runtime-data"),
    ):
        try:
            response = transport.get(endpoint, headers=headers, timeout=_TIMEOUT)
            code = int(getattr(response, "status_code", 0))
            if code == 200:
                text = "可讀取"
            elif code in (401, 403, 429):
                text = _denial_detail(response)
            elif code == 404:
                text = "儲存庫／分支不存在或目前憑證不可見"
            else:
                text = "GitHub 請求未成功；檢查網路、限制或回應狀態"
            items.append(_result_item(label, code, text))
        except Exception as exc:
            items.append(_result_item(label, "ERR", "連線例外：" + type(exc).__name__))
    succeeded = len(items) == 2 and all(row["HTTP"] == "200" for row in items)
    return {
        "ok": succeeded, "config": safe, "steps": items,
        "message": "唯讀授權檢查成功；仍須按寫入測試才能證明 Contents: Write。" if succeeded
                   else "唯讀授權檢查未全過，未進行任何寫入。",
    }


def run_write_probe(*, secrets: Mapping | None = None, environ: Mapping | None = None,
                    transport: Any = requests) -> dict[str, Any]:
    """Explicit tiny Contents API write + readback + delete. Fail closed.

    The generated path is unique, always under data/.h162_auth_probe_*.json;
    an unsuccessful cleanup is conspicuously reported to the operator.
    """
    cfg = resolve_config(secrets=secrets, environ=environ)
    safe = public_config(cfg)
    ok, reason = _valid(cfg)
    if not ok:
        return {"ok": False, "config": safe, "steps": [], "message": reason}
    checked = run_readonly_probe(secrets=secrets, environ=environ, transport=transport)
    if not checked["ok"]:
        checked["message"] = "唯讀診斷未通過；禁止建立測試檔。"
        return checked

    nonce = uuid.uuid4().hex
    path = _PROBE_PREFIX + nonce + ".json"
    url = _base(cfg) + "/contents/" + quote(path, safe="/")
    headers = _headers(cfg["token"])
    data = {"purpose": "V191-H162 disposable github-write probe", "nonce": nonce}
    plain = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    body = {
        "message": "H162 temporary auth probe (create; auto-delete)",
        "branch": "runtime-data",
        "content": base64.b64encode(plain).decode("ascii"),
    }
    steps = list(checked["steps"])
    created = False
    verified = False
    removed = False
    sha = ""
    cleanup_error = ""
    try:
        try:
            put = transport.put(url, headers=headers, json=body, timeout=_TIMEOUT)
            put_code = int(getattr(put, "status_code", 0))
            created = put_code in (200, 201)
            steps.append(_result_item("小型測試檔建立", put_code,
                                     "已接受，將回讀驗證並清除" if created else
                                     "Contents 寫入失敗，未變更推薦紀錄｜" + _denial_detail(put)))
        except Exception as exc:
            steps.append(_result_item("小型測試檔建立", "ERR", "連線例外：" + type(exc).__name__))
        if created:
            try:
                check = transport.get(url, headers=headers, params={"ref": "runtime-data"}, timeout=_TIMEOUT)
                status = int(getattr(check, "status_code", 0))
                if status == 200:
                    meta = check.json()
                    sha = str(meta.get("sha") or "") if isinstance(meta, dict) else ""
                    raw = str(meta.get("content") or "") if isinstance(meta, dict) else ""
                    try:
                        verified = bool(sha and base64.b64decode(raw).strip() == plain)
                    except Exception:
                        verified = False
                steps.append(_result_item("測試檔遠端回讀", status,
                                          "內容完全一致" if verified else ("內容或 SHA 未驗證，不可宣稱寫入成功" + ("｜" + _denial_detail(check) if status != 200 else ""))))
            except Exception as exc:
                steps.append(_result_item("測試檔遠端回讀", "ERR", "連線例外：" + type(exc).__name__))
    finally:
        # Cleanup is mandatory if the initial PUT was accepted; the test never
        # deletes records, settings, existing files, or other peoples' probes.
        if created:
            if not sha:
                try:
                    response = transport.get(url, headers=headers, params={"ref": "runtime-data"}, timeout=_TIMEOUT)
                    if int(getattr(response, "status_code", 0)) == 200:
                        meta = response.json()
                        sha = str(meta.get("sha") or "") if isinstance(meta, dict) else ""
                except Exception:
                    pass
            if sha:
                try:
                    delete = transport.delete(
                        url, headers=headers,
                        json={"message": "H162 remove temporary auth probe", "sha": sha, "branch": "runtime-data"},
                        timeout=_TIMEOUT,
                    )
                    removed = int(getattr(delete, "status_code", 0)) in (200, 204)
                    if removed:
                        check = transport.get(url, headers=headers, params={"ref": "runtime-data"}, timeout=_TIMEOUT)
                        removed = int(getattr(check, "status_code", 0)) == 404
                    steps.append(_result_item("測試檔清除回讀", "200" if removed else "FAILED",
                                              "已確認不存在" if removed else ("未確認清除，需人工檢查專用探針路徑" + ("｜" + _denial_detail(delete) if int(getattr(delete, "status_code", 0)) not in (200, 204) else ""))))
                except Exception as exc:
                    cleanup_error = type(exc).__name__
                    steps.append(_result_item("測試檔清除回讀", "ERR", "清除例外：" + cleanup_error))
            else:
                steps.append(_result_item("測試檔清除回讀", "FAILED", "無法取得測試檔 SHA，請人工檢查專用探針路徑"))
    succeeded = created and verified and removed
    return {
        "ok": succeeded, "config": safe, "steps": steps,
        "message": ("GitHub Contents 權限已完成小型寫入、回讀、清除驗證。"
                    if succeeded else "寫入或回讀／清除未完全成功；不代表推薦紀錄可以永久保存。"),
        "probe_path": "" if removed or not created else path,
    }
