# -*- coding: utf-8 -*-
"""H168: read-only GitHub Actions worker truth for Page17.

The scheduler business statuses live in runtime-data *after* its terminal
checkpoint. GitHub Actions can be RUNNING or QUEUED for minutes before that.
This small reader never dispatches, downloads records, or writes to GitHub.
"""
from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any
import requests

VERSION = "V191-H168-GITHUB-LIVE-WORKER-TRUTH"
WORKFLOW = "godpick_auto_scheduler_v191.yml"
_ACTIVE = frozenset({"queued", "pending", "waiting", "requested", "in_progress"})


def _valid_part(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", str(value or "")))


def _utc_iso(value: Any) -> datetime | None:
    try:
        d = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def read_live_worker(*, token: str, owner: str, repo: str,
                     http_get: Any = None,
                     now: datetime | None = None) -> dict[str, Any]:
    """Get authenticated, nonsecret state for the central GitHub Actions workflow.

    A 204 workflow dispatch is not completion. Only live GitHub run state is
    'occupied'; a successful data refresh still requires runtime-data checkpoints.
    Fail closed for invalid owner/repo. Never emit credentials, response bodies,
    URLs containing token, or exception strings.
    """
    safe = {"available": False, "occupied": False, "running": 0,
            "queued": 0, "recent_cancelled": 0, "runs": [],
            "message": "", "http_status": None}
    if not token:
        safe["message"] = "第17頁未取得有效的 GITHUB_TOKEN，無法查詢即時 Worker。"
        return safe
    if not (_valid_part(owner) and _valid_part(repo)):
        safe["message"] = "儲存庫 owner/repo 格式不合法，未發出請求。"
        return safe
    url = (f"https://api.github.com/repos/{owner}/{repo}/actions/"
           f"workflows/{WORKFLOW}/runs")
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer " + token,
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "StockApp-H168-Page17-worker-truth",
    }
    try:
        get = http_get or requests.get
        response = get(url, headers=headers, params={"per_page": 20}, timeout=(2, 5))
        code = int(getattr(response, "status_code", 0) or 0)
        safe["http_status"] = code
        if code != 200:
            safe["message"] = (
                f"GitHub Actions 即時狀態 HTTP {code}；"
                "無法確認是否有遠端 Worker，請檢查 Token Actions: Read 權限。"
            )
            return safe
        body = response.json()
        if not isinstance(body, dict) or not isinstance(body.get("workflow_runs"), list):
            safe["message"] = "GitHub Actions 回應結構無效，未當作 Worker 空閒。"
            return safe
    except Exception as exc:
        safe["message"] = "GitHub Actions 即時查詢例外：" + type(exc).__name__
        return safe

    now_dt = now or datetime.now(timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    now_dt = now_dt.astimezone(timezone.utc)
    runs = []
    recent_cancelled = 0
    for raw in body["workflow_runs"][:20]:
        if not isinstance(raw, dict):
            continue
        run_id = raw.get("id")
        if isinstance(run_id, bool) or not str(run_id or "").isdigit():
            continue
        status = str(raw.get("status") or "").lower().strip()
        conclusion = str(raw.get("conclusion") or "").lower().strip()
        created = str(raw.get("created_at") or "")
        created_dt = _utc_iso(created)
        age_min = None if created_dt is None else round(max(0, (now_dt - created_dt).total_seconds() / 60), 1)
        if conclusion == "cancelled" and age_min is not None and age_min <= 90:
            recent_cancelled += 1
        if status in _ACTIVE or len(runs) < 6:
            runs.append({
                "run_id": str(run_id),
                "status": status,
                "conclusion": conclusion,
                "event": str(raw.get("event") or "")[:40],
                "created_at_utc": created,
                "age_minutes": age_min,
                "url": f"https://github.com/{owner}/{repo}/actions/runs/{run_id}",
            })
    safe["available"] = True
    safe["runs"] = runs[:20]
    safe["running"] = sum(x["status"] == "in_progress" for x in runs)
    safe["queued"] = sum(x["status"] in _ACTIVE - {"in_progress"} for x in runs)
    safe["occupied"] = bool(safe["running"] or safe["queued"])
    safe["recent_cancelled"] = recent_cancelled
    if safe["running"]:
        safe["message"] = (
            f"GitHub Actions 有 {safe['running']} 個中央 Worker 執行中"
            + (f"、{safe['queued']} 個排隊中。" if safe["queued"] else "。")
            + "runtime-data 的上一筆 checkpoint 可能尚未更新；不可因此重複派送。"
        )
    elif safe["queued"]:
        safe["message"] = (
            f"GitHub Actions 有 {safe['queued']} 個中央 Worker 排隊中；"
            "尚不能稱為已執行。避免每8分鐘重複派送。"
        )
    else:
        safe["message"] = "目前查無執行中／排隊中的中央 Worker；仍須依排程資料判定是否有工作到期。"
    return safe


def should_autodispatch(due: bool, live: dict[str, Any] | None) -> bool:
    """Never enqueue another Page17 H19 wake over a confirmed active run.

    If the live API is unavailable, retain the original H19 due/cooldown logic
    rather than treating an unavailable API as proof of a healthy worker.
    """
    if not due:
        return False
    if isinstance(live, dict) and live.get("available") and live.get("occupied"):
        return False
    return True


__all__ = ["VERSION", "WORKFLOW", "read_live_worker", "should_autodispatch"]
