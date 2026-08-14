# -*- coding: utf-8 -*-
"""V191-H19 external wake helper for Page17.

This module deliberately contains no Streamlit code and never executes scheduler
business jobs itself.  It only submits one GitHub Actions workflow_dispatch so
07/08/09 continue to run in the existing unattended worker environment.
"""
from __future__ import annotations

from typing import Any, Callable
import requests

VERSION = "godpick_scheduler_wakeup_service_v191_h19_20260814"
WORKFLOW_FILE = "godpick_auto_scheduler_v191.yml"


def dispatch_scheduler_wakeup(
    *,
    token: str,
    owner: str,
    repo: str,
    branch: str = "main",
    wakeup_source: str = "page17_auto_catchup",
    timeout_seconds: int = 12,
    http_post: Callable[..., Any] | None = None,
) -> tuple[bool, str]:
    """Submit one workflow_dispatch without exposing the token in URL/log text."""
    token = str(token or "").strip()
    owner = str(owner or "").strip()
    repo = str(repo or "").strip()
    branch = str(branch or "main").strip() or "main"
    wakeup_source = str(wakeup_source or "page17_auto_catchup").strip()[:80] or "page17_auto_catchup"
    if not token:
        return False, "未設定 GITHUB_TOKEN，無法由 Page17 自動補送 GitHub workflow_dispatch；Windows/GitHub 原排程仍會繼續喚醒。"
    if not owner or not repo:
        return False, "GitHub repository 設定不完整，Page17 未送出補跑喚醒。"

    post = http_post or requests.post
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "GodPick-V191-H19-Page17",
    }
    try:
        response = post(
            url,
            headers=headers,
            json={"ref": branch, "inputs": {"wakeup_source": wakeup_source}},
            timeout=max(3, min(int(timeout_seconds or 12), 30)),
        )
        code = int(getattr(response, "status_code", 0) or 0)
        if code == 204:
            return True, "已送出 Page17 自動補跑喚醒；GitHub worker 啟動後，即時狀態區會自動更新目前執行項目。"
        return False, f"Page17 自動補跑喚醒未送出：GitHub HTTP {code or 'unknown'}。不會在 Streamlit 內重跑大型工作，避免重複執行。"
    except Exception as exc:
        return False, f"Page17 自動補跑喚醒失敗：{type(exc).__name__}: {exc}"


__all__ = ["VERSION", "WORKFLOW_FILE", "dispatch_scheduler_wakeup"]
