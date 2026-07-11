# -*- coding: utf-8 -*-
"""Global safety guard for Streamlit runtime GitHub persistence.

All runtime state must be read/written from the non-deployment ``runtime-data``
branch.  Streamlit Community Cloud watches ``main``; committing JSON/cache/UI
state to ``main`` disconnects active sessions with HTTP 503 and redeploys the
app.  This guard intercepts both the legacy and new branch secret names so an
old Streamlit secret cannot accidentally point runtime writes back to main.
"""
from __future__ import annotations

import os
from typing import Any

RUNTIME_DATA_BRANCH = "runtime-data"
_BRANCH_KEYS = {"GITHUB_REPO_BRANCH", "GITHUB_RUNTIME_DATA_BRANCH"}


def install_runtime_branch_guard() -> bool:
    """Force every Streamlit secret branch lookup to ``runtime-data``.

    The patch is process-wide and idempotent. Non-branch secrets keep their
    original behavior. It is installed before the real app/auth modules load.
    """
    os.environ["GITHUB_REPO_BRANCH"] = RUNTIME_DATA_BRANCH
    os.environ["GITHUB_RUNTIME_DATA_BRANCH"] = RUNTIME_DATA_BRANCH

    try:
        from streamlit.runtime.secrets import Secrets
    except Exception:
        return False

    if getattr(Secrets, "_stock_app_runtime_branch_guard_installed", False):
        return True

    original_get = Secrets.get
    original_getitem = Secrets.__getitem__

    def guarded_get(self: Any, key: Any, default: Any = None) -> Any:
        if str(key) in _BRANCH_KEYS:
            return RUNTIME_DATA_BRANCH
        return original_get(self, key, default)

    def guarded_getitem(self: Any, key: Any) -> Any:
        if str(key) in _BRANCH_KEYS:
            return RUNTIME_DATA_BRANCH
        return original_getitem(self, key)

    Secrets._stock_app_original_get = original_get
    Secrets._stock_app_original_getitem = original_getitem
    Secrets.get = guarded_get
    Secrets.__getitem__ = guarded_getitem
    Secrets._stock_app_runtime_branch_guard_installed = True
    return True


install_runtime_branch_guard()
