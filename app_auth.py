# -*- coding: utf-8 -*-
"""Compatibility wrapper: install runtime branch guard, restore runtime authority, then load auth core.

V191-H22 restores the newest business-date market authority before any Streamlit
page continues.  This prevents a new deployment process from displaying or
rewriting the packaged 2026-07-09 macro cache before runtime-data is recovered.
"""
from runtime_branch_bootstrap import install_runtime_branch_guard

install_runtime_branch_guard()

# H22 must run after runtime_branch_bootstrap so godpick_persistence_service reads
# the dedicated runtime-data branch, never the deployment branch.  Failure is
# non-fatal for login; Page0/Page7 still have their own refresh/data guards.
try:
    from macro_runtime_authority import ensure_macro_runtime_authority_current
    ensure_macro_runtime_authority_current()
except Exception:
    pass

import app_auth_core as _core

for _name, _value in vars(_core).items():
    if _name not in {"__name__", "__loader__", "__package__", "__spec__"}:
        globals()[_name] = _value

__all__ = [name for name in vars(_core) if not name.startswith("__")]
