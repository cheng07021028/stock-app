# -*- coding: utf-8 -*-
"""Compatibility wrapper: install runtime branch guard, restore runtime authority, wire H34, then load auth core.

V191-H22 restores the newest business-date market authority before any Streamlit
page continues.  This prevents a new deployment process from displaying or
rewriting the packaged 2026-07-09 macro cache before runtime-data is recovered.

V191-H34 installs the daily 1~3 safe recommendation admission wrapper before
Page07 imports the formal engine symbol.  The wrapper preserves all existing hard
risk/data vetoes and only backfills safe near-miss candidates when the standard
Formal/A- list is below the market-regime target.
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

# H34 production wiring.  This intentionally wraps the existing formal engine
# rather than replacing it: existing Formal/A- decisions remain first priority;
# only an insufficient daily actionable list may receive safe near-miss backfill.
try:
    from godpick_daily_safe_selection import install_daily_safe_selection_guard
    install_daily_safe_selection_guard()
except Exception:
    # Authentication/login must remain available even if the optional selection
    # overlay cannot be imported; Page07 then falls back to the standard engine.
    pass

import app_auth_core as _core

for _name, _value in vars(_core).items():
    if _name not in {"__name__", "__loader__", "__package__", "__spec__"}:
        globals()[_name] = _value

__all__ = [name for name in vars(_core) if not name.startswith("__")]
