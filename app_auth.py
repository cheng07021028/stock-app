# -*- coding: utf-8 -*-
"""Compatibility wrapper: install runtime branch guard, then load auth core."""
from runtime_branch_bootstrap import install_runtime_branch_guard

install_runtime_branch_guard()

import app_auth_core as _core

for _name, _value in vars(_core).items():
    if _name not in {"__name__", "__loader__", "__package__", "__spec__"}:
        globals()[_name] = _value

__all__ = [name for name in vars(_core) if not name.startswith("__")]
