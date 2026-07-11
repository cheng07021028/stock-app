# -*- coding: utf-8 -*-
"""Global runtime safety guards for the Streamlit stock app.

This module is imported before the real app and page modules. It provides three
independent protections:

1. Runtime JSON/cache/UI state is forced to the non-deployment ``runtime-data``
   branch so normal user operations do not redeploy Streamlit Cloud.
2. pandas string inference is forced to Python/object storage. This prevents the
   pandas 3 / PyArrow string constructor path from crashing the whole process
   while the macro page reads wide recommendation JSON records.
3. Daemon threads started by ``pages/0_大盤走勢.py`` are executed safely in the
   current Streamlit script thread.
"""
from __future__ import annotations

import faulthandler
import inspect
import os
import threading
from pathlib import Path
from typing import Any

RUNTIME_DATA_BRANCH = "runtime-data"
_BRANCH_KEYS = {"GITHUB_REPO_BRANCH", "GITHUB_RUNTIME_DATA_BRANCH"}
_MACRO_PAGE_FILENAMES = {
    "0_大盤走勢.py",
    "0_#U5927#U76e4#U8d70#U52e2.py",
}


def install_pandas_string_stability_guard() -> bool:
    """Keep recommendation JSON string columns off the PyArrow string backend.

    The macro page builds a wide DataFrame from recommendation JSON. On the
    failing deployment the process died inside pandas ``string_arrow.py`` before
    Python could raise a normal exception. The production dependency is pinned
    to pandas 2.3.3, and these options provide a second layer of protection.
    """
    os.environ.setdefault("PANDAS_COPY_ON_WRITE", "0")
    try:
        import pandas as pd
    except Exception:
        return False

    try:
        pd.options.future.infer_string = False
    except Exception:
        pass
    try:
        pd.options.mode.string_storage = "python"
    except Exception:
        pass
    return True


def install_runtime_branch_guard() -> bool:
    """Force every Streamlit secret branch lookup to ``runtime-data``."""
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
        try:
            return original_get(self, key, default)
        except Exception:
            return default

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


def _called_from_macro_page() -> bool:
    """Return True only when Thread.start() was called by the macro page."""
    frame = inspect.currentframe()
    try:
        while frame is not None:
            filename = Path(frame.f_code.co_filename).name
            if filename in _MACRO_PAGE_FILENAMES:
                return True
            frame = frame.f_back
    finally:
        del frame
    return False


def install_macro_page_stability_guard() -> bool:
    """Prevent native-library crashes caused by macro-page daemon threads.

    The page's update functions are preserved. Only their execution mode is
    changed: a daemon worker created from the macro page runs synchronously in
    the active Streamlit script thread.
    """
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
    os.environ.setdefault("ARROW_NUM_THREADS", "1")
    os.environ.setdefault("GRPC_POLL_STRATEGY", "poll")
    os.environ.setdefault("PYTHONFAULTHANDLER", "1")
    try:
        faulthandler.enable()
    except Exception:
        pass

    try:
        import streamlit as st
        if "macro_safe_auto_bg_update" not in st.session_state:
            st.session_state["macro_safe_auto_bg_update"] = False
    except Exception:
        pass

    if getattr(threading.Thread, "_stock_app_macro_guard_installed", False):
        return True

    original_start = threading.Thread.start

    def guarded_start(self: threading.Thread) -> Any:
        if bool(getattr(self, "daemon", False)) and _called_from_macro_page():
            if getattr(self, "_stock_app_macro_sync_started", False):
                return None
            self._stock_app_macro_sync_started = True
            return self.run()
        return original_start(self)

    threading.Thread._stock_app_original_start = original_start
    threading.Thread.start = guarded_start
    threading.Thread._stock_app_macro_guard_installed = True
    return True


install_pandas_string_stability_guard()
install_runtime_branch_guard()
install_macro_page_stability_guard()
