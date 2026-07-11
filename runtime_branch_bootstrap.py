# -*- coding: utf-8 -*-
"""Global runtime safety guards for the Streamlit stock app.

Protections:
1. Runtime state is forced to ``runtime-data`` so normal operations do not
   redeploy the application.
2. pandas string inference stays on the Python/object backend.
3. Historical K-line liquidity aliases are normalized before page modules bind
   ``get_history_data``. Missing data is never silently converted to zero.
4. Legacy macro-page daemon workers run in the current Streamlit script thread.
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
_MACRO_PAGE_FILENAMES = {"0_大盤走勢.py", "0_#U5927#U76e4#U8d70#U52e2.py"}


def install_pandas_string_stability_guard() -> bool:
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


def _coalesce_numeric_column(df: Any, target: str, aliases: list[str]) -> None:
    try:
        import pandas as pd
        if not isinstance(df, pd.DataFrame) or df.empty:
            return
        current = pd.to_numeric(df[target], errors="coerce") if target in df.columns else pd.Series(float("nan"), index=df.index)
        for name in aliases:
            if name not in df.columns:
                continue
            source = pd.to_numeric(df[name], errors="coerce")
            current = current.where(current.notna() & current.ne(0), source)
        if current.notna().any():
            df[target] = current
    except Exception:
        return


def _normalize_history_liquidity_frame(result: Any) -> Any:
    """Add canonical volume/turnover columns without changing the return type."""
    try:
        import pandas as pd
        if not isinstance(result, pd.DataFrame) or result.empty:
            return result
        out = result.copy()
        _coalesce_numeric_column(out, "成交股數", ["成交量", "Volume", "volume", "總量", "成交量(股)"])
        _coalesce_numeric_column(out, "成交金額", ["成交額", "成交值", "Amount", "amount", "成交金額(元)"])
        close = pd.to_numeric(out.get("收盤價"), errors="coerce") if "收盤價" in out.columns else pd.Series(float("nan"), index=out.index)
        volume = pd.to_numeric(out.get("成交股數"), errors="coerce") if "成交股數" in out.columns else pd.Series(float("nan"), index=out.index)
        amount = pd.to_numeric(out.get("成交金額"), errors="coerce") if "成交金額" in out.columns else pd.Series(float("nan"), index=out.index)
        recovered = close * volume
        amount = amount.where(amount.notna() & amount.gt(0), recovered)
        if amount.notna().any():
            out["成交金額"] = amount
        if "VOL5" not in out.columns and volume.notna().any():
            out["VOL5"] = volume.rolling(5, min_periods=1).mean()
        if "VOL20" not in out.columns and volume.notna().any():
            out["VOL20"] = volume.rolling(20, min_periods=1).mean()
        return out
    except Exception:
        return result


def install_history_liquidity_guard() -> bool:
    """Wrap utils.get_history_data before pages import it by value."""
    try:
        import utils
    except Exception:
        return False
    original = getattr(utils, "get_history_data", None)
    if not callable(original):
        return False
    if getattr(original, "_stock_app_liquidity_guard_installed", False):
        return True

    def guarded_get_history_data(*args: Any, **kwargs: Any) -> Any:
        return _normalize_history_liquidity_frame(original(*args, **kwargs))

    guarded_get_history_data._stock_app_liquidity_guard_installed = True
    guarded_get_history_data._stock_app_original = original
    utils.get_history_data = guarded_get_history_data
    return True


def install_runtime_branch_guard() -> bool:
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
    frame = inspect.currentframe()
    try:
        while frame is not None:
            if Path(frame.f_code.co_filename).name in _MACRO_PAGE_FILENAMES:
                return True
            frame = frame.f_back
    finally:
        del frame
    return False


def install_macro_page_stability_guard() -> bool:
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
install_history_liquidity_guard()
install_macro_page_stability_guard()
