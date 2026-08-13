# -*- coding: utf-8 -*-
"""V191 headless loader for reusing mature Streamlit-page business functions.

The scheduler must execute the *same* page services/formulas without opening a UI.
This loader removes only the authentication guard and replaces Streamlit with a
small no-op shim; page ``main()`` is never called because ``__name__`` is set to
an internal module name.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace, ModuleType
import sys
from typing import Any
import re


class _SessionState(dict):
    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


class _NoopElement:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def __call__(self, *args, **kwargs):
        return self

    def __getattr__(self, name: str):
        if name in {"progress", "empty", "update", "write", "caption", "info", "warning", "error", "success", "metric", "dataframe", "markdown", "text", "json", "code"}:
            return lambda *args, **kwargs: self
        return self


class _CacheDecorator:
    def __call__(self, func=None, *dargs, **dkwargs):
        if func is None:
            def deco(fn):
                return self._wrap(fn)
            return deco
        return self._wrap(func)

    @staticmethod
    def _wrap(fn):
        def wrapped(*args, **kwargs):
            return fn(*args, **kwargs)
        wrapped.__name__ = getattr(fn, "__name__", "cached")
        wrapped.__doc__ = getattr(fn, "__doc__", None)
        wrapped.clear = lambda: None
        return wrapped

    def clear(self):
        return None


class HeadlessStreamlit:
    """Enough Streamlit surface for page definitions and non-UI worker functions."""
    def __init__(self):
        self.session_state = _SessionState()
        self.cache_data = _CacheDecorator()
        self.cache_resource = _CacheDecorator()
        self.secrets = {}
        self.sidebar = _NoopElement()

    def stop(self):
        raise RuntimeError("headless Streamlit stop() called")

    def rerun(self):
        return None

    def experimental_rerun(self):
        return None

    def container(self, *args, **kwargs):
        return _NoopElement()

    def empty(self):
        return _NoopElement()

    def progress(self, *args, **kwargs):
        return _NoopElement()

    def status(self, *args, **kwargs):
        return _NoopElement()

    @contextmanager
    def spinner(self, *args, **kwargs):
        yield _NoopElement()

    @contextmanager
    def expander(self, *args, **kwargs):
        yield _NoopElement()

    def columns(self, spec, *args, **kwargs):
        count = spec if isinstance(spec, int) else len(spec)
        return [_NoopElement() for _ in range(int(count))]

    def tabs(self, labels, *args, **kwargs):
        return [_NoopElement() for _ in labels]

    def __getattr__(self, name: str):
        # Decorators that may be accessed as attributes have explicit members.
        # All rendering/widget calls are harmless no-ops in headless mode.
        return lambda *args, **kwargs: _NoopElement()


def _strip_auth_guard(source: str) -> str:
    """Remove only the interactive login check inside APP_AUTH_GUARD_V84.

    V191 originally removed the *entire* marked block.  Pages 4/5/7/8/10 also
    keep persistence-service imports in that block, so headless automation lost
    symbols such as ``load_watchlist_permanent`` and ``save_records_permanent``.
    Keep every non-auth line and strip only the ``require_login`` try/except.
    """
    lines = source.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    start_marker = "# >>> APP_AUTH_GUARD_V84"
    end_marker = "# <<< APP_AUTH_GUARD_V84"
    while i < len(lines):
        if start_marker not in lines[i]:
            out.append(lines[i])
            i += 1
            continue

        block: list[str] = []
        i += 1
        while i < len(lines) and end_marker not in lines[i]:
            block.append(lines[i])
            i += 1
        if i < len(lines) and end_marker in lines[i]:
            i += 1

        auth_import = next((n for n, line in enumerate(block) if "from app_auth import require_login" in line), None)
        if auth_import is None:
            out.extend(block)
            continue

        auth_start = auth_import
        while auth_start > 0 and block[auth_start - 1].strip() == "":
            auth_start -= 1
        if auth_start > 0 and block[auth_start - 1].lstrip().startswith("try:"):
            auth_start -= 1

        auth_end = next((n for n in range(auth_import, len(block)) if "st.stop()" in block[n]), None)
        if auth_end is None:
            # Fail safe: if the marker shape changes, keep the block rather than
            # accidentally deleting business imports again.
            out.extend(block)
            continue

        out.extend(block[:auth_start])
        out.extend(block[auth_end + 1:])
    return "".join(out)


def load_page_namespace(page_path: str | Path, *, base_dir: str | Path | None = None, session_state: dict[str, Any] | None = None) -> dict[str, Any]:
    path = Path(page_path)
    if not path.is_absolute():
        path = Path(base_dir or Path(__file__).resolve().parent) / path
    source = path.read_text(encoding="utf-8-sig")
    source = _strip_auth_guard(source)
    source = re.sub(r"(?m)^\s*import streamlit as st\s*$", "st = __HEADLESS_ST__", source)
    source = re.sub(r"(?m)^\s*from streamlit import .*?$", "# V191 headless: streamlit import removed", source)
    st = HeadlessStreamlit()
    if session_state:
        st.session_state.update(session_state)
    ns: dict[str, Any] = {
        "__name__": f"_godpick_headless_{path.stem}",
        "__file__": str(path),
        "__package__": None,
        "__HEADLESS_ST__": st,
    }
    # Some imported business modules (for example utils.py) import streamlit
    # themselves.  Inject the same shim as a temporary module so the headless
    # runner can execute in CI where Streamlit is intentionally absent.
    previous_streamlit = sys.modules.get("streamlit")
    fake_module = ModuleType("streamlit")
    for attr in ["session_state", "cache_data", "cache_resource", "secrets", "sidebar"]:
        setattr(fake_module, attr, getattr(st, attr))
    def _module_getattr(name):
        return getattr(st, name)
    fake_module.__getattr__ = _module_getattr  # type: ignore[attr-defined]
    sys.modules["streamlit"] = fake_module
    try:
        code = compile(source, str(path), "exec")
        exec(code, ns, ns)
    finally:
        if previous_streamlit is None:
            sys.modules.pop("streamlit", None)
        else:
            sys.modules["streamlit"] = previous_streamlit
    ns["__headless_st__"] = st
    return ns


__all__ = ["HeadlessStreamlit", "load_page_namespace"]
