# -*- coding: utf-8 -*-
"""Streamlit entry wrapper with runtime GitHub branch isolation.

The real home page lives in ``streamlit_app_core.py``.  Importing that module
alone does not execute its ``if __name__ == '__main__'`` block, so this wrapper
must call ``main()`` explicitly; otherwise Streamlit shows only the multipage
sidebar and a blank main canvas after login.
"""
from legacy_page_cleanup import cleanup_duplicate_encoded_pages
from runtime_branch_bootstrap import install_runtime_branch_guard

cleanup_duplicate_encoded_pages()
install_runtime_branch_guard()

from streamlit_app_core import *  # noqa: F401,F403
from streamlit_app_core import main as _run_home_page

_run_home_page()
