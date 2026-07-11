# -*- coding: utf-8 -*-
"""OAuth Streamlit entry wrapper with runtime GitHub branch isolation."""
from runtime_branch_bootstrap import install_runtime_branch_guard

install_runtime_branch_guard()

from streamlit_app_oauth_core import *  # noqa: F401,F403
