# -*- coding: utf-8 -*-
from __future__ import annotations

import streamlit as st

try:
    from utils import inject_pro_theme
    inject_pro_theme()
except Exception:
    pass

from app_auth import render_account_management_page

st.set_page_config(page_title="帳號管理", layout="wide")
render_account_management_page()
