# -*- coding: utf-8 -*-

from __future__ import annotations

# >>> APP_AUTH_GUARD_V84
try:
    from app_auth import require_login
    require_login()
except Exception as _auth_e:
    import streamlit as st
    st.error(f"登入系統載入失敗：{_auth_e}")
    st.stop()
# <<< APP_AUTH_GUARD_V84

import pandas as pd
import streamlit as st

st.set_page_config(page_title="帳號管理", layout="wide")

from app_auth import require_admin, load_auth_config, save_auth_config, create_or_update_user, current_user

require_admin()

st.title("15 帳號管理")
st.caption("管理登入帳號、密碼、角色與啟用狀態。")

cfg = load_auth_config()
users = cfg.get("users", {}) if isinstance(cfg.get("users"), dict) else {}

st.subheader("目前帳號")
rows = []
for username, info in users.items():
    if not isinstance(info, dict):
        continue
    rows.append({
        "帳號": username,
        "名稱": info.get("display_name", username),
        "角色": info.get("role", "user"),
        "啟用": bool(info.get("enabled", True)),
        "建立時間": info.get("created_at", ""),
        "更新時間": info.get("updated_at", ""),
    })
st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

st.divider()
st.subheader("修改自己的密碼")
me = current_user().get("username")
with st.form("change_own_password"):
    new_pw = st.text_input("新密碼", type="password")
    new_pw2 = st.text_input("再次輸入新密碼", type="password")
    ok = st.form_submit_button("修改我的密碼", use_container_width=True)
if ok:
    if not new_pw or new_pw != new_pw2:
        st.error("兩次密碼不一致或空白。")
    else:
        info = users.get(me, {})
        if create_or_update_user(me, new_pw, info.get("role", "admin"), info.get("enabled", True), info.get("display_name", me)):
            st.success("已修改密碼，請重新登入確認。")

st.divider()
st.subheader("新增 / 修改帳號")
with st.form("admin_user_form"):
    c1, c2, c3, c4 = st.columns([2, 2, 1, 1])
    with c1:
        username = st.text_input("帳號")
    with c2:
        display_name = st.text_input("顯示名稱")
    with c3:
        role = st.selectbox("角色", ["admin", "user", "viewer"], index=1)
    with c4:
        enabled = st.checkbox("啟用", value=True)
    password = st.text_input("密碼（新增必填；修改時空白代表不變）", type="password")
    submit = st.form_submit_button("儲存帳號", use_container_width=True)
if submit:
    if not username:
        st.error("帳號不可空白。")
    else:
        if create_or_update_user(username, password, role, enabled, display_name or username):
            st.success("帳號已儲存。")
            st.rerun()
        else:
            st.error("儲存失敗。")

st.warning("預設帳密 admin / 0000 請務必修改。若部署在 Streamlit Cloud，也可用 Secrets 設定 APP_USERNAME / APP_PASSWORD。")
