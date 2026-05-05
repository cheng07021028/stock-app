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

import streamlit as st

from app_auth import require_login, require_role, current_user, current_role, change_password, admin_set_user, reset_admin_password_to_0000

st.set_page_config(page_title="帳號管理", layout="wide")
require_login()

st.title("15 帳號管理")
st.caption("管理登入帳號、修改密碼、重設 admin 密碼。")

st.subheader("修改自己的密碼")
with st.form("change_my_password"):
    old_pw = st.text_input("原密碼", type="password")
    new_pw = st.text_input("新密碼", type="password")
    ok = st.form_submit_button("修改密碼")
    if ok:
        success, msg = change_password(current_user(), old_pw, new_pw)
        if success:
            st.success(msg)
        else:
            st.error(msg)

if current_role() == "admin":
    st.divider()
    st.subheader("管理員功能")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### 新增 / 重設帳號")
        with st.form("admin_add_user"):
            username = st.text_input("帳號")
            password = st.text_input("密碼", type="password")
            role = st.selectbox("角色", ["admin", "user", "viewer"], index=1)
            enabled = st.checkbox("啟用", value=True)
            display_name = st.text_input("顯示名稱")
            submit = st.form_submit_button("儲存帳號")
            if submit:
                success, msg = admin_set_user(username, password, role, enabled, display_name)
                if success:
                    st.success(msg)
                else:
                    st.error(msg)

    with c2:
        st.markdown("### 緊急重設")
        st.warning("按下後會把 admin 密碼重設為 0000。請登入後立刻改密碼。")
        if st.button("重設 admin 密碼為 0000", type="primary"):
            if reset_admin_password_to_0000():
                st.success("admin 密碼已重設為 0000")
            else:
                st.error("重設失敗，請確認 GitHub / 檔案寫入權限。")
else:
    st.info("只有 admin 可以新增或重設其他帳號。")
