# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
import streamlit as st

from app_auth import (
    admin_delete_user,
    admin_set_user,
    change_password,
    current_role,
    current_user,
    get_auth_config_status,
    list_users,
    logout,
    require_login,
    reset_admin_password_to_0000,
)

st.set_page_config(page_title="15_帳號管理", layout="wide")
require_login()

st.title("15_帳號管理｜v87 帳密可視版")
st.caption("帳號密碼永久回寫 GitHub auth_config.json；本版新增管理員可查看已建立帳號密碼。")

status = get_auth_config_status()
with st.expander("帳號設定保存狀態", expanded=True):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("設定來源", status.get("source", ""))
    c2.metric("GITHUB_TOKEN", status.get("token_ready", "否"))
    c3.metric("GitHub Repo", status.get("github_repo", ""))
    c4.metric("分支", status.get("github_branch", ""))
    st.caption(f"保存路徑：{status.get('github_path', 'auth_config.json')}")
    msg = status.get("message", "")
    if "已讀取 GitHub" in msg or "永久回寫 GitHub" in msg:
        st.success(msg)
    elif "未設定 GITHUB_TOKEN" in msg or "失敗" in msg:
        st.warning(msg)
    else:
        st.info(msg)

st.divider()

st.subheader("修改自己的密碼")
with st.form("change_my_password_v86"):
    old_pw = st.text_input("原密碼", type="password")
    new_pw = st.text_input("新密碼", type="password")
    ok = st.form_submit_button("修改密碼並永久保存", use_container_width=True)
    if ok:
        success, msg = change_password(current_user(), old_pw, new_pw)
        if success:
            st.success(msg)
            st.info("密碼已寫入 GitHub。建議登出後用新密碼測試。")
        else:
            st.error(msg)

if current_role() == "admin":
    st.divider()
    st.subheader("管理員功能")

    users = list_users()

    def _sha256_local(text: str) -> str:
        import hashlib
        return hashlib.sha256(str(text).encode("utf-8")).hexdigest()

    def _display_password(u: dict) -> str:
        # v87：新建 / 修改後會有 password_plain。
        # 舊版只有 password_hash，SHA256 無法反解；若剛好是預設 0000，這裡可辨識顯示。
        plain = u.get("password_plain", None)
        if plain not in [None, ""]:
            return str(plain)
        if u.get("password") not in [None, ""]:
            return str(u.get("password"))
        if u.get("pwd") not in [None, ""]:
            return str(u.get("pwd"))
        ph = str(u.get("password_hash", ""))
        if ph == _sha256_local("0000"):
            return "0000（預設密碼）"
        return "舊版雜湊密碼，無法反解；請重設後顯示"

    rows = []
    for username, u in users.items():
        if not isinstance(u, dict):
            continue
        rows.append({
            "帳號": username,
            "密碼": _display_password(u),
            "顯示名稱": u.get("display_name", ""),
            "角色": u.get("role", ""),
            "啟用": bool(u.get("enabled", True)),
            "密碼保存狀態": u.get("password_note", "舊版資料"),
            "密碼更新時間": u.get("password_visible_updated_at", ""),
            "建立時間": u.get("created_at", ""),
            "更新時間": u.get("updated_at", ""),
        })
    st.markdown("### 已建立帳號 / 密碼清單")
    st.warning("此頁會顯示密碼明文，請只開放給 admin 使用；舊版雜湊密碼無法反解，請重設一次後即可顯示。")
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("目前尚未建立帳號。")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### 新增 / 修改帳號")
        with st.form("admin_add_user_v86"):
            username = st.text_input("帳號")
            password = st.text_input("新密碼（修改角色/名稱但不改密碼時可留空）", type="password")
            role = st.selectbox("角色", ["admin", "user", "viewer"], index=1)
            enabled = st.checkbox("啟用", value=True)
            display_name = st.text_input("顯示名稱")
            submit = st.form_submit_button("儲存帳號並永久保存", use_container_width=True)
            if submit:
                success, msg = admin_set_user(username, password, role, enabled, display_name)
                if success:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

    with c2:
        st.markdown("### 刪除 / 緊急重設")
        delete_user = st.selectbox("選擇要刪除的帳號", [""] + [u for u in users.keys() if u != "admin"])
        if st.button("刪除帳號並永久保存", use_container_width=True):
            if not delete_user:
                st.warning("請先選擇帳號。")
            else:
                success, msg = admin_delete_user(delete_user)
                if success:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

        st.warning("緊急重設會把 admin 密碼改回 0000，並永久回寫 GitHub。")
        if st.button("重設 admin 密碼為 0000", type="primary", use_container_width=True):
            if reset_admin_password_to_0000():
                st.success("admin 密碼已永久重設為 0000。請立刻改密碼。")
            else:
                st.error("重設失敗，請確認 GITHUB_TOKEN 權限。")

else:
    st.info("只有 admin 可以新增、刪除或重設其他帳號。")

st.divider()
if st.button("登出", use_container_width=True):
    logout()
