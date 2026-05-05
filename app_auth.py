# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Any, Dict

import streamlit as st

AUTH_CONFIG_FILE = Path(__file__).resolve().parent / "auth_config.json"


def _sha256(text: str) -> str:
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()


def _load_auth_config() -> Dict[str, Any]:
    default = {
        "auth_enabled": True,
        "users": {
            "admin": {
                "password_hash": _sha256("0000"),
                "role": "admin",
                "enabled": True,
                "display_name": "系統管理員",
            }
        },
    }

    try:
        if AUTH_CONFIG_FILE.exists():
            data = json.loads(AUTH_CONFIG_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data.get("users"):
                return data
    except Exception:
        pass

    return default


def _save_auth_config(cfg: Dict[str, Any]) -> bool:
    try:
        AUTH_CONFIG_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def _verify_password(user: Dict[str, Any], password: str) -> bool:
    password = str(password)

    # 新版：SHA256 hash
    ph = str(user.get("password_hash", "") or "")
    if ph and _sha256(password) == ph:
        return True

    # 舊版相容：明文 password
    plain = user.get("password", None)
    if plain is not None and str(plain) == password:
        return True

    # 舊版相容：pwd
    plain2 = user.get("pwd", None)
    if plain2 is not None and str(plain2) == password:
        return True

    return False


def is_logged_in() -> bool:
    return bool(st.session_state.get("auth_logged_in"))


def current_user() -> str:
    return str(st.session_state.get("auth_username", ""))


def current_role() -> str:
    return str(st.session_state.get("auth_role", ""))


def logout() -> None:
    for k in ["auth_logged_in", "auth_username", "auth_role", "auth_display_name"]:
        st.session_state.pop(k, None)
    st.rerun()


def require_login() -> bool:
    cfg = _load_auth_config()
    if str(cfg.get("auth_enabled", True)).lower() in ("false", "0", "no", "off"):
        return True

    if is_logged_in():
        try:
            with st.sidebar:
                user = current_user()
                role = current_role()
                st.caption(f"登入帳號：{user}｜{role}")
                if st.button("登出", key="auth_logout_sidebar"):
                    logout()
        except Exception:
            pass
        return True

    st.title("系統登入")
    st.info("請先輸入帳號密碼，登入後才能使用系統。")

    with st.form("auth_login_form_v85"):
        username = st.text_input("帳號", value="admin")
        password = st.text_input("密碼", value="", type="password")
        submitted = st.form_submit_button("登入", use_container_width=True)

    if submitted:
        users = cfg.get("users", {})
        user = users.get(str(username).strip())

        if user and bool(user.get("enabled", True)) and _verify_password(user, password):
            st.session_state["auth_logged_in"] = True
            st.session_state["auth_username"] = str(username).strip()
            st.session_state["auth_role"] = str(user.get("role", "user"))
            st.session_state["auth_display_name"] = str(user.get("display_name", username))
            st.success("登入成功")
            st.rerun()
        else:
            st.error("密碼錯誤。")

    st.stop()


def require_role(roles) -> bool:
    require_login()
    if isinstance(roles, str):
        roles = [roles]
    if current_role() not in roles:
        st.error("你的帳號沒有權限使用此功能。")
        st.stop()
    return True


def change_password(username: str, old_password: str, new_password: str) -> tuple[bool, str]:
    cfg = _load_auth_config()
    users = cfg.setdefault("users", {})
    user = users.get(username)
    if not user:
        return False, "找不到帳號。"
    if not _verify_password(user, old_password):
        return False, "原密碼錯誤。"
    if not new_password or len(str(new_password)) < 4:
        return False, "新密碼至少 4 碼。"
    user.pop("password", None)
    user.pop("pwd", None)
    user["password_hash"] = _sha256(new_password)
    return (_save_auth_config(cfg), "密碼已更新。")


def admin_set_user(username: str, password: str, role: str = "user", enabled: bool = True, display_name: str = "") -> tuple[bool, str]:
    if current_role() != "admin":
        return False, "只有 admin 可以管理帳號。"
    username = str(username).strip()
    if not username:
        return False, "帳號不可空白。"
    if not password or len(str(password)) < 4:
        return False, "密碼至少 4 碼。"
    cfg = _load_auth_config()
    users = cfg.setdefault("users", {})
    users[username] = {
        "password_hash": _sha256(password),
        "role": role or "user",
        "enabled": bool(enabled),
        "display_name": display_name or username,
    }
    return (_save_auth_config(cfg), "帳號已儲存。")


def reset_admin_password_to_0000() -> bool:
    cfg = _load_auth_config()
    users = cfg.setdefault("users", {})
    users["admin"] = {
        "password_hash": _sha256("0000"),
        "role": "admin",
        "enabled": True,
        "display_name": "系統管理員",
    }
    return _save_auth_config(cfg)
