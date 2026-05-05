# -*- coding: utf-8 -*-
"""
app_auth.py｜Streamlit 全站帳號密碼登入鎖
- 預設帳密：admin / 0000
- 可用 auth_config.json 管理多帳號
- 可用 Streamlit secrets 覆蓋單一帳密：APP_USERNAME / APP_PASSWORD / APP_AUTH_ENABLED
"""
from __future__ import annotations

import json
import os
import hashlib
import hmac
from pathlib import Path
from datetime import datetime
from typing import Any, Dict

import streamlit as st

AUTH_FILE = Path("auth_config.json")

DEFAULT_CONFIG = {
    "enabled": True,
    "users": {
        "admin": {
            "password_hash": "",
            "role": "admin",
            "enabled": True,
            "display_name": "系統管理員",
            "created_at": "",
            "updated_at": "",
        }
    },
}


def _hash_password(password: str) -> str:
    raw = (password or "").encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _get_secret(name: str, default: str | None = None) -> str | None:
    try:
        return st.secrets.get(name, default)  # type: ignore[attr-defined]
    except Exception:
        return os.environ.get(name, default)


def _default_config() -> Dict[str, Any]:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG, ensure_ascii=False))
    cfg["users"]["admin"]["password_hash"] = _hash_password("0000")
    cfg["users"]["admin"]["created_at"] = _now()
    cfg["users"]["admin"]["updated_at"] = _now()
    return cfg


def load_auth_config() -> Dict[str, Any]:
    cfg = _default_config()
    if AUTH_FILE.exists():
        try:
            data = json.loads(AUTH_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                cfg.update({k: v for k, v in data.items() if k != "users"})
                if isinstance(data.get("users"), dict):
                    cfg["users"] = data["users"]
        except Exception:
            pass

    # Streamlit Secrets 單帳號覆蓋，適合正式部署
    enabled_secret = str(_get_secret("APP_AUTH_ENABLED", "true")).strip().lower()
    if enabled_secret in {"false", "0", "no", "off"}:
        cfg["enabled"] = False

    sec_user = _get_secret("APP_USERNAME")
    sec_pass = _get_secret("APP_PASSWORD")
    if sec_user and sec_pass:
        cfg["enabled"] = True
        cfg["users"] = {
            str(sec_user): {
                "password_hash": _hash_password(str(sec_pass)),
                "role": "admin",
                "enabled": True,
                "display_name": str(sec_user),
                "created_at": _now(),
                "updated_at": _now(),
            }
        }
    return cfg


def save_auth_config(cfg: Dict[str, Any]) -> bool:
    try:
        AUTH_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def is_logged_in() -> bool:
    return bool(st.session_state.get("auth_logged_in"))


def current_user() -> Dict[str, Any]:
    return {
        "username": st.session_state.get("auth_username", ""),
        "role": st.session_state.get("auth_role", ""),
        "display_name": st.session_state.get("auth_display_name", ""),
    }


def logout_button(location: str = "sidebar") -> None:
    box = st.sidebar if location == "sidebar" else st
    if is_logged_in():
        user = current_user()
        box.caption(f"登入：{user.get('display_name') or user.get('username')}｜{user.get('role')}")
        if box.button("登出", key=f"auth_logout_{location}"):
            for k in ["auth_logged_in", "auth_username", "auth_role", "auth_display_name"]:
                st.session_state.pop(k, None)
            st.rerun()


def require_login(required_role: str | None = None) -> bool:
    cfg = load_auth_config()
    if not bool(cfg.get("enabled", True)):
        return True

    if is_logged_in():
        if required_role:
            role = str(st.session_state.get("auth_role", ""))
            if role != required_role and role != "admin":
                st.error("權限不足，請使用具備權限的帳號登入。")
                st.stop()
        logout_button("sidebar")
        return True

    st.markdown("## 系統登入")
    st.info("請先輸入帳號密碼，登入後才能使用系統。")
    with st.form("auth_login_form", clear_on_submit=False):
        username = st.text_input("帳號", value="")
        password = st.text_input("密碼", type="password", value="")
        submitted = st.form_submit_button("登入", use_container_width=True)

    if submitted:
        users = cfg.get("users", {}) if isinstance(cfg.get("users"), dict) else {}
        user = users.get(username)
        if not user or not user.get("enabled", True):
            st.error("帳號不存在或已停用。")
            st.stop()
        expected = str(user.get("password_hash", ""))
        actual = _hash_password(password)
        if hmac.compare_digest(expected, actual):
            st.session_state["auth_logged_in"] = True
            st.session_state["auth_username"] = username
            st.session_state["auth_role"] = user.get("role", "user")
            st.session_state["auth_display_name"] = user.get("display_name", username)
            st.rerun()
        else:
            st.error("密碼錯誤。")
            st.stop()

    st.stop()


def require_admin() -> bool:
    return require_login("admin")


def create_or_update_user(username: str, password: str, role: str = "user", enabled: bool = True, display_name: str = "") -> bool:
    cfg = load_auth_config()
    users = cfg.setdefault("users", {})
    if not username:
        return False
    old = users.get(username, {}) if isinstance(users.get(username), dict) else {}
    users[username] = {
        "password_hash": _hash_password(password) if password else old.get("password_hash", _hash_password("0000")),
        "role": role or old.get("role", "user"),
        "enabled": bool(enabled),
        "display_name": display_name or username,
        "created_at": old.get("created_at", _now()),
        "updated_at": _now(),
    }
    return save_auth_config(cfg)
