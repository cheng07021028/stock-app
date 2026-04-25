# -*- coding: utf-8 -*-
from __future__ import annotations

"""
auth_manager.py
Streamlit 股票系統權限控制模組

功能：
1. 帳號密碼登入 / 登出
2. 每個模組個別權限：無權限 / 唯讀 / 可寫 / 管理員
3. 記錄登入、登出、模組進入紀錄
4. 統計近 N 分鐘內在線人數、目前停留模組
5. 權限管理頁可維護帳號與模組權限

重要：
- 這是 Streamlit app 內建權限層，適合內部使用。
- 部署在 Streamlit Cloud 時，若要永久保存帳號設定，請把 auth_users.json 一併放入 GitHub。
"""

import base64
import hashlib
import hmac
import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


AUTH_USERS_FILE = Path("auth_users.json")
AUTH_SESSIONS_FILE = Path("auth_sessions.json")
AUTH_LOG_FILE = Path("auth_login_logs.json")

SESSION_TIMEOUT_MINUTES = 480
ONLINE_WINDOW_MINUTES = 15

PERM_NONE = "無權限"
PERM_READ = "唯讀"
PERM_WRITE = "可寫"
PERM_ADMIN = "管理員"

PERMISSION_LEVELS = [PERM_NONE, PERM_READ, PERM_WRITE, PERM_ADMIN]

MODULES: dict[str, str] = {
    "0": "0 大盤走勢",
    "1": "1 儀表板",
    "2": "2 行情查詢",
    "3": "3 歷史K線分析",
    "4": "4 自選股中心",
    "5": "5 排行榜",
    "6": "6 多股比較",
    "7": "7 股神推薦",
    "8": "8 股神推薦紀錄",
    "9": "9 股票主檔更新",
    "10": "10 推薦清單",
    "11": "11 資料診斷",
    "12": "12 權限管理",
}

AUTH_SESSION_KEYS = [
    "auth_logged_in",
    "auth_username",
    "auth_display_name",
    "auth_role",
    "auth_session_id",
    "auth_login_time",
    "auth_current_module_id",
    "auth_current_module_name",
    "auth_can_write",
]


# ============================================================
# 基礎工具
# ============================================================

def _now() -> datetime:
    return datetime.now()


def now_str() -> str:
    return _now().strftime("%Y-%m-%d %H:%M:%S")


def safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _new_salt() -> str:
    return base64.urlsafe_b64encode(os.urandom(16)).decode("utf-8")


def hash_password(password: str, salt: str | None = None) -> dict[str, str]:
    salt = salt or _new_salt()
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        200_000,
    )
    return {
        "algorithm": "pbkdf2_sha256",
        "salt": salt,
        "hash": base64.urlsafe_b64encode(dk).decode("utf-8"),
    }


def verify_password(password: str, password_info: dict[str, str]) -> bool:
    if not isinstance(password_info, dict):
        return False
    salt = password_info.get("salt", "")
    expected = password_info.get("hash", "")
    if not salt or not expected:
        return False
    actual = hash_password(password, salt=salt).get("hash", "")
    return hmac.compare_digest(actual, expected)


def _all_write_permissions() -> dict[str, str]:
    return {mid: PERM_WRITE for mid in MODULES}


def _all_read_permissions() -> dict[str, str]:
    return {mid: PERM_READ for mid in MODULES}


def _admin_permissions() -> dict[str, str]:
    p = {mid: PERM_WRITE for mid in MODULES}
    p["12"] = PERM_ADMIN
    return p


def create_default_users() -> dict[str, Any]:
    """
    首次啟用自動建立。
    上線後請立刻登入 admin 到權限管理頁修改密碼。
    """
    return {
        "version": 1,
        "updated_at": now_str(),
        "users": {
            "admin": {
                "username": "admin",
                "display_name": "系統管理員",
                "active": True,
                "role": "admin",
                "password": hash_password("admin123"),
                "permissions": _admin_permissions(),
                "created_at": now_str(),
                "updated_at": now_str(),
            },
            "manager": {
                "username": "manager",
                "display_name": "主管帳號",
                "active": True,
                "role": "manager",
                "password": hash_password("manager123"),
                "permissions": _all_write_permissions(),
                "created_at": now_str(),
                "updated_at": now_str(),
            },
            "viewer": {
                "username": "viewer",
                "display_name": "唯讀帳號",
                "active": True,
                "role": "viewer",
                "password": hash_password("viewer123"),
                "permissions": _all_read_permissions(),
                "created_at": now_str(),
                "updated_at": now_str(),
            },
        },
    }


def load_users() -> dict[str, Any]:
    if not AUTH_USERS_FILE.exists():
        data = create_default_users()
        _write_json(AUTH_USERS_FILE, data)
        return data

    data = _read_json(AUTH_USERS_FILE, {})
    if not isinstance(data, dict) or not isinstance(data.get("users"), dict):
        data = create_default_users()
        _write_json(AUTH_USERS_FILE, data)
        return data

    # 自動補齊新模組權限，避免新增頁面後 key 不存在
    changed = False
    for username, user in data.get("users", {}).items():
        if "permissions" not in user or not isinstance(user.get("permissions"), dict):
            user["permissions"] = {}
            changed = True
        for mid in MODULES:
            if mid not in user["permissions"]:
                user["permissions"][mid] = PERM_NONE
                changed = True

    if changed:
        data["updated_at"] = now_str()
        _write_json(AUTH_USERS_FILE, data)

    return data


def save_users(data: dict[str, Any]) -> None:
    data["updated_at"] = now_str()
    _write_json(AUTH_USERS_FILE, data)


def get_user(username: str) -> dict[str, Any] | None:
    data = load_users()
    return data.get("users", {}).get(username)


def is_admin_user(username: str | None = None) -> bool:
    username = username or st.session_state.get("auth_username", "")
    user = get_user(username)
    if not user:
        return False
    if user.get("role") == "admin":
        return True
    return user.get("permissions", {}).get("12") == PERM_ADMIN


# ============================================================
# Session / log
# ============================================================

def _load_sessions() -> list[dict[str, Any]]:
    data = _read_json(AUTH_SESSIONS_FILE, [])
    return data if isinstance(data, list) else []


def _save_sessions(data: list[dict[str, Any]]) -> None:
    _write_json(AUTH_SESSIONS_FILE, data)


def _load_logs() -> list[dict[str, Any]]:
    data = _read_json(AUTH_LOG_FILE, [])
    return data if isinstance(data, list) else []


def _save_logs(data: list[dict[str, Any]]) -> None:
    # 避免檔案無限大，只保留最近 5000 筆
    _write_json(AUTH_LOG_FILE, data[-5000:])


def append_auth_log(action: str, username: str = "", module_id: str = "", module_name: str = "", result: str = "", note: str = "") -> None:
    logs = _load_logs()
    logs.append({
        "time": now_str(),
        "action": action,
        "username": username or st.session_state.get("auth_username", ""),
        "display_name": st.session_state.get("auth_display_name", ""),
        "session_id": st.session_state.get("auth_session_id", ""),
        "module_id": module_id or st.session_state.get("auth_current_module_id", ""),
        "module_name": module_name or st.session_state.get("auth_current_module_name", ""),
        "result": result,
        "note": note,
    })
    _save_logs(logs)


def cleanup_expired_sessions() -> None:
    sessions = _load_sessions()
    cutoff = _now() - timedelta(minutes=SESSION_TIMEOUT_MINUTES)
    kept = []
    for s in sessions:
        try:
            last_seen = datetime.strptime(s.get("last_seen", ""), "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        if last_seen >= cutoff:
            kept.append(s)
    if len(kept) != len(sessions):
        _save_sessions(kept)


def update_session_activity(module_id: str = "", module_name: str = "") -> None:
    if not st.session_state.get("auth_logged_in"):
        return

    cleanup_expired_sessions()

    sid = st.session_state.get("auth_session_id") or uuid.uuid4().hex
    st.session_state["auth_session_id"] = sid

    sessions = _load_sessions()
    found = False

    for s in sessions:
        if s.get("session_id") == sid:
            s["username"] = st.session_state.get("auth_username", "")
            s["display_name"] = st.session_state.get("auth_display_name", "")
            s["role"] = st.session_state.get("auth_role", "")
            s["current_module_id"] = module_id or s.get("current_module_id", "")
            s["current_module_name"] = module_name or s.get("current_module_name", "")
            s["last_seen"] = now_str()
            found = True
            break

    if not found:
        sessions.append({
            "session_id": sid,
            "username": st.session_state.get("auth_username", ""),
            "display_name": st.session_state.get("auth_display_name", ""),
            "role": st.session_state.get("auth_role", ""),
            "login_time": st.session_state.get("auth_login_time", now_str()),
            "current_module_id": module_id,
            "current_module_name": module_name,
            "last_seen": now_str(),
        })

    _save_sessions(sessions)


def get_active_sessions(minutes: int = ONLINE_WINDOW_MINUTES) -> list[dict[str, Any]]:
    cleanup_expired_sessions()
    sessions = _load_sessions()
    cutoff = _now() - timedelta(minutes=minutes)
    active = []

    for s in sessions:
        try:
            last_seen = datetime.strptime(s.get("last_seen", ""), "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        if last_seen >= cutoff:
            active.append(s)

    active.sort(key=lambda x: x.get("last_seen", ""), reverse=True)
    return active


def get_active_sessions_df(minutes: int = ONLINE_WINDOW_MINUTES) -> pd.DataFrame:
    data = get_active_sessions(minutes=minutes)
    if not data:
        return pd.DataFrame(columns=["username", "display_name", "current_module_name", "login_time", "last_seen"])
    return pd.DataFrame(data)


def get_auth_logs_df(limit: int = 500) -> pd.DataFrame:
    logs = _load_logs()
    logs = logs[-limit:]
    if not logs:
        return pd.DataFrame(columns=["time", "action", "username", "module_name", "result", "note"])
    return pd.DataFrame(logs).sort_values("time", ascending=False)


# ============================================================
# 登入 / 登出 / 權限判斷
# ============================================================

def login_user(username: str, password: str) -> tuple[bool, str]:
    username = safe_str(username)
    password = safe_str(password)

    if not username or not password:
        return False, "請輸入帳號與密碼。"

    data = load_users()
    user = data.get("users", {}).get(username)

    if not user:
        append_auth_log("login", username=username, result="failed", note="帳號不存在")
        return False, "帳號或密碼錯誤。"

    if not user.get("active", True):
        append_auth_log("login", username=username, result="failed", note="帳號已停用")
        return False, "此帳號已停用。"

    if not verify_password(password, user.get("password", {})):
        append_auth_log("login", username=username, result="failed", note="密碼錯誤")
        return False, "帳號或密碼錯誤。"

    sid = uuid.uuid4().hex
    st.session_state["auth_logged_in"] = True
    st.session_state["auth_username"] = username
    st.session_state["auth_display_name"] = user.get("display_name", username)
    st.session_state["auth_role"] = user.get("role", "")
    st.session_state["auth_session_id"] = sid
    st.session_state["auth_login_time"] = now_str()

    update_session_activity("", "")
    append_auth_log("login", username=username, result="success", note="登入成功")
    return True, "登入成功。"


def logout_user() -> None:
    username = st.session_state.get("auth_username", "")
    sid = st.session_state.get("auth_session_id", "")

    sessions = _load_sessions()
    sessions = [s for s in sessions if s.get("session_id") != sid]
    _save_sessions(sessions)

    append_auth_log("logout", username=username, result="success", note="登出")

    for k in AUTH_SESSION_KEYS:
        if k in st.session_state:
            del st.session_state[k]


def is_logged_in() -> bool:
    return bool(st.session_state.get("auth_logged_in"))


def get_permission(username: str, module_id: str) -> str:
    user = get_user(username)
    if not user:
        return PERM_NONE

    # 系統 admin 一律最高權限
    if user.get("role") == "admin":
        return PERM_ADMIN

    return user.get("permissions", {}).get(str(module_id), PERM_NONE)


def permission_allows_read(perm: str) -> bool:
    return perm in [PERM_READ, PERM_WRITE, PERM_ADMIN]


def permission_allows_write(perm: str) -> bool:
    return perm in [PERM_WRITE, PERM_ADMIN]


def can_write(module_id: str | None = None) -> bool:
    module_id = str(module_id or st.session_state.get("auth_current_module_id", ""))
    username = st.session_state.get("auth_username", "")
    return permission_allows_write(get_permission(username, module_id))


def require_login() -> dict[str, Any]:
    if is_logged_in():
        return get_user(st.session_state.get("auth_username", "")) or {}

    st.markdown("## 股票系統登入")
    st.caption("請輸入帳號密碼後使用系統。")

    with st.form("auth_login_form", clear_on_submit=False):
        username = st.text_input("帳號")
        password = st.text_input("密碼", type="password")
        submitted = st.form_submit_button("登入", use_container_width=True, type="primary")

    if submitted:
        ok, msg = login_user(username, password)
        if ok:
            st.success(msg)
            st.rerun()
        else:
            st.error(msg)

    st.info("預設帳號：admin / admin123、manager / manager123、viewer / viewer123。上線後請立刻修改密碼。")
    st.stop()


def require_module_access(module_id: str, module_name: str | None = None) -> dict[str, Any]:
    """
    每個頁面呼叫這個函式即可控管開啟權限。
    """
    user = require_login()

    module_id = str(module_id)
    module_name = module_name or MODULES.get(module_id, module_id)
    username = st.session_state.get("auth_username", "")

    perm = get_permission(username, module_id)

    st.session_state["auth_current_module_id"] = module_id
    st.session_state["auth_current_module_name"] = module_name
    st.session_state["auth_can_write"] = permission_allows_write(perm)

    update_session_activity(module_id, module_name)
    append_auth_log("open_module", username=username, module_id=module_id, module_name=module_name, result="success" if permission_allows_read(perm) else "denied", note=perm)

    if not permission_allows_read(perm):
        st.error(f"無權限開啟此模組：{module_name}")
        st.caption(f"目前帳號：{username}；權限：{perm}")
        st.stop()

    return user


def require_write_permission(module_id: str | None = None, action_name: str = "寫入 / 修改資料") -> bool:
    """
    建議在每個寫入按鈕真正執行前呼叫。
    """
    module_id = str(module_id or st.session_state.get("auth_current_module_id", ""))
    module_name = MODULES.get(module_id, st.session_state.get("auth_current_module_name", module_id))

    if can_write(module_id):
        return True

    st.error(f"目前帳號只有唯讀權限，無法執行：{action_name}")
    append_auth_log("write_denied", module_id=module_id, module_name=module_name, result="denied", note=action_name)
    return False


def stop_if_readonly(action_name: str = "寫入 / 修改資料") -> None:
    if not require_write_permission(action_name=action_name):
        st.stop()


# ============================================================
# UI
# ============================================================

def render_auth_sidebar() -> None:
    if not is_logged_in():
        return

    update_session_activity(
        st.session_state.get("auth_current_module_id", ""),
        st.session_state.get("auth_current_module_name", ""),
    )

    with st.sidebar:
        st.markdown("---")
        st.markdown("### 權限狀態")
        st.caption(f"帳號：{st.session_state.get('auth_display_name', '')}")
        st.caption(f"模組：{st.session_state.get('auth_current_module_name', '')}")
        st.caption("權限：" + ("可寫" if st.session_state.get("auth_can_write") else "唯讀"))

        active = get_active_sessions()
        st.caption(f"目前在線：{len(active)} 人")

        if st.button("登出", key="auth_logout_btn", use_container_width=True):
            logout_user()
            st.rerun()


def render_readonly_banner() -> None:
    if is_logged_in() and not st.session_state.get("auth_can_write", False):
        st.warning("目前帳號為【唯讀權限】：可以查看資料，但不可新增、刪除、更新或寫入紀錄。")


# ============================================================
# 使用者維護
# ============================================================

def upsert_user(
    username: str,
    display_name: str,
    password: str | None,
    role: str,
    active: bool,
    permissions: dict[str, str],
) -> tuple[bool, str]:
    username = safe_str(username)
    if not username:
        return False, "帳號不可空白。"

    data = load_users()
    users = data.setdefault("users", {})

    if username not in users and not password:
        return False, "新增帳號必須輸入密碼。"

    if username not in users:
        users[username] = {
            "username": username,
            "created_at": now_str(),
        }

    users[username]["display_name"] = safe_str(display_name) or username
    users[username]["role"] = role
    users[username]["active"] = bool(active)
    users[username]["permissions"] = {
        str(mid): permissions.get(str(mid), PERM_NONE)
        for mid in MODULES
    }
    users[username]["updated_at"] = now_str()

    if password:
        users[username]["password"] = hash_password(password)

    save_users(data)
    append_auth_log("user_update", result="success", note=f"更新帳號 {username}")
    return True, f"帳號 {username} 已更新。"


def delete_user(username: str) -> tuple[bool, str]:
    username = safe_str(username)
    current = st.session_state.get("auth_username", "")

    if username == current:
        return False, "不可刪除目前登入中的自己。"

    data = load_users()
    users = data.setdefault("users", {})

    if username not in users:
        return False, "帳號不存在。"

    del users[username]
    save_users(data)
    append_auth_log("user_delete", result="success", note=f"刪除帳號 {username}")
    return True, f"帳號 {username} 已刪除。"
