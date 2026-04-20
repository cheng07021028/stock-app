import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
import streamlit as st

import firebase_admin
from firebase_admin import credentials, auth, firestore


# =========================
# Firebase 初始化
# =========================
@st.cache_resource(show_spinner=False)
def init_firebase():
    """
    初始化 Firebase Admin SDK
    只初始化一次，避免 Streamlit 重跑時重複初始化
    """
    if not firebase_admin._apps:
        service_account_raw = st.secrets["FIREBASE_SERVICE_ACCOUNT"]
        service_account_info = json.loads(service_account_raw)
        cred = credentials.Certificate(service_account_info)
        firebase_admin.initialize_app(cred)

    return firebase_admin.get_app()


@st.cache_resource(show_spinner=False)
def get_firestore_client():
    init_firebase()
    return firestore.client()


def get_firebase_web_config() -> Dict[str, str]:
    return {
        "apiKey": st.secrets["FIREBASE_API_KEY"],
        "authDomain": st.secrets["FIREBASE_AUTH_DOMAIN"],
        "projectId": st.secrets["FIREBASE_PROJECT_ID"],
        "storageBucket": st.secrets["FIREBASE_STORAGE_BUCKET"],
        "messagingSenderId": st.secrets["FIREBASE_MESSAGING_SENDER_ID"],
        "appId": st.secrets["FIREBASE_APP_ID"],
    }


# =========================
# Auth REST API
# =========================
def sign_in_with_email_password(email: str, password: str) -> Dict[str, Any]:
    """
    使用 Firebase Auth REST API 進行 Email/Password 登入
    成功會回傳 idToken / refreshToken / localId 等資料
    """
    api_key = st.secrets["FIREBASE_API_KEY"]
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={api_key}"

    payload = {
        "email": email.strip(),
        "password": password,
        "returnSecureToken": True,
    }

    resp = requests.post(url, json=payload, timeout=20)
    data = resp.json()

    if resp.status_code != 200:
        error_msg = (
            data.get("error", {}).get("message", "Firebase 登入失敗，請檢查帳號密碼")
        )
        raise ValueError(_map_firebase_error(error_msg))

    return data


def _map_firebase_error(error_code: str) -> str:
    mapping = {
        "EMAIL_NOT_FOUND": "找不到此帳號",
        "INVALID_PASSWORD": "密碼錯誤",
        "USER_DISABLED": "此帳號已被停用",
        "INVALID_LOGIN_CREDENTIALS": "帳號或密碼錯誤",
        "TOO_MANY_ATTEMPTS_TRY_LATER": "嘗試次數過多，請稍後再試",
    }
    return mapping.get(error_code, f"登入失敗：{error_code}")


# =========================
# Firestore 使用者資料 / 權限
# =========================
def _server_time_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_user_doc(uid: str, email: str) -> Dict[str, Any]:
    """
    若 users/{uid} 不存在，自動建立預設使用者資料
    預設 role = viewer，enabled = True
    """
    db = get_firestore_client()
    ref = db.collection("users").document(uid)
    snap = ref.get()

    if snap.exists:
        data = snap.to_dict() or {}
        return data

    default_data = {
        "email": email,
        "name": email.split("@")[0],
        "role": "viewer",
        "enabled": True,
        "created_at": _server_time_iso(),
        "last_login_at": _server_time_iso(),
    }
    ref.set(default_data)
    return default_data


def update_last_login(uid: str):
    db = get_firestore_client()
    ref = db.collection("users").document(uid)
    ref.set({"last_login_at": _server_time_iso()}, merge=True)


def get_role_permissions(role: str) -> Dict[str, bool]:
    """
    從 roles_permissions/{role} 讀權限
    如果不存在，依 role 回傳內建預設
    """
    db = get_firestore_client()
    ref = db.collection("roles_permissions").document(role)
    snap = ref.get()

    if snap.exists:
        return snap.to_dict() or {}

    # 預設權限
    presets = {
        "admin": {
            "can_view_dashboard": True,
            "can_view_kline": True,
            "can_view_rankings": True,
            "can_view_compare": True,
            "can_view_recommend": True,
            "can_edit_watchlist": True,
            "can_manage_users": True,
        },
        "manager": {
            "can_view_dashboard": True,
            "can_view_kline": True,
            "can_view_rankings": True,
            "can_view_compare": True,
            "can_view_recommend": True,
            "can_edit_watchlist": True,
            "can_manage_users": False,
        },
        "viewer": {
            "can_view_dashboard": True,
            "can_view_kline": True,
            "can_view_rankings": True,
            "can_view_compare": True,
            "can_view_recommend": False,
            "can_edit_watchlist": False,
            "can_manage_users": False,
        },
    }

    return presets.get(role, presets["viewer"])


def get_or_create_auth_user(email: str, password: str) -> Dict[str, Any]:
    """
    登入 + 讀取 Firestore 使用者資料 + 權限
    最終回傳可直接放進 session_state 的 auth_user
    """
    login_data = sign_in_with_email_password(email, password)

    uid = login_data["localId"]
    id_token = login_data["idToken"]
    refresh_token = login_data.get("refreshToken", "")

    # 驗證 token（確保後端可正常解）
    init_firebase()
    decoded = auth.verify_id_token(id_token)
    uid = decoded["uid"]

    user_doc = ensure_user_doc(uid=uid, email=email)

    enabled = bool(user_doc.get("enabled", True))
    if not enabled:
        raise ValueError("此帳號已被管理者停用")

    role = str(user_doc.get("role", "viewer")).strip() or "viewer"
    permissions = get_role_permissions(role)

    update_last_login(uid)

    auth_user = {
        "uid": uid,
        "email": email,
        "name": user_doc.get("name", email.split("@")[0]),
        "role": role,
        "enabled": enabled,
        "permissions": permissions,
        "id_token": id_token,
        "refresh_token": refresh_token,
    }
    return auth_user


# =========================
# Session helpers
# =========================
def get_current_user() -> Optional[Dict[str, Any]]:
    return st.session_state.get("auth_user")


def is_logged_in() -> bool:
    return bool(st.session_state.get("auth_user"))


def login_user(email: str, password: str) -> Dict[str, Any]:
    auth_user = get_or_create_auth_user(email, password)
    st.session_state["auth_user"] = auth_user
    return auth_user


def logout_user():
    if "auth_user" in st.session_state:
        del st.session_state["auth_user"]


def require_login():
    if not is_logged_in():
        st.warning("請先登入")
        st.stop()


def require_permission(permission_name: str):
    require_login()
    user = get_current_user() or {}
    perms = user.get("permissions", {}) or {}

    if not perms.get(permission_name, False):
        st.error("你沒有此功能權限")
        st.stop()
