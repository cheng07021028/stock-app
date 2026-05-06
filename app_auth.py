# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

import requests
import streamlit as st

AUTH_CONFIG_FILE = Path(__file__).resolve().parent / "auth_config.json"


# =========================================================
# 基礎工具
# =========================================================
def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v).strip()
    except Exception:
        return ""


def _sha256(text: str) -> str:
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _default_auth_config() -> Dict[str, Any]:
    return {
        "auth_enabled": True,
        "version": "v86_github_permanent",
        "updated_at": _now_text(),
        "users": {
            "admin": {
                "password_hash": _sha256("0000"),
                "role": "admin",
                "enabled": True,
                "display_name": "系統管理員",
                "created_at": _now_text(),
                "updated_at": _now_text(),
            }
        },
    }


# =========================================================
# GitHub 永久保存設定
# =========================================================
def _github_config() -> Dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")) or "cheng07021028",
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")) or "stock-app",
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("AUTH_CONFIG_GITHUB_PATH", "auth_config.json")) or "auth_config.json",
    }


def _github_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _read_auth_config_from_github() -> Tuple[Dict[str, Any] | None, str, str]:
    """
    回傳：(config, sha, message)
    config 為 None 表示讀取失敗或未設定 token。
    """
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return None, "", "未設定 GITHUB_TOKEN，帳號密碼只能暫存在 Streamlit 檔案系統，重新部署後可能還原。"

    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )

        if resp.status_code == 404:
            return None, "", f"GitHub 尚未建立 {cfg['path']}，第一次儲存時會自動建立。"

        if resp.status_code != 200:
            return None, "", f"讀取 GitHub auth_config 失敗：{resp.status_code} / {resp.text[:300]}"

        data = resp.json()
        content = data.get("content", "")
        sha = _safe_str(data.get("sha"))
        if not content:
            return None, sha, "GitHub auth_config 內容空白。"

        decoded = base64.b64decode(content).decode("utf-8")
        parsed = json.loads(decoded)
        if not isinstance(parsed, dict) or "users" not in parsed:
            return None, sha, "GitHub auth_config 格式不正確。"

        return parsed, sha, f"已讀取 GitHub：{cfg['owner']}/{cfg['repo']}@{cfg['branch']}:{cfg['path']}"

    except Exception as e:
        return None, "", f"讀取 GitHub auth_config 例外：{e}"


def _write_auth_config_to_github(auth_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN，無法永久回寫 GitHub。"

    current_cfg, sha, read_msg = _read_auth_config_from_github()
    # 404 / 尚未建立時 sha 會是空，可以直接建立；其他讀取錯誤仍允許嘗試 PUT，但訊息要保留。
    if current_cfg is None and "尚未建立" not in read_msg:
        # 不直接中止，避免偶發讀取失敗造成完全不能存；PUT 若缺 sha 且檔案存在會失敗並回報。
        pass

    payload = copy.deepcopy(auth_cfg)
    payload["version"] = "v86_github_permanent"
    payload["updated_at"] = _now_text()

    content_text = json.dumps(payload, ensure_ascii=False, indent=2)
    encoded_content = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")

    body: Dict[str, Any] = {
        "message": f"Update auth_config.json from Streamlit @ {_now_text()}",
        "content": encoded_content,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha

    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已永久回寫 GitHub：{cfg['owner']}/{cfg['repo']}@{cfg['branch']}:{cfg['path']}"
        return False, f"GitHub 永久寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"GitHub 永久寫入例外：{e}"


# =========================================================
# 讀取 / 儲存 auth_config
# =========================================================
def _read_local_auth_config() -> Tuple[Dict[str, Any] | None, str]:
    try:
        if AUTH_CONFIG_FILE.exists():
            data = json.loads(AUTH_CONFIG_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("users"), dict):
                return data, f"已讀取本機 {AUTH_CONFIG_FILE.name}"
    except Exception as e:
        return None, f"讀取本機 auth_config 失敗：{e}"
    return None, "本機 auth_config 不存在。"


def _write_local_auth_config(auth_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        payload = copy.deepcopy(auth_cfg)
        payload["updated_at"] = _now_text()
        AUTH_CONFIG_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True, f"已寫入本機 {AUTH_CONFIG_FILE.name}"
    except Exception as e:
        return False, f"本機寫入失敗：{e}"


def _load_auth_config() -> Dict[str, Any]:
    """
    v86：優先讀 GitHub，失敗才讀本機。
    這樣帳號密碼才會跨重開機 / 重新部署永久保存。
    """
    gh_cfg, gh_sha, gh_msg = _read_auth_config_from_github()
    if gh_cfg is not None:
        st.session_state["auth_config_source"] = "GitHub"
        st.session_state["auth_config_message"] = gh_msg
        st.session_state["auth_config_sha"] = gh_sha
        return gh_cfg

    local_cfg, local_msg = _read_local_auth_config()
    if local_cfg is not None:
        st.session_state["auth_config_source"] = "local"
        st.session_state["auth_config_message"] = f"{gh_msg}；{local_msg}"
        st.session_state["auth_config_sha"] = ""
        return local_cfg

    default = _default_auth_config()
    st.session_state["auth_config_source"] = "default"
    st.session_state["auth_config_message"] = f"{gh_msg}；{local_msg}；使用預設 admin / 0000。"
    st.session_state["auth_config_sha"] = ""
    return default


def _save_auth_config(auth_cfg: Dict[str, Any]) -> Tuple[bool, str]:
    """
    v86：同時寫本機與 GitHub。
    GitHub 寫入成功才代表永久保存成功。
    """
    payload = copy.deepcopy(auth_cfg)
    payload["version"] = "v86_github_permanent"
    payload["updated_at"] = _now_text()

    local_ok, local_msg = _write_local_auth_config(payload)
    gh_ok, gh_msg = _write_auth_config_to_github(payload)

    st.session_state["auth_config_source"] = "GitHub" if gh_ok else ("local" if local_ok else "save_failed")
    st.session_state["auth_config_message"] = f"{local_msg}；{gh_msg}"

    if gh_ok:
        return True, f"{gh_msg}。{local_msg}。"
    if local_ok:
        return False, f"只暫存本機，尚未永久保存：{gh_msg}。請確認 Streamlit Secrets 的 GITHUB_TOKEN 權限。"
    return False, f"儲存失敗：{local_msg}；{gh_msg}"


# =========================================================
# 密碼驗證與登入狀態
# =========================================================
def _verify_password(user: Dict[str, Any], password: str) -> bool:
    password = str(password)

    ph = _safe_str(user.get("password_hash"))
    if ph and _sha256(password) == ph:
        return True

    # 舊版相容：明文欄位
    for k in ("password", "pwd"):
        plain = user.get(k, None)
        if plain is not None and str(plain) == password:
            return True

    return False


def is_logged_in() -> bool:
    return bool(st.session_state.get("auth_logged_in"))


def current_user() -> str:
    return _safe_str(st.session_state.get("auth_username"))


def current_role() -> str:
    return _safe_str(st.session_state.get("auth_role"))


def current_display_name() -> str:
    return _safe_str(st.session_state.get("auth_display_name")) or current_user()


def logout() -> None:
    for k in ["auth_logged_in", "auth_username", "auth_role", "auth_display_name"]:
        st.session_state.pop(k, None)
    st.rerun()





def _detect_current_page_key_for_table_manager() -> str:
    """v108：抓目前 Streamlit 頁面檔名，讓所有重型模組可套用防卡模式。"""
    try:
        import inspect
        for frame in inspect.stack()[1:30]:
            fn = str(getattr(frame, "filename", "") or "").replace("\\", "/")
            if "/pages/" in fn and fn.endswith(".py"):
                return Path(fn).stem
    except Exception:
        pass
    return "global"

def _install_v105_table_manager() -> None:
    """登入後自動啟用全系統表格管理。

    目的：不逐頁改程式，也能讓所有 st.dataframe / st.data_editor 具備：
    - 篩選 / 排序 / 顯示筆數永久記錄
    - 勾選欄位延後套用
    - 不因輸入欄位或勾選而重跑推薦 / 重新抓資料
    """
    try:
        # v108：每次頁面 rerun / require_login 時先重置側邊欄去重旗標，
        # 確保本次 rerun 只顯示一次「表格管理」，下一次 rerun 仍會正常顯示。
        st.session_state["_godpick_table_sidebar_rendered_this_run_v108"] = False
        from godpick_column_manager import install_auto_column_manager
        install_auto_column_manager(_detect_current_page_key_for_table_manager())
    except Exception:
        # 表格管理不可影響登入與主功能
        pass


def _ensure_macro_startup_update_v109() -> None:
    """v109：登入後啟動大盤走勢資料更新。

    目的：不用等使用者點進 0/1 大盤走勢頁，其他模組也能讀到
    market_snapshot.json / macro_mode_bridge.json。
    原則：
    - 不阻塞重型頁面：有舊快照時走背景更新。
    - 缺快照時做一次快速同步補底；失敗也不停止登入與頁面。
    - 同一個 session 不重複啟動。
    """
    try:
        from macro_startup_service import ensure_macro_startup_update
        status = ensure_macro_startup_update()
        if isinstance(status, dict):
            st.session_state["macro_startup_status_v109"] = status
    except Exception as _macro_e:
        # 大盤啟動更新不能影響登入、推薦、紀錄等主功能
        try:
            st.session_state["macro_startup_status_v109"] = {"ok": False, "message": f"v109 大盤啟動更新略過：{_macro_e}"}
        except Exception:
            pass


def require_login() -> bool:
    cfg = _load_auth_config()
    if str(cfg.get("auth_enabled", True)).lower() in ("false", "0", "no", "off"):
        _ensure_macro_startup_update_v109()
        _install_v105_table_manager()
        return True

    if is_logged_in():
        try:
            with st.sidebar:
                st.caption(f"登入帳號：{current_user()}｜{current_role()}")
                src = _safe_str(st.session_state.get("auth_config_source"))
                if src:
                    st.caption(f"帳號設定來源：{src}")
                macro_status = st.session_state.get("macro_startup_status_v109")
                if isinstance(macro_status, dict):
                    msg = _safe_str(macro_status.get("short_message") or macro_status.get("message"))
                    if msg:
                        st.caption(f"大盤啟動更新：{msg}")
                if st.button("登出", key="auth_logout_sidebar"):
                    logout()
        except Exception:
            pass
        _ensure_macro_startup_update_v109()
        _install_v105_table_manager()
        return True

    st.title("系統登入")
    st.info("請先輸入帳號密碼，登入後才能使用系統。")

    with st.form("auth_login_form_v86"):
        username = st.text_input("帳號", value="admin")
        password = st.text_input("密碼", value="", type="password")
        submitted = st.form_submit_button("登入", use_container_width=True)

    if submitted:
        users = cfg.get("users", {})
        user = users.get(_safe_str(username))

        if user and bool(user.get("enabled", True)) and _verify_password(user, password):
            st.session_state["auth_logged_in"] = True
            st.session_state["auth_username"] = _safe_str(username)
            st.session_state["auth_role"] = _safe_str(user.get("role", "user")) or "user"
            st.session_state["auth_display_name"] = _safe_str(user.get("display_name", username)) or _safe_str(username)
            st.success("登入成功")
            st.rerun()
        else:
            st.error("帳號或密碼錯誤。")

    msg = _safe_str(st.session_state.get("auth_config_message"))
    if msg:
        with st.expander("登入設定來源 / 儲存狀態", expanded=False):
            st.caption(msg)

    st.stop()


def require_role(roles) -> bool:
    require_login()
    if isinstance(roles, str):
        roles = [roles]
    if current_role() not in roles:
        st.error("你的帳號沒有權限使用此功能。")
        st.stop()
    return True


# =========================================================
# 帳號管理 API
# =========================================================
def get_auth_config_status() -> Dict[str, str]:
    _load_auth_config()
    cfg = _github_config()
    return {
        "source": _safe_str(st.session_state.get("auth_config_source")),
        "message": _safe_str(st.session_state.get("auth_config_message")),
        "github_repo": f"{cfg['owner']}/{cfg['repo']}",
        "github_branch": cfg["branch"],
        "github_path": cfg["path"],
        "token_ready": "是" if bool(cfg["token"]) else "否",
    }


def list_users() -> Dict[str, Any]:
    cfg = _load_auth_config()
    users = cfg.get("users", {})
    if not isinstance(users, dict):
        return {}
    return users


def change_password(username: str, old_password: str, new_password: str) -> Tuple[bool, str]:
    cfg = _load_auth_config()
    users = cfg.setdefault("users", {})
    username = _safe_str(username)
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
    user["updated_at"] = _now_text()

    return _save_auth_config(cfg)


def admin_set_user(
    username: str,
    password: str = "",
    role: str = "user",
    enabled: bool = True,
    display_name: str = "",
) -> Tuple[bool, str]:
    if current_role() != "admin":
        return False, "只有 admin 可以管理帳號。"

    username = _safe_str(username)
    if not username:
        return False, "帳號不可空白。"

    cfg = _load_auth_config()
    users = cfg.setdefault("users", {})
    old_user = users.get(username, {}) if isinstance(users.get(username), dict) else {}

    if not password and not old_user:
        return False, "新增帳號必須輸入密碼。"
    if password and len(str(password)) < 4:
        return False, "密碼至少 4 碼。"

    new_user = dict(old_user)
    if password:
        new_user.pop("password", None)
        new_user.pop("pwd", None)
        new_user["password_hash"] = _sha256(password)

    new_user["role"] = role or old_user.get("role") or "user"
    new_user["enabled"] = bool(enabled)
    new_user["display_name"] = display_name or old_user.get("display_name") or username
    new_user["updated_at"] = _now_text()
    if "created_at" not in new_user:
        new_user["created_at"] = _now_text()

    users[username] = new_user
    return _save_auth_config(cfg)


def admin_delete_user(username: str) -> Tuple[bool, str]:
    if current_role() != "admin":
        return False, "只有 admin 可以管理帳號。"

    username = _safe_str(username)
    if username == "admin":
        return False, "不可刪除 admin 帳號。"

    cfg = _load_auth_config()
    users = cfg.setdefault("users", {})
    if username not in users:
        return False, "找不到帳號。"

    users.pop(username, None)
    return _save_auth_config(cfg)


def reset_admin_password_to_0000() -> bool:
    cfg = _load_auth_config()
    users = cfg.setdefault("users", {})
    users["admin"] = {
        "password_hash": _sha256("0000"),
        "role": "admin",
        "enabled": True,
        "display_name": "系統管理員",
        "updated_at": _now_text(),
        "created_at": users.get("admin", {}).get("created_at", _now_text()) if isinstance(users.get("admin"), dict) else _now_text(),
    }
    ok, _msg = _save_auth_config(cfg)
    return ok
