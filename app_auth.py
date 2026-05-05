# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

import streamlit as st

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

AUTH_CONFIG_FILE = Path("auth_config.json")
DEFAULT_USER = "admin"
DEFAULT_PASSWORD_HASH = "9af15b336e6a9619928537df30b2e6a2376569fcf9d7e773eccede65606529a0"  # 0000


def _sha256(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_secret(name: str, default: str = "") -> str:
    try:
        val = st.secrets.get(name, "")
        if val is not None and str(val).strip():
            return str(val).strip()
    except Exception:
        pass
    val = os.getenv(name, "")
    return str(val).strip() if val else default


def _default_config() -> dict[str, Any]:
    return {
        "enabled": True,
        "version": "v73_multi_account",
        "accounts": [
            {
                "username": DEFAULT_USER,
                "password_sha256": DEFAULT_PASSWORD_HASH,
                "role": "admin",
                "enabled": True,
                "display_name": "系統管理員",
                "must_change_password": True,
                "created_at": _now_ts(),
                "updated_at": _now_ts(),
            }
        ],
        "note": "預設帳號 admin / 密碼 0000。請登入後到 帳號管理 修改密碼或新增帳號。",
    }


def _normalize_config(cfg: Any) -> dict[str, Any]:
    if not isinstance(cfg, dict):
        cfg = {}

    # 舊版單一帳密相容
    if "accounts" not in cfg:
        username = str(cfg.get("username") or DEFAULT_USER).strip() or DEFAULT_USER
        pwd_hash = str(cfg.get("password_sha256") or cfg.get("password_hash") or DEFAULT_PASSWORD_HASH).strip()
        cfg = {
            "enabled": cfg.get("enabled", True),
            "version": "v73_multi_account",
            "accounts": [
                {
                    "username": username,
                    "password_sha256": pwd_hash,
                    "role": "admin",
                    "enabled": True,
                    "display_name": username,
                    "must_change_password": pwd_hash == DEFAULT_PASSWORD_HASH,
                    "created_at": cfg.get("created_at") or _now_ts(),
                    "updated_at": cfg.get("updated_at") or _now_ts(),
                }
            ],
            "note": cfg.get("note", "single-user config migrated to multi-account config"),
        }

    accounts = []
    seen = set()
    for acc in cfg.get("accounts", []) or []:
        if not isinstance(acc, dict):
            continue
        username = str(acc.get("username", "")).strip()
        if not username or username.lower() in seen:
            continue
        seen.add(username.lower())
        role = str(acc.get("role") or "user").strip().lower()
        if role not in {"admin", "user", "viewer"}:
            role = "user"
        accounts.append(
            {
                "username": username,
                "password_sha256": str(acc.get("password_sha256") or acc.get("password_hash") or DEFAULT_PASSWORD_HASH),
                "role": role,
                "enabled": bool(acc.get("enabled", True)),
                "display_name": str(acc.get("display_name") or username),
                "must_change_password": bool(acc.get("must_change_password", False)),
                "created_at": str(acc.get("created_at") or _now_ts()),
                "updated_at": str(acc.get("updated_at") or _now_ts()),
                "last_login_at": str(acc.get("last_login_at") or ""),
            }
        )
    if not accounts:
        accounts = _default_config()["accounts"]
    if not any(a.get("role") == "admin" and a.get("enabled") for a in accounts):
        accounts[0]["role"] = "admin"
        accounts[0]["enabled"] = True
    cfg["accounts"] = accounts
    cfg["enabled"] = str(cfg.get("enabled", True)).lower() not in {"0", "false", "no", "off", "disabled"}
    cfg["version"] = "v73_multi_account"
    return cfg


def load_auth_config() -> dict[str, Any]:
    cfg = _normalize_config(_read_json(AUTH_CONFIG_FILE, {}))

    # Streamlit Secrets / ENV 可覆蓋或新增一組管理員帳號。注意：Secrets 不能由頁面改寫。
    secrets_password = _get_secret("APP_PASSWORD") or _get_secret("AUTH_PASSWORD")
    secrets_username = _get_secret("APP_USERNAME") or _get_secret("AUTH_USERNAME")
    users_json = _get_secret("APP_USERS_JSON") or _get_secret("AUTH_USERS_JSON")
    enabled_text = _get_secret("APP_AUTH_ENABLED")
    if enabled_text:
        cfg["enabled"] = enabled_text.lower() not in {"0", "false", "no", "off", "disabled"}

    if users_json:
        try:
            secret_cfg = json.loads(users_json)
            if isinstance(secret_cfg, list):
                cfg["accounts"] = _normalize_config({"enabled": cfg.get("enabled", True), "accounts": secret_cfg})["accounts"]
                cfg["source"] = "Streamlit Secrets: APP_USERS_JSON"
        except Exception:
            cfg["secret_users_error"] = "APP_USERS_JSON 格式錯誤，已改用 auth_config.json"

    if secrets_password:
        su = secrets_username or DEFAULT_USER
        exists = False
        for acc in cfg["accounts"]:
            if acc["username"].lower() == su.lower():
                acc["password_sha256"] = _sha256(secrets_password)
                acc["role"] = "admin"
                acc["enabled"] = True
                acc["source"] = "Streamlit Secrets / ENV"
                exists = True
                break
        if not exists:
            cfg["accounts"].insert(
                0,
                {
                    "username": su,
                    "password_sha256": _sha256(secrets_password),
                    "role": "admin",
                    "enabled": True,
                    "display_name": su,
                    "must_change_password": False,
                    "created_at": _now_ts(),
                    "updated_at": _now_ts(),
                    "source": "Streamlit Secrets / ENV",
                },
            )
        cfg["source"] = "Streamlit Secrets / ENV + auth_config.json"
    else:
        cfg.setdefault("source", "auth_config.json")
    return cfg


def _find_account(cfg: dict[str, Any], username: str) -> dict[str, Any] | None:
    u = str(username or "").strip().lower()
    for acc in cfg.get("accounts", []) or []:
        if str(acc.get("username", "")).strip().lower() == u:
            return acc
    return None


def verify_credentials(username: str, password: str) -> tuple[bool, str, dict[str, Any] | None]:
    cfg = load_auth_config()
    acc = _find_account(cfg, username)
    if not acc:
        return False, "帳號不存在", None
    if not acc.get("enabled", True):
        return False, "帳號已停用", None
    if _sha256(password) != str(acc.get("password_sha256")):
        return False, "密碼錯誤", None
    return True, "登入成功", acc


def _github_save_file(path: str, content: str, message: str) -> tuple[bool, str]:
    token = _get_secret("GITHUB_TOKEN")
    repo = _get_secret("GITHUB_REPOSITORY") or _get_secret("GITHUB_REPO")
    branch = _get_secret("GITHUB_BRANCH", "main")
    if not token or not repo:
        return False, "未設定 GITHUB_TOKEN / GITHUB_REPOSITORY，僅儲存在目前環境。"
    if requests is None:
        return False, "requests 套件不可用，無法同步 GitHub。"
    api = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    sha = None
    try:
        r = requests.get(api, headers=headers, params={"ref": branch}, timeout=15)
        if r.status_code == 200:
            sha = r.json().get("sha")
        payload = {
            "message": message,
            "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
            "branch": branch,
        }
        if sha:
            payload["sha"] = sha
        r2 = requests.put(api, headers=headers, json=payload, timeout=20)
        if r2.status_code in {200, 201}:
            return True, "已同步 GitHub。"
        return False, f"GitHub 同步失敗：HTTP {r2.status_code} {r2.text[:160]}"
    except Exception as e:
        return False, f"GitHub 同步例外：{e}"


def save_auth_config(cfg: dict[str, Any], sync_github: bool = True) -> tuple[bool, str]:
    cfg = _normalize_config(cfg)
    cfg["updated_at"] = _now_ts()
    text = json.dumps(cfg, ensure_ascii=False, indent=2)
    try:
        AUTH_CONFIG_FILE.write_text(text, encoding="utf-8")
    except Exception as e:
        return False, f"寫入 auth_config.json 失敗：{e}"
    msg = "已儲存 auth_config.json。"
    if sync_github:
        ok, gh_msg = _github_save_file("auth_config.json", text, "update auth accounts")
        msg += " " + gh_msg
    return True, msg


def current_user() -> str:
    return str(st.session_state.get("app_auth_user", ""))


def current_role() -> str:
    return str(st.session_state.get("app_auth_role", ""))


def is_admin() -> bool:
    return current_role() == "admin"


def logout_button(location: str = "sidebar") -> None:
    target = st.sidebar if location == "sidebar" else st
    if st.session_state.get("app_auth_ok"):
        user = st.session_state.get("app_auth_user", "")
        role = st.session_state.get("app_auth_role", "")
        target.caption(f"已登入：{user}｜{role}")
        if target.button("登出", key="app_auth_logout_btn", use_container_width=True):
            for k in ["app_auth_ok", "app_auth_user", "app_auth_role", "app_auth_must_change"]:
                st.session_state.pop(k, None)
            st.rerun()


def require_login() -> None:
    """在 streamlit_app.py 與每個 pages/*.py 最上方呼叫，未登入就停止頁面。"""
    cfg = load_auth_config()
    if not cfg.get("enabled", True):
        return
    if st.session_state.get("app_auth_ok") is True:
        return

    st.markdown(
        """
    <style>
    [data-testid="stSidebar"] {display:none;}
    .auth-card{max-width:520px;margin:7vh auto 12px auto;padding:32px;border-radius:22px;border:1px solid #e5e7eb;box-shadow:0 12px 34px rgba(15,23,42,.12);background:#fff;}
    .auth-title{font-size:30px;font-weight:900;color:#0f172a;margin-bottom:6px;}
    .auth-sub{color:#64748b;margin-bottom:18px;}
    </style>
    <div class="auth-card">
      <div class="auth-title">股票系統登入</div>
      <div class="auth-sub">請先輸入帳號密碼，通過後才能使用各模組。</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    with st.form("app_login_form", clear_on_submit=False):
        username = st.text_input("帳號", value="admin")
        password = st.text_input("密碼", type="password")
        submitted = st.form_submit_button("登入", use_container_width=True, type="primary")

    if submitted:
        ok, msg, acc = verify_credentials(username, password)
        if ok and acc:
            st.session_state["app_auth_ok"] = True
            st.session_state["app_auth_user"] = str(acc.get("username"))
            st.session_state["app_auth_role"] = str(acc.get("role") or "user")
            st.session_state["app_auth_must_change"] = bool(acc.get("must_change_password", False))
            # 更新 last_login_at，但不強制 GitHub，避免登入變慢
            cfg2 = load_auth_config()
            acc2 = _find_account(cfg2, acc.get("username"))
            if acc2:
                acc2["last_login_at"] = _now_ts()
                try:
                    _write_json(AUTH_CONFIG_FILE, cfg2)
                except Exception:
                    pass
            st.success("登入成功")
            st.rerun()
        else:
            st.error(f"登入失敗：{msg}")

    if cfg.get("secret_users_error"):
        st.warning(cfg.get("secret_users_error"))
    if any(a.get("password_sha256") == DEFAULT_PASSWORD_HASH for a in cfg.get("accounts", [])):
        st.warning("偵測到預設密碼 0000。上線後請到『帳號管理』變更密碼。")
    st.stop()


def _password_strength_ok(pwd: str) -> tuple[bool, str]:
    if len(pwd or "") < 6:
        return False, "密碼至少 6 碼。"
    if pwd in {"0000", "1234", "123456", "password", "admin"}:
        return False, "密碼太簡單，請更換。"
    return True, "OK"


def render_account_management_page() -> None:
    require_login()
    logout_button("sidebar")
    cfg = load_auth_config()
    user = current_user()
    role = current_role()

    st.title("帳號管理｜v73 多帳號版")
    st.caption("可自行修改密碼；管理員可新增、停用、重設帳號。設定會寫入 auth_config.json；若 GitHub Token 正常，會同步回 GitHub。")

    if st.session_state.get("app_auth_must_change"):
        st.warning("目前帳號仍使用預設或需要變更密碼，請先修改密碼。")

    st.subheader("修改我的密碼")
    with st.form("change_my_password_form", clear_on_submit=True):
        old = st.text_input("目前密碼", type="password")
        new1 = st.text_input("新密碼", type="password")
        new2 = st.text_input("確認新密碼", type="password")
        submit = st.form_submit_button("更新我的密碼", type="primary", use_container_width=True)
    if submit:
        ok, msg, acc = verify_credentials(user, old)
        if not ok or not acc:
            st.error("目前密碼錯誤。")
        elif new1 != new2:
            st.error("兩次新密碼不一致。")
        else:
            ok2, msg2 = _password_strength_ok(new1)
            if not ok2:
                st.error(msg2)
            else:
                cfg2 = load_auth_config()
                acc2 = _find_account(cfg2, user)
                if acc2:
                    acc2["password_sha256"] = _sha256(new1)
                    acc2["must_change_password"] = False
                    acc2["updated_at"] = _now_ts()
                    saved, save_msg = save_auth_config(cfg2)
                    if saved:
                        st.session_state["app_auth_must_change"] = False
                        st.success("密碼已更新。" + save_msg)
                    else:
                        st.error(save_msg)

    if role != "admin":
        st.info("你不是管理員，只能修改自己的密碼。")
        return

    st.divider()
    st.subheader("帳號清單")
    safe_rows = []
    for a in cfg.get("accounts", []):
        safe_rows.append(
            {
                "帳號": a.get("username"),
                "名稱": a.get("display_name"),
                "角色": a.get("role"),
                "啟用": bool(a.get("enabled", True)),
                "需改密碼": bool(a.get("must_change_password", False)),
                "最後登入": a.get("last_login_at", ""),
                "更新時間": a.get("updated_at", ""),
            }
        )
    st.dataframe(safe_rows, use_container_width=True, hide_index=True)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("新增帳號")
        with st.form("add_account_form", clear_on_submit=True):
            new_user = st.text_input("新帳號")
            display_name = st.text_input("顯示名稱")
            new_role = st.selectbox("角色", ["user", "viewer", "admin"], index=0)
            new_pwd = st.text_input("初始密碼", type="password")
            add_submit = st.form_submit_button("新增帳號", type="primary", use_container_width=True)
        if add_submit:
            username = str(new_user).strip()
            if not username:
                st.error("請輸入帳號。")
            elif _find_account(cfg, username):
                st.error("帳號已存在。")
            else:
                okp, msgp = _password_strength_ok(new_pwd)
                if not okp:
                    st.error(msgp)
                else:
                    cfg["accounts"].append(
                        {
                            "username": username,
                            "display_name": display_name.strip() or username,
                            "password_sha256": _sha256(new_pwd),
                            "role": new_role,
                            "enabled": True,
                            "must_change_password": True,
                            "created_at": _now_ts(),
                            "updated_at": _now_ts(),
                        }
                    )
                    saved, save_msg = save_auth_config(cfg)
                    st.success(save_msg) if saved else st.error(save_msg)

    with c2:
        st.subheader("管理既有帳號")
        usernames = [a["username"] for a in cfg.get("accounts", [])]
        target = st.selectbox("選擇帳號", usernames)
        target_acc = _find_account(cfg, target)
        if target_acc:
            with st.form("edit_account_form", clear_on_submit=False):
                dn = st.text_input("顯示名稱", value=str(target_acc.get("display_name") or target))
                rr = st.selectbox("角色", ["user", "viewer", "admin"], index=["user", "viewer", "admin"].index(str(target_acc.get("role") or "user")))
                en = st.checkbox("啟用帳號", value=bool(target_acc.get("enabled", True)))
                must = st.checkbox("下次登入要求改密碼", value=bool(target_acc.get("must_change_password", False)))
                reset_pwd = st.text_input("重設密碼，留空則不變", type="password")
                col_a, col_b = st.columns(2)
                save_edit = col_a.form_submit_button("儲存修改", type="primary", use_container_width=True)
                delete_acc = col_b.form_submit_button("刪除帳號", use_container_width=True)
            if save_edit:
                if target == user and rr != "admin":
                    st.error("不能把自己的管理員權限移除。")
                else:
                    if reset_pwd:
                        okp, msgp = _password_strength_ok(reset_pwd)
                        if not okp:
                            st.error(msgp)
                            st.stop()
                        target_acc["password_sha256"] = _sha256(reset_pwd)
                        target_acc["must_change_password"] = True
                    target_acc["display_name"] = dn.strip() or target
                    target_acc["role"] = rr
                    target_acc["enabled"] = bool(en)
                    target_acc["must_change_password"] = bool(must or bool(reset_pwd))
                    target_acc["updated_at"] = _now_ts()
                    saved, save_msg = save_auth_config(cfg)
                    st.success(save_msg) if saved else st.error(save_msg)
            if delete_acc:
                if target == user:
                    st.error("不能刪除目前登入中的自己。")
                elif target_acc.get("role") == "admin" and sum(1 for a in cfg.get("accounts", []) if a.get("role") == "admin" and a.get("enabled", True)) <= 1:
                    st.error("至少要保留一個啟用的管理員帳號。")
                else:
                    cfg["accounts"] = [a for a in cfg.get("accounts", []) if str(a.get("username")) != target]
                    saved, save_msg = save_auth_config(cfg)
                    st.success("帳號已刪除。" + save_msg) if saved else st.error(save_msg)

    with st.expander("Streamlit Secrets 設定說明", expanded=False):
        st.code(
            'APP_AUTH_ENABLED = "true"\nAPP_USERNAME = "admin"\nAPP_PASSWORD = "請改成強密碼"\n# 或使用多帳號 JSON：\n# APP_USERS_JSON = \'[{"username":"admin","password_sha256":"...","role":"admin","enabled":true}]\'',
            language="toml",
        )
