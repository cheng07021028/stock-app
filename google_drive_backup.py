# google_drive_backup_oauth.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
import streamlit as st
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


TAIWAN_TZ = timezone(timedelta(hours=8))
DRIVE_SCOPE = ["https://www.googleapis.com/auth/drive"]


def _get_tw_now() -> datetime:
    return datetime.now(TAIWAN_TZ)


def _get_oauth_client_config() -> dict[str, Any]:
    client_id = st.secrets.get("GDRIVE_OAUTH_CLIENT_ID", "").strip()
    client_secret = st.secrets.get("GDRIVE_OAUTH_CLIENT_SECRET", "").strip()
    redirect_uri = st.secrets.get("GDRIVE_OAUTH_REDIRECT_URI", "").strip()

    if not client_id:
        raise RuntimeError("未設定 GDRIVE_OAUTH_CLIENT_ID")
    if not client_secret:
        raise RuntimeError("未設定 GDRIVE_OAUTH_CLIENT_SECRET")
    if not redirect_uri:
        raise RuntimeError("未設定 GDRIVE_OAUTH_REDIRECT_URI")

    return {
        "web": {
            "client_id": client_id,
            "project_id": st.secrets.get("GDRIVE_PROJECT_ID", "stock-app-b137f"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": client_secret,
            "redirect_uris": [redirect_uri],
        }
    }


def _get_redirect_uri() -> str:
    redirect_uri = st.secrets.get("GDRIVE_OAUTH_REDIRECT_URI", "").strip()
    if not redirect_uri:
        raise RuntimeError("未設定 GDRIVE_OAUTH_REDIRECT_URI")
    return redirect_uri


def _build_flow(state: str | None = None) -> Flow:
    flow = Flow.from_client_config(
        _get_oauth_client_config(),
        scopes=DRIVE_SCOPE,
        state=state,
    )
    flow.redirect_uri = _get_redirect_uri()
    return flow


def get_google_auth_url() -> str:
    flow = _build_flow()
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    st.session_state["gdrive_oauth_state"] = state
    return authorization_url


def handle_google_oauth_callback():
    query_params = st.query_params
    code = query_params.get("code")
    state = query_params.get("state")
    error = query_params.get("error")

    if error:
        raise RuntimeError(f"Google 授權失敗：{error}")

    if not code:
        return False

    expected_state = st.session_state.get("gdrive_oauth_state")
    if expected_state and state != expected_state:
        raise RuntimeError("Google OAuth state 不符，請重新授權")

    flow = _build_flow(state=state)
    flow.fetch_token(code=code)

    creds = flow.credentials
    st.session_state["gdrive_oauth_credentials"] = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }

    # 清掉 callback 參數，避免每次重跑都重複交換 token
    try:
        st.query_params.clear()
    except Exception:
        pass

    return True


def is_google_connected() -> bool:
    return "gdrive_oauth_credentials" in st.session_state


def disconnect_google_drive():
    st.session_state.pop("gdrive_oauth_credentials", None)
    st.session_state.pop("gdrive_oauth_state", None)
    try:
        st.query_params.clear()
    except Exception:
        pass


def _get_credentials() -> Credentials:
    creds_info = st.session_state.get("gdrive_oauth_credentials")
    if not creds_info:
        raise RuntimeError("尚未完成 Google Drive 授權")

    creds = Credentials(
        token=creds_info.get("token"),
        refresh_token=creds_info.get("refresh_token"),
        token_uri=creds_info.get("token_uri"),
        client_id=creds_info.get("client_id"),
        client_secret=creds_info.get("client_secret"),
        scopes=creds_info.get("scopes"),
    )
    return creds


def get_drive_service():
    creds = _get_credentials()
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _escape_query_value(text: str) -> str:
    return text.replace("\\", "\\\\").replace("'", "\\'")


def ensure_backup_folder(service, folder_name: str, parent_folder_id: str | None = None) -> str:
    query_parts = [
        f"name = '{_escape_query_value(folder_name)}'",
        "mimeType = 'application/vnd.google-apps.folder'",
        "trashed = false",
    ]
    if parent_folder_id:
        query_parts.append(f"'{parent_folder_id}' in parents")

    query = " and ".join(query_parts)

    resp = (
        service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id, name)",
            pageSize=10,
        )
        .execute()
    )

    files = resp.get("files", [])
    if files:
        return files[0]["id"]

    metadata: dict[str, Any] = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_folder_id:
        metadata["parents"] = [parent_folder_id]

    created = (
        service.files()
        .create(
            body=metadata,
            fields="id, name",
        )
        .execute()
    )
    return created["id"]


def backup_github_repo_to_google_drive(
    repo_owner: str,
    repo_name: str,
    branch: str = "main",
    github_token: str | None = None,
    root_folder_name: str = "stock-app-backups",
) -> dict:
    service = get_drive_service()

    zip_url = f"https://github.com/{repo_owner}/{repo_name}/archive/refs/heads/{branch}.zip"

    headers = {"User-Agent": "streamlit-stock-app-drive-backup"}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"

    resp = requests.get(zip_url, headers=headers, timeout=120)
    resp.raise_for_status()

    zip_bytes = resp.content
    if not zip_bytes:
        raise RuntimeError("GitHub 回傳空內容，備份失敗")

    now = _get_tw_now()
    ts = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{repo_name}_{branch}_{ts}.zip"

    parent_folder_id = st.secrets.get("GDRIVE_BACKUP_PARENT_FOLDER_ID", "").strip() or None
    backup_folder_id = ensure_backup_folder(service, root_folder_name, parent_folder_id)

    file_metadata = {
        "name": filename,
        "parents": [backup_folder_id],
        "description": f"GitHub repo backup | {repo_owner}/{repo_name} | {branch} | {now.strftime('%Y-%m-%d %H:%M:%S')}",
    }

    media = MediaIoBaseUpload(
        io.BytesIO(zip_bytes),
        mimetype="application/zip",
        resumable=True,
    )

    created = (
        service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, name, webViewLink, webContentLink, createdTime, size",
        )
        .execute()
    )

    return {
        "ok": True,
        "file_id": created.get("id", ""),
        "file_name": created.get("name", filename),
        "web_view_link": created.get("webViewLink", ""),
        "size_bytes": int(created.get("size", len(zip_bytes)) or len(zip_bytes)),
        "backup_time_tw": now.strftime("%Y-%m-%d %H:%M:%S"),
        "folder_id": backup_folder_id,
    }


def list_recent_drive_backups(
    root_folder_name: str = "stock-app-backups",
    limit: int = 10,
) -> list[dict]:
    service = get_drive_service()

    parent_folder_id = st.secrets.get("GDRIVE_BACKUP_PARENT_FOLDER_ID", "").strip() or None
    backup_folder_id = ensure_backup_folder(service, root_folder_name, parent_folder_id)

    query = (
        f"'{backup_folder_id}' in parents and "
        "trashed = false and "
        "mimeType != 'application/vnd.google-apps.folder'"
    )

    resp = (
        service.files()
        .list(
            q=query,
            spaces="drive",
            orderBy="createdTime desc",
            pageSize=limit,
            fields="files(id, name, webViewLink, createdTime, size)",
        )
        .execute()
    )

    rows = []
    for f in resp.get("files", []):
        rows.append(
            {
                "備份時間": f.get("createdTime", ""),
                "檔名": f.get("name", ""),
                "大小(bytes)": int(f.get("size", 0) or 0),
                "Drive連結": f.get("webViewLink", ""),
                "檔案ID": f.get("id", ""),
            }
        )
    return rows
