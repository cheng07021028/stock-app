# google_drive_backup.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


TAIWAN_TZ = timezone(timedelta(hours=8))
DRIVE_SCOPE = ["https://www.googleapis.com/auth/drive"]


def _get_tw_now() -> datetime:
    return datetime.now(TAIWAN_TZ)


def _build_drive_credentials():
    project_id = st.secrets.get("GDRIVE_PROJECT_ID", "").strip()
    client_email = st.secrets.get("GDRIVE_CLIENT_EMAIL", "").strip()
    private_key = st.secrets.get("GDRIVE_PRIVATE_KEY", "")

    if not project_id:
        raise RuntimeError("未設定 GDRIVE_PROJECT_ID")
    if not client_email:
        raise RuntimeError("未設定 GDRIVE_CLIENT_EMAIL")
    if not private_key:
        raise RuntimeError("未設定 GDRIVE_PRIVATE_KEY")

    private_key = private_key.replace("\\n", "\n").strip()
    if "BEGIN PRIVATE KEY" not in private_key:
        raise RuntimeError("GDRIVE_PRIVATE_KEY 格式不正確")
    if not private_key.endswith("\n"):
        private_key += "\n"

    info = {
        "type": "service_account",
        "project_id": project_id,
        "private_key": private_key,
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    }

    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=DRIVE_SCOPE,
    )
    return creds


def get_drive_service():
    creds = _build_drive_credentials()
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
