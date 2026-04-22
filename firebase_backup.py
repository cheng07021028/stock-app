# firebase_backup.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone

import requests
import streamlit as st

import firebase_admin
from firebase_admin import credentials, firestore, storage


TAIWAN_TZ = timezone(timedelta(hours=8))


def _get_tw_now() -> datetime:
    return datetime.now(TAIWAN_TZ)


def _build_credential_info_from_secrets() -> dict:
    project_id = st.secrets.get("FIREBASE_PROJECT_ID", "").strip()
    client_email = st.secrets.get("FIREBASE_CLIENT_EMAIL", "").strip()
    private_key = st.secrets.get("FIREBASE_PRIVATE_KEY", "")

    if not project_id:
        raise RuntimeError("未設定 FIREBASE_PROJECT_ID")
    if not client_email:
        raise RuntimeError("未設定 FIREBASE_CLIENT_EMAIL")
    if not private_key:
        raise RuntimeError("未設定 FIREBASE_PRIVATE_KEY")

    # 某些情況 Secrets 內是字面上的 \n，需要轉成真正換行
    private_key = private_key.replace("\\n", "\n").strip()
    if "BEGIN PRIVATE KEY" not in private_key:
        raise RuntimeError("FIREBASE_PRIVATE_KEY 格式不正確，缺少 BEGIN PRIVATE KEY")
    if not private_key.endswith("\n"):
        private_key += "\n"

    return {
        "type": "service_account",
        "project_id": project_id,
        "private_key": private_key,
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    }


def init_firebase():
    """
    初始化 Firebase Admin。
    只初始化一次，避免重複 initialize_app() 報錯。
    這個版本不吃整份 JSON，直接從 Secrets 拆欄位組 credentials。
    """
    if firebase_admin._apps:
        return firebase_admin.get_app()

    bucket_name = st.secrets.get("FIREBASE_STORAGE_BUCKET", "").strip()
    if not bucket_name:
        raise RuntimeError("未設定 FIREBASE_STORAGE_BUCKET")

    info = _build_credential_info_from_secrets()
    cred = credentials.Certificate(info)

    app = firebase_admin.initialize_app(
        cred,
        {
            "storageBucket": bucket_name,
        },
    )
    return app


def backup_github_repo_to_firebase(
    repo_owner: str,
    repo_name: str,
    branch: str = "main",
    github_token: str | None = None,
) -> dict:
    """
    從 GitHub 下載 repo zip，備份到 Firebase Storage，
    並同步寫入 Firestore 備份紀錄。
    """
    init_firebase()

    zip_url = f"https://github.com/{repo_owner}/{repo_name}/archive/refs/heads/{branch}.zip"

    headers = {"User-Agent": "streamlit-stock-app-backup"}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"

    resp = requests.get(zip_url, headers=headers, timeout=120)
    resp.raise_for_status()

    zip_bytes = resp.content
    if not zip_bytes:
        raise RuntimeError("GitHub 回傳空內容，備份失敗")

    now = _get_tw_now()
    ts = now.strftime("%Y%m%d_%H%M%S")
    blob_name = f"github_backups/{repo_name}/{branch}/{repo_name}_{branch}_{ts}.zip"

    bucket_name = st.secrets.get("FIREBASE_STORAGE_BUCKET", "").strip()
    if not bucket_name:
        raise RuntimeError("未設定 FIREBASE_STORAGE_BUCKET")

    bucket = storage.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_file(
        io.BytesIO(zip_bytes),
        content_type="application/zip",
    )

    blob.metadata = {
        "repo_owner": repo_owner,
        "repo_name": repo_name,
        "branch": branch,
        "backup_time_tw": now.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "github_repo_zip",
    }
    blob.patch()

    db = firestore.client()
    record = {
        "repo_owner": repo_owner,
        "repo_name": repo_name,
        "branch": branch,
        "storage_path": blob_name,
        "size_bytes": len(zip_bytes),
        "backup_time_tw": now.strftime("%Y-%m-%d %H:%M:%S"),
        "backup_time_iso": now.isoformat(),
        "status": "success",
        "source_url": zip_url,
    }
    db.collection("github_repo_backups").add(record)

    return {
        "ok": True,
        "storage_path": blob_name,
        "size_bytes": len(zip_bytes),
        "backup_time_tw": now.strftime("%Y-%m-%d %H:%M:%S"),
    }


def list_recent_backups(limit: int = 10) -> list[dict]:
    """
    讀取最近幾筆備份紀錄。
    """
    init_firebase()
    db = firestore.client()

    docs = (
        db.collection("github_repo_backups")
        .order_by("backup_time_iso", direction=firestore.Query.DESCENDING)
        .limit(limit)
        .stream()
    )

    rows = []
    for doc in docs:
        d = doc.to_dict() or {}
        rows.append(
            {
                "備份時間": d.get("backup_time_tw", ""),
                "專案": f"{d.get('repo_owner', '')}/{d.get('repo_name', '')}",
                "分支": d.get("branch", ""),
                "檔案路徑": d.get("storage_path", ""),
                "大小(bytes)": d.get("size_bytes", 0),
                "狀態": d.get("status", ""),
            }
        )
    return rows
