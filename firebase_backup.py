# firebase_backup.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import json
from datetime import datetime, timezone, timedelta

import requests
import streamlit as st

import firebase_admin
from firebase_admin import credentials, firestore, storage


TAIWAN_TZ = timezone(timedelta(hours=8))


def _get_tw_now():
    return datetime.now(TAIWAN_TZ)


def init_firebase():
    if firebase_admin._apps:
        return firebase_admin.get_app()

    raw_json = st.secrets.get("FIREBASE_SERVICE_ACCOUNT_JSON", "")
    bucket_name = st.secrets.get("FIREBASE_STORAGE_BUCKET", "")

    if not raw_json:
        raise RuntimeError("未設定 FIREBASE_SERVICE_ACCOUNT_JSON")
    if not bucket_name:
        raise RuntimeError("未設定 FIREBASE_STORAGE_BUCKET")

    info = json.loads(raw_json)
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
):
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

    bucket = storage.bucket()
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


def list_recent_backups(limit: int = 10):
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
