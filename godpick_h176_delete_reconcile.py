# -*- coding: utf-8 -*-
"""H176: conservative, non-destructive Firestore gap reconciliation for record deletion.

Pure planning only. This module NEVER manufactures missing record identities and
NEVER treats a metadata summary as proof of a complete collection.
"""
from __future__ import annotations
import json
from collections.abc import Mapping
from typing import Any

VERSION = "V191-H176-FS-EXACT-GAP-RECONCILE"


def _stable(row: Mapping[str, Any]) -> str:
    # Firestore writers add/replace updated_at with server timestamps.  It is
    # transport metadata, not an original recommendation-record field.
    return json.dumps({str(k): v for k, v in row.items() if k != "updated_at"},
                      ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def plan_missing_record_repair(local_rows: list[dict], remote_rows: list[dict], *,
                               remote_doc_ids: list[str], max_missing: int = 300) -> dict:
    """Allow only missing IDs to be inserted. Any added/changed ID blocks.

    Both lists are actual document bodies, *not* cached remote summaries.
    The caller must separately prove an immutable GitHub authority with the
    same complete local payload hash and check Firestore revision freshness.
    """
    if not isinstance(local_rows, list) or not local_rows:
        return {"allowed": False, "reason": "本機完整紀錄不存在"}
    if len(remote_rows) != len(remote_doc_ids):
        return {"allowed": False, "reason": "Firestore實際文件與ID數量不符"}
    def make_index(rows: list[dict], ids: list[str]) -> tuple[dict[str, dict], str]:
        result: dict[str, dict] = {}
        for row, rid in zip(rows, ids):
            if not isinstance(row, dict) or not isinstance(rid, str) or not rid:
                return {}, "有空白或無效record_id"
            if rid in result or str(row.get("record_id") or rid) != rid:
                return {}, "有重複record_id或文件ID與內容不符"
            result[rid] = row
        return result, ""
    local_idx, err = make_index(local_rows, [str(r.get("record_id") or "") if isinstance(r,dict) else "" for r in local_rows])
    if err:
        return {"allowed": False, "reason": "本機" + err}
    remote_idx, err = make_index(remote_rows, remote_doc_ids)
    if err:
        return {"allowed": False, "reason": "Firestore" + err}
    extra = sorted(set(remote_idx) - set(local_idx))
    if extra:
        return {"allowed": False, "reason": f"Firestore另有{len(extra)}筆本機不存在的紀錄，不得覆蓋或刪除", "extra_ids": extra[:20]}
    conflicts = [rid for rid in remote_idx if _stable(remote_idx[rid]) != _stable(local_idx[rid])]
    if conflicts:
        return {"allowed": False, "reason": f"Firestore有{len(conflicts)}筆同ID但內容不同，不得自動修復", "conflict_ids": conflicts[:20]}
    missing_ids = [rid for rid in local_idx if rid not in remote_idx]
    if len(missing_ids) > max_missing:
        return {"allowed": False, "reason": f"Firestore缺{len(missing_ids)}筆，超過單次修復上限{max_missing}筆；需人工核對"}
    return {"allowed": True, "missing_ids": missing_ids,
            "missing_rows": [local_idx[rid] for rid in missing_ids],
            "existing_count": len(remote_idx), "expected_count": len(local_idx),
            "reason": f"已比對{len(remote_idx)}筆既有紀錄；僅需補入{len(missing_ids)}個缺漏record_id"}


__all__ = ["VERSION", "plan_missing_record_repair"]
