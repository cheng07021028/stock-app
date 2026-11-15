# -*- coding: utf-8 -*-
"""V191-H163: in-process Firestore quota fuse and short summary cache.

This module never grants authority, changes persisted history or stores secrets.
A cached summary is only an optimization of an *already returned* remote summary;
all H141/H161 hash, ID, and durable-write gates remain in the caller.
"""
from __future__ import annotations

import copy
import threading
import time
from collections.abc import Mapping
from typing import Any

VERSION = "V191-H163-FIRESTORE-READ-GUARD"


def is_quota_exception(error: BaseException) -> bool:
    """Recognize Firestore/gRPC quota responses without importing firebase_admin."""
    for item in (error, getattr(error, "__cause__", None)):
        if item is None:
            continue
        name = type(item).__name__.lower()
        if "resourceexhausted" in name or "toomanyrequests" in name:
            return True
        value = str(item).lower()
        if any(term in value for term in (
            "quota exceeded", "quota_exceeded", "resource_exhausted",
            "resource exhausted", "429 quota", "429 too many requests",
            "daily quota", "read quota",
        )):
            return True
        code = getattr(item, "code", None)
        try:
            c = code() if callable(code) else code
            if "RESOURCE_EXHAUSTED" in str(c).upper():
                return True
        except Exception:
            pass
    return False


class FirestoreReadGuard:
    """Process-wide bounded checks, no persistent or remote mutations.

    On 429 the fuse blocks all Firestore calls in this process for 30 minutes.
    A failed large collection read is independently cooled for 3 minutes, so
    repeatedly opening the page cannot repeatedly charge thousands of reads.
    """

    def __init__(self, *, clock=time.monotonic, cache_seconds=90.0,
                 quota_seconds=1800.0, full_retry_seconds=180.0):
        self._lock = threading.RLock()
        self._clock = clock
        self.cache_seconds = float(cache_seconds)
        self.quota_seconds = float(quota_seconds)
        self.full_retry_seconds = float(full_retry_seconds)
        self._summary: dict[str, Any] = {}
        self._summary_at = 0.0
        self._has_summary = False
        self._quota_until = 0.0
        self._quota_origin = ""
        self._full_failure_until = 0.0
        self._full_fail_key = ""
        self._summary_hits = 0
        self._full_blocked = 0
        self._quota_skipped = 0

    def quota_reason(self) -> str:
        with self._lock:
            left = max(0, self._quota_until - self._clock())
            if left <= 0:
                return ""
            self._quota_skipped += 1
            return f"H163 Firestore 429 配額保護中，暫停讀寫約 {int(left + 59) // 60} 分鐘；改以已驗證 GitHub/本機權威為準，禁止冒充遠端成功"

    def note_error(self, error: BaseException, origin: str) -> bool:
        if not is_quota_exception(error):
            return False
        with self._lock:
            self._quota_until = max(self._quota_until, self._clock() + self.quota_seconds)
            self._quota_origin = str(origin)[:60]
            self._has_summary = False
            self._summary.clear()
        return True

    def cached_summary(self) -> dict[str, Any] | None:
        with self._lock:
            if (not self._has_summary or self._quota_until > self._clock() or
                    self._clock() - self._summary_at >= self.cache_seconds):
                return None
            self._summary_hits += 1
            return copy.deepcopy(self._summary)

    def note_summary(self, payload: Any) -> None:
        if not isinstance(payload, Mapping) or not payload:
            return
        with self._lock:
            self._summary = copy.deepcopy(dict(payload))
            self._summary_at = self._clock()
            self._has_summary = True

    def invalidate_summary(self) -> None:
        with self._lock:
            self._has_summary = False
            self._summary.clear()

    def full_read_blocked(self, key: str) -> str:
        with self._lock:
            if self._full_fail_key == str(key) and self._full_failure_until > self._clock():
                self._full_blocked += 1
                left = max(1, int(self._full_failure_until - self._clock()))
                return f"H163 同一份 Firestore 全量讀取前次未成功，冷卻 {left} 秒；避免每次頁面刷新重讀數千筆"
            return ""

    def note_full_failure(self, key: str) -> None:
        with self._lock:
            self._full_fail_key = str(key)
            self._full_failure_until = self._clock() + self.full_retry_seconds

    def note_full_success(self) -> None:
        with self._lock:
            self._full_failure_until = 0.0
            self._full_fail_key = ""

    def status(self) -> dict[str, Any]:
        with self._lock:
            now = self._clock()
            return {
                "quota_blocked": self._quota_until > now,
                "quota_remaining_seconds": max(0, int(self._quota_until - now)),
                "quota_origin": self._quota_origin if self._quota_until > now else "",
                "summary_cache_valid": bool(self._has_summary and self._summary_at + self.cache_seconds > now),
                "summary_cache_hits": self._summary_hits,
                "full_read_cooldown": bool(self._full_failure_until > now),
                "full_read_prevented": self._full_blocked,
                "quota_calls_prevented": self._quota_skipped,
            }


firestore_read_guard = FirestoreReadGuard()
