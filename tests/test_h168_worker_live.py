# -*- coding: utf-8 -*-
"""H168 live Actions worker-state & Page17 gate regression, no external network."""
from __future__ import annotations

import ast
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from godpick_h168_worker_live import read_live_worker, should_autodispatch

TOKEN = "github_pat_PRIVATE_H168_DO_NOT_PRINT"
NOW = datetime(2026, 11, 18, 8, 30, 0, tzinfo=timezone.utc)


class MockResponse:
    def __init__(self, status: int, body=None):
        self.status_code = status
        self.body = body

    def json(self):
        return self.body


def run_item(i: int, status: str, conclusion=None, minutes=10, event="workflow_dispatch"):
    return {
        "id": i, "status": status, "conclusion": conclusion,
        "created_at": (NOW - timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z"),
        "event": event,
    }


class MockHttp:
    def __init__(self, status=200, items=None):
        self.status = status
        self.items = items
        self.calls = []

    def get(self, url, **kw):
        self.calls.append((url, kw))
        return MockResponse(self.status, {"workflow_runs": self.items or []})


def check(items, status=200):
    fake = MockHttp(status, items)
    result = read_live_worker(token=TOKEN, owner="cheng07021028", repo="stock-app",
                              http_get=fake.get, now=NOW)
    return result, fake


def test_running_worker_prevents_repeat_wakeup():
    a, fake = check([
        run_item(44264974046, "in_progress", minutes=18),
        run_item(44267240309, "pending", minutes=2),
        run_item(44266737910, "completed", "cancelled", minutes=6),
    ])
    assert a["available"]
    assert a["occupied"]
    assert a["running"] == 1 and a["queued"] == 1
    assert a["recent_cancelled"] == 1
    assert not should_autodispatch(True, a)
    assert fake.calls[0][0].endswith("/actions/workflows/godpick_auto_scheduler_v191.yml/runs")
    assert fake.calls[0][1]["params"]["per_page"] == 20


def test_pending_only_is_not_free_worker():
    a, _ = check([run_item(123, "pending", minutes=2)])
    assert a["running"] == 0 and a["queued"] == 1
    assert a["occupied"] and not should_autodispatch(True, a)


def test_queued_only_and_waiting_are_busy():
    a, _ = check([run_item(1, "queued"), run_item(2, "waiting")])
    assert a["occupied"] and a["queued"] == 2


def test_completed_history_does_not_block_due_work():
    a, _ = check([run_item(4, "completed", "failure")])
    assert a["available"] and not a["occupied"]
    assert should_autodispatch(True, a)
    assert not should_autodispatch(False, a)


def test_api_403_is_not_assumed_empty_or_working():
    a, _ = check([], status=403)
    assert not a["available"]
    assert not a["occupied"]
    assert a["http_status"] == 403
    # H19's existing 8-minute session cooldown remains fallback if API breaks.
    assert should_autodispatch(True, a)


def test_missing_token_is_safe():
    fake = MockHttp()
    a = read_live_worker(token="", owner="cheng07021028", repo="stock-app",
                         http_get=fake.get, now=NOW)
    assert not a["available"] and not fake.calls


def test_invalid_repository_does_not_issue_request():
    fake = MockHttp()
    a = read_live_worker(token=TOKEN, owner="../../api", repo="stock-app",
                         http_get=fake.get, now=NOW)
    assert not a["available"] and not fake.calls


def test_no_secret_in_returned_payload():
    a, fake = check([run_item(100000, "in_progress")])
    dumped = json.dumps(a, ensure_ascii=False)
    assert TOKEN not in dumped
    assert "PRIVATE" not in dumped
    assert "github_pat_" not in dumped
    assert "Authorization" not in dumped
    assert fake.calls[0][1]["headers"]["Authorization"] == "Bearer " + TOKEN
    assert a["runs"][0]["url"].endswith("/actions/runs/100000")


def test_stale_worker_still_honestly_reported_active():
    a, _ = check([run_item(666, "in_progress", minutes=95)])
    assert a["occupied"]
    assert not should_autodispatch(True, a)
    assert a["runs"][0]["age_minutes"] >= 95


def test_bad_api_response_never_masquerades_as_idle():
    class Bad:
        def get(self, *args, **kwargs):
            return MockResponse(200, {"workflow_runs": {}})
    a = read_live_worker(token=TOKEN, owner="cheng07021028", repo="stock-app",
                         http_get=Bad().get, now=NOW)
    assert not a["available"]


def test_page17_syntax_and_live_guard_before_wakeup():
    content = (Path(__file__).resolve().parents[1] / "pages" / "17_系統健康檢查.py").read_text("utf-8")
    ast.parse(content)
    assert "if _h168_should_dispatch:" in content
    assert "_h168_should_dispatch = _h168_allow_auto_dispatch(" in content
    assert "_h168_fragment_live = _h168_get_actions_snapshot()" in content
    assert content.index("_h168_should_dispatch = _h168_allow_auto_dispatch(") < content.index(
        "if _h168_should_dispatch:"
    )
    assert "H168｜GitHub Actions 已有執行中" in content
    assert "H168｜GitHub Actions 即時 Worker" in content


def test_original_dispatch_buttons_and_h126_remain_untouched():
    page = (Path(__file__).resolve().parents[1] / "pages" / "17_系統健康檢查.py").read_text("utf-8")
    assert '_h156_dispatch_refresh("market_refresh_and_recommend")' in page
    assert '_h156_dispatch_refresh("full_refresh_and_recommend")' in page
    assert 'dispatch_auto_scheduler_wakeup(' in page
    assert 'allow_buy=True' not in page


def test_many_cancelled_pending_is_exposed_not_green():
    a, _ = check([run_item(1,"completed","cancelled",minutes=4),
                  run_item(2,"completed","cancelled",minutes=8),
                  run_item(3,"completed","cancelled",minutes=19)])
    assert a["recent_cancelled"] == 3
    assert not a["occupied"]
    assert should_autodispatch(True, a)


def test_bad_run_id_does_not_leak_untrusted_links():
    a, _ = check([{"id":"https://x","status":"in_progress","created_at":"2026-11-18T08:22:00Z"}])
    assert a["available"]
    assert a["running"] == 0
    assert a["runs"] == []
