# -*- coding: utf-8 -*-
"""V191-H150: nonblocking login kick-off for existing official GitHub Actions collectors.

A workflow dispatch is NOT proof of data freshness. Never reads/replaces user
settings, official caches, H126 decisions, trade logs, or runtime-data files.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo
import re
import requests

VERSION = 'V191-H150.0'
WORKFLOW = 'h150_data_orchestrator.yml'
TZ = ZoneInfo('Asia/Taipei')
API = 'https://api.github.com'


def _stamp(s: str) -> datetime | None:
    try:
        d = datetime.fromisoformat(str(s).replace('Z', '+00:00'))
        return d.astimezone(timezone.utc) if d.tzinfo else None
    except (ValueError, TypeError, OverflowError):
        return None


def _config_ok(cfg: dict[str, Any]) -> bool:
    return bool(cfg.get('token') and re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', str(cfg.get('owner', '')))
                and re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', str(cfg.get('repo', '')))
                and cfg.get('branch') == 'main')


def plan_from_runs(runs: Any, *, now: datetime, cooldown_minutes: int = 90) -> dict[str, Any]:
    """Host-independent fail-closed idempotency decision.

    Every matching workflow run counts, including failed runs, to prevent
    one-user-per-refresh retry storms. Only a completed success is marked done.
    """
    if not isinstance(runs, list):
        return {'action': 'BLOCKED', 'reason': 'INVALID_REMOTE_RUNS'}
    moment = now.astimezone(timezone.utc)
    latest = None
    for x in runs[:30]:
        if not isinstance(x, dict) or x.get('head_branch') != 'main':
            continue
        t = _stamp(x.get('created_at', ''))
        if t is None or t > moment + timedelta(minutes=2):
            continue
        if latest is None or t > latest[0]:
            latest = (t, x)
    if latest:
        created, item = latest
        elapsed = moment - created
        if elapsed < timedelta(minutes=cooldown_minutes):
            return {'action': 'SKIP', 'reason': 'RECENT_WORKFLOW_EXISTS',
                    'workflow_state': str(item.get('status', '')),
                    'conclusion': str(item.get('conclusion') or ''),
                    'workflow_url': str(item.get('html_url') or '')}
        # A successful night run can serve the rest of the current market day.
        if (item.get('status') == 'completed' and item.get('conclusion') == 'success'
                and created.astimezone(TZ).date() == moment.astimezone(TZ).date()):
            return {'action': 'SKIP', 'reason': 'COMPLETED_TODAY',
                    'workflow_state': 'completed', 'conclusion': 'success',
                    'workflow_url': str(item.get('html_url') or '')}
    return {'action': 'DISPATCH', 'reason': 'NO_RECENT_WORKFLOW'}


def start_once(*, config: dict[str, Any], session: Any, http: Any = requests,
               now: datetime | None = None) -> dict[str, Any]:
    """Run only once per authenticated Streamlit session, bounded HTTP calls.

    Must be invoked in a Streamlit script thread, NOT in a Streamlit background
    thread. GitHub Actions performs the lengthy ingestion outside the web app.
    """
    key = 'h150_login_sync_result'
    if key in session and isinstance(session[key], dict):
        return dict(session[key])
    result: dict[str, Any] = {'status': 'NOT_CONFIGURED', 'message': '未設定 Actions 寫入權限；既有盤後排程仍可獨立執行'}
    session[key] = dict(result)  # also suppress retry storms when remote fails
    if not _config_ok(config):
        return result
    base = f"{API}/repos/{config['owner']}/{config['repo']}/actions/workflows/{WORKFLOW}"
    hdr = {'Authorization': 'Bearer ' + config['token'],
           'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'}
    try:
        response = http.get(base + '/runs', params={'per_page': 10, 'branch': 'main'},
                            headers=hdr, timeout=(2, 4), allow_redirects=False)
        if response.status_code != 200:
            result = {'status': 'CHECK_FAILED', 'message': '雲端執行紀錄查詢失敗：HTTP ' + str(response.status_code)}
        else:
            payload = response.json()
            plan = plan_from_runs(payload.get('workflow_runs'), now=now or datetime.now(timezone.utc)) if isinstance(payload, dict) else {'action': 'BLOCKED', 'reason': 'BAD_RESPONSE'}
            if plan['action'] == 'SKIP':
                result = {'status': 'EXISTING_JOB', 'message': '已有近期雲端更新工作，避免重複抓取；尚未驗證各來源資料', **plan}
            elif plan['action'] != 'DISPATCH':
                result = {'status': 'CHECK_FAILED', 'message': '無法確認雲端更新狀態，拒絕重複啟動'}
            else:
                posted = http.post(base + '/dispatches', json={'ref': 'main', 'inputs': {'trigger_source': 'login'}},
                                   headers=hdr, timeout=(2, 4), allow_redirects=False)
                if posted.status_code in (200, 204):
                    result = {'status': 'QUEUED', 'message': '已送出雲端資料更新要求（非完成證明）'}
                else:
                    result = {'status': 'DISPATCH_FAILED', 'message': '雲端啟動失敗：HTTP ' + str(posted.status_code)}
    except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
        result = {'status': 'CHECK_FAILED', 'message': '無法連接雲端排程；既有資料未被修改'}
    session[key] = result
    return dict(result)


def streamlit_login_hook(st: Any) -> dict[str, Any]:
    """Read only approved config; never print or persist the token."""
    try:
        s = st.secrets
        token = str(s.get('GITHUB_TOKEN', '') or '')
        cfg = {'token': token, 'owner': str(s.get('GITHUB_REPO_OWNER', 'cheng07021028')),
               'repo': str(s.get('GITHUB_REPO_NAME', 'stock-app')), 'branch': 'main'}
        return start_once(config=cfg, session=st.session_state)
    except Exception:
        return {'status': 'CHECK_FAILED', 'message': '登入後資料同步檢查未完成，不影響使用'}
