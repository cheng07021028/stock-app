# -*- coding: utf-8 -*-
"""H244: identify the exact GitHub run; never promote a wake to business success."""
from datetime import datetime, timedelta, timezone
import re
import requests

ACTIVE = frozenset({'queued', 'pending', 'waiting', 'requested', 'in_progress'})
MANUAL_MODES = frozenset({'full_refresh_and_recommend', 'market_refresh_and_recommend'})


def taipei_now():
    return datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S UTC+8')


def mode_label(mode):
    return {'full_refresh_and_recommend': '強制全模組', 'market_refresh_and_recommend': '核心6項', '': '僅到期工作'}.get(mode, str(mode))


def read_run(*, token, owner, repo, run_id, http_get=None):
    """Exact run ID, no guessing from latest run. No secrets in returned text."""
    rid = str(run_id or '')
    if not token or not rid.isdigit() or not all(re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', str(x or '')) for x in (owner, repo)):
        return {'available': False, 'message': '缺少有效的 Run ID 或授權設定'}
    try:
        r = (http_get or requests.get)(f'https://api.github.com/repos/{owner}/{repo}/actions/runs/{rid}', headers={'Accept': 'application/vnd.github+json', 'Authorization': 'Bearer ' + token, 'X-GitHub-Api-Version': '2022-11-28'}, timeout=(2, 5))
        if r.status_code != 200:
            return {'available': False, 'message': f'GitHub HTTP {r.status_code}'}
        data = r.json()
        if str(data.get('id')) != rid:
            return {'available': False, 'message': '回傳 Run ID 不相符'}
        return {'available': True, 'run_id': rid, 'attempt': str(data.get('run_attempt') or '1'), 'status': str(data.get('status') or ''), 'conclusion': str(data.get('conclusion') or ''), 'url': f'https://github.com/{owner}/{repo}/actions/runs/{rid}'}
    except Exception as exc:
        return {'available': False, 'message': type(exc).__name__}


def describe(receipt, run, status):
    """Return level/text; summary evidence must belong to same run AND attempt."""
    mode = str(receipt.get('manual_job') or '')
    title = mode_label(mode)
    rid = str(receipt.get('run_id') or '')
    prefix = f'{title}｜Run {rid or "未取得"}｜'
    if not receipt.get('ok'):
        return 'error', prefix + '派送未確認；' + str(receipt.get('message') or '')
    if not run.get('available') or str(run.get('run_id') or '') != rid or not rid:
        return 'warning', prefix + '已受理，但無法確認本筆遠端狀態；' + str(run.get('message') or '不可用最新其他批次替代')
    state = run.get('status')
    if state in ACTIVE:
        return 'info', prefix + ('執行中，尚未完成驗收' if state == 'in_progress' else '安全排隊中，尚未執行')
    if state != 'completed':
        return 'warning', prefix + '未確認終態：' + str(state or 'unknown')
    conclusion = str(run.get('conclusion') or '')
    if conclusion in {'cancelled', 'timed_out', 'skipped', 'action_required', 'stale'}:
        return 'error', prefix + f'未正常完成（{conclusion}）；請查該 Run 日誌'
    attempt = str(run.get('attempt') or '1')
    batch = status.get('last_manual_batch') or {}
    summary = {}
    valid = {}
    if mode in MANUAL_MODES and str(batch.get('batch_id')) == rid + '.' + attempt and batch.get('batch_mode') == mode:
        summary = batch.get('summary') or {}
        valid = batch.get('validation') or {}
    elif mode not in MANUAL_MODES and str(status.get('last_wakeup_run_id')) == rid and str(status.get('last_wakeup_attempt') or '1') == attempt:
        summary = status.get('last_summary') or {}
    count = ''
    if summary and not summary.get('in_progress'):
        count = '｜執行 {executed}／成功 {success}／警示 {warning}／失敗 {failed}／阻擋 {blocked}'.format(**{k: summary.get(k, 0) for k in ('executed','success','warning','failed','blocked')})
    if conclusion != 'success':
        return 'error', prefix + '遠端批次未通過驗收（' + (conclusion or 'unknown') + '）' + count
    if not count:
        return 'warning', prefix + 'GitHub 流程成功，但本批次永久結果尚未確認／已被較新批次取代'
    if mode in MANUAL_MODES and not valid.get('all_success'):
        return 'warning', prefix + '手動批次未達完整驗收' + count
    if summary.get('warning') or summary.get('failed') or summary.get('blocked'):
        return 'warning', prefix + '工作有警示／失敗／阻擋' + count
    if not summary.get('executed'):
        return ('warning', prefix + '手動批次零項執行，不算完成' + count) if mode else ('info', prefix + '普通排程零項到期執行，不代表全模組完成' + count)
    return 'success', prefix + '本批次已完成驗收' + count
