# -*- coding: utf-8 -*-
"""V191-H167 | render-only authority reconciliation for Page 17.

Never writes remote/local data or advances scheduler status. All audit results
are tagged with the source and observation timestamps so old dashboard status
cannot be mistaken for a new worker result.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

VERSION = 'V191-H167-REMOTE-TRUTH'


def _clock(obj: Mapping[str, Any] | None, *keys: str) -> datetime | None:
    if not isinstance(obj, Mapping):
        return None
    for key in keys:
        raw = str(obj.get(key) or '').strip()
        if not raw:
            continue
        try:
            return datetime.fromisoformat(raw.replace('Z', '+00:00')).replace(tzinfo=None)
        except (ValueError, TypeError):
            continue
    return None


def select_super_ai_snapshot(local: Any, remote: Any, *, remote_reachable: bool = True) -> dict[str, Any]:
    """Use newest dated snapshot; never treat a failed fetch as proof of absence.

    A hash/size or nonempty row count is not source verification. H166 still
    applies all PIT/market/TAIFEX freshness and TPEx coverage checks.
    """
    l = local if isinstance(local, Mapping) else {}
    r = remote if isinstance(remote, Mapping) else {}
    ld = _clock(l, 'updated_at')
    rd = _clock(r, 'updated_at')
    if r and rd is not None and (ld is None or rd >= ld):
        chosen, src = dict(r), 'GitHub runtime-data'
    elif l:
        chosen, src = dict(l), 'Streamlit 本機快照'
    else:
        chosen, src = {}, '未取得可用快照'
    return {
        'context': chosen,
        'source': src,
        'local_updated_at': str(l.get('updated_at') or ''),
        'remote_updated_at': str(r.get('updated_at') or ''),
        'remote_available': bool(r and rd),
        'remote_reachable': bool(remote_reachable),
        'warning': (
            'GitHub runtime-data SuperAI 快照尚未取得；本機數值不能冒稱最新遠端權威。'
            if not remote_reachable else
            'Streamlit 本機快照較新，尚未以 GitHub runtime-data 同輪確認。'
            if src == 'Streamlit 本機快照' else ''
        ),
    }


def scheduler_transition(settings: Any, status: Any) -> dict[str, Any]:
    """Explain when settings were enabled AFTER an old disabled heartbeat.

    Never rewrite a persisted status, and never infer that an accepted dispatch
    is a successful job. A worker must checkpoint a newer heartbeat first.
    """
    cfg = settings if isinstance(settings, Mapping) else {}
    st = status if isinstance(status, Mapping) else {}
    enabled = bool(cfg.get('enabled'))
    config_time = _clock(cfg, 'updated_at')
    status_time = _clock(st, 'updated_at', 'last_wakeup_at')
    previous_disabled = (
        st.get('scheduler_enabled') is False
        or str(st.get('last_wakeup_result') or '') == 'SKIPPED_DISABLED'
    )
    stale = bool(enabled and previous_disabled and config_time and
                 (status_time is None or config_time > status_time))
    inconsistent = bool(enabled and previous_disabled and config_time and status_time
                        and config_time <= status_time)
    message = (
        '中央排程已在 runtime-data 啟用，但目前顯示的 SKIPPED_DISABLED '
        '發生在啟用設定之前。請以後續 GitHub Worker 的新 checkpoint 為準；'
        '派送成功不等於工作完成。'
        if stale else
        '中央設定已啟用，但較新的 Worker 狀態仍顯示停用；'
        '需檢查 Worker 是否讀取同一份 runtime-data 設定，不可直接宣稱已執行。'
        if inconsistent else ''
    )
    return {
        'pending_new_checkpoint': stale,
        'settings_status_conflict': inconsistent,
        'settings_updated_at': str(cfg.get('updated_at') or ''),
        'status_updated_at': str(st.get('updated_at') or ''),
        'last_wakeup_result': str(st.get('last_wakeup_result') or ''),
        'message': message,
    }

__all__ = ['VERSION', 'select_super_ai_snapshot', 'scheduler_transition']
