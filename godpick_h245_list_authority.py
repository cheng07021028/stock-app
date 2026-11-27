# -*- coding: utf-8 -*-
"""H245: read-only evidence for Page10; never create or revive recommendation IDs."""
from __future__ import annotations
import hashlib
import json
from collections import Counter
from datetime import date, datetime
from zoneinfo import ZoneInfo

LIST = 'godpick_recommend_list.json'
LATEST = 'godpick_latest_recommendations.json'
PREFIX = 'H245_AUTHORITY:'


def digest(obj):
    return hashlib.sha256(json.dumps(obj, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), default=str).encode('utf-8')).hexdigest()


def state_name(name):
    return name.removesuffix('.json') + '_sync_state.json'


def valid_state(payload, state):
    if not isinstance(state, dict) or payload is None:
        return False
    rev = state.get('revision')
    return (type(rev) is int and rev > 0 and state.get('payload_hash') == digest(payload))


def text(value):
    if value is None or isinstance(value, (dict, list, bool)):
        return ''
    s = str(value).strip()
    return '' if s.lower() in {'nan', 'none', 'nat', '<na>', 'null'} else s


def date_value(value):
    import re
    s = text(value)
    m = re.fullmatch(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', s)
    if not m:
        return None
    try:
        d = date(*map(int, m.groups()))
        return d.isoformat() if date(2000, 1, 1) <= d <= datetime.now(ZoneInfo('Asia/Taipei')).date() else None
    except ValueError:
        return None


def identity(row):
    return (text(row.get('record_id')), text(row.get('股票代號')), text(row.get('市場別')))


def eligible(row):
    return isinstance(row, dict) and all(identity(row)) and bool(date_value(row.get('推薦日期')))


def audit_empty_list(payload, latest, read_remote, verified_history=None):
    """Return a classification only. Verified remote state, not a fallback default,
    must establish emptiness. A newer snapshot may be recovered only when all its
    identities exist in verified current Page08 authority (never deleted rows).
    """
    def result(code, detail, n=0):
        return {'code': code, 'detail': detail, 'snapshot_rows': n}
    if not isinstance(payload, list) or payload:
        return result('UNVERIFIED', '本輪不是明確空陣列')
    try:
        ls, _ = read_remote(state_name(LIST), None)
        rs, _ = read_remote(state_name(LATEST), None)
        actual, _ = read_remote(LIST, None)
        if actual != [] or not valid_state(actual, ls) or not valid_state(latest, rs):
            return result('UNVERIFIED', '空清單或最新快照未取得有效遠端內容雜湊')
        # Re-read both publication pointers to reject a concurrent change.
        if read_remote(state_name(LIST), None)[0] != ls or read_remote(state_name(LATEST), None)[0] != rs:
            return result('UNVERIFIED', '查核期間遠端版本變動')
        if not isinstance(latest, dict) or not isinstance(latest.get('recommendations'), list):
            return result('UNVERIFIED', '最新快照沒有明確 recommendations 清單')
        rows = latest['recommendations']
        count = latest.get('recommendation_count')
        if count is not None and (type(count) is not int or count != len(rows)):
            return result('QUARANTINED', '最新快照宣告筆數與實際清單不符', len(rows))
        if not rows:
            return result('NO_CURRENT_LIST', '遠端07與10皆確認為空清單，不復活08舊紀錄')
        if ls['revision'] >= rs['revision']:
            return result('QUARANTINED', '10空清單不早於07快照；禁止自動復活舊清單', len(rows))
        if not all(eligible(r) for r in rows):
            return result('QUARANTINED', '較新07快照包含缺原始身分或日期的列', len(rows))
        if verified_history is None:
            return result('QUARANTINED', '較新07快照尚未取得08權威核對', len(rows))
        hist = verified_history()
        if hist is None:
            return result('UNVERIFIED', '08現行權威無法核對', len(rows))
        index = {}
        counts = Counter(text(r.get('record_id')) for r in hist if isinstance(r, dict))
        for r in hist:
            if eligible(r) and counts[identity(r)[0]] == 1:
                index[identity(r)[0]] = r
        ids = [identity(r)[0] for r in rows]
        if len(set(ids)) != len(ids) or any(rid not in index or identity(r) != identity(index[rid])
                or date_value(r.get('推薦日期')) != date_value(index[rid].get('推薦日期'))
                for rid, r in zip(ids, rows)):
            return result('QUARANTINED', '較新快照有重複、歷史不存在或身分不符的紀錄', len(rows))
        # The existing H7 write has no CAS across Page07 and Page10. Do not
        # enable automatic resurrection even with a plausible match in H245.
        return result('RECOVERY_REVIEW', '較新快照與08逐筆相符；待H246跨檔安全提交，不自動覆寫', len(rows))
    except Exception as exc:
        return result('UNVERIFIED', '權威查核例外：'+type(exc).__name__)


def plan_dates(rows, history):
    """Read-only evidence proposals; exact existing ID + code + market, no guessing.
    Local file users must separately verify history is the remote authority.
    """
    hist = [r for r in history if isinstance(r, dict)]
    curr = [r for r in rows if isinstance(r, dict)]
    hc = Counter(identity(r)[0] for r in hist)
    cc = Counter(identity(r)[0] for r in curr)
    index = {identity(r)[0]: r for r in hist if eligible(r) and hc[identity(r)[0]] == 1}
    out = []
    for i, r in enumerate(curr):
        rid, code, market = identity(r)
        d = date_value(r.get('推薦日期'))
        src = index.get(rid)
        if not rid or not code or not market:
            status = 'MISSING_IDENTITY'
        elif cc[rid] != 1 or hc[rid] > 1:
            status = 'DUPLICATE_ID'
        elif src is None:
            status = 'NO_AUTHORITY_MATCH'
        elif identity(r) != identity(src):
            status = 'IDENTITY_CONFLICT'
        elif d and d != date_value(src.get('推薦日期')):
            status = 'DATE_CONFLICT'
        elif d:
            status = 'MATCHED'
        elif text(r.get('推薦日期')):
            status = 'INVALID_EXPLICIT_DATE'
        else:
            status = 'DATE_EVIDENCE_AVAILABLE'
        item = {'row': i+1, 'record_id': rid, 'code': code, 'market': market, 'status': status}
        if status == 'DATE_EVIDENCE_AVAILABLE':
            item['proposed_date'] = date_value(src.get('推薦日期'))
        out.append(item)
    return out
