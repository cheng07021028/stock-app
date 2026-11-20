# -*- coding: utf-8 -*-
"""H182 read-only, content-preserving replica diff for recommendation histories.

No mutation, reconciliation, ID repair or deletion happens in this module.
Counts and *column names*, not private cell values, are returned for diagnostics.
"""
from __future__ import annotations
from collections import Counter
from collections.abc import Mapping
from typing import Any
import json
import math

VERSION = 'V191-H182-READONLY-REPLICA-DIFF'
_TRANSPORT = frozenset({'updated_at'})
_IDENTITY = ('股票代號', '推薦日期', '推薦模式')


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list,tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if hasattr(value, 'isoformat') and callable(value.isoformat):
        try: return value.isoformat()
        except Exception: pass
    return value


def _equal(left: Any, right: Any) -> bool:
    return json.dumps(_plain(left), ensure_ascii=False, sort_keys=True, default=str, allow_nan=False) == json.dumps(
        _plain(right), ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)


def _fields_diff(local: Mapping, remote: Mapping) -> tuple[list[str],list[str],list[str]]:
    lkeys, rkeys = set(local)-_TRANSPORT, set(remote)-_TRANSPORT
    local_only = sorted(lkeys-rkeys)
    remote_only = sorted(rkeys-lkeys)
    changed = sorted(k for k in (lkeys&rkeys) if not _equal(local[k],remote[k]))
    return local_only, remote_only, changed


def audit_replicas(local_rows: list[dict], remote_rows: list[dict], remote_doc_ids: list[str],
                   selected_ids: list[str] | None = None) -> dict:
    """No implicit authority promotion. Never treats same-ID different content as safe.

    Selected records are separately identity-checked to guide human review;
    `selected_identity_verified` is *not* permission to execute a delete.
    """
    if not isinstance(local_rows,list) or not isinstance(remote_rows,list) or len(remote_rows)!=len(remote_doc_ids):
        return {'valid':False, 'reason':'資料清單／Firestore文件ID數量不符', 'version':VERSION}
    local:dict[str,dict]={};remote:dict[str,dict]={}
    for row in local_rows:
        if not isinstance(row,Mapping):return {'valid':False,'reason':'本機含非物件紀錄','version':VERSION}
        rid=str(row.get('record_id') or '').strip()
        if not rid or rid in local:return {'valid':False,'reason':'本機record_id缺失或重複','version':VERSION}
        local[rid]=dict(row)
    for row, rid in zip(remote_rows,remote_doc_ids):
        if not isinstance(row,Mapping) or not isinstance(rid,str) or not rid or rid in remote:
            return {'valid':False,'reason':'Firestore文件ID缺失或重複','version':VERSION}
        if row.get('record_id') not in (None,'',rid):
            return {'valid':False,'reason':'Firestore文件ID與內容record_id不一致','version':VERSION}
        remote[rid]=dict(row)
        remote[rid].setdefault('record_id',rid)
    local_keys=set(local); remote_keys=set(remote)
    shared=local_keys&remote_keys
    classifications=Counter(); added=Counter(); removed=Counter(); changed_fields=Counter()
    examples={}
    for rid in sorted(shared):
        lk,rk,ck=_fields_diff(local[rid],remote[rid])
        for k in lk:added[k]+=1
        for k in rk:removed[k]+=1
        for k in ck:changed_fields[k]+=1
        if ck:label='VALUE_CONFLICT'
        elif lk and rk:label='BIDIRECTIONAL_SCHEMA_DRIFT'
        elif lk:label='LOCAL_EXTRA_FIELDS_ONLY'
        elif rk:label='REMOTE_EXTRA_FIELDS_ONLY'
        else:label='IDENTICAL'
        classifications[label]+=1
        if label not in examples: examples[label]=rid[:16]
    selected=list(dict.fromkeys(str(x).strip() for x in (selected_ids or []) if str(x).strip()))
    id_reasons={}
    for rid in selected:
        if rid not in local:
            id_reasons[rid[:16]]='本機未找到勾選record_id';continue
        if rid not in remote:
            id_reasons[rid[:16]]='Firestore無對應文件，需核對刪除與同步歷程';continue
        for field in _IDENTITY:
            lv=str(local[rid].get(field) or '').strip()
            rv=str(remote[rid].get(field) or '').strip()
            if not lv or not rv:
                id_reasons[rid[:16]]=f'必要身分欄位「{field}」缺失';break
            if lv!=rv:
                id_reasons[rid[:16]]=f'必要身分欄位「{field}」不一致';break
    return {
        'valid':True,'version':VERSION,
        'local_count':len(local),'remote_count':len(remote),'shared_count':len(shared),
        'missing_remote_count':len(local_keys-remote_keys),'extra_remote_count':len(remote_keys-local_keys),
        'classifications':dict(classifications),
        'local_added_fields_top':added.most_common(10),
        'remote_added_fields_top':removed.most_common(10),
        'changed_fields_top':changed_fields.most_common(10),
        'examples':examples,
        'selected_count':len(selected),'selected_identity_issues':id_reasons,
        'selected_identity_verified':bool(selected) and not id_reasons,
        'destructive_action_authorized':False,
        'note':'僅作欄位級差異稽核；即使勾選ID身分一致，也沒有執行刪除或授權自動覆蓋遠端紀錄。',
    }


def succinct_reason(audit: Mapping) -> str:
    if not audit.get('valid'):
        return 'H182差異稽核無法執行：'+str(audit.get('reason',''))
    groups=audit.get('classifications') or {}
    cols=audit.get('changed_fields_top') or []
    added=audit.get('local_added_fields_top') or []
    changed='、'.join(f'{key}({n})' for key,n in cols[:4]) or '無'
    local_only='、'.join(f'{key}({n})' for key,n in added[:4]) or '無'
    return (f'H182差異分類：同ID實質值衝突{groups.get("VALUE_CONFLICT",0)}筆、'
            f'僅本機新增欄位{groups.get("LOCAL_EXTRA_FIELDS_ONLY",0)}筆、'
            f'雙向欄位差異{groups.get("BIDIRECTIONAL_SCHEMA_DRIFT",0)}筆；'
            f'相同欄位值差異重點：{changed}；本機新增欄位重點：{local_only}。未執行刪除。')

__all__=['VERSION','audit_replicas','succinct_reason']
