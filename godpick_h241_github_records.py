# -*- coding: utf-8 -*-
"""H241: GitHub-only recommendation history. Commit immutable shards then CAS
exact parent head. Never authorize a mutation from a Firestore replica or a
page's projected dataframe. Deletion tombstones survive subsequent writes.
"""
from __future__ import annotations
import base64, hashlib, json, time
from godpick_h127_sharded_records import MODE, manifest_path, write_snapshot, load_snapshot
import godpick_persistence_service as p

DISABLED = 'H241 已停用推薦紀錄 Firestore 讀寫／備份（其他模組不變）'


def _ids(rows):
    ids = [p._safe_str(r.get('record_id')) for r in rows]
    if not all(ids) or len(set(ids)) != len(ids):
        raise ValueError('record_id 缺失或重複')
    return ids


def _key(row):
    return hashlib.sha256(p._record_business_key_authority(row).encode()).hexdigest()


def _valid_head(head):
    if not isinstance(head, dict) or head.get('storage_mode') != MODE:
        return False
    try:
        return (head['shard_manifest_path'] == manifest_path(head['payload_hash'])
                and len(head['shard_manifest_sha256']) == 64
                and isinstance(head['count'], int) and head['count'] >= 0
                and int(head['revision']) > 0)
    except (KeyError, TypeError, ValueError):
        return False


def _same(rows, head):
    return (_valid_head(head) and len(rows) == head['count'] and
            p._json_hash(rows) == head['payload_hash'] and
            p._latest_record_recommendation_date(rows) == p._recommendation_date_text(head.get('latest_recommendation_date')))


def _head():
    """Read head content AND blob SHA from the SAME HTTP response."""
    cfg = p.github_config()
    if not cfg.get('token') or cfg.get('branch') in ('', 'main', 'master', None):
        raise RuntimeError('H241 GitHub runtime-data Token／分支未設定')
    res = p.requests.get(p._github_url(p.RECORDS_STATE_FILE), headers=p._github_headers(cfg['token']),
                         params={'ref':cfg['branch'], '_':time.time_ns()}, timeout=(10,30))
    if res.status_code != 200:
        raise RuntimeError(f'H241 GitHub 權威讀取 HTTP {res.status_code}')
    meta = res.json()
    raw = base64.b64decode(''.join(meta['content'].split()), validate=True)
    sha = hashlib.sha1(f'blob {len(raw)}\0'.encode()+raw).hexdigest()
    if meta.get('sha') != sha:
        raise ValueError('H241 權威 blob SHA 不符')
    head = json.loads(raw.decode('utf-8-sig'))
    if not _valid_head(head):
        raise ValueError('H241 未取得有效 H127 現行權威；拒絕舊檔')
    return head, sha


def _cas(head, sha, new):
    """One parent-specific PUT; never fetch new SHA and retry stale payload."""
    cfg = p.github_config()
    raw = json.dumps(new, ensure_ascii=False, separators=(',', ':')).encode()
    if len(raw) > 650_000:
        raise ValueError('H241 權威索引超過安全上限；需遷移刪除索引，未提交')
    try:
        res = p.requests.put(p._github_url(p.RECORDS_STATE_FILE), headers=p._github_headers(cfg['token']),
                             json={'message':'H241 verified records mutation','branch':cfg['branch'],
                                   'sha':sha,'content':base64.b64encode(raw).decode()}, timeout=(15,60))
        status = res.status_code
    except Exception:
        status = 0
    # Read back even on ambiguous transport errors. No retry on a new parent.
    try:
        current, _ = _head()
        if p._json_hash(current) == p._json_hash(new):
            return True, 'H241 GitHub 權威已提交並回讀驗證'
    except Exception:
        pass
    return False, f'H241 GitHub 提交未確認 HTTP {status}；請重新載入核對，未重試覆蓋'


def _put_immutable(path, body):
    """Bounded create-only attempts. Never overwrite an existing immutable blob."""
    cfg = p.github_config()
    raw = json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode()
    for attempt in range(3):
        try:
            res = p.requests.put(p._github_url(path), headers=p._github_headers(cfg['token']),
                                 json={'message':'H241 immutable record shard','branch':cfg['branch'],
                                       'content':base64.b64encode(raw).decode()},timeout=(15,90))
            code=res.status_code
        except Exception:
            code=0
        if code in (200,201):
            return True, f'H241 immutable shard HTTP {code}；H127將回讀驗證'
        if code in (401,403):
            return False, f'H241 GitHub 寫入權限 HTTP {code}'
        check, msg = p.read_github_json(path,None)
        if check==body: return True,'H241 不確定回應後回讀一致'
        if check is not None or not any(t in str(msg).lower() for t in ('404','not found','尚未建立')):
            return False,f'H241 immutable shard HTTP {code}｜不能確認原始物件不存在'
        if code not in (0,409,422): break
        if attempt < 2: time.sleep(0.5*(attempt+1))
    return False, f'H241 immutable shard HTTP {code}；未提交權威'


def _local():
    raw, _, _ = p.read_local_json(p.RECORDS_FILE, [])
    return p.records_as_rows_exact(raw)


def _install(rows, head, old=None):
    # Preserve divergent prior local snapshots without re-publishing them.
    if old is not None and old and p._json_hash(old) != p._json_hash(rows):
        ok, msg=p.write_local_json_atomic(f'.cache/h240_pre_restore/{p._json_hash(old)}.json', old)
        if not ok:
            return False, msg
    for path, obj in ((p.RECORDS_FILE, rows),(p.RECORDS_STATE_FILE, head),
                      (p.RECORDS_MANIFEST_FILE,p._records_manifest(rows,head['payload_hash']))):
        ok,msg=p.write_local_json_atomic(path,obj)
        if not ok:
            p._h240_status(rows,status='BLOCKED',head=head,reason='GitHub 已確認，本機待重新載入')
            return False,msg
    p._h240_status(rows,status='VERIFIED',source='H241 GitHub 單一權威',head=head)
    return True,'H241 本機已原子還原並驗證'


def ensure():
    with p._RECORDS_LOCAL_LOCK:
        local=_local()
        try:
            head, msg=p.read_github_json(p.RECORDS_STATE_FILE,{})
            if not _valid_head(head):
                raise ValueError('GitHub 現行權威無法核實｜'+str(msg))
            prior,_,_=p.read_local_json(p.RECORDS_STATE_FILE,{})
            # Previously verified GitHub revisions cannot silently roll back.
            if (_same(local,prior) and (int(prior['revision'])>int(head['revision']) or
                     (int(prior['revision'])==int(head['revision']) and prior['payload_hash']!=head['payload_hash']))):
                raise ValueError('GitHub revision 回退，保留本機參考')
            if _same(local,head):
                _ids(local)
                local_manifest,_,_=p.read_local_json(p.RECORDS_MANIFEST_FILE,{})
                if prior != head or local_manifest.get('payload_hash') != head['payload_hash'] or local_manifest.get('count') != len(local):
                    ok,msg=_install(local,head)
                    if not ok: raise RuntimeError(msg)
                p._h240_status(local,status='VERIFIED',source='H241 GitHub 同Hash',head=head)
                return local,['H241 GitHub 現行權威同Hash',DISABLED],False
            rows,msg=p._h240_read_current_shards(head)
            if rows is None or not _same(rows,head): raise ValueError(str(msg))
            _ids(rows)
            ok,msg=_install(rows,head,local)
            if not ok: raise RuntimeError(msg)
            return rows,[msg,DISABLED],True
        except Exception as exc:
            p._h240_status(local,status='BLOCKED',reason=str(exc))
            return local,['H241 唯讀參考，禁止回寫｜'+str(exc),DISABLED],False


def _blocked(msg):
    return p.PersistenceReport(local_message=str(msg),github_message=str(msg),firestore_message=DISABLED,updated_at=p._now_text())


def _base(expected=''):
    if expected and expected != p.records_authority_signature():
        raise ValueError('權威檔已被其他頁面更新；請重新載入後再操作')
    local=_local(); head,sha=_head()
    if not _same(local,head):
        raise ValueError('權威檔已被遠端更新或本機未核實；請重新載入，不得舊檔覆寫')
    prior,_,_=p.read_local_json(p.RECORDS_STATE_FILE,{})
    if _same(local,prior) and int(prior['revision'])>int(head['revision']):
        raise ValueError('H241 GitHub revision 回退；禁止覆寫')
    _ids(local)
    return local,head,sha


def _tombs(head):
    ids=head.get('h241_deleted_id_hashes',[]); keys=head.get('h241_deleted_key_hashes',[])
    if not isinstance(ids,list) or not isinstance(keys,list) or any(not isinstance(x,str) or len(x)!=64 or any(c not in '0123456789abcdef' for c in x) for x in ids+keys):
        raise ValueError('H241 刪除索引格式異常')
    return set(ids),set(keys)


def _id_hash(rid): return hashlib.sha256(rid.encode()).hexdigest()


def _merge(old, patch):
    # Preserve all canonical columns omitted by the editor/other modules.
    out=dict(old)
    # R9 evidence is written only through the exact-ID archive engine's CAS.
    # Legacy editors/08 quote updaters may submit an old projected dataframe;
    # they must not erase or replace already confirmed proof fields.
    from godpick_h250r9_evidence import EVIDENCE_COLUMNS
    owned=set(EVIDENCE_COLUMNS)
    if p._safe_str(old.get('H250R7_觸價證據')):
        owned.update(('作戰追蹤狀態','作戰追蹤資料源','作戰命中摘要','作戰追蹤更新時間'))
        for name in ('進場點命中','突破價命中','停損價觸發','第一壓力命中'):
            owned.update((name,name+'日期'))
        for lab in ('隔日','3日','5日','10日'):
            owned.update((lab+'最高漲幅%',lab+'最低回撤%'))
    for k,v in patch.items():
        if k in owned or k in ('匯入自選','刪除'): continue
        if k in ('推薦日期','date','股票代號','code','推薦模式','mode') and not p._safe_str(v): continue
        if v is not None or k not in out: out[k]=v
    out['record_id']=old['record_id']
    if _key(out)!=_key(old): raise ValueError('H241 禁止修改原始推薦識別（日期／股票／模式）')
    return out


def _commit(base,head,sha,rows,removed,reason):
    _ids(rows)
    old_ids=set(_ids(base)); new_ids=set(_ids(rows))
    if old_ids-new_ids != removed:
        raise ValueError('H241 非勾選ID消失；拒絕整檔縮水')
    deleted_ids, deleted_keys=_tombs(head)
    for r in rows:
        rid=p._safe_str(r['record_id'])
        if rid not in old_ids and (_id_hash(rid) in deleted_ids or _key(r) in deleted_keys):
            raise ValueError('H241 已刪除紀錄禁止由舊清單復活')
    report=p.PersistenceReport(firestore_message=DISABLED,updated_at=p._now_text(),payload_hash=p._json_hash(rows))
    if report.payload_hash==head['payload_hash']:
        report.github_ok=True; report.permanent_ok=True
        report.github_message='H241 現行GitHub同Hash；無需重傳'
        prior,_,_=p.read_local_json(p.RECORDS_STATE_FILE,{})
        if prior!=head: report.local_ok,report.local_message=_install(rows,head)
        else: report.local_ok=True; report.local_message='本機同Hash'
        return report
    remaining_keys={_key(r) for r in rows}
    for r in base:
        if r['record_id'] in removed:
            deleted_ids.add(_id_hash(r['record_id']))
            if _key(r) not in remaining_keys: deleted_keys.add(_key(r))
    state=p._new_state('godpick_records_h241_github_only',rows,count=len(rows),latest_recommendation_date=p._latest_record_recommendation_date(rows),
                       mutation_kind='destructive' if removed else 'upsert',mutation_reason=reason,mutation_deleted_count=len(removed),
                       github_pending=False,parent_payload_hash=head['payload_hash'],parent_revision=head['revision'],
                       h241_deleted_id_hashes=sorted(deleted_ids),h241_deleted_key_hashes=sorted(deleted_keys))
    state['revision']=max(int(head['revision'])+1,int(state['revision']))
    # Shards are immutable and fully read back before head publication.
    path=manifest_path(state['payload_hash'])
    old_manifest,manifest_msg=p.read_github_json(path,None)
    if isinstance(old_manifest,dict):
        # An earlier revision may already have exactly this semantic content,
        # but a different gzip encoding. Reuse only after complete revalidation.
        raw=json.dumps(old_manifest,ensure_ascii=False,separators=(',',':'),default=str).encode()
        ext={'storage_mode':MODE,'shard_manifest_path':path,'shard_manifest_sha256':hashlib.sha256(raw).hexdigest()}
        snapshot,msg=load_snapshot(dict(state,**ext),get_json=p.read_github_json,record_hash_func=p._json_hash,workers=4)
        if snapshot is None or not _same(snapshot,dict(state,**ext)): return _blocked(msg)
    elif old_manifest is None and any(t in str(manifest_msg).lower() for t in ('404','not found','尚未建立')):
        ext,msg=write_snapshot(rows,state,get_json=p.read_github_json,put_json=_put_immutable,json_safe=p._json_safe_value,record_hash_func=p._json_hash)
        if ext is None: return _blocked(msg)
    else:
        return _blocked('H241 無法確認目標封存狀態；未提交權威')
    state.update(ext)
    ok,msg=_cas(head,sha,state)
    report.github_ok=bool(ok);report.github_message=msg;report.permanent_ok=bool(ok)
    if not ok:
        report.local_message='H241 未確認遠端提交；本機未刪除或覆寫，請重新載入'
        return report
    report.local_ok,report.local_message=_install(rows,state)
    p.write_local_json_atomic(p.RECORDS_GITHUB_SYNC_STATUS_FILE,{'status':'success','payload_hash':state['payload_hash'],'count':len(rows),'updated_at':state['updated_at']})
    return report


def mutate(payload, *, expected_authority_signature='', deleted_ids=None, upsert_rows=None, previous_count=None, reason='record mutation', require_remote_confirm=False):
    with p._RECORDS_LOCAL_LOCK,p._RECORDS_GITHUB_UPLOAD_LOCK:
        try:
            base,head,sha=_base(expected_authority_signature)
            view=p.records_as_rows_exact(payload); view_ids=set(_ids(view)); old_ids=set(_ids(base))
            removed=set(p._safe_str(x) for x in (deleted_ids or []) if p._safe_str(x))
            if previous_count is not None and int(previous_count)!=len(base): raise ValueError('H241 來源筆數已改變，請重新載入')
            if removed:
                if not removed<=old_ids or view_ids!=old_ids-removed or upsert_rows: raise ValueError('H241 勾選ID或剩餘集合不符；未刪除')
                rows=[r for r in base if r['record_id'] not in removed]
            else:
                patches=p.records_as_rows_exact(list(upsert_rows or [])); by={r['record_id']:r for r in patches}; _ids(patches)
                if view_ids!=old_ids|set(by): raise ValueError('H241 編輯／新增ID集合不符；未保存')
                rows=[_merge(r,by[r['record_id']]) if r['record_id'] in by else r for r in base]
                rows.extend(r for r in patches if r['record_id'] not in old_ids)
            return _commit(base,head,sha,rows,removed,reason)
        except Exception as exc: return _blocked(exc)


def sync(payload, reason='explicit record sync', expected_authority_signature='', *, allow_destructive_replace=False, require_remote_confirm=False):
    # Full-sync callers may send projections; never implicitly delete or resurrect.
    with p._RECORDS_LOCAL_LOCK,p._RECORDS_GITHUB_UPLOAD_LOCK:
        try:
            base,head,sha=_base(expected_authority_signature)
            incoming=p.records_as_rows_exact(payload); ids=set(_ids(incoming)); old=set(_ids(base))
            if not old<=ids: raise ValueError('H241 完整保存不允許隱含刪除，請使用勾選刪除')
            by={r['record_id']:r for r in incoming}
            rows=[_merge(r,by[r['record_id']]) for r in base]+[r for r in incoming if r['record_id'] not in old]
            if not expected_authority_signature and any(p._json_hash(a)!=p._json_hash(b) for a,b in zip(base,rows)):
                raise ValueError('H241 修改既有紀錄必須提供原始權威簽章；請重新載入')
            return _commit(base,head,sha,rows,set(),reason)
        except Exception as exc: return _blocked(exc)


def upsert(upsert_rows, *, reason='record authority upsert', require_remote_confirm=False):
    with p._RECORDS_LOCAL_LOCK,p._RECORDS_GITHUB_UPLOAD_LOCK:
        stats={'before':0,'after':0,'added':0,'updated':0,'changed':0,'deleted_skipped':0}
        try:
            ensure(); base,head,sha=_base(); stats.update(before=len(base),after=len(base))
            rows=list(base); byid={r['record_id']:i for i,r in enumerate(rows)}; bykey={_key(r):i for i,r in enumerate(rows)}
            tomb_ids,tomb_keys=_tombs(head)
            for r in p.records_as_rows_exact(list(upsert_rows or [])):
                rid=r['record_id']; k=_key(r)
                if _id_hash(rid) in tomb_ids or k in tomb_keys:
                    stats['deleted_skipped']+=1;continue
                i=byid.get(rid,bykey.get(k))
                if i is None:
                    byid[rid]=bykey[k]=len(rows);rows.append(r);stats['added']+=1
                else:
                    new=_merge(rows[i],r)
                    if p._json_hash(new)!=p._json_hash(rows[i]): rows[i]=new;stats['updated']+=1
            report=_commit(base,head,sha,rows,set(),reason)
            if report.permanent_ok: stats.update(after=len(rows),changed=stats['added']+stats['updated'])
            else: stats.update(added=0,updated=0)
            if stats['deleted_skipped']: report.local_message+=f"｜略過已刪除 {stats['deleted_skipped']} 筆"
            return report,stats
        except Exception as exc:return _blocked(exc),stats
