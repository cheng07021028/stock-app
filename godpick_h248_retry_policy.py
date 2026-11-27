# -*- coding: utf-8 -*-
"""H248: conservative same-slot retry policy. No network, writes, or pick authority.
The scheduler owns execution and checkpoints. Never promote warning to success.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

VERSION = 'V191-H248-SELECTIVE-RECOVERY'
MODE = 'h248_retry_due'
TZ = ZoneInfo('Asia/Taipei')
MAX_ATTEMPTS = 3  # actual handler calls per configured slot, not wake count
COOLDOWN_MINUTES = 20
REFRESH = frozenset({'stock_master','macro_full','official_factors','super_ai_context','watchlist_runtime','durability_retry'})
TRACKERS = frozenset({'record_latest_price','record_performance','recommend_list_performance','recommend_list_n_day','recommend_list_hits'})
# No whole recommendation or learning rerun in response to an ambiguous write.
LABELS = {
    'SUCCESS': ('已完成','不補跑'),
    'NOT_RUN': ('尚無執行紀錄','依正式排程'),
    'AUTHORITY_CONFLICT': ('資料權威／版本衝突','先核對原始證據與權威檔'),
    'PERMISSION': ('權限或憑證異常','修復授權後人工確認'),
    'CODE_ENV': ('程式或相依套件異常','修正程式／執行環境'),
    'EVIDENCE_PENDING': ('官方資料／原始證據未齊','等待官方或補證流程，不重算'),
    'WAIT_DEPENDENCY': ('前置工作未通過','前置條件改變後再判定'),
    'DURABILITY_ONLY': ('封存或遠端保存未確認','使用17永久化／H246核對；勿重跑選股'),
    'CONTINUATION': ('工作明示尚有待續資料','限次續跑原工作'),
    'TRANSIENT': ('暫時性網路／服務異常','限次補跑安全更新工作'),
    'DEGRADED': ('警示，未證明可安全補跑','保留警示，檢查原始結果'),
    'UNKNOWN': ('原因未明或重跑安全性未知','人工查核，避免重複寫入'),
}


def _map(value): return value if isinstance(value,dict) else {}

def _str(value): return str(value or '')

def _clock(value):
    try:
        t=datetime.fromisoformat(_str(value).replace('Z','+00:00'))
        return (t.replace(tzinfo=TZ) if t.tzinfo is None else t.astimezone(TZ))
    except (ValueError,TypeError):return None


def _any(text, terms):return any(x in text for x in terms)


def _out(code):
    label, action = LABELS[code]
    return {'version':VERSION,'code':code,'label':label,'action':action}


def classify(job, result, *, status=None):
    """Precedence: explicit authority/permission/code/evidence before retry hints.
    Only inspect known diagnostic fields; never copy provider tokens or raw payloads.
    A successful scan is not proof of an archive, and zero recommendations is valid.
    """
    r=_map(result); d=_map(r.get('details'))
    if status is None:
        status='WARNING' if r.get('ok') and r.get('warning') else ('SUCCESS' if r.get('ok') else ('BLOCKED' if r.get('blocked') else 'FAILED'))
    if status=='SUCCESS':return _out('SUCCESS')
    if status not in ('WARNING','BLOCKED','FAILED'):return _out('NOT_RUN')
    msg=_str(r.get('message'))[:12000].lower()
    sync=d.get('sync_messages')
    if isinstance(sync,list):msg += ' ' + ' '.join(_str(x)[:600] for x in sync[:20]).lower()
    seal=_map(d.get('h138_auto_seal')); consistent=_map(d.get('h246_consistency'))
    cs=_str(consistent.get('status')).upper(); eod=_str(seal.get('eod_status')).upper()
    if d.get('record_integrity_failure') is True or _any(msg,('權威衝突','權威未核實','權威仍為0','禁止回寫','版本回退','同id衝突','hash mismatch','payload_hash mismatch','integrity failure','原始推薦識別衝突')) or _any(cs,('CONFLICT','IDENTITY','HISTORY_FAILED','STALE_RUN')):
        return _out('AUTHORITY_CONFLICT')
    if _any(msg,('http 401','http 403','401 unauthorized','403 forbidden','invalid token','bad credentials','permission denied','resource not accessible by integration','未設定 github_token','token 未設定','token missing','權限不足','憑證失效')):
        return _out('PERMISSION')
    if _any(msg,('modulenotfounderror','importerror','indentationerror','syntaxerror','nameerror','找不到工作處理器','no module named','相依套件缺失')):
        return _out('CODE_ENV')
    if _any(msg,('隔離待補證','缺原始推薦日期','原始推薦日期缺失','缺有效身分','缺推薦日期','缺少原始證據','待補證','evidence_pending','market_day_unverified','wait_for_h117','官方資料尚未','官方因子未對齊','官方日期未','覆蓋不足','來源日期落後','未達完整標準')) or _any(eod,('WAIT','NOT_VERIFIED','UNVERIFIED','INVALID','READ_FAILED')) or seal.get('status')=='MARKET_DAY_UNVERIFIED':
        return _out('EVIDENCE_PENDING')
    if status=='BLOCKED' and _any(msg,('前置尚未','h28最終推薦閘門等待','h155','前置未通過','前置尚未成功')):
        return _out('WAIT_DEPENDENCY')
    # Explicit remote-write uncertainty is not a request to repeat calculation.
    if _any(msg,('永久同步失敗','永久化失敗','同步失敗','遠端封存尚未確認','遠端永久同步暫停','遠端未確認','remote_unconfirmed','未完成永久保存','遠端保存未確認','遠端hash未確認')) or (consistent and not consistent.get('confirmed')) or (seal and not seal.get('confirmed')):
        return _out('DURABILITY_ONLY')
    if r.get('retry_pending') is True and job in TRACKERS|REFRESH:
        return _out('CONTINUATION')
    if _any(msg,('timeout','timed out','connectionerror','connection reset','connection refused','temporarily unavailable','http 429','http 500','http 502','http 503','http 504','連線逾時','連線超時','請求逾時','暫時無法連線','服務暫時不可用')) and job in REFRESH:
        return _out('TRANSIENT')
    return _out('DEGRADED' if status=='WARNING' else 'UNKNOWN')


def slot_record(row, slot):
    """Legacy status fallback is exact same slot only. Never borrow another date.
    Scheduled evidence is independent of manual FORCE outcomes.
    """
    row=_map(row); tag=slot.strftime('%Y-%m-%d %H:%M')
    rec=_map(row.get('h248_scheduled'))
    if rec.get('slot')==tag:
        # Invalid/missing state cannot silently reset a used retry budget.
        if (rec.get('code') not in LABELS or type(rec.get('attempts')) is not int or rec['attempts']<0):
            return {'slot':tag,'code':'UNKNOWN','attempts':MAX_ATTEMPTS,'at':rec.get('at','')}
        return rec
    if row.get('last_slot')!=tag:return None
    # A newer FORCE row tagged by H248 must not be mistaken for scheduled history.
    if _map(row.get('h248_diagnostic')).get('force') is True:return None
    status=row.get('last_status')
    # Legacy success is already governed by completed_run_keys. It may have
    # come from a FORCE execution in the same minute; do not suppress a real slot.
    if status not in ('WARNING','FAILED','BLOCKED'):return None
    diag=classify('',{'message':row.get('last_message')},status=status)
    # Job-aware legacy classification is performed by decision() below.
    return {'slot':tag,'code':diag['code'],'attempts':0 if status=='BLOCKED' else 1,
            'at':row.get('last_run_at',''),'legacy':True,'status':status}


def decision(job, row, slot, now, *, dependency_ready=False):
    """Return scheduling decision and a separate selective-retry eligibility bit.
    New due slots remain normal work; manual force is intentionally outside here.
    """
    rec=slot_record(row,slot)
    if rec is None:return {'allow':True,'retry':False,'reason':'NEW_SLOT','attempts':0,'next_at':''}
    code=rec['code']; attempts=rec['attempts']
    if rec.get('legacy'):
        code=classify(job,{'message':_map(row).get('last_message'),'retry_pending':_map(row).get('retry_pending')},status=rec['status'])['code']
    out={'allow':False,'retry':False,'reason':code,'attempts':attempts,'next_at':''}
    if code=='SUCCESS':return out
    if attempts>=MAX_ATTEMPTS:return {**out,'reason':'RETRY_LIMIT'}
    if code=='WAIT_DEPENDENCY':
        return {**out,'allow':bool(dependency_ready),'retry':bool(dependency_ready),'reason':'DEPENDENCY_READY' if dependency_ready else code}
    if code not in ('CONTINUATION','TRANSIENT'):return out
    at=_clock(rec.get('at'))
    if at is None or now.tzinfo is None or at>now:return {**out,'reason':'RETRY_CLOCK_UNVERIFIED'}
    next_at=at+timedelta(minutes=COOLDOWN_MINUTES)
    if now<next_at:return {**out,'reason':'COOLDOWN','next_at':next_at.strftime('%Y-%m-%d %H:%M:%S')}
    return {**out,'allow':True,'retry':True,'reason':'RETRY_READY','next_at':next_at.strftime('%Y-%m-%d %H:%M:%S')}


def record_result(job, js, result, row, slot, *, force=False, simulate=False):
    if simulate:return
    prior=slot_record(js,slot) if not force else None
    diag=classify(job,result,status=row['status'])
    diag.update(force=bool(force),at=row.get('finished_at',''),slot=slot.strftime('%Y-%m-%d %H:%M'),batch_id=row.get('batch_id',''))
    js['h248_diagnostic']=diag
    if force:return
    # The caller must record before overwriting last_* fields, so legacy rows
    # represent the previous attempt, not the newly completed one.
    count=prior['attempts'] if prior else 0
    actual=0 if row['status']=='BLOCKED' else max(1,int(result.get('attempt') or 1))
    js['h248_scheduled']={'slot':diag['slot'],'code':diag['code'],'attempts':count+actual,'at':diag['at'],'batch_id':diag['batch_id']}


def immediate_retry_allowed(job,result):
    """Only explicit transient refresh failures may use an existing manual
    immediate retry setting. Automatic work always gets one call per wake.
    """
    return job in REFRESH and classify(job,result)['code']=='TRANSIENT'


def display(job, row, slot, now, *, dependency_ready=False, due=False):
    row=_map(row); diag=_map(row.get('h248_diagnostic'))
    if diag.get('code') not in LABELS:
        diag=classify(job,{'message':row.get('last_message'),'retry_pending':row.get('retry_pending')},status=row.get('last_status') or 'NOT_RUN')
    plan=decision(job,row,slot,now,dependency_ready=dependency_ready) if slot is not None else None
    reasons={'NEW_SLOT':'新正式時段','SUCCESS':'已完成','RETRY_READY':'可補跑','DEPENDENCY_READY':'可補跑','COOLDOWN':'冷卻中','RETRY_LIMIT':'本時段已達上限','RETRY_CLOCK_UNVERIFIED':'時間未核實','WAIT_DEPENDENCY':'等待前置'}
    reason=plan['reason'] if plan else ''
    eligibility=(reasons.get(reason,LABELS.get(reason,('待查',''))[0]) if due and plan else '非目前到期時段')
    return {'H248診斷':diag['label'] if 'label' in diag else LABELS[diag['code']][0],
            'H248處理建議':diag.get('action',LABELS[diag['code']][1]),
            'H248補跑資格':eligibility,'H248本時段嘗試':f"{plan['attempts']}/{MAX_ATTEMPTS}" if plan and reason!='NEW_SLOT' else '—',
            'H248最早補跑':plan['next_at'] if plan else '',
            'H248可選擇補跑':bool(due and plan and plan['retry'])}


def retry_receipt(batch_id, planned, executed, summary, finished_at):
    """Persist separate manual selective-batch truth; never infer from a green wake."""
    rows=[{'job':r.get('job'),'status':r.get('status')} for r in executed]
    ids=[r['job'] for r in rows]
    good=(bool(planned) and all(isinstance(k,str) and k for k in ids) and list(planned)==ids and len(ids)==len(set(ids)) and
          all(r['status']=='SUCCESS' for r in rows) and summary.get('executed')==len(ids) and
          summary.get('success')==len(ids) and all(summary.get(k)==0 for k in ('warning','failed','blocked')) and summary.get('in_progress') is False)
    return {'version':VERSION,'batch_id':batch_id,'batch_mode':MODE,'planned_jobs':list(planned),
            'executed_jobs':rows,'summary':dict(summary),'finished_at':finished_at,'all_success':good,
            'status':'VERIFIED_EXECUTION_PENDING_REMOTE' if good else ('NO_ELIGIBLE_RETRY' if not planned and not rows else 'INCOMPLETE_OR_WARNING')}


def verify_retry_receipt(receipt,run_id,attempt):
    r=_map(receipt); expected=f'{run_id}.{attempt}'
    if not run_id or r.get('batch_id')!=expected or r.get('batch_mode')!=MODE:
        return False,'H248 receipt does not match this run and attempt'
    p=r.get('planned_jobs'); rows=r.get('executed_jobs'); s=r.get('summary')
    if not isinstance(p,list) or not all(isinstance(k,str) and k for k in p) or not isinstance(rows,list) or not all(isinstance(x,dict) for x in rows) or not isinstance(s,dict):
        return False,'H248 invalid batch receipt'
    recomputed=retry_receipt(expected,p,rows,s,r.get('finished_at'))
    if not (r.get('all_success') is True and recomputed['all_success']):
        return False,'H248 selective batch has zero work, warning, failure or incomplete evidence'
    return True,'H248 selective batch execution verified'
