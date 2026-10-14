# -*- coding: utf-8 -*-
"""V191-H116: immutable decision snapshots and evidence-based strategy review.

Research-only; no buy signals/weights/settings changes. This module intentionally
never assumes a local Streamlit Cloud filesystem is durable.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import quote
import base64
import hashlib
import json
import os
import re
import statistics

from godpick_h115_pit_evidence import TZ_TW, _datetime, _num, _code
from godpick_h115_walkforward import evaluate_snapshots, HORIZONS

VERSION = 'v191_h116_closed_loop_20261014'
FIELDS = ('H114主導策略','H114研究優先分','H114入榜身分','H113入榜資格',
          'H110研究資訊截止日','H110行情交易日','H109決策權威','H109否決原因',
          'H114營收資料狀態','H114估值資料狀態','H114K線資料狀態',
          'H113法人逐日來源驗證','H113大戶鎖碼合格','H113流動性合格')
ALLOWED_SOURCES = ('blackhorse_overview', 'h114_parallel_watch', 'research')


def _stamp(x: Any) -> str:
    dt = _datetime(x)
    return dt.isoformat(timespec='seconds') if dt else ''


def _safe_runid(v: Any) -> str:
    raw = str(v or '').strip()
    if not raw or len(raw) > 120 or not re.fullmatch(r'[\w-]+', raw, re.ASCII):
        raise ValueError('scan run_id 必須為1~120字元英數底線或連字號')
    return raw


def _simple(v: Any) -> Any:
    # JSON serialize only scalar evidence to keep snapshots small and portable.
    if v is None or isinstance(v, (str, int, bool)):
        return v
    if isinstance(v, float):
        return v if v == v and abs(v) != float('inf') else None
    return str(v)[:300]


def make_decision_snapshot(core: Mapping[str, Any], *, run_id: str,
                           decided_at: str, market_day: str, cap_per_source: int = 50,
                           market_day_basis: str = 'UNKNOWN') -> dict[str, Any]:
    """Capture actual bounded core tables; NEVER backfill yesterday by re-running the model.

    If per-stock publication/first-observed evidence times cannot be proven,
    latest_evidence_at remains null, causing H115 walkforward to refuse
    an asserted verified as-of signal. Research snapshot is still preserved.
    """
    rid = _safe_runid(run_id)
    date_time = _datetime(decided_at)
    market_dt=_datetime(market_day)
    if not date_time or not market_dt:
        raise ValueError('缺少有效推薦時間或行情資料日')
    market_day=market_dt.date().isoformat()
    if market_day > date_time.date().isoformat():
        raise ValueError('行情日不得晚於研究決策日')
    if not isinstance(core, Mapping):
        raise ValueError('缺少本次掃描當下的決策core，不可用過期推薦代替')
    if str(core.get('run_id') or '') != rid:
        raise ValueError('H97決策core與目前scan run_id不同，禁止保存舊推薦')
    decisions=[]; seen=set()
    for source in ALLOWED_SOURCES:
        raw = core.get(source, [])
        if not isinstance(raw, list):
            continue
        for row in raw[:cap_per_source]:
            if not isinstance(row, dict): continue
            code = _code(row.get('股票代號') or row.get('code'))
            if not code or not re.fullmatch(r'\d{4,6}', code): continue
            strat = str(row.get('H114主導策略') or row.get('strategy') or 'H113_LEGACY').strip()[:80]
            tier = str(row.get('H114入榜身分') or row.get('H113入榜資格') or row.get('候選身分') or 'UNKNOWN')[:80]
            unique=(code, strat, source)
            if unique in seen:continue
            seen.add(unique)
            event_key = f'{rid}|{source}|{code}|{strat}'
            eid=hashlib.sha256(event_key.encode('utf-8')).hexdigest()[:24]
            raw_evidence = (row.get('latest_evidence_at') or row.get('最晚證據可用時間') or '')
            # A date-only/naive timestamp is not proof that evidence was
            # available before this evening's scan; require offset and time.
            evidence_raw=str(raw_evidence).strip()
            explicit_time=bool(re.search(r'T?\d{2}:\d{2}.*(?:Z|[+-]\d{2}:\d{2})$',evidence_raw))
            evidence_ts=_stamp(raw_evidence) if explicit_time else ''
            if evidence_ts and _datetime(evidence_ts)>date_time:
                evidence_ts='' # future-data evidence cannot be certified
            decisions.append({
                'event_id':eid,'run_id':rid,'code':code,'name':str(row.get('股票名稱') or '')[:80],
                'source_table':source,'strategy':strat,'tier':tier,
                'decided_at':_stamp(decided_at),'decision_date':date_time.date().isoformat(),
                'market_day':market_day,'latest_evidence_at':evidence_ts or None,
                'asof_status':'VERIFIED_PER_ROW' if evidence_ts else 'UNVERIFIED_PER_ROW',
                'score':_num(row.get('H114研究優先分') if row.get('H114研究優先分') is not None else row.get('H110最終研究優先分')),
                'diagnostics':{k:_simple(row.get(k)) for k in FIELDS if k in row},
                'permission':'RESEARCH_ONLY_NO_TRADE',
            })
    # Immutable snapshot even when no candidates; zero is a valid scan outcome.
    body={'version':VERSION,'run_id':rid,'decided_at':_stamp(decided_at),
          'market_day':market_day,'market_day_basis':str(market_day_basis)[:80],
          'verified_asof_rows':sum(x['asof_status']=='VERIFIED_PER_ROW' for x in decisions),
          'unverified_asof_rows':sum(x['asof_status']=='UNVERIFIED_PER_ROW' for x in decisions),
          'core_version':str(core.get('version') or '')[:100],
          'core_created_at':str(core.get('created_at') or '')[:60],
          'scan_status':'EMPTY_SAFE' if not decisions else 'SNAPSHOT_CAPTURED',
          'decision_count':len(decisions),'decisions':decisions,
          'formal_policy':'UNCHANGED_LOCKED','immutable_policy':'ONE_FILE_PER_SCAN_NO_OVERWRITE'}
    canonical=json.dumps(body,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
    body['snapshot_sha256']=hashlib.sha256(canonical).hexdigest()
    return body


def verify_snapshot(snapshot: Mapping[str, Any]) -> bool:
    if not isinstance(snapshot, Mapping) or not snapshot.get('snapshot_sha256'):return False
    payload=dict(snapshot);digest=payload.pop('snapshot_sha256','')
    raw=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
    return digest==hashlib.sha256(raw).hexdigest()


def _github_settings(secrets: Mapping[str, Any]|None = None) -> dict[str,str]:
    s=secrets or {}
    def v(k: str) -> str:return str(s.get(k) or os.getenv(k) or '').strip()
    repository=v('H116_GITHUB_REPOSITORY')
    branch=v('H116_GITHUB_BRANCH') or 'main'
    token=v('GITHUB_TOKEN') or v('H116_GITHUB_TOKEN')
    enabled=v('H116_GITHUB_PERSIST').lower() in ('1','true','yes','on')
    if not enabled:raise ValueError('H116_GITHUB_PERSIST未啟用：不能宣稱永久保存')
    if not token or not re.fullmatch(r'[\w.-]+/[\w.-]+',repository):
        raise ValueError('缺少GITHUB_TOKEN或H116_GITHUB_REPOSITORY=owner/repo')
    if not branch or not re.fullmatch(r'[\w./-]+',branch) or '..' in branch:
        raise ValueError('不合法的分支設定')
    return {'repo':repository,'branch':branch,'token':token}


def snapshot_path(snapshot: Mapping[str, Any]) -> str:
    rid=_safe_runid(snapshot.get('run_id'))
    day=_datetime(snapshot.get('decided_at'))
    if not day:raise ValueError('缺少推薦時間')
    return f'data/h116/decisions/{day.date().isoformat()}/{rid}.json'


def github_publish_verified(snapshot: Mapping[str, Any], *, secrets: Mapping[str, Any]|None = None,
                            session: Any = None, timeout: int = 10) -> dict[str, Any]:
    """GitHub Contents API synchronous PUT + GET verify, one immutable file/run.

    Never claim durable success for scheduling, local-only writes or unverified
    PUT. Existing but different contents => immutable conflict, NOT overwrite.
    """
    if not verify_snapshot(snapshot):return {'ok':False,'status':'INVALID_SNAPSHOT','permanent':False}
    try:cfg=_github_settings(secrets)
    except ValueError as exc:return {'ok':False,'status':'NOT_CONFIGURED','message':str(exc),'permanent':False}
    import requests
    http=session or requests.Session()
    path=snapshot_path(snapshot)
    api=f'https://api.github.com/repos/{cfg["repo"]}/contents/{quote(path,safe="/")}'
    headers={'Authorization':f'Bearer {cfg["token"]}','Accept':'application/vnd.github+json',
             'X-GitHub-Api-Version':'2022-11-28'}
    canonical=json.dumps(snapshot,ensure_ascii=False,sort_keys=True,indent=2).encode('utf-8')
    expected=hashlib.sha256(canonical).hexdigest()
    def read_remote() -> tuple[int,bytes|None]:
        response=http.get(api,params={'ref':cfg['branch']},headers=headers,timeout=timeout)
        if response.status_code==404:return 404,None
        if response.status_code!=200:raise RuntimeError(f'GitHub讀取失敗 HTTP {response.status_code}')
        obj=response.json()
        if obj.get('encoding')!='base64':raise RuntimeError('GitHub回讀編碼不符')
        raw=base64.b64decode(obj.get('content',''))
        return 200,raw
    try:
        before,blob=read_remote()
        if before==200:
            if hashlib.sha256(blob or b'').hexdigest()!=expected:
                return {'ok':False,'permanent':False,'status':'IMMUTABLE_CONFLICT','path':path}
            return {'ok':True,'permanent':True,'status':'ALREADY_VERIFIED','path':path,'sha256':expected}
        data={'message':f'H116 immutable decision snapshot {snapshot["run_id"]}',
              'branch':cfg['branch'],'content':base64.b64encode(canonical).decode('ascii')}
        put=http.put(api,headers=headers,json=data,timeout=timeout)
        if put.status_code not in (200,201,409,422):
            return {'ok':False,'permanent':False,'status':f'GITHUB_WRITE_HTTP_{put.status_code}','path':path}
        # 409/422 may be race with another writer: verify exact content, fail on mismatch.
        after,remote=read_remote()
        if after==200 and hashlib.sha256(remote or b'').hexdigest()==expected:
            return {'ok':True,'permanent':True,'status':'WRITTEN_AND_READBACK_VERIFIED','path':path,'sha256':expected}
        return {'ok':False,'permanent':False,'status':'READBACK_MISMATCH_OR_NOT_FOUND','path':path}
    except Exception as exc:
        return {'ok':False,'permanent':False,'status':'REMOTE_ERROR','message':f'{type(exc).__name__}: {exc}'[:250], 'path':path}


def load_snapshot_files(directory: Path) -> list[dict[str,Any]]:
    root=Path(directory)
    result=[];seen={}
    if not root.exists():return result
    for file in sorted(root.rglob('*.json')):
        if not file.is_file() or file.stat().st_size>1_000_000:raise ValueError(f'快照過大：{file.name}')
        item=json.loads(file.read_text(encoding='utf-8'))
        if not verify_snapshot(item):raise ValueError(f'快照雜湊核驗失敗：{file}')
        rid=item['run_id']
        if rid in seen and seen[rid]!=item['snapshot_sha256']:raise ValueError(f'同一run_id不同內容：{rid}')
        seen[rid]=item['snapshot_sha256'];result.append(item)
    return result


def _snapshots_to_events(snapshots: Iterable[Mapping[str,Any]]) -> list[dict[str,Any]]:
    events=[];seen=set()
    for snap in snapshots:
        if not verify_snapshot(snap):raise ValueError('無效的歷史推薦快照')
        for event in snap['decisions']:
            eid=str(event['event_id'])
            if eid in seen:continue
            seen.add(eid)
            events.append({k:event.get(k) for k in ('event_id','code','strategy','tier','decided_at','latest_evidence_at','score','source_table')})
    return events


def review_strategies(snapshots: Iterable[Mapping[str,Any]], prices: Iterable[Mapping[str,Any]],
                      *, benchmark: Iterable[Mapping[str,Any]] | None = None,
                      min_samples: int=30,min_distinct_days: int=20,
                      min_exposure_days: int=60, **costs:Any) -> dict[str,Any]:
    """Compute observed outcomes, block repeated same-day symbol/strategy vote stacking.

    Proposal-only; cannot mutate production weights, Formal privileges, scores,
    gate thresholds or live recommendation records.
    """
    events=_snapshots_to_events(snapshots)
    raw=evaluate_snapshots(events,prices,benchmark=benchmark,**costs)
    dedup={};repeated=0
    for r in raw['details']:
        key=(r['decision_date'],r['code'],r['strategy'],r['source_table'] if 'source_table' in r else '',r['horizon'])
        # H115 details have no source_table; intentional collapse across same stock/strategy on same day.
        key=(r['decision_date'],r['code'],r['strategy'],r['horizon'])
        prev=dedup.get(key)
        if prev is not None:
            repeated+=1
            # prefer earliest captured event ID deterministically; no hindsight winner selection
            continue
        dedup[key]=r
    clean=list(dedup.values())
    groups=defaultdict(list)
    for r in clean:
        if r['data_status']=='OK':groups[(r['strategy'],r['horizon'])].append(r)
    summaries=[];proposals=[]
    for (strategy,horizon),items in sorted(groups.items()):
        day_groups=defaultdict(list)
        for it in items:day_groups[it['decision_date']].append(it)
        by_day=[statistics.mean(x['net_return_pct'] for x in rr) for rr in day_groups.values()]
        alpha_day=[statistics.mean(x['alpha_pp'] for x in rr if x['alpha_pp'] is not None)
                   for rr in day_groups.values() if any(x['alpha_pp'] is not None for x in rr)]
        days=sorted(day_groups)
        exp=(datetime.fromisoformat(days[-1])-datetime.fromisoformat(days[0])).days if len(days)>1 else 0
        ready=(len(items)>=min_samples and len(days)>=min_distinct_days and exp>=min_exposure_days and len(alpha_day)==len(days))
        mean_net=statistics.mean(by_day)
        mean_alpha=statistics.mean(alpha_day) if alpha_day else None
        decision='KEEP_COLLECTING_DATA'
        if ready:
            decision='REVIEW_FOR_RESEARCH_PRIORITY' if mean_alpha>0 and mean_net>0 else 'REVIEW_FOR_RISK_OR_DEMOTION'
        summaries.append({'strategy':strategy,'horizon':horizon,'stock_day_samples':len(items),
                          'distinct_decision_days':len(days),'calendar_span_days':exp,
                          'verified_benchmark_days':len(alpha_day),'equal_day_mean_net_pct':round(mean_net,4),
                          'equal_day_mean_alpha_pp':round(mean_alpha,4) if mean_alpha is not None else None,
                          'positive_day_rate_pct':round(100*sum(x>0 for x in by_day)/len(by_day),2),
                          'validation':'ELIGIBLE_FOR_HUMAN_REVIEW' if ready else 'INSUFFICIENT_OBSERVATIONS',
                          'proposal':decision})
        proposals.append({'strategy':strategy,'horizon':horizon,'proposal':decision,
                          'requires_human_approval':True,'auto_apply':False})
    failure_counts=defaultdict(int)
    for row in clean:
        if row['data_status']!='OK':failure_counts[row['data_status']]+=1
    return {'version':VERSION,'mode':'PAPER_ONLY_NOT_AUTOTRADING',
            'source_events':len(events),'deduplicated_evaluation_rows':len(clean),
            'same_stock_day_strategy_duplicate_rows':repeated,
            'evaluation_horizons':list(HORIZONS),
            'metric_policy':'EQUAL_DAY_WEIGHTED;NO_LIVE_WEIGHT_AUTOMATION',
            'details':clean,'summary':summaries,'recommendations':proposals,
            'data_failures':dict(failure_counts),
            'guardrails':{'min_stock_day_samples':min_samples,'min_distinct_days':min_distinct_days,
                          'min_calendar_span_days':min_exposure_days,
                          'strict_evidence_required':True,'production_weights_changed':False,
                          'formal_buy_permission_changed':False}}
