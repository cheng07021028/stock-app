# -*- coding: utf-8 -*-
"""H187 regression: bounded scan vs unchanged deep official recovery."""
from __future__ import annotations
import ast
import sys
from datetime import date
from pathlib import Path
from unittest.mock import Mock
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h186_evidence_integrity import volume_quality


def frame(n=162, *, weak=False, stale=False):
    days = pd.bdate_range('2026-03-16', periods=n)
    data = pd.DataFrame({'日期':days, '開盤價':[99.0]*n,'最高價':[101.0]*n,
        '最低價':[98.0]*n,'收盤價':[100.0]*n,'成交股數':[100000.0]*n})
    if weak:data.loc[data.index[-12:],'成交股數']=None
    if stale:data['日期']=data['日期']-pd.Timedelta(days=40)
    return data


def invoke(cache=None,yahoo=None,official=None, *,fresh=True):
    text=(ROOT/'utils.py').read_text('utf-8')
    fun=next(x for x in ast.parse(text).body if isinstance(x,ast.FunctionDef) and x.name=='get_history_data')
    calls={'yahoo':[],'official':[],'save':[],'events':[]}
    cache=frame() if cache is None else cache
    yahoo=frame() if yahoo is None else yahoo
    official=frame() if official is None else official
    def yahoo_fn(*args,**kwargs):
        calls['yahoo'].append(kwargs)
        return yahoo
    def official_fn(*args,**kwargs):
        calls['official'].append(kwargs)
        return official,'source'
    env={'pd':pd,'date':date,'time':__import__('time'),
         '_load_history_disk_cache':lambda *a,**k:cache,
         '_save_history_disk_cache':lambda *a,**k:calls['save'].append(k),
         '_fetch_yahoo_history_fast':yahoo_fn,
         '_fetch_twse_history_month':official_fn,
         '_fetch_tpex_history_month':official_fn,
         '_history_market_candidates':lambda *a,**k:['上市'],
         '_history_frame_business_lag':lambda *a,**k:0,
         '_history_frame_is_fresh_enough':lambda *a,**k:fresh,
         '_normalize_history_df':lambda data,*a,**k:data,
         '_diag_add_event':lambda *a,**k:calls['events'].append(a),
         }
    exec(compile(ast.fix_missing_locations(ast.Module(body=[fun],type_ignores=[])),str(ROOT/'utils.py'),'exec'),env)
    return env['get_history_data'],calls


DATES={'start_date':date(2026,3,1),'end_date':date(2026,11,23)}


def test_valid_cache_no_network():
    get,calls=invoke(cache=frame())
    output=get('2330',scan_fast=True,**DATES)
    assert len(output)==162 and not calls['yahoo'] and not calls['official']
    assert volume_quality(output)['status']=='VALID_60_DATED_POSITIVE_VOLUME'


def test_weak_cached_volume_is_deferred_without_network():
    get,calls=invoke(cache=frame(weak=True))
    output=get('2330',scan_fast=True,**DATES)
    assert len(output)==162 and not calls['yahoo'] and not calls['official']
    assert volume_quality(output)['status']=='INCOMPLETE_VOLUME'
    assert any(x[1]=='H187DeferredVolumeRepair' for x in calls['events'])


def test_uncached_yahoo_complete_is_certified():
    get,calls=invoke(cache=pd.DataFrame(),yahoo=frame())
    output=get('2330',scan_fast=True,**DATES)
    assert len(output)==162 and len(calls['yahoo'])==1 and not calls['official']
    assert calls['yahoo'][0]['quick_scan'] is True
    assert len(calls['save'])==1
    assert volume_quality(output)['status']=='VALID_60_DATED_POSITIVE_VOLUME'


def test_uncached_yahoo_partial_is_saved_but_not_certified():
    get,calls=invoke(cache=pd.DataFrame(),yahoo=frame(weak=True))
    output=get('2330',scan_fast=True,**DATES)
    assert len(output)==162 and len(calls['yahoo'])==1
    assert not calls['official']
    assert len(calls['save'])==1
    assert volume_quality(output)['status']=='INCOMPLETE_VOLUME'
    assert any(x[1]=='H187QuickYahoo' and not x[2] for x in calls['events'])


def test_yahoo_absent_never_triggers_n_month_official_requests():
    get,calls=invoke(cache=pd.DataFrame(),yahoo=pd.DataFrame())
    output=get('2330',scan_fast=True,**DATES)
    assert output.empty and len(calls['yahoo'])==1
    assert not calls['official']
    assert any(x[1]=='H187SourceGap' for x in calls['events'])


def test_yahoo_stale_never_returns_future_or_stale_history():
    get,calls=invoke(cache=pd.DataFrame(),yahoo=frame(),fresh=False)
    output=get('2330',scan_fast=True,**DATES)
    assert output.empty and not calls['official']


def test_default_h186_still_repairs_with_official_history():
    get,calls=invoke(cache=frame(weak=True),yahoo=frame(weak=True),official=frame())
    output=get('2330',scan_fast=False,**DATES)
    assert len(output)>=162 and calls['official']
    assert volume_quality(output)['status']=='VALID_60_DATED_POSITIVE_VOLUME'


def test_fast_path_source_has_no_trade_permission_or_record_mutation():
    source=(ROOT/'utils.py').read_text('utf-8')
    page=(ROOT/'pages'/'7_股神推薦.py').read_text('utf-8')
    assert 'scan_fast=True,  # H187' in page
    assert 'H187DeferredVolumeRepair' in source
    assert 'H187QuickYahoo' in source
    assert 'H126能否買進' not in source
    assert 'godpick_records.json' not in source[source.index('def get_history_data('):source.index('def get_history_data_debug(')]


def test_source_circuit_breaker_after_repeated_remote_failures():
    text=(ROOT/'utils.py').read_text('utf-8')
    tree=ast.parse(text)
    names={'_h187_quick_source_healthy','_h187_quick_source_result'}
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    import threading,time
    env={'_H187_SOURCE_LOCK':threading.Lock(), '_H187_YAHOO_CIRCUIT':{'failures':0,'until':0.0},
         '_H187_SOURCE_FAILURE_LIMIT':8,'_H187_SOURCE_COOLDOWN_SECONDS':90,'time':time}
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes,type_ignores=[])),str(ROOT/'utils.py'),'exec'),env)
    allowed=env['_h187_quick_source_healthy'];note=env['_h187_quick_source_result']
    assert allowed()
    for i in range(8):note(True)
    assert not allowed()
    note(False)
    assert allowed()


def test_yahoo_fast_source_limits_host_suffix_and_timeout():
    text=(ROOT/'utils.py').read_text('utf-8')
    tree=ast.parse(text)
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_fetch_yahoo_history_fast')
    import time
    from datetime import datetime,timedelta
    from zoneinfo import ZoneInfo
    got=[]
    class BadResp:status_code=429
    class Session:
        def get(self,url,**kwargs):
            got.append((url,kwargs));return BadResp()
    class SEM:
        def __enter__(self):return self
        def __exit__(self,*args):return False
    env={'pd':pd,'date':date,'datetime':datetime,'timedelta':timedelta,'ZoneInfo':ZoneInfo,'time':time,
         '_history_yahoo_range_param':lambda *args:'6mo', '_YAHOO_HISTORY_SEMAPHORE':SEM(),
         '_h187_quick_source_healthy':lambda:True,
         '_h187_quick_http_session':lambda:Session(),
         '_h187_quick_source_result':lambda x:None,
         '_diag_add_event':lambda *a,**k:None,
         'get_requests_session':lambda:Session(),'REQUEST_TIMEOUT_FAST':7}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])),str(ROOT/'utils.py'),'exec'),env)
    out=env['_fetch_yahoo_history_fast']('2330',market_type='上市',quick_scan=True,**DATES)
    assert out.empty and len(got)==1
    assert '/2330.TW' in got[0][0] and 'query1' in got[0][0]
    assert got[0][1]['timeout']==(2.0,3.0)


def prepare_h186_root(target:Path):
    source=Path('/mnt/data/work_h186/patch')
    for rel in ('utils.py','pages/7_股神推薦.py','godpick_h186_evidence_integrity.py'):
        path=target/rel
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes((source/rel).read_bytes())
    return target


def test_installer_accepts_exact_h186_and_reinstalls(tmp_path):
    from tools.h187_preflight import status,apply
    root=prepare_h186_root(tmp_path/'app')
    code,info=status(root,ROOT)
    assert code=='READY_APPLY_H187'
    applied=apply(root,ROOT)
    assert applied.startswith('APPLIED_H187') and 'backup=' in applied
    assert status(root,ROOT)[0]=='ALREADY_H187'
    assert apply(root,ROOT)=='ALREADY_H187'
    backup=next(tmp_path.glob('app_H187_backup_*'))
    assert (backup/'utils.py').read_bytes()==(Path('/mnt/data/work_h186/patch')/'utils.py').read_bytes()


def test_installer_rejects_unknown_local_modification(tmp_path):
    from tools.h187_preflight import status,apply
    root=prepare_h186_root(tmp_path/'app')
    with (root/'utils.py').open('a',encoding='utf-8') as f:f.write('\n# unknown new version\n')
    assert status(root,ROOT)[0]=='BLOCKED_UNKNOWN_OR_CORRUPT_VERSION'
    with pytest.raises(RuntimeError):apply(root,ROOT)


def test_installer_rejects_missing_h186_and_tampered_zip(tmp_path):
    from tools.h187_preflight import status
    root=prepare_h186_root(tmp_path/'app')
    (root/'godpick_h186_evidence_integrity.py').unlink()
    assert status(root,ROOT)[0]=='BLOCKED_MISSING_OR_UNKNOWN_H186_DEPENDENCY'
    (root/'godpick_h186_evidence_integrity.py').write_bytes(
        (Path('/mnt/data/work_h186/patch')/'godpick_h186_evidence_integrity.py').read_bytes())
    altered=tmp_path/'altered'
    for rel in ('utils.py','pages/7_股神推薦.py'):
        p=altered/rel;p.parent.mkdir(parents=True,exist_ok=True)
        p.write_bytes((ROOT/rel).read_bytes())
    (altered/'utils.py').write_text('malformed',encoding='utf-8')
    assert status(root,altered)[0]=='BLOCKED_UNKNOWN_OR_CORRUPT_VERSION'
