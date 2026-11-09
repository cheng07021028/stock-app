# -*- coding: utf-8 -*-
"""H153.3 targeted regressions for stalled nightly hit tracking and sync fanout."""
from __future__ import annotations
import inspect
from pathlib import Path
import pandas as pd
import godpick_auto_scheduler as scheduler
import godpick_auto_update_tasks as tasks


def test_scheduler_failure_circuit_only_when_confirmed():
    assert scheduler._h1533_remote_failure('record_latest_price', {'ok':False,'message':'推薦紀錄永久同步失敗'})
    assert scheduler._h1533_remote_failure('recommend_list_performance', {'ok':False,'message':'永久同步失敗'})
    assert not scheduler._h1533_remote_failure('stock_master', {'ok':False,'message':'同步失敗'})
    assert not scheduler._h1533_remote_failure('record_latest_price', {'ok':False,'message':'無歷史資料'})
    assert not scheduler._h1533_remote_failure('record_latest_price', {'ok':True,'message':'永久同步失敗'})
    assert scheduler.DEFAULT_JOB_OPTIONS['recommend_list_hits']['max_rows']<=6
    assert scheduler.DEFAULT_JOB_OPTIONS['recommend_list_hits']['scheduler_immediate_retries']==0


def test_intermediate_checkpoints_local_only_and_terminal_remote(monkeypatch):
    calls=[]
    import sys
    from types import SimpleNamespace
    monkeypatch.setitem(sys.modules,'godpick_persistence_service',SimpleNamespace(write_local_json_atomic=lambda name,data:calls.append(('local',name)) or (True,'saved')))
    monkeypatch.setattr(scheduler,'_persist_runtime',lambda name,data,reason:calls.append(('remote',name,reason)) or (True,'confirmed'))
    cfg={'history_keep':100}
    scheduler._checkpoint_runtime({'jobs':{}}, [],cfg,'before-recommend_list_hits')
    assert len(calls)==2 and all(x[0]=='local' for x in calls)
    calls.clear()
    scheduler._checkpoint_runtime({'jobs':{}}, [],cfg,'run-finished')
    assert len(calls)==2 and all(x[0]=='remote' for x in calls)


def test_hit_page_update_uses_budget_and_returns_remaining(monkeypatch):
    from godpick_headless_page_loader import load_page_namespace
    ns=load_page_namespace('pages/10_推薦清單.py',base_dir=Path(__file__).resolve().parents[1])
    func=ns['_update_night_hit_tracking_v101']
    assert 'time_budget_seconds' in inspect.signature(func).parameters
    row={'股票代號':'2330','股票名稱':'測試股','推薦日期':'2026-11-06'}
    data=pd.DataFrame([{**row,'作戰追蹤更新時間':''},{**row,'股票代號':'2317'},{**row,'股票代號':'2454'}])
    hits=[]
    ns['_calc_night_hit_for_row_v101']=lambda row,timeout=7,**kwargs: hits.append((row['股票代號'],timeout,kwargs)) or {
        '作戰追蹤狀態':'進場點命中','作戰追蹤更新時間':'2026-11-09 18:00:00'
    }
    updated,summary=func(data,max_rows=2,show_progress=False,time_budget_seconds=30,per_request_timeout_seconds=3)
    assert summary['processed']==2 and summary['remaining']==1
    assert summary['success']==2 and all(timeout<=3 for _,timeout,_ in hits)
    assert len(updated)==3 and updated['作戰追蹤更新時間'].notna().sum()==2
    hits.clear()
    # Already processed codes must not get fetched repeatedly when new rows remain.
    updated2,s2=func(updated,max_rows=2,show_progress=False,time_budget_seconds=30,per_request_timeout_seconds=3)
    assert s2['processed']==1 and s2['remaining']==0 and len(hits)==1


def test_hit_page_budget_expiry_preserves_every_row(monkeypatch):
    from godpick_headless_page_loader import load_page_namespace
    ns=load_page_namespace('pages/10_推薦清單.py',base_dir=Path(__file__).resolve().parents[1])
    fn=ns['_update_night_hit_tracking_v101']
    rows=pd.DataFrame([{'股票代號':'2330','股票名稱':'測試','推薦日期':'2026-11-06'}]*3)
    # At 1 second budget less than the 2s floor, not even one network fetch starts.
    ns['_calc_night_hit_for_row_v101']=lambda *args,**kwargs: (_ for _ in ()).throw(AssertionError('unexpected fetch'))
    result, summary=fn(rows,max_rows=3,show_progress=False,time_budget_seconds=1)
    assert len(result)==3 and summary['processed']==0 and summary['remaining']==3
    assert summary['budget_exhausted'] is True


def test_task_no_processing_does_not_resend_all_remote_sync(monkeypatch):
    from godpick_headless_page_loader import load_page_namespace
    ns=load_page_namespace('pages/10_推薦清單.py',base_dir=Path(__file__).resolve().parents[1])
    called=[]
    ns['_update_night_hit_tracking_v101']=lambda df,**kwargs:(df,{'processed':0,'remaining':2,'success':0,'fail':0})
    ns['_sync_records']=lambda df: called.append(True) or (True,[])
    ns['upsert_records_authority_fast']=lambda *args,**kwargs:None
    ns['save_named_json_permanent']=lambda *args,**kwargs:None
    monkeypatch.setattr('godpick_headless_page_loader.load_page_namespace',lambda *args,**kwargs:ns)
    monkeypatch.setattr(tasks,'_load_recommend_list',lambda:(pd.DataFrame([{'股票代號':'2330'}]),[]))
    result=tasks.task_recommend_list_hits({'max_rows':6})
    assert result['ok'] and result['warning'] and not called
    assert '下次續跑' in result['message']


def test_scheduled_history_skips_120_day_official_fallback(monkeypatch):
    import godpick_history_sources as sources
    seen=[]
    monkeypatch.setattr(sources,'fetch_yahoo_chart',lambda *args,**kwargs:pd.DataFrame())
    monkeypatch.setattr(sources,'fetch_stooq_csv',lambda *args,**kwargs:pd.DataFrame())
    monkeypatch.setattr(sources,'fetch_twse_tpex_daily_range',lambda *args,**kwargs:seen.append(True) or pd.DataFrame())
    _, label=sources.fetch_multi_source_history('2330',market='上市',start_date='2026-11-01',end_date='2026-11-09',timeout=2,allow_daily_fallback=False)
    assert not seen
    assert 'YahooChart' in label and 'StooqCSV' in label
    sources.fetch_multi_source_history('2330',market='上市',start_date='2026-11-01',end_date='2026-11-09',timeout=2)
    assert len(seen)==1  # explicit/manual workflow remains compatible


def test_no_remote_success_when_hit_list_sync_fails(monkeypatch):
    from godpick_headless_page_loader import load_page_namespace
    ns=load_page_namespace('pages/10_推薦清單.py',base_dir=Path(__file__).resolve().parents[1])
    ns['_update_night_hit_tracking_v101']=lambda df,**kwargs:(df,{'processed':1,'remaining':3,'success':1,'fail':0})
    ns['_sync_records']=lambda df:(False,['remote permission denied'])
    ns['upsert_records_authority_fast']=lambda *args,**kwargs:None
    ns['save_named_json_permanent']=lambda *args,**kwargs:None
    monkeypatch.setattr('godpick_headless_page_loader.load_page_namespace',lambda *args,**kwargs:ns)
    monkeypatch.setattr(tasks,'_load_recommend_list',lambda:(pd.DataFrame([{'股票代號':'2330'}]),[]))
    result=tasks.task_recommend_list_hits({'max_rows':6})
    assert not result['ok'] and '同步失敗' in result['message']
    assert result['details']['summary']['remaining']==3
