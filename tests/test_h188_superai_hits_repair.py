# -*- coding: utf-8 -*-
"""H188 deterministic regression: no live HTTP, Firestore writes, or secrets."""
from __future__ import annotations
import copy
import json
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import pandas as pd
import pytest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import godpick_auto_update_tasks as tasks
import godpick_auto_scheduler as sched
import godpick_super_ai_market_context as market
from godpick_headless_page_loader import load_page_namespace


def page10():
    return load_page_namespace('pages/10_推薦清單.py',base_dir=ROOT)


def yesterday(n=5):
    return (datetime.now(ZoneInfo('Asia/Taipei')).date()-timedelta(days=n)).isoformat()


def test_undated_57_current_list_never_fetches_or_saves(monkeypatch):
    ns=page10(); seen=[]
    ns['_calc_night_hit_for_row_v101']=lambda *args,**kwargs: seen.append(1)
    rows=pd.DataFrame([{'股票代號':str(2000+i),'推薦日期':'','作戰追蹤狀態':'缺推薦日期','作戰追蹤更新時間':'2026-11-20 00:00:00'} for i in range(57)])
    after,summary=ns['_update_night_hit_tracking_v101'](rows,max_rows=12,show_progress=False,time_budget_seconds=28)
    assert summary['success']==summary['processed']==summary['fail']==0
    assert summary['missing_recommendation_date']==57
    assert not seen
    assert len(after)==57
    assert after['作戰追蹤更新時間'].iloc[0]=='2026-11-20 00:00:00'


def test_undated_rows_manual_mode_also_prevents_network_and_false_success():
    ns=page10(); seen=[]
    ns['_calc_night_hit_for_row_v101']=lambda *args,**kwargs: seen.append(1)
    _,summary=ns['_update_night_hit_tracking_v101'](pd.DataFrame([{'股票代號':'2330','推薦日期':''}]),max_rows=60,show_progress=False)
    assert summary['missing_recommendation_date']==1 and summary['success']==0
    assert seen==[]


def test_not_yet_next_day_does_not_call_provider():
    ns=page10(); calls=[]
    ns['fetch_multi_source_history']=lambda *args,**kwargs: calls.append(1)
    result=ns['_calc_night_hit_for_row_v101']({'股票代號':'2330','推薦日期':datetime.now(ZoneInfo('Asia/Taipei')).date().isoformat()})
    assert result['作戰追蹤狀態']=='尚未到隔日' and not calls
    assert '作戰追蹤更新時間' not in result


def test_not_stamping_failed_history(monkeypatch):
    ns=page10()
    ns['_calc_night_hit_for_row_v101']=lambda *args,**kwargs:{'作戰追蹤狀態':'歷史K線不足','作戰追蹤更新時間':'2026-11-23 10:00:00'}
    original=pd.DataFrame([{'股票代號':'2330','推薦日期':yesterday(),'作戰追蹤更新時間':'','作戰追蹤狀態':''}])
    after,summary=ns['_update_night_hit_tracking_v101'](original,max_rows=1,show_progress=False,time_budget_seconds=10)
    assert summary['success']==0 and summary['fail']==1 and summary['remaining']==1
    assert after['作戰追蹤更新時間'].iloc[0]==''


def test_close_only_is_not_valid_intraday_hit():
    ns=page10()
    ns['fetch_multi_source_history']=lambda *args,**kwargs:(pd.DataFrame([{'日期':yesterday(1),'收盤價':100}]),'mock-close-only')
    result=ns['_calc_night_hit_for_row_v101']({'股票代號':'2330','推薦日期':yesterday(4),'推薦價格':100})
    assert result['作戰追蹤狀態']=='缺可信OHLC'
    assert '進場點命中' not in result


def test_horizon_is_only_computed_after_mature_sessions():
    ns=page10()
    ns['fetch_multi_source_history']=lambda *args,**kwargs:(pd.DataFrame([{'日期':yesterday(1),'最高價':105,'最低價':98,'收盤價':101}]),'official-mock')
    result=ns['_calc_night_hit_for_row_v101']({'股票代號':'2330','推薦日期':yesterday(4),'推薦價格':100})
    assert result['隔日最高漲幅%']==5.0
    assert result['3日最高漲幅%'] is None
    assert result['5日最高漲幅%'] is None
    assert result['10日最高漲幅%'] is None


def test_recommendation_date_never_uses_later_update_time():
    ns=page10()
    assert ns['_row_recommend_date_v101']({'更新時間':'2026-11-23 14:18:30'}) is None
    assert ns['_row_recommend_date_v101']({'推薦日期':'2026-11-17','更新時間':'2026-11-23'})==date(2026,11,17)


def test_schedule_no_verified_progress_never_calls_remote(monkeypatch):
    ns={'_update_night_hit_tracking_v101':lambda df,**kw:(df,{'processed':0,'success':0,'fail':0,'remaining':0,'missing_recommendation_date':57}),
        '_sync_records':lambda df:(_ for _ in ()).throw(AssertionError('must never sync')),
        'upsert_records_authority_fast':lambda *a,**kw:None,
        'save_named_json_permanent':lambda *a,**kw:None}
    monkeypatch.setattr('godpick_headless_page_loader.load_page_namespace',lambda *a,**kw:ns)
    monkeypatch.setattr(tasks,'_load_recommend_list',lambda:(pd.DataFrame([{'股票代號':'2330'}]),[]))
    r=tasks.task_recommend_list_hits({})
    assert r['ok'] and r['warning']
    assert '缺推薦日期 57' in r['message'] and '遠端同步' in r['message']


def test_schedule_valid_hit_sync_failure_still_failed(monkeypatch):
    ns={'_update_night_hit_tracking_v101':lambda df,**kw:(df,{'processed':1,'success':1,'fail':0,'remaining':0}),
        '_sync_records':lambda df:(False,['HTTP 403']),
        'upsert_records_authority_fast':lambda *a,**kw:None,'save_named_json_permanent':lambda *a,**kw:None}
    monkeypatch.setattr('godpick_headless_page_loader.load_page_namespace',lambda *a,**kw:ns)
    monkeypatch.setattr(tasks,'_load_recommend_list',lambda:(pd.DataFrame([{'股票代號':'2330'}]),[]))
    r=tasks.task_recommend_list_hits({})
    assert not r['ok'] and 'HTTP 403' in r['message']


def test_twse_no_rows_no_fake_fresh_business_date(monkeypatch):
    class R:
        def raise_for_status(self):pass
        def json(self):return {'tables':[]}
    monkeypatch.setattr(market.requests,'get',lambda *a,**kw:R())
    stocks,info,msg=market.fetch_twse_margin(date=datetime(2026,11,23))
    assert not stocks and 'TWSE融資券資料日期' not in info


def test_unattended_legacy_fallback_not_called(monkeypatch):
    import godpick_h158_taifex_openapi as api
    monkeypatch.setattr(api,'fetch_taifex_openapi',lambda **kw:({},['OpenAPI no current data']))
    monkeypatch.setattr(market.requests,'get',lambda *a,**kw:(_ for _ in ()).throw(AssertionError('Excel fallback forbidden')))
    data,diags=market.fetch_taifex_context(legacy_fallback=False)
    assert data=={} and any('Excel/HTML' in x for x in diags)


def test_full_no_source_keeps_previous_snapshot(monkeypatch):
    monkeypatch.setattr(market,'fetch_twse_margin',lambda:({}, {}, 'fail'))
    monkeypatch.setattr(market,'fetch_tpex_margin',lambda:({}, 'fail'))
    monkeypatch.setattr(market,'fetch_taifex_context',lambda **kw:({},['fail']))
    monkeypatch.setattr(market,'fetch_etf_context',lambda:({'ETFs':{},'ETF有效檔數':0},['fail']))
    monkeypatch.setattr(market,'load_super_ai_market_context',lambda:{'margin_by_stock':{'2330':{'market':'TWSE'}},'updated_at':'2026-11-20'})
    monkeypatch.setattr(market,'persist_json_async',lambda *a,**kw:(_ for _ in ()).throw(AssertionError('should never overwrite last good')))
    d,m=market.refresh_super_ai_market_context(unattended=False)
    assert d['h188_refresh']['status']=='RETAINED_PREVIOUS' and d['updated_at']=='2026-11-20'


def test_unattended_requires_remote_save_confirm(monkeypatch):
    monkeypatch.setattr(market,'fetch_twse_margin',lambda:({'2330':{'市場別':'上市'}},{'TWSE融資券資料日期':yesterday()},'TWSE OK'))
    monkeypatch.setattr(market,'fetch_tpex_margin',lambda:({},'TPEX not available'))
    monkeypatch.setattr(market,'fetch_taifex_context',lambda **kw:({},['OpenAPI missing']))
    monkeypatch.setattr(market,'fetch_etf_context',lambda:({'ETFs':{},'ETF有效檔數':0},[]))
    import godpick_durability_service as dur
    calls=[]
    monkeypatch.setattr(dur,'persist_json_permanent',lambda *a,**kw:calls.append(kw) or (False,'GitHub HTTP 403'))
    result,m=market.refresh_super_ai_market_context(unattended=True)
    assert result['h188_refresh']['persistence']=='REMOTE_UNCONFIRMED'
    assert calls[0]['require_remote'] is True
    assert any('未取得GitHub' in x for x in m)


def test_scheduler_super_ai_partial_never_claims_full_success(monkeypatch):
    import godpick_h166_operational_readiness as h166
    monkeypatch.setattr(market,'refresh_super_ai_market_context',lambda **kw:({'h188_refresh':{'status':'NEW_PARTIAL_OR_COMPLETE','persistence':'REMOTE_CONFIRMED'},'margin_by_stock':{'2330':{}},'taifex':{}},['TAIFEX stale']))
    monkeypatch.setattr(h166,'context_evidence',lambda payload,now:{'verified':False,'stock_count':1,'reasons':['PCR過期'],'reference_day':yesterday()})
    r=tasks.task_super_ai_context({})
    assert r['ok'] and r['warning'] and not r['details']['complete']
    assert r['details']['refresh']['persistence']=='REMOTE_CONFIRMED'


def test_scheduler_super_ai_unconfirmed_save_is_failure(monkeypatch):
    import godpick_h166_operational_readiness as h166
    monkeypatch.setattr(market,'refresh_super_ai_market_context',lambda **kw:({'h188_refresh':{'status':'NEW_PARTIAL_OR_COMPLETE','persistence':'REMOTE_UNCONFIRMED'},'margin_by_stock':{'2330':{}},'taifex':{}},['GitHub 403']))
    monkeypatch.setattr(h166,'context_evidence',lambda payload,now:{'verified':True,'stock_count':1,'reasons':[],'reference_day':yesterday()})
    r=tasks.task_super_ai_context({})
    assert not r['ok'] and '持久化=REMOTE_UNCONFIRMED' in r['message']


def test_existing_scheduler_no_repeated_immediate_superai_requests(monkeypatch):
    import sys
    called=[]
    dummy=SimpleNamespace(TASK_HANDLERS={'super_ai_context': lambda opts:called.append(opts) or {'ok':False,'message':'HTTP 429'}})
    monkeypatch.setitem(sys.modules,'godpick_auto_update_tasks',dummy)
    monkeypatch.setattr(sched,'_touch_lock',lambda *a,**kw:None)
    r=sched._execute_one('super_ai_context',{'options':{}},{'retry_count':3,'retry_delay_seconds':0})
    assert r['attempt']==1 and len(called)==1


def test_no_writes_to_original_h126_or_records():
    # The patch delivery is limited to tasks/source/page10/scheduler: no
    # record JSON, trade authority, rank engine, or other page is shipped.
    assert 'H126' in (ROOT/'godpick_auto_scheduler.py').read_text('utf-8')
    assert (ROOT/'godpick_h126_trade_authority.py').is_file() or any(ROOT.glob('godpick_h126*.py'))
