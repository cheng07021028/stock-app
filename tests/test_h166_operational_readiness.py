# -*- coding: utf-8 -*-
"""V191 H166: test freshness, core gate, date alignment, admin-only activation."""
from datetime import datetime
from typing import Any
from pathlib import Path
from types import ModuleType
from zoneinfo import ZoneInfo
import ast
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h166_operational_readiness import (expected_finished_session, context_evidence, evaluate_operational_readiness)

TZ = ZoneInfo('Asia/Taipei')
NOW = datetime(2026, 11, 18, 13, 5, tzinfo=TZ)


def context(margin='2026-11-17', pcr='2026-11-17', futures='2026-11-17', source='TAIFEX_OPENAPI_VERIFIED'):
    return {'margin_by_stock':{str(i):{'市場別':'上櫃' if i>=120 else '上市'} for i in range(200)},
            'market_margin':{'TWSE融資券資料日期':margin},
            'taifex':{'TAIFEX_PCR資料日期':pcr,'TAIFEX期貨資料日期':futures,
                      'PCR成交量比%':91.2,'PCR未平倉量比%':93.1,
                      '外資臺指期未平倉淨口數':-1000,'來源':source}}


def status(rec='SUCCESS', super_ai='SUCCESS',rec_date='2026-11-17'):
    jobs={name:{'last_status':'SUCCESS','last_success_at':'2026-11-17 22:00:00'}
          for name in ('stock_master','macro_full','official_factors','super_ai_context','watchlist_runtime')}
    jobs['super_ai_context']['last_status']=super_ai
    jobs['godpick_recommendation']={'last_status':rec,'last_success_at':rec_date+' 22:30:00'}
    return {'jobs':jobs,'last_wakeup_result':'SUCCESS'}


def test_expected_session_before_close_and_after():
    assert expected_finished_session(NOW).isoformat()=='2026-11-17'
    assert expected_finished_session(datetime(2026,11,18,22,tzinfo=TZ)).isoformat()=='2026-11-18'
    assert expected_finished_session(datetime(2026,11,16,13,tzinfo=TZ)).isoformat()=='2026-11-13'
    assert expected_finished_session(datetime(2026,11,21,23,tzinfo=TZ)).isoformat()=='2026-11-20'


def test_stale_taifex_must_not_become_green_merely_if_recent():
    x=context_evidence(context(pcr='2026-11-16',futures='2026-11-16'),NOW)
    assert not x['verified']
    assert len(x['reasons'])>=2
    assert '外資臺指期' in ' '.join(x['reasons'])


def test_full_matching_official_source_can_pass():
    assert context_evidence(context(),NOW)['verified']


def test_distinct_release_windows_prevent_false_missing_margin_or_old_taifex():
    pre_margin=datetime(2026,11,17,18,0,tzinfo=TZ)
    # 18:00 futures must be today's feed, but end-of-day margin may be yesterday.
    assert context_evidence(context(margin='2026-11-16',pcr='2026-11-17',futures='2026-11-17'),pre_margin)['verified']
    old=context_evidence(context(margin='2026-11-17',pcr='2026-11-16',futures='2026-11-16'),pre_margin)
    assert not old['verified']
    assert old['taifex_expected_day']=='2026-11-17'


def test_tpex_timeout_cannot_pass_whole_market_context():
    old=context()
    old['margin_by_stock']={str(i): {'市場別':'上市'} for i in range(1068)}
    result=context_evidence(old,NOW)
    assert not result['verified']
    assert result['tpex_count']==0
    assert any('TPEx' in r for r in result['reasons'])


def test_margin_missing_fails_even_when_taifex_valid():
    x=context(margin='2026-11-16')
    assert not context_evidence(x,NOW)['verified']
    x['margin_by_stock']={}
    assert not context_evidence(x,NOW)['verified']


def test_unverified_legacy_oi_is_not_official():
    assert not context_evidence(context(source='TAIFEX_LEGACY_UNVERIFIED'),NOW)['verified']


def test_disabled_scheduler_is_blocker_and_never_automatic_enabled():
    settings={'enabled':False}
    a=evaluate_operational_readiness(settings,status(),context(),NOW)
    assert a['status']=='NOT_READY'
    assert not settings['enabled']
    assert any('總開關' in x for x in a['blockers'])


def test_stale_page07_last_success_is_fatal_not_heartbeat_success():
    a=evaluate_operational_readiness({'enabled':True},status(rec='BLOCKED',rec_date='2026-10-19'),context(),NOW)
    assert a['status']=='NOT_READY'
    assert a['last_recommendation_success']=='2026-10-19'
    assert any('07推薦' in x for x in a['blockers'])


def test_superai_warning_blocks_core_data_even_if_context_local_verified():
    a=evaluate_operational_readiness({'enabled':True},status(super_ai='WARNING'),context(),NOW)
    assert a['status']=='NOT_READY'
    assert any('super_ai_context' in x for x in a['blockers'])


def test_full_fresh_research_does_not_grant_trading_or_performance():
    a=evaluate_operational_readiness({'enabled':True},status(),context(),NOW)
    assert a['status']=='READY_FOR_RESEARCH_ONLY'
    assert a['h126_trade_authority']=='NOT_EVALUATED_HERE'
    assert a['out_of_sample_performance']=='NOT_VERIFIED'


def test_pending_backup_is_not_remote_success():
    a=evaluate_operational_readiness({'enabled':True},status(),context(),NOW,
                                     records_state={'github_pending':True})
    assert a['ready_for_current_research']  # backup pending != corrupted research data
    assert '08歷史' in ' '.join(a['advisories'])


def test_quota_guard_does_not_change_formal_auth():
    from godpick_h158_taifex_openapi import complete
    assert complete(context()['taifex'],now=NOW)
    assert not context_evidence(context(pcr='2026-11-16',futures='2026-11-16'),NOW)['verified']


def test_page17_does_not_auto_enable_and_checks_admin_and_remote():
    source=(ROOT/'pages/17_系統健康檢查.py').read_text(encoding='utf-8')
    ast.parse(source)
    assert 'h166_confirm_resume_scheduler' in source
    assert 'auth_role' in source and '"admin"' in source
    assert '_h166_candidate["enabled"] = True' in source
    assert 'read_github_json(' in source and 'data/config/godpick_auto_scheduler_settings.json' in source
    assert 'runtime-data' in source
    assert 'H126正式權威獨立判斷' in source


def test_superai_task_exposes_missing_dates_not_false_success(monkeypatch):
    s=(ROOT/'godpick_auto_update_tasks.py').read_text(encoding='utf-8')
    module=ast.parse(s)
    func=next(f for f in module.body if isinstance(f,ast.FunctionDef) and f.name=='task_super_ai_context')
    code=compile(ast.Module(body=[func],type_ignores=[]),'<H166-task>','exec')
    fake=ModuleType('godpick_super_ai_market_context')
    fake.refresh_super_ai_market_context=lambda **kw:(context(pcr='2026-11-16',futures='2026-11-16'), ['api lag'])
    monkeypatch.setitem(sys.modules,'godpick_super_ai_market_context',fake)
    from unittest.mock import patch
    ns={'time':time,'Any':Any,'_report':lambda ok,message,**kw:{'ok':ok,'message':message,**kw}}
    exec(code,ns)
    # Reference date may change if actually run later; missing 2026-11-16 is stale
    # relative to the current 2026-11-18 session.
    with patch('godpick_h166_operational_readiness.expected_finished_session',return_value=datetime(2026,11,18,tzinfo=TZ).date()):
        result=ns['task_super_ai_context']()
    assert result['warning'] is True
    assert result['details']['complete'] is False
    assert '資料日' in result['message'] and '未通過' in result['message']


def test_older_sources_missing_fields_do_not_crash():
    x=evaluate_operational_readiness({}, {}, {}, NOW)
    assert x['status']=='NOT_READY'
    assert not x['super_ai']['verified']


def test_tpex_single_timeout_then_success_is_bounded(monkeypatch):
    import godpick_super_ai_market_context as context_service
    from requests.exceptions import Timeout
    class Response:
        def raise_for_status(self): return None
        def json(self):
            return [{'SecuritiesCompanyCode':'3293','MarginPurchaseYesterdayBalance':'100',
                     'MarginPurchaseTodayBalance':'120','ShortSaleYesterdayBalance':'4',
                     'ShortSaleTodayBalance':'2'}]
    attempts={'n':0}
    def get(*a, **k):
        attempts['n']+=1
        if attempts['n']==1: raise Timeout('mock timeout')
        return Response()
    monkeypatch.setattr(context_service.requests,'get',get)
    rows, message=context_service.fetch_tpex_margin()
    assert attempts['n']==2
    assert rows['3293']['融資增減張']==20
    assert '逾時後成功' in message


def test_tpex_http_error_does_not_loop(monkeypatch):
    import godpick_super_ai_market_context as context_service
    attempts={'n':0}
    class Response:
        def raise_for_status(self): raise ValueError('HTTP mock 429')
    def get(*a, **k):
        attempts['n']+=1
        return Response()
    monkeypatch.setattr(context_service.requests,'get',get)
    rows, message=context_service.fetch_tpex_margin()
    assert not rows and attempts['n']==1
    assert 'failed' in message
