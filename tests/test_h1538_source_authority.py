# -*- coding: utf-8 -*-
"""H153.8: data *content* authority, no July reboot rollback.

All tests are deterministic; no live network or remote write.
"""
import ast
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
import godpick_h1538_source_authority as mod

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / 'pages' / '7_股神推薦.py'
TODAY = date(2026, 11, 11)


def market(day):
    return {'twse_data_date':day,'updated_at':'2026-11-11 22:05:14','market_score':58.5,
            'required_by_godpick':{'market_trend':'震盪','risk_gate':'selective'}}


def official(day):
    return {'data_date':day,'updated_at':'2026-11-11 22:05:56',
            'records':[{'股票代號':'2330','官方資料日期':day,'法人資料日期':day}]}


def test_content_date_not_reboot_file_time():
    a=market('2026-07-09');a['updated_at']='2026-11-11 23:58:00'
    b=market('2026-11-11');b['updated_at']='2026-11-11 17:00:00'
    out=mod.select_authority('market_snapshot.json',a,b,today=TODAY)
    assert out['source']=='github-runtime-data-readonly'
    assert out['business_date']=='2026-11-11'
    assert out['payload']==b


def test_official_records_and_compact_yyyymmdd():
    out=mod.select_authority('official_factors_cache.json', official('20260709'),
                             official('20261111'),today=TODAY)
    assert out['source']=='github-runtime-data-readonly'
    assert out['business_date']=='2026-11-11'
    assert mod.date_of(20261111)==TODAY


def test_no_downgrade_when_remote_is_older():
    a=market('2026-11-11'); b=market('2026-11-10')
    out=mod.select_authority('market_snapshot.json',a,b,today=TODAY)
    assert out['source']=='local' and out['payload']==a


def test_runtime_unreachable_quarantines_packaged_july():
    a=market('2026-07-09')
    out=mod.select_authority('market_snapshot.json',a,None,today=TODAY)
    assert out['source']=='stale-quarantined'
    assert not out['payload']
    o=mod.select_authority('official_factors_cache.json',official('20260709'),None,today=TODAY)
    assert o['source']=='stale-quarantined' and not o['payload']


def test_future_or_metadata_only_does_not_gain_authority():
    future=market('2026-11-12')
    out=mod.select_authority('market_snapshot.json',market('2026-07-09'),future,today=TODAY)
    assert not out['payload']
    fake={'updated_at':'2026-11-11 23:59:59','records':[{'股票代號':'2330'}]}
    assert mod.business_date('official_factors_cache.json',fake) is None
    assert mod.business_date('market_snapshot.json',{'updated_at':'2026-11-11'}) is None


def test_reconcile_writes_only_verified_strictly_newer_local(tmp_path):
    old=market('2026-07-09')
    p=tmp_path/'market_snapshot.json';p.write_text(json.dumps(old))
    fetch=lambda k: (market('2026-11-11'),'REMOTE_OK')
    out=mod.reconcile_json(p.name,old,root=tmp_path,fetcher=fetch,today=TODAY,write_local=True)
    assert out['source']=='github-runtime-data-readonly' and out['local_restored']
    assert json.loads(p.read_text())['twse_data_date']=='2026-11-11'
    out2=mod.reconcile_json(p.name, market('2026-11-11'),root=tmp_path,
                            fetcher=lambda k:(market('2026-07-09'),'OLDER'),today=TODAY,write_local=True)
    assert out2['source']=='local'
    assert json.loads(p.read_text())['twse_data_date']=='2026-11-11'


def test_public_read_never_requires_write_token(monkeypatch):
    mod.reset_test_cache()
    monkeypatch.setattr(mod,'_repo_config',lambda:('cheng07021028','stock-app','runtime-data',''))
    monkeypatch.setattr(mod,'_repo_is_public',lambda o,r:True)
    calls=[]
    class Resp:
        status_code=200
        def iter_content(self,size):
            yield json.dumps(market('2026-11-11')).encode()
        def close(self): pass
    def request(url,**kwargs):
        calls.append((url,kwargs))
        return Resp()
    monkeypatch.setattr(mod.requests,'get',request)
    a,msg=mod.fetch_runtime_json('market_snapshot.json')
    b,msg2=mod.fetch_runtime_json('market_snapshot.json')
    assert a==b and msg==msg2=='RUNTIME_REMOTE_OK' and len(calls)==1
    assert not any('Authorization' in x[1].get('headers',{}) for x in calls)
    assert 'runtime-data' in calls[0][0]
    mod.reset_test_cache()


def _func(name):
    code=ast.parse(PAGE.read_text(encoding='utf-8'))
    n=next(x for x in code.body if isinstance(x,ast.FunctionDef) and x.name==name)
    n.decorator_list=[]
    return n


def _page_read_function():
    ns={'Path':Path,'json':json,'__file__':str(PAGE),'Any':Any}
    exec(compile(ast.Module(body=[_func('_read_project_json_file')],type_ignores=[]),str(PAGE),'exec'),ns)
    return ns['_read_project_json_file']


def test_page07_reads_current_when_main_is_july(monkeypatch):
    def fetch(kind):
        if kind in ('market_snapshot.json','macro_mode_bridge.json'):
            return market('2026-11-11'),'RUNTIME_OK'
        if kind=='official_factors_cache.json':
            return official('20261111'),'RUNTIME_OK'
        return None,'MISSING'
    def safe_reconcile(kind, old, **kwargs):
        new, status = fetch(kind)
        decision = mod.select_authority(kind, old, new, today=TODAY)
        decision['remote_status'] = status
        return decision
    monkeypatch.setattr(mod,'reconcile_json',safe_reconcile)
    read=_page_read_function()
    assert read('market_snapshot.json')['twse_data_date']=='2026-11-11'
    assert read('official_factors_cache.json')['data_date']=='20261111'
    # The existing local source must never be mutated by this unit fixture.
    assert json.loads((ROOT/'market_snapshot.json').read_text())['data_date']=='2026-07-09'
    mod.reset_test_cache()


def test_page07_data_freshness_must_not_use_row_dates_as_source_authority(monkeypatch):
    fnames=['_parse_date_v173','_business_lag_v173','_max_row_date_v173',
            '_project_data_freshness_snapshot_v173']
    ns={'pd':pd,'Any':Any,'MARKET_SNAPSHOT_FILE':'market_snapshot.json',
        'OFFICIAL_FACTORS_CACHE_FILE':'official_factors_cache.json',
        'evaluate_twse_t86_release_timing':None,'__file__':str(PAGE)}
    exec(compile(ast.Module(body=[_func(n) for n in fnames],type_ignores=[]),str(PAGE),'exec'),ns)
    ns['_expected_latest_trade_date_v173']=lambda:pd.Timestamp('2026-11-11')
    ns['_load_latest_recommendation_authority_v185']=lambda:({
        'candidate_diagnosis':[{'股票代號':'2330','本輪市場最新交易日':'2026-11-11',
                               '大盤資料日期':'2026-11-11','官方因子資料日期':'2026-11-11'}],
        'saved_at':'2026-11-11','recommendations':[]},[])
    local_old_market=market('2026-07-09'); local_old_official=official('20260709')
    ns['_read_project_json_file']=lambda kind: (
        local_old_market if kind=='market_snapshot.json' else local_old_official)
    o=ns['_project_data_freshness_snapshot_v173']()
    assert o['hard_block']
    assert o['market_date']=='2026-07-09' and o['official_date']=='2026-07-09'
    ns['_read_project_json_file']=lambda kind: (
        market('2026-11-11') if kind=='market_snapshot.json' else official('20261111'))
    o2=ns['_project_data_freshness_snapshot_v173']()
    assert o2['market_date']==o2['official_date']=='2026-11-11'
    assert not o2['hard_block']
    # No market source, even if scan-row tags claim latest date: BLOCK.
    ns['_read_project_json_file']=lambda kind: {}
    o3=ns['_project_data_freshness_snapshot_v173']()
    assert not o3['market_date'] and not o3['official_date'] and o3['hard_block']


def test_h40_fallback_has_stale_date_guard():
    node=_func('_read_market_snapshot_v33')
    code=ast.get_source_segment(PAGE.read_text(encoding='utf-8'),node)
    assert 'select_authority("market_snapshot.json", _h40_restored, None)' in code


def test_other_settings_read_path_does_not_fetch_remote(monkeypatch):
    def no_fetch(name):raise AssertionError('must not fetch remote for settings')
    monkeypatch.setattr(mod,'fetch_runtime_json',no_fetch)
    read=_page_read_function()
    assert isinstance(read('godpick_manager_dashboard_settings.json'),dict)
