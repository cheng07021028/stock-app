# -*- coding: utf-8 -*-
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import ast,sys
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h167_remote_truth import scheduler_transition, select_super_ai_snapshot


def test_newer_remote_wins_without_modifying_local():
    l={'updated_at':'2026-11-16 01:00:00','margin_by_stock':{}}
    r={'updated_at':'2026-11-17 22:03:58','margin_by_stock':{'3293':{'市場別':'上櫃'}}}
    x=select_super_ai_snapshot(l,r)
    assert x['source']=='GitHub runtime-data'
    assert len(x['context']['margin_by_stock'])==1
    assert l['margin_by_stock']=={} and r['margin_by_stock']['3293']['市場別']=='上櫃'


def test_missing_remote_is_not_misrepresented_as_verified():
    local={'updated_at':'2026-11-17 09:00:00','margin_by_stock':{'2892':{}}}
    x=select_super_ai_snapshot(local,{},remote_reachable=False)
    assert x['context']==local
    assert x['remote_available'] is False
    assert '不能冒稱最新' in x['warning']


def test_missing_both_never_claims_data():
    x=select_super_ai_snapshot({}, {}, remote_reachable=False)
    assert not x['context']
    assert x['source']=='未取得可用快照'


def test_newer_local_is_not_overwritten_with_older_remote():
    local={'updated_at':'2026-11-18 08:00:00','margin_by_stock':{'A':{}}}
    remote={'updated_at':'2026-11-17 22:03:58','margin_by_stock':{}}
    x=select_super_ai_snapshot(local,remote)
    assert x['source']=='Streamlit 本機快照'
    assert x['warning']


def test_equal_timestamps_prefer_confirmed_remote():
    x=select_super_ai_snapshot({'updated_at':'2026-11-17 22:03:58','margin_by_stock':{}},
         {'updated_at':'2026-11-17 22:03:58','margin_by_stock':{'3293':{}}})
    assert '3293' in x['context']['margin_by_stock']


def test_new_settings_after_old_disabled_checkpoint():
    cfg={'enabled':True,'updated_at':'2026-11-18 16:12:06'}
    status={'scheduler_enabled':False,'updated_at':'2026-11-18 10:21:03',
            'last_wakeup_result':'SKIPPED_DISABLED'}
    r=scheduler_transition(cfg,status)
    assert r['pending_new_checkpoint']
    assert not r['settings_status_conflict']
    assert '派送成功不等於工作完成' in r['message']


def test_newer_disabled_checkpoint_is_true_conflict():
    cfg={'enabled':True,'updated_at':'2026-11-18 16:12:06'}
    st={'scheduler_enabled':False,'updated_at':'2026-11-18 16:15:00',
        'last_wakeup_result':'SKIPPED_DISABLED'}
    r=scheduler_transition(cfg,st)
    assert r['settings_status_conflict']


def test_disabled_is_not_assumed_active():
    cfg={'enabled':False,'updated_at':'2026-11-18 16:12:06'}
    status={'scheduler_enabled':False,'updated_at':'2026-11-18 10:21:03'}
    r=scheduler_transition(cfg,status)
    assert not r['settings_status_conflict']
    assert not r['pending_new_checkpoint']


def test_new_enabled_heartbeat_clears_transition():
    cfg={'enabled':True,'updated_at':'2026-11-18 16:12:06'}
    st={'scheduler_enabled':True,'updated_at':'2026-11-18 16:15:00',
        'last_wakeup_result':'RUNNING'}
    r=scheduler_transition(cfg,st)
    assert not r['settings_status_conflict'] and not r['pending_new_checkpoint']


def test_page17_audit_live_refresh_throttling_and_duplicate_dispatch():
    content=(Path(__file__).resolve().parents[1]/'pages/17_系統健康檢查.py').read_text('utf-8')
    ast.parse(content)
    assert 'from godpick_h167_remote_truth import' in content
    assert "@st.cache_data(ttl=150" in content
    assert "_h166_market = _h167_snapshot['context']" in content
    assert content.count('st.session_state["_v191_h19_autocatchup_try_epoch"] = time.time()')==2
    assert 'st.session_state["_v191_h19_autocatchup_try_epoch"] = _now_epoch' in content
    assert "read_github_json('super_ai_market_context.json', {})" in content
    assert 'save_auto_scheduler_settings(_h166_candidate)' in content


def test_no_git_write_or_h126_modification_in_authority_helper():
    src=(Path(__file__).resolve().parents[1]/'godpick_h167_remote_truth.py').read_text('utf-8')
    for call in ('requests.post','requests.put','write_github_json','save_settings','open(', 'st.secrets'):
        assert call not in src
