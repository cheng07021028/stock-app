# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import copy
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import godpick_auto_scheduler as scheduler

def _base_cfg():
    cfg = scheduler.normalize_settings(copy.deepcopy(scheduler.DEFAULT_SETTINGS))
    cfg['enabled'] = True
    cfg['weekdays_only'] = True
    return cfg

def test_h28_default_recommendation_is_last():
    cfg = scheduler.normalize_settings({})
    assert cfg['execution_order'][-1] == 'godpick_recommendation'
    assert cfg['recommendation_run_last_after_all_enabled'] is True
    assert set(cfg['execution_order']) == set(scheduler.JOB_LABELS)

def test_h28_custom_order_persists_when_final_gate_disabled():
    raw = copy.deepcopy(scheduler.DEFAULT_SETTINGS)
    raw['recommendation_run_last_after_all_enabled'] = False
    raw['execution_order'] = ['feedback_learning', 'godpick_recommendation', 'stock_master']
    cfg = scheduler.normalize_settings(raw)
    assert cfg['execution_order'][:3] == ['feedback_learning', 'godpick_recommendation', 'stock_master']

def test_h28_final_gate_waits_for_latest_daily_slot():
    cfg = _base_cfg()
    for key, jc in cfg['jobs'].items():
        jc['enabled'] = key in {'stock_master', 'feedback_learning', 'godpick_recommendation'}
    cfg['execution_order'] = ['stock_master', 'feedback_learning', 'godpick_recommendation'] + [
        x for x in cfg['execution_order'] if x not in {'stock_master', 'feedback_learning', 'godpick_recommendation'}
    ]
    now = datetime(2026, 8, 17, 21, 36, tzinfo=scheduler.TZ)
    stock_slot = scheduler._slot_datetime(now, max(scheduler._normalize_times(cfg['jobs']['stock_master']['times'])))
    status = {'completed_run_keys': [scheduler._run_key('stock_master', stock_slot)], 'jobs': {}}
    ok, msg = scheduler._recommendation_final_gate(cfg, status, now, 'godpick_recommendation')
    assert not ok and 'AI｜績效回饋＋每日學習狀態重建' in msg
    feedback_slot = scheduler._slot_datetime(now, max(scheduler._normalize_times(cfg['jobs']['feedback_learning']['times'])))
    status['completed_run_keys'].append(scheduler._run_key('feedback_learning', feedback_slot))
    ok2, msg2 = scheduler._recommendation_final_gate(cfg, status, now, 'godpick_recommendation')
    assert ok2 and msg2 == ''

def test_h28_final_gate_blocks_before_future_predecessor_slot():
    cfg = _base_cfg()
    for key, jc in cfg['jobs'].items():
        jc['enabled'] = key in {'durability_retry', 'godpick_recommendation'}
    cfg['execution_order'] = ['durability_retry', 'godpick_recommendation'] + [x for x in cfg['execution_order'] if x not in {'durability_retry','godpick_recommendation'}]
    now = datetime(2026, 8, 17, 21, 0, tzinfo=scheduler.TZ)
    ok, msg = scheduler._recommendation_final_gate(cfg, {'completed_run_keys': [], 'jobs': {}}, now, 'godpick_recommendation')
    assert not ok and '21:40 尚未到' in msg

def test_h28_execution_loop_uses_saved_order(monkeypatch):
    cfg = _base_cfg()
    wanted = ['feedback_learning', 'stock_master', 'godpick_recommendation']
    cfg['recommendation_run_last_after_all_enabled'] = False
    cfg['execution_order'] = wanted + [x for x in cfg['execution_order'] if x not in wanted]
    for key, jc in cfg['jobs'].items():
        jc['enabled'] = key in wanted
    monkeypatch.setattr(scheduler, 'load_settings', lambda: copy.deepcopy(cfg))
    monkeypatch.setattr(scheduler, 'load_status', lambda: {})
    monkeypatch.setattr(scheduler, 'load_history', lambda: [])
    res = scheduler.run_due_jobs(now=datetime(2026, 8, 17, 22, 0, tzinfo=scheduler.TZ), force_all_enabled=True, simulate=True)
    assert [row['job'] for row in res['executed']] == wanted

def test_h28_page17_has_persistent_rank_controls():
    src = (ROOT / 'pages' / '17_系統健康檢查.py').read_text(encoding='utf-8')
    assert 'execution_order' in src
    assert 'recommendation_run_last_after_all_enabled' in src
    assert '當日最後時段完成' in src
    assert 'v191_rank_' in src
