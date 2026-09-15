# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

from godpick_scheduler_wakeup_service import dispatch_scheduler_wakeup

class Resp:
    status_code=204

def main():
    captured={}
    def fake_post(url, headers=None, json=None, timeout=None):
        captured.update({'url':url,'headers':headers or {},'json':json or {},'timeout':timeout})
        return Resp()
    ok,msg=dispatch_scheduler_wakeup(token='secret', owner='o', repo='r', manual_job='official_factors', wakeup_source='h71_test', http_post=fake_post)
    assert ok, msg
    assert captured['json']['inputs']['manual_job']=='official_factors'
    assert captured['json']['inputs']['wakeup_source']=='h71_test'
    assert 'secret' not in captured['url']
    wf=(ROOT/'.github/workflows/godpick_auto_scheduler_v191.yml').read_text(encoding='utf-8')
    runner=(ROOT/'tools/run_godpick_auto_scheduler_v191.py').read_text(encoding='utf-8')
    assert 'manual_job:' in wf and 'GODPICK_MANUAL_JOB' in wf
    assert 'selected_jobs=[manual_job]' in runner and 'force_all_enabled=True' in runner
    print('PASS H71 manual central official worker')

if __name__=='__main__': main()
