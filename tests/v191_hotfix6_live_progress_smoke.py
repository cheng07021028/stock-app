# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import godpick_auto_scheduler as sched

TZ=ZoneInfo('Asia/Taipei')
NOW=datetime(2026,8,13,16,5,tzinfo=TZ)

# 1) Force-all simulate must emit run_start -> job_start/job_complete -> run_complete
# with truthful counts and labels.
events=[]
out=sched.run_due_jobs(now=NOW,force_all_enabled=True,simulate=True,progress_callback=lambda e: events.append(dict(e)))
assert out.get('executed'), out
assert events and events[0].get('event')=='run_start', events[:2]
assert events[-1].get('event')=='run_complete', events[-2:]
starts=[e for e in events if e.get('event')=='job_start']
ends=[e for e in events if e.get('event')=='job_complete']
assert len(starts)==len(out['executed'])==len(ends), (len(starts),len(ends),len(out['executed']))
assert starts[0].get('index')==1, starts[0]
assert starts[0].get('job_label')==sched.JOB_LABELS.get(starts[0].get('job')), starts[0]
assert ends[-1].get('completed_jobs')==len(out['executed']), ends[-1]
assert events[-1].get('success')==sum(1 for r in out['executed'] if r.get('status')=='SUCCESS'), events[-1]

# 2) A broken UI callback must never alter scheduler execution.
def broken_callback(evt):
    raise RuntimeError('mock Streamlit placeholder failure')
out2=sched.run_due_jobs(now=NOW,force_all_enabled=True,simulate=True,progress_callback=broken_callback)
assert len(out2.get('executed') or [])==len(out.get('executed') or []), out2

# 3) Page17 must use the callback for both due-run and force-all, and old static
# spinner-only force text must be gone.
page=(ROOT/'pages/17_系統健康檢查.py').read_text(encoding='utf-8')
assert 'progress_callback=_on_progress' in page
assert '_run_v191_with_live_progress(force_all=True)' in page
assert '_run_v191_with_live_progress(force_all=False)' in page
assert '目前第 {_index} 項' in page
assert '即時統計' in page
assert 'V191 強制驗證執行中...' not in page

print(f"PASS V191-H6 live progress | jobs={len(out['executed'])} | events={len(events)} | callback-isolation=PASS")
