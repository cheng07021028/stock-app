#!/usr/bin/env python3
"""Bounded allowlisted GitHub workflow dispatch. Never claim a dispatch is a recovery."""
import argparse,json,subprocess,sys
from datetime import datetime,timedelta,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h213_quality_gate import WORKFLOWS,VERSION
from godpick_h194_performance import stamp,ar

def due(runs,now):
    now=stamp(now);active={'queued','in_progress','waiting','requested','pending'}
    if any(r.get('status') in active for r in runs):return False,'ACTIVE_RUN'
    ts=[stamp(r['createdAt']) for r in runs]
    if any(t>now or now-t<timedelta(minutes=30) for t in ts):return False,'COOLDOWN'
    if sum(r.get('event')=='workflow_dispatch' and t.date()==now.date() for r,t in zip(runs,ts))>=2:return False,'DAILY_DISPATCH_LIMIT'
    return True,'DUE'

def dispatch(report,now,*,invoke=subprocess.run):
    now=stamp(now);obs=stamp(report['asof'])
    if report.get('version')!=VERSION or report.get('trade_authority_changed') is not False or obs>now or now-obs>timedelta(minutes=10) or obs.date()!=now.date() or now.weekday()>=5 or not time(18,15)<=now.time()<time(23,35):return [{'status':'REPORT_STALE_OR_OUTSIDE_WINDOW'}]
    if report.get('errors'):return [{'status':'ERROR_REQUIRES_REVIEW'}]
    out=[]
    for name in dict.fromkeys(report['repair_plan']):
        if name not in WORKFLOWS:raise ValueError('unknown workflow')
        wf=WORKFLOWS[name]
        try:
            proc=invoke(['gh','run','list','--workflow',wf,'--branch','main','--limit','30','--json','status,createdAt,event'],check=True,capture_output=True,text=True,timeout=30)
            ok,why=due(json.loads(proc.stdout),now)
            if ok:
                invoke(['gh','workflow','run',wf,'--ref','main'],check=True,capture_output=True,text=True,timeout=30);why='DISPATCH_REQUESTED_NOT_COMPLETED'
            out.append({'workflow':name,'status':why})
        except (subprocess.SubprocessError,ValueError,KeyError,TypeError,OSError) as e:out.append({'workflow':name,'status':'DISPATCH_ERROR','reason':type(e).__name__})
    return out

def main():
    p=argparse.ArgumentParser();p.add_argument('report',type=Path);p.add_argument('--out',type=Path,required=True);a=p.parse_args();r=dispatch(json.loads(a.report.read_text('utf-8')),datetime.now(ar.TZ));a.out.write_text(json.dumps(r,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(r,ensure_ascii=False));return 2 if any(x['status']=='DISPATCH_ERROR' for x in r) else 0
if __name__=='__main__':raise SystemExit(main())
