# -*- coding: utf-8 -*-
"""Read signed H116 decisions + H117 quotes, create separate H172 research report.

No network or permanent record writing; generated file becomes durable only after
GitHub workflow actually commits it and readback is confirmed.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h116_daily_feedback import load_snapshot_files
from godpick_h117_official_market_closure import load_official_days
from godpick_h172_closed_loop import build_research_report,load_verified_report


def _atomic(path:Path,payload:bytes)->None:
    path.parent.mkdir(parents=True,exist_ok=True)
    fd,temp=tempfile.mkstemp(prefix='.'+path.name,suffix='.tmp',dir=path.parent)
    try:
        with os.fdopen(fd,'wb') as f:
            f.write(payload);f.flush();os.fsync(f.fileno())
        os.replace(temp,path)
    finally:
        if os.path.exists(temp):os.unlink(temp)


def run(decisions:Path,daily:Path,output:Path,*,as_of:str|None=None)->dict:
    # Missing inputs never erase earlier authority. Skips are honest, not success.
    decisions=Path(decisions);daily=Path(daily);output=Path(output)
    if not decisions.is_dir() or not any(decisions.rglob('*.json')):
        return {'status':'SKIPPED_NO_IMMUTABLE_H116_DECISIONS','saved':False}
    if not daily.is_dir() or not any(daily.rglob('*.json.gz')):
        return {'status':'SKIPPED_NO_VERIFIED_H117_DAILY','saved':False}
    snaps=load_snapshot_files(decisions)
    official=load_official_days(daily)
    if as_of is None:
        stamps=[str(m.get('collected_at')) for group in official.values()
                for m in group.values() if m.get('collected_at')]
        if not stamps:
            return {'status':'SKIPPED_NO_OBSERVED_H117_TIMES','saved':False}
        from godpick_h172_closed_loop import _moment
        moments=[_moment(t) for t in stamps]
        if any(t is None for t in moments):
            raise ValueError('invalid H117 collected_at timestamp')
        as_of=max(moments).isoformat()
    report=build_research_report(snaps, official, evaluated_at=as_of)
    if not report['results'] or not report['official_index_observed_sessions']:
        return {'status':'SKIPPED_NO_PIT_VERIFIED_EVENTS_OR_OFFICIAL_SESSIONS',
                'saved':False,'excluded':len(report['excluded_events'])}
    before=load_verified_report(output)
    if before and before['report_sha256']==report['report_sha256']:
        return {'status':'ALREADY_VERIFIED_IDENTICAL','saved':False,'sha256':report['report_sha256'],
                'eligible':len(report['results'])}
    blob=(json.dumps(report,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode('utf-8')
    _atomic(output,blob)
    check=load_verified_report(output)
    if check is None or check['report_sha256']!=report['report_sha256']:
        raise RuntimeError('H172 after-write SHA integrity mismatch')
    return {'status':'LOCAL_WRITTEN_READBACK_VERIFIED_NOT_YET_GITHUB_PERMANENT',
            'saved':True,'sha256':report['report_sha256'],'eligible':len(report['results']),
            'excluded':len(report['excluded_events']),
            'partial_market_sessions':len(report['partial_official_market_sessions']),
            'official_calendar_certified':False,'trade_authority_changed':False}


def main(argv=None):
    p=argparse.ArgumentParser(description='H172 official retrospective T+1/T+5/T+10 research sidecar')
    p.add_argument('--decisions',type=Path,default=ROOT/'data/h116/decisions')
    p.add_argument('--daily',type=Path,default=ROOT/'data/h117/daily')
    p.add_argument('--output',type=Path,default=ROOT/'reports/h172/H172_performance_research.json')
    p.add_argument('--as-of',help='Optional aware ISO timestamp; default latest collected official H117 moment, deterministic')
    args=p.parse_args(argv)
    result=run(args.decisions,args.daily,args.output,as_of=args.as_of)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return result

if __name__=='__main__':main()
