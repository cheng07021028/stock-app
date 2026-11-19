# -*- coding: utf-8 -*-
"""H173 local signed report writer, no network, no legacy recommendation mutation."""
from __future__ import annotations
import argparse,json,os,sys,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h172_closed_loop import load_verified_report
from godpick_h117_official_market_closure import load_official_days
from godpick_h173_relative_edge import build_relative_report,load_relative_report


def run(h172_path:Path,daily_path:Path,output:Path) -> dict:
    h172_path=Path(h172_path);daily_path=Path(daily_path);output=Path(output)
    original=load_verified_report(h172_path)
    if original is None:
        return {'status':'SKIPPED_NO_VERIFIED_H172_REPORT','saved':False}
    if not daily_path.is_dir() or not any(daily_path.rglob('*.json.gz')):
        return {'status':'SKIPPED_NO_VERIFIED_H117_DAILY','saved':False}
    evidence=load_official_days(daily_path)
    result=build_relative_report(original,evidence)
    if not any(result['overall'][f'T+{n}']['evaluated_distinct_code_days'] for n in (1,5,10)):
        return {'status':'SKIPPED_NO_RELATIVE_EVALUATIONS','saved':False,
                'research_events':result['research_events']}
    old=load_relative_report(output)
    if old and old['report_sha256']==result['report_sha256']:
        return {'status':'ALREADY_VERIFIED_IDENTICAL','saved':False,'sha256':result['report_sha256']}
    output.parent.mkdir(parents=True,exist_ok=True)
    blob=(json.dumps(result,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode('utf-8')
    fd,tmp=tempfile.mkstemp(prefix='.'+output.name+'.',suffix='.tmp',dir=output.parent)
    try:
        with os.fdopen(fd,'wb') as f:
            f.write(blob);f.flush();os.fsync(f.fileno())
        os.replace(tmp,output)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)
    verified=load_relative_report(output)
    if not verified or verified['report_sha256']!=result['report_sha256']:
        raise RuntimeError('H173 write/read-back verification failed')
    return {'status':'LOCAL_VERIFIED_NOT_YET_REMOTE_PERMANENT','saved':True,
            'sha256':result['report_sha256'],'h172_sha256':original['report_sha256'],
            'research_events':result['research_events'],'trade_authority_changed':False}


def main(argv=None):
    p=argparse.ArgumentParser(description='H173 official relative-index ex-post research')
    p.add_argument('--h172',type=Path,default=ROOT/'reports/h172/H172_performance_research.json')
    p.add_argument('--daily',type=Path,default=ROOT/'data/h117/daily')
    p.add_argument('--output',type=Path,default=ROOT/'reports/h173/H173_relative_index_audit.json')
    a=p.parse_args(argv)
    output=run(a.h172,a.daily,a.output)
    print(json.dumps(output,ensure_ascii=False,indent=2))
    return output

if __name__=='__main__':main()
