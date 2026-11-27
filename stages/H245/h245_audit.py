# -*- coding: utf-8 -*-
"""Offline read-only audit of local JSON evidence. Does not certify remote authority."""
import argparse
import json
from collections import Counter
from pathlib import Path
from godpick_h245_list_authority import plan_dates, digest, valid_state, state_name


def rows(p):
    if isinstance(p, list): return p
    if isinstance(p, dict):
        for k in ('recommendations','records','data','rows','items'):
            if isinstance(p.get(k), list): return p[k]
    return []


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('project',type=Path)
    ap.add_argument('--details',action='store_true',help='show row-level evidence (stdout only)')
    a=ap.parse_args()
    data={}; states={}
    for name in ('godpick_recommend_list.json','godpick_latest_recommendations.json','godpick_records.json'):
        f=a.project/name
        if not f.is_file():
            data[name]=None;states[name]={'read':'MISSING'};continue
        try:
            data[name]=json.loads(f.read_text(encoding='utf-8-sig'))
            sf=a.project/state_name(name)
            state=json.loads(sf.read_text(encoding='utf-8-sig')) if sf.is_file() else None
            states[name]={'rows':len(rows(data[name])),'local_hash':digest(data[name]),'sidecar_matches':valid_state(data[name],state)}
        except Exception as exc:
            data[name]=None;states[name]={'read':type(exc).__name__}
    hist=rows(data['godpick_records.json'])
    report={'scope':'LOCAL_ONLY_NOT_REMOTE_VERIFIED','files':states}
    for name in ('godpick_recommend_list.json','godpick_latest_recommendations.json'):
        plan=plan_dates(rows(data[name]),hist)
        report[name]={'counts':dict(Counter(r['status'] for r in plan))}
        if a.details:report[name]['rows']=plan
    print(json.dumps(report,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
