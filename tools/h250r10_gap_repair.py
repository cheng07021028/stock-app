# -*- coding: utf-8 -*-
"""R10 targeted H190/H212 official additive quote recovery. Dry-run default.
No recommendation file reads/writes, no remote publication. Existing archive
files are immutable. Apply only to a local complete project after review.
"""
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import argparse,csv,json,re,sys,time
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
import godpick_h190_market_archive as ar


def targets(items,a):
    out=defaultdict(set)
    for x in items:
        m,d,c=(str(x.get(k,'')).strip() for k in ('market','day','code'))
        if m not in ar.URLS or not re.fullmatch(r'\d{4,6}',c) or d not in a.cal.dayset:continue
        if d in a.bars.get((m,c),{}):continue
        out[m,d].add(c)
    return sorted(((m,d,sorted(cs)) for (m,d),cs in out.items()),key=lambda x:(-len(x[2]),x[0],x[1]))


def recover(items,root=ROOT,*,apply=False,budget=8,http=None,now=None,pause=1.2):
    from godpick_h250r7_tracking import load_archive
    if not 1<=budget<=12:raise ValueError('budget outside 1..12')
    root=Path(root);p=root/'data/h190';a=load_archive(root,now);queue=targets(items,a);result=[];count=0
    if apply and http is None:
        import requests
        http=requests.Session()
    for m,d,codes in queue:
        row={'market':m,'day':d,'codes':codes,'status':'QUEUED'};result.append(row)
        if d not in a.markets.get(m,{}):row['status']='H190_PARTITION_MISSING';continue
        if ar.supplemental_path(p,m,d).exists():row['status']='EXISTING_SUPPLEMENT_REVIEW';continue
        if not apply:continue
        if count>=budget:row['status']='BUDGET_DEFERRED';continue
        count+=1
        try:
            params={'date':d.replace('-',''),'type':'ALLBUT0999','response':'json'} if m=='TWSE' else {'date':d.replace('-','/'),'type':'EW','response':'json'}
            raw=ar._get(http,ar.URLS[m],params,12)
            obs=(now or datetime.now(ar.TZ)).isoformat()
            n=ar.save_supplement(p,m,d,raw,obs)
            verified=ar.quote_rows(p,m,d,datetime.fromisoformat(obs))
            found=[c for c in codes if c in verified]
            row.update(status='TARGET_QUOTES_VERIFIED' if found else 'NO_TARGET_QUOTES_CONFIRMED',added=n,verified_codes=found,remaining_codes=[c for c in codes if c not in verified])
        except Exception as exc:row.update(status='SOURCE_OR_ARCHIVE_CONFLICT',error_type=type(exc).__name__)
        if pause:time.sleep(pause)
    return {'mode':'LOCAL_ADD_ONLY' if apply else 'DRY_RUN','requests':count,'targets':result,'remote_publication':False,'recommendation_data_changed':False}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--queue',required=True,type=Path);p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--budget',type=int,default=8);p.add_argument('--apply-local',action='store_true');a=p.parse_args(argv)
    with a.queue.open(encoding='utf-8-sig',newline='') as f:items=list(csv.DictReader(f))
    print(json.dumps(recover(items,a.root,apply=a.apply_local,budget=a.budget),ensure_ascii=False,indent=2));return 0

if __name__=='__main__':raise SystemExit(main())
