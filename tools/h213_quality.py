#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h213_quality_gate import collect,quality,excel,WORKFLOWS

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=('collect','report'));p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--budget',type=int,default=6);p.add_argument('--out',type=Path,default=Path('h213_output'));a=p.parse_args()
    if a.action=='collect':
        r=collect(a.root,budget=a.budget);print(json.dumps({k:v for k,v in r.items() if k!='attempts'},ensure_ascii=False));return 2 if any(x['status']=='ERROR' for x in r['tried']) else 0
    r=quality(a.root);a.out.mkdir(parents=True,exist_ok=True);(a.out/'H213_Quality.xlsx').write_bytes(excel(r));(a.out/'quality.json').write_text(json.dumps(r,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8');print(json.dumps({k:r[k] for k in ('asof','session','status','passed','total','coverage','counts','recall','repair_plan','errors')},ensure_ascii=False));return 0
if __name__=='__main__':raise SystemExit(main())
