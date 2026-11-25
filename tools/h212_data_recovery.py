#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h212_data_recovery import repair,run,excel

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=('repair','report'));p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--budget',type=int,default=8);p.add_argument('--out',type=Path,default=Path('h212_output'));a=p.parse_args()
    if a.action=='repair':
        r=repair(a.root,budget=a.budget);print(json.dumps({k:v for k,v in r.items() if k!='attempts'},ensure_ascii=False));return 2 if r['archive_errors'] else 0
    r=run(a.root);a.out.mkdir(parents=True,exist_ok=True);(a.out/'H212_DataRecovery.xlsx').write_bytes(excel(r));summary={k:r[k] for k in ('version','asof','status','counts','capture','intersection_summary','outcome_summary','leader_coverage','prior_research')};summary['coverage']=r['gap']['coverage'];summary['gap_summary']=r['gap']['gap_summary'];summary['archive_errors']=r['gap']['archive_errors'];(a.out/'H212_health.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2,allow_nan=False),'utf-8');print(json.dumps(summary,ensure_ascii=False));return 0
if __name__=='__main__':raise SystemExit(main())
