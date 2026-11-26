#!/usr/bin/env python3
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h225_integrated_quality import run,excel

def main():
    a=argparse.ArgumentParser();a.add_argument('--root',type=Path,default=ROOT);a.add_argument('--out',type=Path,required=True);a=a.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    r=run(a.root);h={k:v for k,v in r.items() if k not in ('rows','shortlist','scope','detail','maturity','recall')};(a.out/'health.json').write_text(json.dumps(h,ensure_ascii=False,allow_nan=False,indent=2),'utf-8');(a.out/'H225_IntegratedQuality.xlsx').write_bytes(excel(r));print(json.dumps(h,ensure_ascii=False));return 0 if r['status']=='DATA_GATES_PASS' else 2
if __name__=='__main__':raise SystemExit(main())
