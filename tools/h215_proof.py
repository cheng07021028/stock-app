#!/usr/bin/env python3
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h215_proof import run,collect,excel

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--out',type=Path,required=True);p.add_argument('--collect',action='store_true');p.add_argument('--strict',action='store_true');a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    if a.collect:(a.out/'status_collection.json').write_text(json.dumps(collect(a.root),ensure_ascii=False,indent=2),'utf-8')
    r=run(a.root);(a.out/'health.json').write_text(json.dumps({k:v for k,v in r.items() if k!='status_detail'},ensure_ascii=False,allow_nan=False,indent=2),'utf-8');(a.out/'H215_Proof.xlsx').write_bytes(excel(r));print(json.dumps(r['gate'],ensure_ascii=False));return 2 if a.strict and r['gate']['daily_status']=='FAIL_DAILY' else 0
if __name__=='__main__':raise SystemExit(main())
