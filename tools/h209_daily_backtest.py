#!/usr/bin/env python3
from pathlib import Path
import argparse,json,sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h209_rolling_backtest import run,excel

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--out',type=Path,required=True);p.add_argument('--budget',type=int,default=24);a=p.parse_args()
    r=run(a.root,update=True,budget=a.budget);a.out.mkdir(parents=True,exist_ok=True)
    (a.out/'H209_Backtest.xlsx').write_bytes(excel(r));(a.out/'health.json').write_text(json.dumps({k:v for k,v in r.items() if k not in ('detail','cohorts','summary','folds')},ensure_ascii=False,allow_nan=False,indent=2),'utf-8')
    print(r['status'],r['capture'],r['signal_count'],r['valid_rows'])
    return 0
if __name__=='__main__':raise SystemExit(main())
