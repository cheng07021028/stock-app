#!/usr/bin/env python3
from pathlib import Path
import argparse,json,sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h210_strategy_competition import run,excel

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
    r=run(a.root);a.out.mkdir(parents=True,exist_ok=True)
    (a.out/'H210_StrategyCompetition.xlsx').write_bytes(excel(r))
    (a.out/'h210_health.json').write_text(json.dumps({k:v for k,v in r.items() if k not in ('audit','scores','folds','recommendations','models')},ensure_ascii=False,allow_nan=False,indent=2),'utf-8')
    print(r['status'],r['review_count'],r['upstream']);return 0
if __name__=='__main__':raise SystemExit(main())
