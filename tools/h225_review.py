#!/usr/bin/env python3
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h225_integrated_quality import run,excel

def main():
    a=argparse.ArgumentParser();a.add_argument('--root',type=Path,default=ROOT);a.add_argument('--out',type=Path,required=True);a=a.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    r=run(a.root,include_detail=True);h={k:v for k,v in r.items() if k not in ('rows','shortlist','scope','detail','maturity','recall','first_errors','first_detail','first','miss','flows','action_rows','recovery','financial_health','first_signals')};(a.out/'health.json').write_text(json.dumps(h,ensure_ascii=False,allow_nan=False,indent=2),'utf-8');(a.out/'H225_IntegratedQuality.xlsx').write_bytes(excel(r))
    from godpick_h226_live_acceptance import run as acceptance,excel as acceptance_excel
    q=acceptance(a.root,r['asof'],base=r);d=a.out/'h226';d.mkdir(exist_ok=True)
    (d/'H226_LiveAcceptance.xlsx').write_bytes(acceptance_excel(q));(d/'health.json').write_text(json.dumps({k:v for k,v in q.items() if k not in ('tasks','stock_gaps','recall','miss','shortlist','batch')},ensure_ascii=False,allow_nan=False,indent=2),'utf-8')
    print(json.dumps(h,ensure_ascii=False));return 0 if r['status']=='DATA_GATES_PASS' and q['status']=='DATA_GATES_PASS' else 2
if __name__=='__main__':raise SystemExit(main())
