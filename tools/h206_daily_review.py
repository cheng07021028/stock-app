#!/usr/bin/env python3
from pathlib import Path
import argparse,json,sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h206_market_gap import publish,workbook

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--out',type=Path,required=True);a=p.parse_args();b=publish(a.root);a.out.mkdir(parents=True,exist_ok=True);health={k:v for k,v in b.items() if k not in ('rows','h208')};recall=b.get('h208',{});health['h208']={k:v for k,v in recall.items() if k!='rows'};health['h208']['candidate_count']=len(recall.get('rows',[]));(a.out/'health.json').write_text(json.dumps(health,ensure_ascii=False,indent=2),'utf-8');(a.out/'H206_MarketGap.xlsx').write_bytes(workbook(b));print(b['status'],b['day'],len(b['rows']));return 0
if __name__=='__main__':raise SystemExit(main())
