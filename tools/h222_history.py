#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h222_financial_history import collect,inspect,excel

def main():
    p=argparse.ArgumentParser(description='H222 historical financial evidence');p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--collect',action='store_true');p.add_argument('--months',type=int,default=13);p.add_argument('--quarters',type=int,default=8);p.add_argument('--budget',type=int,default=8);p.add_argument('--out',type=Path);p.add_argument('--company-codes',help='Optional individually verified historical supplement, comma-separated current stock codes (1..20)');a=p.parse_args()
    if a.company_codes:
        if not a.collect:p.error('--company-codes requires --collect')
        from godpick_h231_company_history import collect as company_collect
        r=company_collect(a.root,codes=[c.strip() for c in a.company_codes.split(',') if c.strip()],quarters=a.quarters,budget=a.budget)
        print(json.dumps(r,ensure_ascii=False,indent=2));return 2 if r['status']!='COMPANY_SCOPE_VERIFIED' else 0
    r=collect(a.root,months=a.months,quarters=a.quarters,budget=a.budget) if a.collect else inspect(a.root)
    if a.out and not a.collect:
        a.out.mkdir(parents=True,exist_ok=True);(a.out/'H222_FinancialHistory.xlsx').write_bytes(excel(r))
    print(json.dumps(r if a.collect else {k:v for k,v in r.items() if k!='stocks'},ensure_ascii=False,indent=2));return 2 if a.collect and r['status']!='VERIFIED_SCOPE' else 0
if __name__=='__main__':raise SystemExit(main())
