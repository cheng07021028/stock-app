#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h222_financial_history import collect,inspect,excel

def main():
    p=argparse.ArgumentParser(description='H222 historical financial evidence');p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--collect',action='store_true');p.add_argument('--months',type=int,default=13);p.add_argument('--quarters',type=int,default=8);p.add_argument('--budget',type=int,default=8);p.add_argument('--out',type=Path);p.add_argument('--company-codes',help='Optional individually verified historical supplement, comma-separated current stock codes (1..20)');p.add_argument('--auto-company',action='store_true',help='H233 bounded bulk + fair/growth individual supplement');p.add_argument('--batch-archive',type=Path,action='append',help='H234 local operator-downloaded MOPS ZIP (repeat up to 12)');p.add_argument('--batch-dir',type=Path,help='Directory of original operator-downloaded MOPS ZIPs (max 12)');a=p.parse_args()
    if a.batch_dir:
        if not a.batch_dir.is_dir():p.error('--batch-dir must exist')
        a.batch_archive=(a.batch_archive or [])+sorted(a.batch_dir.glob('*.zip'))
        if not a.batch_archive:p.error('no ZIP files found in --batch-dir')
    if a.batch_archive:
        if a.collect or a.company_codes or a.auto_company or not a.out:p.error('--batch-archive requires --out and excludes collection modes')
        from godpick_h234_batch_financial import review,excel as batch_excel
        r=review(a.root,a.batch_archive);a.out.mkdir(parents=True,exist_ok=True)
        (a.out/'H234_BatchFinancialReview.xlsx').write_bytes(batch_excel(r))
        print(json.dumps({k:v for k,v in r.items() if k not in ('rows','profiles','candidates')},ensure_ascii=False,indent=2));return 0 if r['rows'] and not any(x.get('狀態')=='ARCHIVE_REJECTED' for x in r['log']) else 2
    if a.auto_company:
        if not a.collect or a.company_codes:p.error('--auto-company requires --collect and excludes --company-codes')
        from godpick_h233_auto_history import run
        r=run(a.root,months=a.months,quarters=a.quarters,budget=a.budget)
        print(json.dumps(r,ensure_ascii=False,indent=2));return 0 if r['bulk_status']=='VERIFIED_SCOPE' and not r['errors'] else 2
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
