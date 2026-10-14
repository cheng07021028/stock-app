# -*- coding: utf-8 -*-
"""H120 offline official raw reconciliation and horizon comparison read-only CLI."""
from __future__ import annotations
from pathlib import Path
import argparse,csv,json,sys,hashlib
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h118_performance_authority import read_csv,load_actions,load_coverage,CSV_ACTION_COLUMNS,CSV_COVERAGE_COLUMNS
from godpick_h120_evidence_reconciliation import PROOF_COLUMNS,reconcile_coverage
from godpick_h120_strategy_validation import rolling_comparison

def main(argv=None):
    ap=argparse.ArgumentParser(description='H120 reconciliation and rolling 60/120-session validation; read-only')
    subs=ap.add_subparsers(dest='cmd',required=True)
    au=subs.add_parser('audit');au.add_argument('--raw',type=Path,default=ROOT/'data/h119/raw');au.add_argument('--proofs',type=Path,default=ROOT/'data/h120/reviewed/day_proofs.csv');au.add_argument('--actions',type=Path,default=ROOT/'data/h118/corporate_actions.csv');au.add_argument('--coverage',type=Path,default=ROOT/'data/h118/daily_coverage.csv')
    cmp=subs.add_parser('compare');cmp.add_argument('--details',type=Path,default=ROOT/'reports/h116/H116_T1_T3_T5_details.csv');cmp.add_argument('--benchmark',type=Path,default=ROOT/'data/h116/benchmark_ohlc.csv');cmp.add_argument('--baseline',default='H113_LEGACY');cmp.add_argument('--source-manifest',type=Path,default=ROOT/'reports/h116/H116_source_manifest.json');cmp.add_argument('--out',type=Path,default=ROOT/'reports/h120/H120_strategy_comparison.json')
    args=ap.parse_args(argv)
    if args.cmd=='audit':
        proofs=read_csv(args.proofs,PROOF_COLUMNS)
        report=reconcile_coverage(raw_root=args.raw,proofs=proofs,
               actions=load_actions(read_csv(args.actions,CSV_ACTION_COLUMNS)),
               coverage=load_coverage(read_csv(args.coverage,CSV_COVERAGE_COLUMNS)))
        # Full coverage records are not automatically accepted.
        safe={k:v for k,v in report.items() if k!='attested_coverage'}
        print(json.dumps(safe,ensure_ascii=False,indent=2))
        return 0 if report['rejected_days']==0 and report['accepted_days']>0 else 2
    if not args.details.exists() or not args.benchmark.exists():
        print(json.dumps({'status':'MISSING_VERIFIED_FILES','auto_apply':False},ensure_ascii=False))
        return 2
    if not args.source_manifest.exists():
        print(json.dumps({'status':'MISSING_STRICT_SOURCE_MANIFEST','auto_apply':False}));return 2
    provenance=json.loads(args.source_manifest.read_text(encoding='utf-8'))
    expected=provenance.get('h120_details_csv_sha256')
    actual=hashlib.sha256(args.details.read_bytes()).hexdigest()
    if provenance.get('h118_quality_policy')!='STRICT' or provenance.get('h120_policy')!='MANUAL_RAW_PROOF_ENFORCED' or not provenance.get('h120_proof_accepted_days') or expected!=actual:
        print(json.dumps({'status':'UNVERIFIED_OR_STALE_STRATEGY_DETAILS','auto_apply':False,'strict':provenance.get('h118_quality_policy'),'h120':provenance.get('h120_policy')},ensure_ascii=False))
        return 2
    if provenance.get('benchmark_input_sha256')!=hashlib.sha256(args.benchmark.read_bytes()).hexdigest():
        print(json.dumps({'status':'BENCHMARK_SOURCE_MISMATCH','auto_apply':False},ensure_ascii=False));return 2
    with args.details.open(encoding='utf-8-sig',newline='') as f:details=list(csv.DictReader(f))
    with args.benchmark.open(encoding='utf-8-sig',newline='') as f:bars=list(csv.DictReader(f))
    sessions=[x['date'] for x in bars if str(x.get('code') or '')=='BENCHMARK']
    report=rolling_comparison(details,market_sessions=sessions,baseline=args.baseline)
    args.out.parent.mkdir(parents=True,exist_ok=True)
    canonical=json.dumps(report,ensure_ascii=False,indent=2)+'\n'
    if not args.out.exists() or args.out.read_text(encoding='utf-8')!=canonical:
        args.out.write_text(canonical,encoding='utf-8')
    print(json.dumps({'status':report['status'],'comparison_rows':len(report.get('windows',[])),
                      'auto_apply':False,'out':str(args.out)},ensure_ascii=False))
    return 0
if __name__=='__main__':raise SystemExit(main())
