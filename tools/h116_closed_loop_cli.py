# -*- coding: utf-8 -*-
"""Read-only H116 strategy evaluator; outputs derived reports, not trades.

Example:
  py -3 tools/h116_closed_loop_cli.py review --prices verified_ohlc.csv \
       --benchmark benchmark.csv --out H116_report
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from godpick_h116_daily_feedback import load_snapshot_files,review_strategies,VERSION
from godpick_h118_performance_authority import (
    read_csv as read_quality_csv,load_actions,load_coverage,manifest as quality_manifest,
    CSV_ACTION_COLUMNS,CSV_COVERAGE_COLUMNS,
)


def csv_rows(path:Path):
    with path.open('r',newline='',encoding='utf-8-sig') as stream:
        reader=csv.DictReader(stream)
        if not reader.fieldnames:raise ValueError(f'空白行情檔：{path}')
        if not {'code','date','open','high','low','close'}.issubset(reader.fieldnames):
            raise ValueError(f'行情檔缺少 code,date,open,high,low,close 欄位：{path}')
        return list(reader)


def csv_write(path:Path,records:list[dict]):
    columns=list(dict.fromkeys(key for item in records for key in item.keys())) or ['status']
    with path.open('w',newline='',encoding='utf-8-sig') as stream:
        writer=csv.DictWriter(stream,fieldnames=columns)
        writer.writeheader();writer.writerows(records)


def main():
    ap=argparse.ArgumentParser(description='H116 immutable daily evidence closed loop; no live weight changes')
    sub=ap.add_subparsers(dest='command',required=True)
    insp=sub.add_parser('inspect',help='verify all decision snapshot SHA-256 and list coverage')
    rev=sub.add_parser('review',help='evaluate PIT snapshots against user-provided verified future OHLC')
    for p in (insp,rev):
        p.add_argument('--decisions',type=Path,default=ROOT/'data'/'h116'/'decisions')
    rev.add_argument('--prices',type=Path,required=True)
    rev.add_argument('--benchmark',type=Path)
    rev.add_argument('--out',type=Path,default=ROOT/'reports'/'h116')
    rev.add_argument('--buy-bps',type=float,default=14.25)
    rev.add_argument('--sell-bps',type=float,default=14.25)
    rev.add_argument('--tax-bps',type=float,default=30)
    rev.add_argument('--slippage-bps',type=float,default=5)
    rev.add_argument('--h118-strict',action='store_true',
        help='Require audited official company-action coverage; do not claim unaudited outcomes')
    rev.add_argument('--h118-actions',type=Path,default=ROOT/'data'/'h118'/'corporate_actions.csv')
    rev.add_argument('--h118-coverage',type=Path,default=ROOT/'data'/'h118'/'daily_coverage.csv')
    args=ap.parse_args()
    snaps=load_snapshot_files(args.decisions)
    if args.command=='inspect':
        print(json.dumps({'version':VERSION,'valid_decision_files':len(snaps),
                          'total_event_rows':sum(len(s['decisions']) for s in snaps),
                          'first':snaps[0]['decided_at'] if snaps else None,
                          'last':snaps[-1]['decided_at'] if snaps else None,
                          'durability':'VERIFIED_BYTES_NOT_PROOF_OF_EXTERNAL_BACKUP',
                          'status':'OK' if snaps else 'NO_SNAPSHOTS'},ensure_ascii=False,indent=2))
        return
    if not snaps:
        print('H116 NO_SNAPSHOTS：缺少真實當晚的不可變快照；拒絕偽造歷史績效')
        return
    acts=load_actions(read_quality_csv(args.h118_actions,CSV_ACTION_COLUMNS)) if args.h118_strict else []
    cov=load_coverage(read_quality_csv(args.h118_coverage,CSV_COVERAGE_COLUMNS)) if args.h118_strict else []
    check=quality_manifest(acts,cov)
    result=review_strategies(snaps,csv_rows(args.prices),benchmark=csv_rows(args.benchmark) if args.benchmark else None,
                             h118_policy='STRICT' if args.h118_strict else 'LEGACY',
                             h118_actions=acts,h118_coverage=cov,
                             buy_commission_bps=args.buy_bps,sell_commission_bps=args.sell_bps,
                             sell_tax_bps=args.tax_bps,slippage_bps=args.slippage_bps)
    out=args.out
    summary={k:v for k,v in result.items() if k!='details'}
    manifest={'version':VERSION,
              'price_input_sha256':hashlib.sha256(args.prices.read_bytes()).hexdigest(),
              'benchmark_input_sha256':hashlib.sha256(args.benchmark.read_bytes()).hexdigest() if args.benchmark else None,
              'snapshot_runs':[[s['run_id'],s['snapshot_sha256']] for s in snaps],
              'data_status':'INSUFFICIENT' if not result['summary'] else 'PARTIAL_REVIEW_ONLY',
              'h118_quality_policy':result['h118_quality_policy'],
              'h118_corporate_evidence_sha256':check['sha256'],
              'h118_action_rows':len(acts),'h118_coverage_rows':len(cov)}
    previous=out/'H116_source_manifest.json'
    if previous.exists():
        try:
            if json.loads(previous.read_text(encoding='utf-8'))==manifest:
                print(json.dumps({'status':'NO_NEW_INPUT','reason':'same immutable runs and exact price bytes',
                    'auto_apply':False},ensure_ascii=False))
                return
        except (ValueError,OSError):pass
    out.mkdir(parents=True,exist_ok=True)
    (out/'H116_strategy_summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    (out/'H116_source_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    csv_write(out/'H116_T1_T3_T5_details.csv',result['details'])
    csv_write(out/'H116_strategy_comparison.csv',result['summary'])
    csv_write(out/'H116_approval_required.csv',result['recommendations'])
    print(json.dumps({'status':'REVIEW_FINISHED_WITH_LIMITATIONS','snapshot_runs':len(snaps),
                      'input_events':result['source_events'],
                      'duplicates_removed':result['same_stock_day_strategy_duplicate_rows'],
                      'data_failures':result['data_failures'],
                      'comparison_groups':len(result['summary']),
                      'h118_quality_policy':result['h118_quality_policy'],
                      'auto_apply':False,'out':str(out)},ensure_ascii=False,indent=2))

if __name__=='__main__':main()
