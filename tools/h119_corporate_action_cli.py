# -*- coding: utf-8 -*-
"""H119 source capture and audited corporate-action diagnostics; no trading."""
from __future__ import annotations
from pathlib import Path
import argparse,json,sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h119_official_source import fetch_official_sources
from godpick_h119_total_return import load_adjustments,ADJUSTMENT_COLUMNS,fingerprint
from godpick_h118_performance_authority import read_csv,load_actions,load_coverage,CSV_ACTION_COLUMNS,CSV_COVERAGE_COLUMNS

def main(argv=None):
    ap=argparse.ArgumentParser(description='H119 TWSE+TPEx raw corporate action evidence capture; audit required')
    sub=ap.add_subparsers(dest='command',required=True)
    sub.add_parser('collect');sub.add_parser('inspect')
    ap.add_argument('--raw',type=Path,default=ROOT/'data'/'h119'/'raw')
    ap.add_argument('--adjustments',type=Path,default=ROOT/'data'/'h119'/'reviewed'/'official_adjustments.csv')
    ap.add_argument('--actions',type=Path,default=ROOT/'data'/'h118'/'corporate_actions.csv')
    ap.add_argument('--coverage',type=Path,default=ROOT/'data'/'h118'/'daily_coverage.csv')
    a=ap.parse_args(argv)
    if a.command=='collect':
        print(json.dumps(fetch_official_sources(a.raw),ensure_ascii=False,indent=2));return 0
    numeric=load_adjustments(read_csv(a.adjustments,ADJUSTMENT_COLUMNS))
    actions=load_actions(read_csv(a.actions,CSV_ACTION_COLUMNS))
    coverage=load_coverage(read_csv(a.coverage,CSV_COVERAGE_COLUMNS))
    report={'status':'READY_FOR_H119_RESEARCH_EVALUATION' if coverage else 'UNVERIFIED_NO_COVERAGE',
            'raw_captures':len(list(a.raw.glob('*/*.json.gz'))) if a.raw.exists() else 0,
            'numeric_reviewed_events':len(numeric),'official_action_rows':len(actions),'complete_market_days':len(coverage),
            'adjustments_sha256':fingerprint(numeric),'no_automatic_promotion':True,
            'note':'只有H118每日完整核對＋事件目錄＋H119股利數值互相一致才能評估；未完成仍留隔離'}
    print(json.dumps(report,ensure_ascii=False,indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
