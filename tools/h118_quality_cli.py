# -*- coding: utf-8 -*-
"""H118 read-only source / company-action audit and EOD archive inspection.

Never generates or certifies missing official corporate-action records.
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from godpick_h117_official_market_closure import load_official_days
from godpick_h118_performance_authority import (
    load_actions,load_coverage,read_csv,manifest,
    CSV_ACTION_COLUMNS,CSV_COVERAGE_COLUMNS,
)


def summarize(market_days, actions, coverage):
    common=set(market_days.get('TWSE',{}))&set(market_days.get('TPEX',{}))&set(market_days.get('TAIEX',{}))
    known={(r['market'],r['date']) for r in coverage}
    needs={m:sorted(d for d in common if (m,d) not in known) for m in ('TWSE','TPEX')}
    return {
        'version':'V191-H118.1','immutable_official_price_days':{k:len(v) for k,v in market_days.items()},
        'three_source_complete_days':len(common),'first_complete_day':min(common) if common else None,
        'last_complete_day':max(common) if common else None,
        'verified_company_action_records':len(actions),'verified_daily_coverage_records':len(coverage),
        'missing_company_action_coverage_count':{k:len(v) for k,v in needs.items()},
        'first_missing_company_action_day':{k:(v[0] if v else None) for k,v in needs.items()},
        'company_action_evidence_sha256':manifest(actions,coverage)['sha256'],
        'status':'READY_FOR_PER_STOCK_STRICT_REVIEW' if common and all(not x for x in needs.values()) else 'ACTION_COVERAGE_INCOMPLETE',
        'disclaimer':'紀錄僅驗證格式與附來源；官方公告是否覆蓋完整市場需由資料管理流程核查，不可自動證明',
        'production_weights_changed':False,
    }


def main(argv=None):
    p=argparse.ArgumentParser(description='H118 market-source and corporate action audit (read only)')
    p.add_argument('--daily',type=Path,default=ROOT/'data'/'h117'/'daily')
    p.add_argument('--actions',type=Path,default=ROOT/'data'/'h118'/'corporate_actions.csv')
    p.add_argument('--coverage',type=Path,default=ROOT/'data'/'h118'/'daily_coverage.csv')
    args=p.parse_args(argv)
    d=load_official_days(args.daily)
    a=load_actions(read_csv(args.actions,CSV_ACTION_COLUMNS))
    c=load_coverage(read_csv(args.coverage,CSV_COVERAGE_COLUMNS))
    print(json.dumps(summarize(d,a,c),ensure_ascii=False,indent=2))
    return 0

if __name__=='__main__':raise SystemExit(main())
