# -*- coding: utf-8 -*-
"""Read-only H123 strategy evidence quality audit with H120 provenance contract."""
from __future__ import annotations
import argparse,csv,io,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h123_strategy_attribution import review_quality
from godpick_h116_daily_feedback import load_snapshot_files


def main(argv=None):
    p=argparse.ArgumentParser();p.add_argument('--decisions',type=Path,default=ROOT/'data/h116/decisions')
    p.add_argument('--details',type=Path,default=ROOT/'reports/h116/H116_T1_T3_T5_details.csv')
    p.add_argument('--manifest',type=Path,default=ROOT/'reports/h116/H116_source_manifest.json')
    p.add_argument('--out',type=Path,default=ROOT/'reports/h123/H123_quality_attribution.json')
    args=p.parse_args(argv)
    snaps=load_snapshot_files(args.decisions)
    raw=args.details.read_bytes() if args.details.exists() else b''
    mani=json.loads(args.manifest.read_text(encoding='utf-8')) if args.manifest.exists() else {}
    details=list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig')))) if raw else []
    report=review_quality(snapshots=snaps,details=details,source_manifest=mani,details_bytes=raw)
    args.out.parent.mkdir(parents=True,exist_ok=True)
    value=json.dumps(report,ensure_ascii=False,indent=2,sort_keys=True)+'\n'
    if not args.out.exists() or args.out.read_text(encoding='utf-8')!=value:args.out.write_text(value,encoding='utf-8')
    print(value)
    return 0 if report['status'] in ('EVIDENCE_COHORTS_RESEARCH_ONLY','NO_VERIFIED_MATCHED_PAIRS') else 2

if __name__=='__main__':raise SystemExit(main())
