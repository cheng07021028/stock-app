# -*- coding: utf-8 -*-
"""CLI for H115 imported evidence and honest next-session walk-forward.

Run from stock-app root: py -3 tools/h115_pit_cli.py help
Requires manually supplied verified historic price + evidence snapshots.
"""
from __future__ import annotations
import argparse
import csv
import json
import sys
from pathlib import Path
from datetime import datetime

ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from godpick_h115_pit_evidence import (append_revenue,append_valuations, build_h114_evidence,
                                         evidence_digest, TZ_TW, _period)
from godpick_h115_walkforward import evaluate_snapshots


def rows(path: Path):
    with Path(path).open("r", encoding="utf-8-sig", newline="") as fp:
        reader=csv.DictReader(fp)
        if not reader.fieldnames:raise ValueError("CSV缺少標題列")
        return list(reader)


def write_csv(p:Path, payload:list[dict]):
    p.parent.mkdir(parents=True,exist_ok=True)
    fields=list(dict.fromkeys(k for row in payload for k in row.keys()))
    with p.open("w",encoding="utf-8-sig",newline="") as fp:
        writer=csv.DictWriter(fp,fieldnames=fields or ["status"])
        writer.writeheader();writer.writerows(payload)


def from_twse_latest(raw: list[dict], *, observed_at: str) -> list[dict]:
    """TWSE current-month endpoint is only a first-observed snapshot.

    Important: 出表日期 is NOT presumed equal to individual announcement time.
    """
    out=[]
    for r in raw:
        c=str(r.get("公司代號","")).strip()
        ym=_period(r.get("資料年月"))
        amount=r.get("營業收入-當月營收")
        try: value=int(round(float(str(amount).replace(",",""))*1000))
        except (ValueError,TypeError): continue
        if not c or not ym or value<0:continue
        out.append({"code":c,"year_month":ym,"revenue_twd":value,
                    "available_at":observed_at,"source":"TWSE",
                    "source_url":"https://openapi.twse.com.tw/v1/opendata/t187ap05_L",
                    "availability_basis":"FIRST_OBSERVED"})
    return out


def main():
    ap=argparse.ArgumentParser(description="H115 point-in-time research CLI (never trades)")
    sub=ap.add_subparsers(dest="command",required=True)
    for command in ("import-revenue","import-valuation"):
        p=sub.add_parser(command,help="Import normalized verified CSV into an append-only-versioned JSONL ledger")
        p.add_argument("--csv",type=Path,required=True)
        p.add_argument("--store",type=Path,default=ROOT/"data"/"h115")
    p=sub.add_parser("fetch-twse-current",help="Explicit opt-in: fetch latest TWSE revenue snapshot; never use to backfill past")
    p.add_argument("--store",type=Path,default=ROOT/"data"/"h115")
    p=sub.add_parser("inspect",help="Read-only evidence statistics and hashes")
    p.add_argument("--root",type=Path,default=ROOT)
    p=sub.add_parser("asof",help="Read-only point-in-time H114 stock evidence without rewriting runtime data")
    p.add_argument("--root",type=Path,default=ROOT)
    p.add_argument("--cutoff",required=True,help="Example: 2026-10-14T22:00:00+08:00")
    p.add_argument("--code",required=True)
    p=sub.add_parser("backtest",help="Evaluate immutable snapshots; does not create/rewrite recommendation records")
    p.add_argument("--signals",type=Path,required=True,help="decision-time CSV snapshots: code,decided_at,latest_evidence_at,strategy,tier")
    p.add_argument("--prices",type=Path,required=True,help="code,date,open,high,low,close")
    p.add_argument("--benchmark",type=Path,help="optional code=BENCHMARK daily OHLC CSV")
    p.add_argument("--output-dir",type=Path,required=True)
    p.add_argument("--buy-bps",type=float,default=14.25)
    p.add_argument("--sell-bps",type=float,default=14.25)
    p.add_argument("--sell-tax-bps",type=float,default=30.0)
    p.add_argument("--slippage-bps",type=float,default=5.0)
    args=ap.parse_args()
    if args.command=="import-revenue":
        result=append_revenue(args.store, rows(args.csv))
        print(json.dumps(result,ensure_ascii=False,indent=2))
        print("持久保存警告：若 Streamlit Cloud 使用暫存磁碟，請提交 data/h115/*.jsonl 到私有Git或使用持久儲存；本機寫入不等於重啟永久保存。")
    elif args.command=="import-valuation":
        result=append_valuations(args.store, rows(args.csv))
        print(json.dumps(result,ensure_ascii=False,indent=2))
        print("估值中位數需同交易日/同產業至少5家同業，否則不啟用低估分。")
    elif args.command=="fetch-twse-current":
        from urllib.request import Request, urlopen
        url="https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
        request=Request(url,headers={"User-Agent":"SPT-H115-Evidence/1.0"})
        with urlopen(request, timeout=30) as response:
            raw=json.loads(response.read().decode("utf-8-sig"))
        if not isinstance(raw,list):raise ValueError("官方端點回傳非 list，停止寫入")
        observed=datetime.now(TZ_TW).isoformat(timespec="seconds")
        snap=from_twse_latest(raw,observed_at=observed)
        if not snap:raise ValueError("官方回傳無可轉換營收資料；不修改儲存")
        print(json.dumps({"observed_at":observed,"source_rows":len(raw),"mapped_rows":len(snap),
                          "result":append_revenue(args.store,snap)},ensure_ascii=False,indent=2))
        print("本端點只有當期；該資料只從本次觀察時間開始可用，不能回填為歷史已知營收。")
    elif args.command=="inspect":print(json.dumps(evidence_digest(args.root),ensure_ascii=False,indent=2))
    elif args.command=="asof":
        evidence=build_h114_evidence(args.root,args.cutoff)
        print(json.dumps(evidence.get(args.code,{"status":"NO_VERIFIED_ASOF_EVIDENCE"}),ensure_ascii=False,indent=2))
    elif args.command=="backtest":
        result=evaluate_snapshots(rows(args.signals),rows(args.prices),
            benchmark=rows(args.benchmark) if args.benchmark else None,
            buy_commission_bps=args.buy_bps,sell_commission_bps=args.sell_bps,
            sell_tax_bps=args.sell_tax_bps,slippage_bps=args.slippage_bps)
        args.output_dir.mkdir(parents=True,exist_ok=True)
        (args.output_dir/"H115_backtest_summary.json").write_text(json.dumps({k:v for k,v in result.items() if k!="details"},ensure_ascii=False,indent=2),encoding="utf-8")
        write_csv(args.output_dir/"H115_backtest_details.csv",result["details"])
        write_csv(args.output_dir/"H115_strategy_comparison.csv",result["summary"])
        print(json.dumps({"status":"DONE_WITH_DATA_LIMITATIONS","verified_events":result["verified_events"],
                          "failure_counts":result["failures"],"group_count":len(result["summary"]),
                          "output_dir":str(args.output_dir)},ensure_ascii=False,indent=2))

if __name__=="__main__":main()
