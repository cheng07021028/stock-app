# -*- coding: utf-8 -*-
"""H115.3 T+1/T+3/T+5 next-session-open walk-forward evaluator.

Input is an IMMUTABLE decision-time snapshot (not an ex-post reconstructed
ranking). No internet, no on-demand factor updates, no trading authorisation.
All returns are after user-specified transaction costs, not promises.
"""
from __future__ import annotations
from datetime import date, datetime
from collections import defaultdict
from typing import Any, Iterable, Mapping
from statistics import mean, median
import math
from godpick_h115_pit_evidence import _datetime, _num, _code, VERSION as PIT_VERSION

VERSION = "v191_h115_walkforward_t1_t3_t5_20261014"
HORIZONS = (1, 3, 5)


def _datestr(x: Any) -> str:
    dt = _datetime(x)
    return dt.date().isoformat() if dt else ""


def _to_price(raw: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buf=defaultdict(dict)
    for x in raw:
        code = _code(x.get("code") or x.get("股票代號"))
        day = _datestr(x.get("date") or x.get("日期"))
        o = _num(x.get("open") if "open" in x else x.get("開盤價"))
        h = _num(x.get("high") if "high" in x else x.get("最高價"))
        l = _num(x.get("low") if "low" in x else x.get("最低價"))
        c = _num(x.get("close") if "close" in x else x.get("收盤價"))
        if not code or not day or any(v is None or v<=0 for v in (o,h,l,c)) or h < max(o,l,c) or l > min(o,h,c):
            continue
        if day in buf[code]:raise ValueError(f"重複行情日：{code} {day}，拒絕自動覆蓋")
        buf[code][day] = {"date":day,"open":o,"high":h,"low":l,"close":c,"exchange":str(x.get("exchange") or "").upper()}
    return {k:sorted(v.values(),key=lambda x:x["date"]) for k,v in buf.items()}


def _cost_rate(buy_commission_bps: float, sell_commission_bps: float, sell_tax_bps:float,slippage_bps:float) -> float:
    if min(buy_commission_bps,sell_commission_bps,sell_tax_bps,slippage_bps)<0:raise ValueError("交易成本不可為負")
    if max(buy_commission_bps,sell_commission_bps,sell_tax_bps,slippage_bps)>1000:raise ValueError("交易成本不合理")
    return 0.0


def evaluate_snapshots(
    snapshots: Iterable[Mapping[str,Any]],
    prices: Iterable[Mapping[str,Any]],
    *, benchmark: Iterable[Mapping[str,Any]] | None = None,
    buy_commission_bps: float = 14.25,
    sell_commission_bps: float = 14.25,
    sell_tax_bps: float = 30.0,
    slippage_bps: float = 5.0,
    min_confident_samples: int = 30,
    h118_policy: str = "LEGACY",
    h118_actions: Iterable[Mapping[str, Any]] | None = None,
    h118_coverage: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str,Any]:
    """Buy next trading session OPEN and exit on horizon's CLOSE.

    MFE/MAE use high/low during holding period, independent of intraday order.
    Bench comparator enters on SAME DATE open, exits on SAME DATE close.
    Missing future candles => UNEVALUABLE, never fabricated return 0.
    """
    _cost_rate(buy_commission_bps,sell_commission_bps,sell_tax_bps,slippage_bps)
    if h118_policy not in ("LEGACY", "STRICT"):
        raise ValueError("H118風險政策不合法")
    guard=None
    if h118_policy=="STRICT":
        from godpick_h118_performance_authority import assess_window, load_actions, load_coverage
        action_rows=load_actions(h118_actions or [])
        coverage_rows=load_coverage(h118_coverage or [])
        guard=assess_window
    pb = _to_price(prices)
    benchmark_prices=_to_price(benchmark or [])
    bm = benchmark_prices.get("BENCHMARK", [])
    bm_by_day={x["date"]:x for x in bm}
    rows=[]; seen=set()
    for s in snapshots:
        code=_code(s.get("code") or s.get("股票代號"))
        decision=_datetime(s.get("decided_at") or s.get("推薦時間"))
        strategy=str(s.get("strategy") or s.get("策略") or "UNKNOWN").strip()
        tier=str(s.get("tier") or s.get("候選身分") or "UNKNOWN").strip()
        event_id=str(s.get("event_id") or f"{code}:{decision}:{strategy}:{tier}")
        if event_id in seen:raise ValueError(f"重複事件ID，拒絕重複計績效：{event_id}")
        seen.add(event_id)
        base={"event_id":event_id,"code":code,"strategy":strategy,"tier":tier,"decision_date":decision.date().isoformat() if decision else "",
              "data_status":"","entry_date":"","exit_date":"","horizon":None,"gross_return_pct":None,"net_return_pct":None,
              "benchmark_net_pct":None,"alpha_pp":None,"mfe_pct":None,"mae_pct":None,"score":_num(s.get("score") or s.get("H114研究優先分"))}
        evidence=_datetime(s.get("latest_evidence_at") or s.get("最晚證據可用時間"))
        # Strictly require proof of the issuance cutoff when assessing
        # a NEW walk-forward claim; legacy snapshots are marked as unverified.
        if not (code and decision and evidence) or evidence > decision:
            rows.append(dict(base,data_status="UNVERIFIED_ASOF_SNAPSHOT"));continue
        hist=pb.get(code,[])
        if not hist:
            rows.append(dict(base,data_status="MISSING_PRICE_HISTORY"));continue
        nexts=[x for x in hist if x["date"]>decision.date().isoformat()]
        for n in HORIZONS:
            r=dict(base,horizon=n)
            if len(nexts)<n:
                r["data_status"]="MISSING_FUTURE_BARS"
                rows.append(r);continue
            # Do not turn halted ticker's next recorded candle weeks later into T+1.
            first=nexts[0];last=nexts[n-1]
            if (date.fromisoformat(last["date"])-date.fromisoformat(first["date"])).days > 3*n + 4:
                r["data_status"]="SUSPENSION_OR_STALE_PRICE_GAP"
                rows.append(r);continue
            # If benchmark calendar is supplied, T+n must mean market T+n,
            # not the Nth observed ticker bar after one or more suspended days.
            if bm:
                bdays=[x["date"] for x in bm if x["date"]>decision.date().isoformat()]
                if len(bdays)<n or nexts[n-1]["date"] != bdays[n-1]:
                    r["data_status"]="MISSING_MARKET_SESSION_OR_HALTED_STOCK"
                    rows.append(r);continue
            if (date.fromisoformat(first["date"])-decision.date()).days>8:
                r["data_status"]="LATE_NEXT_TRADE"
                rows.append(r);continue
            if guard is not None:
                # Fail-closed: do not evaluate windows touching confirmed company
                # actions, large unexplained gaps, or unaudited action calendars.
                prior=[x for x in hist if x["date"]<first["date"]]
                prior_close=prior[-1]["close"] if prior else None
                if bm:
                    market_window=[x["date"] for x in bm if first["date"]<=x["date"]<=last["date"]]
                else:
                    market_window=[]
                quality=guard(code=code,
                    market=next((x.get("exchange") or "" for x in nexts[:n] if x.get("exchange")),""),
                    entry_day=first["date"],exit_day=last["date"],market_days=market_window,
                    stock_bars=nexts[:n],previous_close=prior_close,
                    action_events=action_rows,coverage=coverage_rows)
                r["h118_quality_status"]=quality["status"]
                r["h118_quality_reason"]=quality["reason"]
                if quality["status"]!="CERTIFIED_UNADJUSTED_PRICE_WINDOW":
                    r["data_status"]=quality["status"]
                    rows.append(r)
                    continue
            entry_price=first["open"]*(1+(slippage_bps+buy_commission_bps)/10000)
            exit_price=last["close"]*(1-(slippage_bps+sell_commission_bps+sell_tax_bps)/10000)
            gross=(last["close"]/first["open"]-1)*100
            net=(exit_price/entry_price-1)*100
            r.update(data_status="OK",entry_date=first["date"],exit_date=last["date"],
                     gross_return_pct=round(gross,4),net_return_pct=round(net,4),
                     mfe_pct=round((max(x["high"] for x in nexts[:n])/first["open"]-1)*100,4),
                     mae_pct=round((min(x["low"] for x in nexts[:n])/first["open"]-1)*100,4))
            if bm:
                bi=bm_by_day.get(first["date"]);bx=bm_by_day.get(last["date"])
                if bi and bx:
                    bentry=bi["open"]*(1+(slippage_bps+buy_commission_bps)/10000)
                    bexit=bx["close"]*(1-(slippage_bps+sell_commission_bps+sell_tax_bps)/10000)
                    bm_net=(bexit/bentry-1)*100
                    r["benchmark_net_pct"]=round(bm_net,4)
                    r["alpha_pp"]=round(net-bm_net,4)
            rows.append(r)
    summary=[]
    groups=defaultdict(list)
    for r in rows:
        if r["data_status"]=="OK":groups[(r["strategy"],r["tier"],r["horizon"])].append(r)
    for (strat,tier,h),rr in sorted(groups.items()):
        v=[x["net_return_pct"] for x in rr]
        alpha=[x["alpha_pp"] for x in rr if x["alpha_pp"] is not None]
        summary.append({"strategy":strat,"tier":tier,"horizon":h,"n":len(v),
                        "confidence":"EXPLORATORY_ONLY" if len(v)<min_confident_samples else "SAMPLE_SUFFICIENT_FOR_FURTHER_VALIDATION",
                        "win_rate_pct":round(100*sum(x>0 for x in v)/len(v),2),
                        "mean_net_pct":round(mean(v),4),"median_net_pct":round(median(v),4),
                        "mean_alpha_pp":round(mean(alpha),4) if alpha else None,
                        "alpha_n":len(alpha),"mean_mae_pct":round(mean(x["mae_pct"] for x in rr),4),
                        "mean_mfe_pct":round(mean(x["mfe_pct"] for x in rr),4)})
    failures=defaultdict(int)
    for r in rows:
        if r["data_status"]!="OK":failures[r["data_status"]]+=1
    return {"version":VERSION,"price_basis":"NEXT_SESSION_OPEN_TO_Tn_CLOSE",
            "cost_bps":{"buy_commission":buy_commission_bps,"sell_commission":sell_commission_bps,
                        "sell_tax":sell_tax_bps,"slippage_each_side":slippage_bps},
            "data_policy":"PIT_IMMUTABLE_SNAPSHOTS_ONLY_NO_FUTURE_DATA",
            "h118_policy":h118_policy,
            "details":rows,"summary":summary,"failures":dict(failures),"verified_events":len({r['event_id'] for r in rows if r['data_status']=='OK'}),
            "disclaimer":"策略樣本績效非因果證明／非獲利保證；需要樣本外滾動評估及資料權威核查"}
