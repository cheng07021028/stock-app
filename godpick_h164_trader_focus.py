# -*- coding: utf-8 -*-
"""V191-H164 | Evidence-only trader cockpit extension to H157 workbook.

Original 14 tabs and every source row remain untouched. H164 appends a concise
study-queue to the existing Sheet 11, not a new rank or an official buy signal.
No network, no future values, no invented PE/EPS/technical signals.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any, Mapping

VERSION = "V191-H164-TRADER-POOL-REVIEW"


def _text(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _int(x: Any, fallback: int = 9999) -> int:
    try:
        return int(float(str(x).strip()))
    except (ValueError, TypeError, OverflowError):
        return fallback


def _date(x: Any) -> str:
    if isinstance(x, datetime):
        return x.date().isoformat()
    if isinstance(x, date):
        return x.isoformat()
    v = _text(x)
    try:
        return date.fromisoformat(v[:10]).isoformat()
    except ValueError:
        return ""


def next_workday(research_day: Any) -> str:
    """Only weekend-aware, NOT exchange-holiday/calendar verification."""
    d = _date(research_day)
    if not d:
        return "未驗證"
    v = date.fromisoformat(d) + timedelta(days=1)
    while v.weekday() >= 5:
        v += timedelta(days=1)
    return v.isoformat()


def _classify(record: Mapping) -> tuple[str, str, str, str]:
    """Return (lane, evidence, risk, scenario) without inventing verification."""
    source = _text(record.get("來源股票池"))
    qualified = _text(record.get("H126正式交易資格"))
    growth = _text(record.get("成長價值證據"))
    margin = _text(record.get("三率×獲利突破"))
    resilient = _text(record.get("跌市抗跌"))
    technical = _text(record.get("技術型態與反證"))
    market = record.get("大盤漲跌%")
    individual = record.get("個股漲跌%")
    fundamental_ok = growth.startswith("已核PE/EPS")
    three_ok = margin.startswith("已核三率")
    resilience_ok = (market is not None and individual is not None and
                     resilient.startswith(("逆勢", "相對抗跌")))
    # Wider, separately labelled R2 research: the stock still falls, but drops
    # no more than half the verified index move and beats it by >=3 pp. H157
    # already withholds these numbers unless the stock/benchmark/date align.
    relative = record.get("相對強度pp")
    relative_only = (not resilience_ok and isinstance(market, (int, float))
                     and isinstance(individual, (int, float))
                     and isinstance(relative, (int, float))
                     and market <= -2.0 and individual < -0.5
                     and individual >= market / 2.0
                     and relative >= 3.0
                     and abs(individual-market-relative) <= 0.06)
    technical_breakout = "箱型突破" in technical or "MA黃金交叉" in technical
    technical_setup = ("接近箱頂" in technical or "布林帶收斂" in technical)

    if "H145" in qualified or "禁止" in qualified or "未授權" in qualified:
        trade = "研究觀察；H126禁止/未授權"
    else:
        trade = "請回00_H126主管決策逐股核對，不從研究表授權"

    if source.startswith("C"):
        lane = "C｜風控否決隔離"
    elif source.startswith("W"):
        lane = "W｜非交易核查"
    elif source.startswith("原研究"):
        lane = "U｜舊研究待核"
    elif resilience_ok:
        lane = "R｜逆勢近持平/收紅（研究）"
    elif relative_only:
        lane = "R｜相對抗跌但仍下跌（研究）"
    elif fundamental_ok and three_ok:
        lane = "G｜三率＋高成長同時認證"
    elif fundamental_ok:
        lane = "G｜成長價值已核（研究）"
    elif technical_breakout:
        lane = "T｜技術突破待續守"
    elif technical_setup:
        lane = "P｜預發動待突破"
    else:
        lane = "U｜資料缺口待補證"
    # Note: technically supported research does not become a verified fundamental.
    evidence = ("基本面已核；" if fundamental_ok else "PE/EPS及營收未雙證；") + (
        "三率×獲利已核；" if three_ok else "三率×獲利未雙證；") + (
        resilient + "；" if resilience_ok else
        (f"跌市相對強度 {relative:+.2f}pp（仍下跌）；" if relative_only else "抗跌缺同日核實；")) + technical[:66]

    missing = []
    if not fundamental_ok:
        missing.append("官方PE/EPS+營收公告日")
    if not three_ok:
        missing.append("官方季度三率+獲利突破")
    if market is None or individual is None:
        missing.append("研究日大盤/個股同日漲跌")
    elif relative_only:
        missing.append("個股仍下跌，須再核3/5日相對強度及隔日續守")
    if _text(record.get("RSI14")) == "" or record.get("RSI14") is None:
        missing.append("RSI/MACD/ATR同日驗證")
    if "H145" in qualified:
        missing.append("H145完整性阻擋")
    warning = "；".join(missing) if missing else "仍須確認最新一日量價／風控"
    # Existing H157's scenario is the highest available evidence; no new odds.
    scenario = _text(record.get("下一交易日觀察情境")) or "方向缺經過校準的證據，觀望"
    if market is None or individual is None:
        scenario = "缺同日可驗證大盤/個股資料；隔日漲跌不能預測"
    return lane, evidence, warning, trade + "｜" + scenario


def build_priority_rows(ledger: Mapping, *, limit: int = 20) -> dict:
    """A bounded review queue; does not reshuffle original H153 ranks."""
    rows = list(ledger.get("rows") or [])
    ranked = []
    duplicates = set()
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        code = _text(item.get("股票代號"))
        if not code or code in duplicates:
            continue
        duplicates.add(code)
        lane, evidence, warning, scenario = _classify(item)
        ranked.append({
            "股票代號": code,
            "股票名稱": _text(item.get("股票名稱")),
            "H153排名": item.get("H153研究排名"),
            "研究分": item.get("原研究證據分"),
            "研究來源池": _text(item.get("來源股票池")),
            "交易員分池": lane,
            "有憑據的訊號": evidence,
            "關鍵缺口": warning,
            "隔日條件情境（非預測機率）": scenario,
        })
    ordering = {"R": 0, "G": 1, "T": 2, "P": 3, "U": 4, "W": 5, "C": 6}
    # Within a lane, old H153 rank and original input order are retained.
    ranked.sort(key=lambda r: (ordering.get(r["交易員分池"][0], 7),
                               _int(r.get("H153排名")),
                               -(_int(r.get("研究分"), 0))))
    counts = Counter(r["交易員分池"].split("｜")[0] for r in ranked)
    limit = max(1, min(int(limit), 30))
    return {"rows": ranked[:limit], "counts": dict(counts), "total": len(ranked),
            "omitted": max(0, len(ranked)-limit),
            "sources_complete": bool(ledger.get("valid_comparison")) and
                                bool(ledger.get("growth_verified"))}


def append_focus_to_overview(wb: Any, ledger: Mapping, *, research_day: Any,
                             h145_status: str = "", run_id: str = "") -> dict:
    """Append in 11_ only; do not change the existing 14-tab contract."""
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter
    name = "11_交易員重點導航"
    if name not in wb.sheetnames:
        raise ValueError("H164 缺原11交易員導航表；禁止建立空白替代權威")
    ws = wb[name]
    if any("H164" in _text(ws.cell(i,1).value) for i in range(1,ws.max_row+1)):
        raise ValueError("H164精簡池已存在；禁止重複追加")
    summary = build_priority_rows(ledger)
    start = ws.max_row + 3
    ws.cell(start,1,"H164｜交易員最先看：分池精簡研究（非買進名單）")
    ws.cell(start,2,"研究日 "+(_date(research_day) or "未驗證") +
            "｜推定下一工作日 "+next_workday(research_day)+
            "｜H145 "+(_text(h145_status) or "未核驗")+
            "｜H126 仍以原主管決策表為唯一權威")
    ws.cell(start+1,1,"證據總結")
    ws.cell(start+1,2,
            f"{summary['total']}檔具名、僅列前{len(summary['rows'])}檔；同日抗跌已核"
            f" {int(ledger.get('anti_fall') or 0)}、官方成長雙證"
            f" {int(ledger.get('growth_verified') or 0)}、三率×獲利雙證"
            f" {int(ledger.get('margin_verified') or 0)}；缺證不能當作0成長。")
    ws.cell(start+2,1,"反向挑戰")
    ws.cell(start+2,2,"若研究日/最新交易日不符，全部個股方向僅是舊資料情境；"
            "不可宣稱當期抗跌；同分研究順位≠明日漲幅機率；官方基本面缺失不應加買進分。")
    ws.cell(start+3,1,"下次須更新")
    ws.cell(start+3,2,"同日加權與逐股收盤、OHLCV/量價、公告月營收、官方PE/EPS、"
            "單季毛利/營益/淨利與公告日、法人/TDCC、H109/H126；先驗證H145。")
    headers = list(summary["rows"][0]) if summary["rows"] else [
        "股票代號","股票名稱","H153排名","研究分","研究來源池", "交易員分池", "有憑據的訊號","關鍵缺口","隔日條件情境（非預測機率）"]
    header_row=start+5
    for col, title in enumerate(headers,1):
        ws.cell(header_row,col,title)
    for i, rec in enumerate(summary["rows"],header_row+1):
        for col, key in enumerate(headers,1):
            ws.cell(i,col,rec.get(key))
    ws.cell(start,1).fill=PatternFill("solid",fgColor="123A58")
    ws.cell(start,1).font=Font(name="Microsoft JhengHei",size=12,bold=True,color="FFFFFF")
    ws.cell(start,2).font=Font(name="Microsoft JhengHei",size=10,color="9F1239",bold=True)
    ws.row_dimensions[start].height=34
    for rowno in range(start+1,start+4):
        ws.row_dimensions[rowno].height=38
        ws.cell(rowno,1).font=Font(name="Microsoft JhengHei",color="123A58",bold=True)
        ws.cell(rowno,2).alignment=Alignment(wrap_text=True,vertical="center")
    for col in range(1,len(headers)+1):
        c=ws.cell(header_row,col)
        c.fill=PatternFill("solid",fgColor="0B6E75")
        c.font=Font(name="Microsoft JhengHei",color="FFFFFF",bold=True,size=10)
        c.alignment=Alignment(wrap_text=True,vertical="center")
    ws.row_dimensions[header_row].height=34
    for r in range(header_row+1,header_row+len(summary["rows"])+1):
        ws.row_dimensions[r].height=46
        for col in range(1,len(headers)+1):
            c=ws.cell(r,col)
            c.alignment=Alignment(wrap_text=col in (5,6,7,8,9),vertical="center")
            if r%2==0:c.fill=PatternFill("solid",fgColor="F0F7FA")
    for col,width in {"A":17,"B":29,"C":15,"D":14,"E":27,"F":32,"G":64,"H":56,"I":82}.items():
        # Existing Sheet11 A and B layout is preserved.
        if col in ("A","B"):
            continue
        ws.column_dimensions[col].width=width
    ws.cell(1,3,"H164精簡研究清單位於第"+str(start)+"列起（原H157摘要保留下方）。")
    ws.cell(1,3).font=Font(name="Microsoft JhengHei",color="C2410C",bold=True)
    return {"focus_row":start,"table_row":header_row,"total":summary["total"],
            "shown":len(summary["rows"]),"counts":summary["counts"]}


__all__ = ["VERSION", "next_workday", "build_priority_rows", "append_focus_to_overview"]
