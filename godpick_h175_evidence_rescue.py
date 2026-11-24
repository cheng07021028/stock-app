# -*- coding: utf-8 -*-
"""H175 same-run market-evidence gap triage and clearly NON-TRADABLE radar.

This module does not create evidence, fetch new data, revise ranks, override
H109/H126, or suggest buys. It makes the existing H85 *waiting* candidates
visible even when the evidence-qualified H153 list is empty.
"""
from __future__ import annotations
from collections import Counter
from collections.abc import Mapping
from typing import Any
import re
import pandas as pd

VERSION = "V191-H175-QUALITY-TRIAGE-H85-SEPARATE"
HEADERS = ["原來源順序", "股票代號", "股票名稱", "股票池", "研究日", "行情證據日",
           "H114量價狀態", "最近60根有效量數", "缺量根數", "H109原風控",
           "先補的資料／禁止誤判", "資格", "H175僅價格型態", "H175近20日漲幅%"]


def _code(v: Any) -> str:
    s = _text(v).removesuffix(".0")
    return s if re.fullmatch(r"[0-9]{4}", s) else ""


def _text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in {"nan", "none", "nat"} else s


def _num(v: Any) -> int:
    try:
        f = float(v)
        return int(f) if 0 <= f < 100000 else 0
    except (ValueError, TypeError, OverflowError):
        return 0


def _float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if -1e10 < f < 1e10 else None
    except (ValueError, TypeError, OverflowError):
        return None


def _date(v: Any) -> str:
    try:
        day = pd.to_datetime(v, errors="coerce") if _text(v) else pd.NaT
        return day.strftime("%Y-%m-%d") if not pd.isna(day) else ""
    except (ValueError, TypeError, AttributeError):
        return ""


def _rows(obj: Any) -> list[dict]:
    if isinstance(obj, pd.DataFrame):
        return obj.to_dict("records")
    if isinstance(obj, list):
        return [dict(row) for row in obj if isinstance(row, Mapping)]
    return []


def build_h175_audit(*, diagnosis: Any, tables: Any, h154_audit: Any,
                     study_day: Any) -> dict[str, Any]:
    """Bound to current export; conflicting or unverified identities stay blocked."""
    diag_rows = _rows(diagnosis)
    counts = Counter(_text(r.get("H114K線資料狀態")) or "缺狀態" for r in diag_rows)
    index: dict[str, dict] = {}
    conflicts: set[str] = set()
    for row in diag_rows:
        code = _code(row.get("股票代號"))
        if not code:
            continue
        if code in index:
            conflicts.add(code)
        else:
            index[code] = row
    audit = h154_audit if isinstance(h154_audit, Mapping) else {}
    # H189: H145 guards *formal* candidate/export authority, not whether
    # same-run H85 names may be displayed in an explicitly NON-TRADABLE queue.
    # Requiring H145 green here concealed all 23 waiting stocks while the
    # independently verified H154 run identity was MATCHED.
    # Also verify that this specific H85 payload matches H154's SAME-RUN
    # list, not merely that another stage's run IDs happen to match.
    def _src_codes(name: str) -> list[str]:
        return list(dict.fromkeys(c for r in _rows(tables.get(name))
                                  if (c := _code(r.get("股票代號")))))
    h85_verified = (_src_codes("waiting") == list(audit.get("h85_waiting_codes") or [])
                    and _src_codes("emerging_watch") == list(audit.get("h85_emerging_codes") or []))
    same_run = (audit.get("run_identity") == "MATCHED" and
                bool(_text(audit.get("run_id"))) and h85_verified)
    formal_blocked = _text(audit.get("h145_status")).startswith("BLOCKED_")
    day = _date(study_day)
    tables = tables if isinstance(tables, Mapping) else {}
    names: list[tuple[str, dict]] = []
    for source in ("waiting", "emerging_watch"):
        for row in _rows(tables.get(source)):
            names.append((source, row))
    seen: set[str] = set()
    radar: list[dict] = []
    for pos, (source, candidate) in enumerate(names, 1):
        code = _code(candidate.get("股票代號"))
        if not code or code in seen:
            continue
        seen.add(code)
        raw = index.get(code, {})
        status = _text(raw.get("H114K線資料狀態")) or "未取得診斷"
        issued = _date(raw.get("H114K線證據日"))
        bars = _num(raw.get("H114K線有效根數"))
        valid_vol = _num(raw.get("H175最近60根量價完整數"))
        bad_vol = _num(raw.get("H175最近60根成交量缺失數")) + _num(raw.get("H175最近60根成交量非正數"))
        veto = _text(raw.get("H109決策權威")) or "未取得風控"
        verified = status == "VALID_OHLCV" and bars >= 60 and issued == day and bool(day)
        if code.startswith("0"):
            reason = "ETF／非一般股票，隔離；不能納入高成長個股選股"
        elif code in conflicts:
            reason = "同代號診斷紀錄重複，禁止挑有利的一列，先核對資料來源"
        elif not raw:
            reason = "H85有候選但診斷母體無同代號；查本輪資料交接"
        elif issued != day:
            reason = "K線交易日與研究日不同；先補當日有效歷史行情"
        elif status == "INVALID_OHLCV_VOLUME":
            reason = ("近60根成交量未達完整契約；" +
                      (f"缺量{bad_vol}根；" if bad_vol else "缺量根數待掃描更新；") +
                      "驗證原始供應商成交股數單位／停牌／零成交，勿以股數反推成長分")
        elif not verified:
            reason = f"H114 {status}，有效K={bars}；需要同日OHLCV與來源核對"
        elif veto not in {"RESEARCH_ELIGIBLE", "ALLOW", "PASS", "NONE"}:
            reason = "H114初步有量，但H109原風控未通過；不得自行解除否決"
        else:
            reason = "只有量價來源初步完整，仍缺官方PE/EPS、三率、法人／TDCC與H126"
        if not same_run:
            reason = "同輪身分或H145完整性未證；不得顯示為本輪候選。" + reason
        radar.append({
            "原來源順序": pos, "股票代號": code,
            "股票名稱": _text(candidate.get("股票名稱"))[:70],
            "股票池": "H85等待／雷達" if source == "waiting" else "H85新興觀察",
            "研究日": day, "行情證據日": issued,
            "H114量價狀態": status, "最近60根有效量數": valid_vol,
            "缺量根數": bad_vol, "H109原風控": veto,
            "先補的資料／禁止誤判": reason,
            "資格": ("禁止直接買進｜H145正式阻擋、僅同輪非交易核查" if formal_blocked
                   else "禁止直接買進｜同輪非交易核查") if same_run else "隔離｜非同輪已驗資料",
            "H175僅價格型態":_text(raw.get("H175價格型態（僅OHLC）")) or "未提供",
            "H175近20日漲幅%":_float(raw.get("H175價格20日報酬%")),
        })
    # Price-only rescue: reveal unranked early-stage price structure from the
    # entire same-run diagnosis, NEVER as A/B, a verified breakout, or a buy.
    # It must pass real data cutoffs and the existing H109 eligibility flag.
    # H113 turnover values are INTERNAL scan evidence only, not official audit.
    for pos, raw in enumerate(diag_rows, 1):
        code = _code(raw.get("股票代號"))
        if not same_run or not code or code.startswith('0') or code in seen or code in conflicts:
            continue
        if _date(raw.get("H114K線證據日")) != day or _num(raw.get("H114K線有效根數")) < 60:
            continue
        if not _text(raw.get("H175價格型態（僅OHLC）")).startswith("初步蓄勢"):
            continue
        if _text(raw.get("H109決策權威")) not in {"RESEARCH_ELIGIBLE","ALLOW","PASS","NONE"}:
            continue
        turnover = _float(raw.get("H113當日成交額百萬"))
        avg_turnover = _float(raw.get("H113二十日均成交額百萬"))
        today_lots = _float(raw.get("最新成交量_張"))
        if (turnover is None or turnover < 150 or avg_turnover is None or avg_turnover < 150
                or today_lots is None or today_lots < 500):
            continue
        seen.add(code)
        radar.append({
            "原來源順序":pos,"股票代號":code,"股票名稱":_text(raw.get("股票名稱"))[:70],
            "股票池":"H175同輪價格型態初篩（非交易）","研究日":day,
            "行情證據日":_date(raw.get("H114K線證據日")),
            "H114量價狀態":_text(raw.get("H114K線資料狀態")),
            "最近60根有效量數":_num(raw.get("H175最近60根量價完整數")),
            "缺量根數":_num(raw.get("H175最近60根成交量缺失數")) +
                       _num(raw.get("H175最近60根成交量非正數")),
            "H109原風控":_text(raw.get("H109決策權威")),
            "先補的資料／禁止誤判":"價格型態初步符合但不是已驗證量價突破；需補完整60根成交量、官方PE/EPS、月營收、三率、法人/TDCC，H126未授權不得買進。",
            "資格":"僅價格初篩｜禁止直接買進",
            "H175僅價格型態":_text(raw.get("H175價格型態（僅OHLC）")),
            "H175近20日漲幅%":_float(raw.get("H175價格20日報酬%")),
        })
        if sum(x['股票池'].startswith('H175') for x in radar) >= 12:
            break
    # Price-only cases are displayed first (but their source order is NOT a
    # research rank). Do not prioritize H85 finance simply by a former score.
    radar.sort(key=lambda x:(0 if x['股票池'].startswith('H175') else 1,
                             x['原來源順序'],x['股票代號']))
    return {
        "version": VERSION, "run_id": _text(audit.get("run_id")),
        "same_run": same_run, "h85_codes_reconciled": h85_verified,
        "formal_h145_blocked":formal_blocked,
        "formal_h145_status":_text(audit.get("h145_status")),
        "study_day": day, "diagnosis_rows": len(diag_rows),
        "status_counts": dict(counts), "invalid_volume_rows": counts.get("INVALID_OHLCV_VOLUME", 0),
        "strict_ohlcv_rows": sum(1 for r in diag_rows if
               _text(r.get("H114K線資料狀態")) == "VALID_OHLCV" and
               _num(r.get("H114K線有效根數")) >= 60 and
               _date(r.get("H114K線證據日")) == day),
        "radar": radar if same_run else [], "isolated_rows": len(radar) if not same_run else 0,
        "price_only_research":sum(x['股票池'].startswith('H175') for x in radar) if same_run else 0,
        "h126_trade_authority_changed": False,
        "note": "H85來源序號不等於H153排名或上漲機率；來源有缺口時不能成為H126正式推薦。",
    }


def append_h175_excel(wb: Any, report: Mapping[str, Any]) -> dict[str, int]:
    """Only append to manager & existing trader NAV. Keep all 14 legacy sheets."""
    from openpyxl.styles import Font, PatternFill, Alignment
    if not all(n in wb.sheetnames for n in ("00_主管重點觀察榜", "11_交易員重點導航", "00_H126主管決策")):
        raise ValueError("H175原有主管／交易員／H126表缺失；不可自行建立替代表")
    original_sheets = wb.sheetnames[:]
    original_g2 = wb["00_H126主管決策"]["G2"].value
    radar = list(report.get("radar") or [])
    nav = wb["11_交易員重點導航"]
    manager = wb["00_主管重點觀察榜"]
    fmt = lambda ws, row, col, value: ws.cell(row, col, value)
    # Manager home: compact visible named priorities after existing protected board.
    top = manager.max_row + 3
    fmt(manager, top, 1, "H175｜本輪候選補證雷達（非正式推薦）")
    fmt(manager, top, 2, f"H114有效60根{report.get('strict_ohlcv_rows',0)}/{report.get('diagnosis_rows',0)}；"
                          f"INVALID_OHLCV_VOLUME={report.get('invalid_volume_rows',0)}；"
                          f"獨立價格初篩{report.get('price_only_research',0)}檔／補證雷達{len(radar)}檔；H126不變")
    for c in (1, 2):
        manager.cell(top,c).fill = PatternFill('solid',fgColor='17324D')
        manager.cell(top,c).font = Font(name='Microsoft JhengHei',color='FFFFFF',bold=True)
    fmt(manager, top+1, 1, "閱讀須知")
    fmt(manager, top+1, 2, "只為補資料，非A/B研究排名、非隔日上漲預測。完整逐股證據請看11表H175區。")
    fmt(manager, top+2, 1, "來源序／代號")
    fmt(manager, top+2, 2, "股票名稱｜H114資料問題｜優先查證")
    for i, row in enumerate(radar[:10], top+3):
        fmt(manager, i, 1, f"{row['原來源順序']}｜{row['股票代號']}")
        fmt(manager, i, 2, f"{row['股票名稱']}｜{row['股票池']}｜{row['H114量價狀態']}｜{row['先補的資料／禁止誤判']}")
        manager.cell(i,2).alignment=Alignment(wrap_text=True,vertical='center')
        manager.row_dimensions[i].height=46
    manager.row_dimensions[top].height=34
    manager.column_dimensions['B'].width=max(manager.column_dimensions['B'].width or 15,68)
    # Existing H157/H169/H173 navigation sections retained intact.
    start = nav.max_row+3
    fmt(nav,start,1,"H175｜同輪候選補證清單與K線缺口（非交易排名）")
    fmt(nav,start,2,f"研究日 {report.get('study_day') or '未驗證'}；同輪核實={report.get('same_run')}；"
                     f"診斷{report.get('diagnosis_rows',0)}，嚴格量價{report.get('strict_ohlcv_rows',0)}，"
                     f"量缺失{report.get('invalid_volume_rows',0)}；H175僅價型{report.get('price_only_research',0)}；正式H126權威不變")
    nav.cell(start,1).fill=PatternFill('solid',fgColor='17324D')
    nav.cell(start,1).font=Font(name='Microsoft JhengHei',bold=True,color='FFFFFF')
    fmt(nav,start+1,1,"A/B=0不等於沒有可查股票，H85等待與新興只表示原來源候補；"
                      "不得將H85序號改名成研究排名；ETF／原H109否決應保留隔離。")
    for j,head in enumerate(HEADERS,1):
        cell=fmt(nav,start+3,j,head)
        cell.fill=PatternFill('solid',fgColor='0B6E75')
        cell.font=Font(name='Microsoft JhengHei',bold=True,color='FFFFFF')
    for ri,row in enumerate(radar,start+4):
        for ci,head in enumerate(HEADERS,1):
            cell=fmt(nav,ri,ci,row[head])
            cell.alignment=Alignment(vertical='center',wrap_text=ci>=7)
        nav.row_dimensions[ri].height=50
    if not radar:
        fmt(nav,start+4,1,"沒有足以證明同輪身分的H85核查名單，禁止用舊快照湊榜。")
    for col,width in {'A':18,'B':19,'C':19,'D':18,'E':17,'F':17,'G':27,
                      'H':20,'I':16,'J':23,'K':66,'L':34,'M':45,'N':22}.items():
        d=nav.column_dimensions[col]
        d.width=max(d.width or 12,width)
    if wb.sheetnames != original_sheets or wb['00_H126主管決策']['G2'].value != original_g2:
        raise ValueError("H175意外變動H126交易權威或工作表，阻擋輸出")
    return {'manager_start_row':top, 'navigation_start_row':start, 'radar_rows':len(radar)}


__all__=["VERSION", "build_h175_audit", "append_h175_excel"]
