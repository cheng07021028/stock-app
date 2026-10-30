# -*- coding: utf-8 -*-
"""V191-H148.3: diagnostic-pool research recall, strictly not H126 trades.

The limited H113 audit pool must not determine which diagnostic stocks may be
examined for market strength.  No network, persistence, fabricated facts or
changes to any formal gate.  Every displayed data item comes from supplied
per-stock evidence; never back-fill a missing field from a different source.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Mapping
import pandas as pd

from godpick_h147_five_gate_review import _code, _day, _n, _s
from godpick_h148_strength_provenance import _evidence_time

VERSION = 'V191-H148.3-DIAGNOSTIC-RESEARCH-RECALL'
RESEARCH_COLUMNS = (
    '研究層級','股票代號','股票名稱','產業','H114行情日期','來源證據',
    '當日成交額百萬','20日均成交額百萬','成交量張','量比','近5日漲幅%',
    '突破／續攻判定','H109風控','法人／大戶狀態','尚需驗證與不得買進原因',
)
RESEARCH_ONLY = '研究線索／非正式買進授權'


def _frame(v: Any) -> pd.DataFrame:
    return v if isinstance(v, pd.DataFrame) else pd.DataFrame()


def _num(v: Any) -> float | None:
    try:
        n = float(_s(v).replace(',','').replace('%',''))
        return n if pd.notna(n) and n not in (float('inf'),float('-inf')) else None
    except (ValueError, TypeError, OverflowError):
        return None


def build_research_recall(*, diagnosis_frame: Any, audit_frame: Any,
                          research_day: Any, h145_bridge: Mapping[str,Any] | None = None,
                          limit: int = 12) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Return visible research leads, rejection summary and non-trading diagnostics.

    Priority 1: same-day strong and fully evidenced liquidity/technical structure.
    Priority 2: same-day momentum with explicitly missing formal evidence.
    Priority 3: numerical momentum with stale/unproven date, clearly labelled.
    Neither a research lead nor a H109 veto can give H126 buying permission.
    No row without an observable strength criterion is used merely to fill 12.
    """
    frame, audit = _frame(diagnosis_frame), _frame(audit_frame)
    cutoff = _day(research_day)
    reason_counts: Counter[str] = Counter()
    evidence_counts: Counter[str] = Counter()
    duplicates: set[str] = set()
    invalid = 0
    record: dict[str,dict] = {}
    for rec in frame.to_dict('records'):
        code = _code(rec.get('股票代號'))
        if not code:
            invalid += 1
            continue
        if code in record:
            duplicates.add(code)
        else:
            record[code] = rec
    audited_codes = {_code(r.get('股票代號')) for r in audit.to_dict('records')}
    audited_codes.discard('')
    leads: list[tuple[tuple,dict]] = []
    for code, r in record.items():
        if code in duplicates:
            reason_counts['重複代號隔離'] += 1
            continue
        date_state = _evidence_time(r, cutoff)
        evidence_counts[date_state] += 1
        valid_day = date_state == '同日完整OHLCV（內部標記）'
        # Explicit unit contracts, no guessing that arbitrary volume means lots.
        lots = _n(r, 'H114當日成交量張', 'H113當日成交量張', '最新成交量_張')
        amount = _n(r, 'H113當日成交額百萬', '成交額百萬')
        avg_amount = _n(r, 'H113二十日均成交額百萬', '20日均成交額百萬')
        ratio = _n(r, 'H114K線量比')
        gap20 = _n(r, '收盤距MA20%', 'MA20乖離%')
        gap60 = _n(r, '收盤距MA60%', 'MA60乖離%')
        ret5 = _n(r, '近5日漲幅%')
        high_gap = _n(r, 'H114距20日高點%')
        bullish = bool((high_gap is not None and high_gap <= 2.0) or
                       (ret5 is not None and ret5 >= 5.0) or
                       'ALREADY-IGNITED' in _s(r.get('H105發動階段')).upper())
        has_turnover = amount is not None and amount >= 150
        has_volume = lots is not None and lots >= 500
        rising_volume = ratio is not None and ratio >= 1.2
        # An observed signal must be numerical and corroborated (never score-only).
        if not (bullish and has_turnover and has_volume and rising_volume):
            reason_counts['缺乏可核對的強勢量價共同訊號'] += 1
            continue
        technical_good = (gap20 is not None and gap60 is not None and
                          -6 <= gap20 <= 12 and gap60 >= 0)
        avg_good = avg_amount is not None and avg_amount >= 150
        if valid_day and avg_good and technical_good:
            tier, phase, stage = 0, 'A｜同日量價研究已核', '量能、技術與同日K線齊；尚非交易授權'
        elif valid_day:
            tier, phase, stage = 1, 'B｜同日強勢線索待補證', '已見同日量價動能，但20日均額或均線未完全核實'
        else:
            tier, phase, stage = 2, 'C｜量價線索日期待核', '非當日強勢認證；'+date_state
        risk = _s(r.get('H109決策權威')) or 'UNKNOWN'
        inst = _s(r.get('H123法人實證狀態')) or 'PENDING_EVIDENCE'
        holder = _s(r.get('H124大戶實證狀態')) or 'PENDING_EVIDENCE'
        task = [RESEARCH_ONLY]
        if tier == 2:task.append('行情未與本輪研究日一致')
        if not avg_good:task.append('20日均成交額未證')
        if not technical_good:task.append('MA20/MA60結構未證或風險較高')
        if risk not in {'ALLOW','PASS','NONE','RESEARCH_ELIGIBLE'}:
            task.append('H109:'+risk+' 原否決保留')
        if inst != 'PROVEN_FLOW':task.append('H123法人資料待補／未證連買')
        if holder != 'VERIFIED_RISING':task.append('H124集保週增待補／未證鎖碼')
        if code not in audited_codes:task.append('未進H113限量稽核／仍需正式資格複核')
        bridge = h145_bridge if isinstance(h145_bridge,Mapping) else {}
        if bridge.get('same_run_proven') is not True or bridge.get('candidate_complete') is not True:
            task.append('H145同輪母體尚未確認')
        # No synthetic joins: the per-stock source row owns every measurement.
        lead = {
            '研究層級':phase, '股票代號':code, '股票名稱':_s(r.get('股票名稱')),
            '產業':_s(r.get('類別')),
            'H114行情日期':_s(r.get('H114K線證據日')) or '未核實',
            '來源證據':'H114同日內部K線標記／仍需官方核對' if valid_day else date_state,
            '當日成交額百萬':amount, '20日均成交額百萬':avg_amount,
            '成交量張':lots, '量比':ratio, '近5日漲幅%':ret5,
            '突破／續攻判定':('接近／突破前20日高點研究' if high_gap is not None and high_gap<=2
                                else '五日漲幅動能研究' if ret5 is not None and ret5>=5
                                else '已發動分型／防追高'),
            'H109風控':risk, '法人／大戶狀態':inst+'／'+holder,
            '尚需驗證與不得買進原因':'；'.join(task),
        }
        # Same-tier: evidence-driven, not H110=52 or stock code rank.
        sort_key = (tier, risk not in {'ALLOW','PASS','NONE','RESEARCH_ELIGIBLE'},
                    -(ratio or 0), -(ret5 or 0), -(amount or 0), code)
        leads.append((sort_key,lead))
    leads.sort(key=lambda item:item[0])
    visible = pd.DataFrame([r for _,r in leads[:max(1,min(int(limit),20))]],columns=RESEARCH_COLUMNS)
    total_by_tier = Counter(r['研究層級'][:1] for _,r in leads)
    counts = pd.DataFrame([
       {'檢查項目':'診斷母體原始列','結果':len(frame),'說明':'H148.3本輪傳入診斷池；未必等於整個上市櫃市場'},
       {'檢查項目':'有效四碼不重複','結果':len(record)-len(duplicates),'說明':'重複代號隔離而不挑有利一筆'},
       {'檢查項目':'當輪同日K線已核','結果':evidence_counts['同日完整OHLCV（內部標記）'],'說明':'內部K線標記，須與真實來源再核'},
       {'檢查項目':'具名強勢研究線索','結果':len(leads),'說明':'量比／成交額／張數＋突破或5日動能，非H126買進'},
       {'檢查項目':'A同日量價研究已核','結果':total_by_tier['A'],'說明':'尚需核對族群、法人、大戶及H126'},
       {'檢查項目':'B同日強勢待補證','結果':total_by_tier['B'],'說明':'同日強勢量價線索；正式條件仍不足'},
       {'檢查項目':'C行情日期待核','結果':total_by_tier['C'],'說明':'不能說這些是當日強勢股'},
       {'檢查項目':'其他未列強勢線索','結果':reason_counts['缺乏可核對的強勢量價共同訊號'],'說明':'不是判斷基本面差，僅無足夠量價共同訊號'},
       {'檢查項目':'重複／無效代號','結果':len(duplicates)+invalid,'說明':'不得混用來源或自動挑選較高分'},
    ])
    bridge = h145_bridge if isinstance(h145_bridge,Mapping) else {}
    meta = {
        'version':VERSION,'research_day':str(cutoff) if cutoff else '未核實',
        'diagnosis_rows':len(frame),'diagnosis_unique':len(record)-len(duplicates),
        'same_day_k':evidence_counts['同日完整OHLCV（內部標記）'],
        'found':len(leads),'displayed':len(visible),'tier_counts':dict(total_by_tier),
        'duplicate_codes':sorted(duplicates),'invalid_codes':invalid,
        'scope_proven':bridge.get('same_run_proven') is True and bridge.get('candidate_complete') is True,
        'formal_h126_modified':False,
        'warning':('本輪沒有具同股成交量、成交額、量比及技術動能的研究證據，不能用股票代號或舊分數補榜。'
                   if not leads else f'具名研究線索{len(leads)}檔（僅顯示{len(visible)}檔）；所有股票均非正式交易授權。'),
    }
    if not meta['scope_proven']:
        meta['warning'] += ' H145母體未證：上述為局部診斷線索，非全市場強勢排名。'
    return visible, counts, meta


def append_research_to_sheet(ws: Any, board: Any, summary: Any, *, max_rows: int = 12) -> dict[str,int]:
    """Append an identified research-only block on worksheet 1, before H148.2."""
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter
    research, summary = _frame(board), _frame(summary)
    first = max(ws.max_row+3,11)
    ws.cell(first,1,'H148.3｜具名強勢研究線索（非推薦、非交易，不代表H126可買）')
    ws.merge_cells(start_row=first,start_column=1,end_row=first,end_column=15)
    ws.cell(first,1).font=Font(bold=True,color='FFFFFF',size=13)
    ws.cell(first,1).fill=PatternFill('solid',fgColor='185C70')
    ws.row_dimensions[first].height=28
    at=first+1
    for rec in summary.to_dict('records'):
        ws.cell(at,1,_s(rec.get('檢查項目')))
        ws.cell(at,2,rec.get('結果'))
        ws.cell(at,3,_s(rec.get('說明')))
        ws.row_dimensions[at].height=22
        at+=1
    at+=1
    for ci,field in enumerate(RESEARCH_COLUMNS,1):
        cell=ws.cell(at,ci,field)
        cell.font=Font(bold=True,color='FFFFFF')
        cell.fill=PatternFill('solid',fgColor='23647B')
        cell.alignment=Alignment(wrap_text=True,vertical='center')
    ws.row_dimensions[at].height=31
    output=0
    for rec in research.head(max(0,int(max_rows))).to_dict('records'):
        at+=1
        for ci,key in enumerate(RESEARCH_COLUMNS,1):
            val=rec.get(key)
            if isinstance(val,str) and val.lstrip().startswith(('=','+','-','@')):
                val="'"+val
            cell=ws.cell(at,ci,val)
            if ci in (1,6,12,15):cell.alignment=Alignment(wrap_text=True,vertical='center')
        ws.row_dimensions[at].height=48
        output+=1
    if not output:
        ws.cell(at+1,1,'當輪沒有符合量價共同證據的具名研究線索；先補來源，不得以冷門股填榜。')
        at+=1
    # Never move the old header or make appendix rows part of top-board filter.
    return {'first_row':first,'research_rows':output,'last_row':at}


__all__ = ['VERSION','RESEARCH_COLUMNS','build_research_recall','append_research_to_sheet']
