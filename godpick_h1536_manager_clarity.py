# -*- coding: utf-8 -*-
"""H153.6 | honest, usable first-page research vs trading overview.

Read-only and Excel-display-only. No network access, model execution, price
estimates, H109 changes, H126 authorization or persistence writes.
"""
from __future__ import annotations
from collections.abc import Mapping
from collections import Counter
from copy import copy
from typing import Any
import pandas as pd
from godpick_h147_five_gate_review import _code, _day

VERSION='V191-H153.6-MANAGER-CLARITY'


def _df(value:Any)->pd.DataFrame:
    return value if isinstance(value,pd.DataFrame) else pd.DataFrame()


def _nonnegative(value:Any)->int:
    try:return max(0,int(value))
    except (ValueError,TypeError,OverflowError):return 0


def _codes(frame:pd.DataFrame)->set[str]:
    if '股票代號' not in frame:return set()
    return {_code(v) for v in frame['股票代號'].tolist()}-{''}


def clarify_manager_board(wb:Any, *, ranked:Any=None, watch:Any=None,
                          research:Any=None, diagnosis:Any=None,
                          h126_buyable:Any=0, h145_bridge:Any=None,
                          research_day:Any=None, h115_ledger_rows:Any=None)->pd.DataFrame:
    """Compact 00 summary; archive ALL replaced text in the existing 02 sheet.

    A/B ranked stocks and W verification candidates are distinct concepts.
    Never sum these as independent stocks without de-duplicating identifiers.
    An H145 audit block is NOT proof that the scanner found no research ideas.
    """
    if '00_主管重點觀察榜' not in wb.sheetnames:
        return pd.DataFrame(columns=['檢查項目','結果','說明'])
    ws=wb['00_主管重點觀察榜']
    ranked=_df(ranked);watch=_df(watch);research=_df(research);diagnosis=_df(diagnosis)
    original={(r,c):str(ws.cell(r,c).value or '') for r,c in
              ((1,1),(1,2),(4,1),(4,2),(6,1),(6,2))}
    ranks=ranked.get('排名等級',pd.Series(dtype=str)).fillna('').astype(str)
    a=int(ranks.str.startswith('A').sum());b=int(ranks.str.startswith('B').sum())
    c=int(ranks.str.startswith('C').sum())
    normal=ranked.loc[ranks.str.startswith(('A','B'))].copy() if '排名等級' in ranked else pd.DataFrame()
    normal_codes=_codes(normal)
    watch_codes=_codes(watch)
    overlap=normal_codes&watch_codes
    named_codes=_codes(research)
    trade=_nonnegative(h126_buyable)
    # The H114 same-day status and the H153.2 >=85 full-bar indicator status
    # are different data contracts; count them separately to explain why a
    # research stock has a price but BB/OBV/ADX still says "未驗證".
    valid_h114=0;valid_h1532=0;tech_reasons=Counter()
    if not diagnosis.empty:
        d=diagnosis
        if {'H114K線資料狀態','H114K線有效根數'}.issubset(d.columns):
            rows=d['H114K線資料狀態'].fillna('').astype(str).eq('VALID_OHLCV')
            roots=pd.to_numeric(d['H114K線有效根數'],errors='coerce').fillna(0)
            valid_h114=int((rows & (roots>=60)).sum())
        if 'H1532技術來源' in d.columns:
            tech=d['H1532技術來源'].fillna('').astype(str)
            valid_h1532=int(tech.str.startswith('同股同日H114').sum())
            tech_reasons=Counter(tech[~tech.str.startswith('同股同日H114')])
    db=h115_ledger_rows if isinstance(h115_ledger_rows,Mapping) else {}
    revenue=_nonnegative(db.get('revenue',0));valuation=_nonnegative(db.get('valuation',0))
    quarter=_nonnegative(db.get('quarterly_margins',0))
    bridge=h145_bridge if isinstance(h145_bridge,Mapping) else {}
    status=str(bridge.get('status') or '來源未提供')
    expected=_nonnegative(bridge.get('expected_final'))
    audit_coverage=bridge.get('audit_final_coverage')
    actual_final=_nonnegative(audit_coverage.get('wanted')) if isinstance(audit_coverage,Mapping) else None
    coverage_note=f'掃描預期{expected}／最終唯一代號{actual_final}' if actual_final is not None else f'掃描正式候選期望{expected}'
    day=_day(research_day)
    day_str=day.isoformat() if day else '未驗證'
    show=normal.head(5)
    top='、'.join(f"{r.get('研究排名','?')}:{_code(r.get('股票代號'))}" for r in show.to_dict('records'))
    watch_top='、'.join(f"{r.get('核查優先序','?')}:{_code(r.get('股票代號'))}" for r in watch.head(5).to_dict('records'))
    # Replace cluttered one-cell prose with a concise decision navigation.
    ws.cell(1,1,'H153.6｜主管研究與風控導覽（保留H148.1五關；非交易推薦）')
    ws.cell(1,2,f'研究日 {day_str}｜A {a}／B {b}／C {c}；另列W核查 {len(watch_codes)} 檔｜H126正式買進 {trade} 檔')
    ws.cell(4,1,'具名研究／核查優先')
    ws.cell(4,2,f'正式研究A/B {len(normal_codes)} 檔；W核查 {len(watch_codes)} 檔；01表可見唯一代號 {len(named_codes)} 檔（不同池不可直接相加）')
    ws.cell(6,1,'今天優先確認事項')
    # H153.9: make "mainstream verified" vs "technical-only repeat" explicit
    # on the manager first page; unknown data must never become a negative vote.
    mainstream=normal.get('H1539主流證據等級',pd.Series(dtype=str)).fillna('').astype(str)
    verified_mainstream=int(mainstream.str.startswith('A｜').sum())
    internal_mainstream=int(mainstream.str.startswith(('B｜','I｜')).sum())
    unknown_mainstream=int(mainstream.str.startswith('U｜').sum())
    repeat_series=pd.to_numeric(normal.get('H1539跨日快照次數',pd.Series(dtype=float)),errors='coerce')
    repeats=int((repeat_series>=2).sum())
    points=[f'H153研究前五：{top or "無合格A/B候選"}',
            f'H153.9主流交叉佐證{verified_mainstream}／內部族群線索{internal_mainstream}／主流未驗證{unknown_mainstream}；歷史重複至少2次{repeats}（無歷史不等於新股）',
            f'W非交易補證前五：{watch_top or "沒有額外具名候選"}',
            f'H145：{status}（{coverage_note}，未通過不得下單）',
            f'技術來源：同日K標記{valid_h114}／H153.2完整型態{valid_h1532}；缺85根有效日K不能加技術分',
            f'官方估值 {valuation} 筆／季度三率 {quarter} 筆／月營收 {revenue} 筆；缺來源不得加成長分',
            '01看具名研究、02看完整稽核、00_H126看正式交易資格']
    if '全部有效研究分相同' in original[(6,2)]:
        points.append('全部有效研究分相同，不得以同分當作可比較的選股名次')
    ws.cell(6,2,'｜'.join(points))
    ws.row_dimensions[1].height=38
    ws.row_dimensions[4].height=42
    ws.row_dimensions[6].height=108
    for r in (1,4,6):
        style=copy(ws.cell(r,2).alignment)
        style.wrap_text=True;style.vertical='center'
        ws.cell(r,2).alignment=style
    audit=[
        {'檢查項目':'H153.9主管研究主流交叉佐證','結果':verified_mainstream,'說明':'有H108跨期兩指標與有日期的法人或集保交叉；研究仍非H126'},
        {'檢查項目':'H153.9主管研究主流未驗證','結果':unknown_mainstream,'說明':'資料不足不能稱非主流，亦不能稱新主流'},
        {'檢查項目':'H153.9歷史快照重複至少2次','結果':repeats,'說明':'僅以可讀既有輪動快照分析；無歷史不等於新股'},
        {'檢查項目':'H153.6研究日','結果':day_str,'說明':'研究日不等於 Excel 匯出時間；未更新的次日行情不能充當新預測'},
        {'檢查項目':'H153.6可排列研究A','結果':a,'說明':'H153技術／基本面研究，非H126交易授權'},
        {'檢查項目':'H153.6可排列研究B','結果':b,'說明':'基本面或風控證據待補，不得認定可買進'},
        {'檢查項目':'H153.6隔離研究C','結果':c,'說明':'明確否決留在反證稽核，不納入主管五檔'},
        {'檢查項目':'H153.6額外非交易核查W實際不同股票','結果':len(watch_codes),'說明':'先排除01既有具名候選才從全部W池選出；不和A/B重複計數'},
        {'檢查項目':'H153.6研究與W重疊警示','結果':len(overlap),'說明':'應為0；若非0需檢查去重與同輪資料'},
        {'檢查項目':'H153.6 01具名表不重複股票','結果':len(named_codes),'說明':'僅代表這份 Excel 的具名股票，不保證全市場排名'},
        {'檢查項目':'H153.6 H126正式可買','結果':trade,'說明':'唯一授權層；不受研究榜排序影響'},
        {'檢查項目':'H153.6 H145正式輸出覆蓋狀態','結果':status,'說明':f'{coverage_note}；此狀態與具名研究池不同，不得誤認研究0檔'},
        {'檢查項目':'H153.6 H114同日有效60根K標記','結果':valid_h114,
         '說明':'內部行情標記，仍需實際行情來源稽核；不能等同布林/ADX已可計算'},
        {'檢查項目':'H153.6 H1532完整85根技術指標已核','結果':valid_h1532,
         '說明':'只有完整同股同日OHLCV及成交量單位，才可計算布林、OBV、ADX'},
        {'檢查項目':'H153.6 H1532技術缺證主要原因','結果':'；'.join(f'{k}:{v}' for k,v in tech_reasons.most_common(4)) or '診斷池沒有逐股技術來源',
         '說明':'未驗證是來源缺口，不是已判斷技術訊號不成立'},
        {'檢查項目':'H153.6官方PE/EPS／三率來源缺口','結果':f'估值{valuation}／季報{quarter}／月營收{revenue}',
         '說明':'未有公告及可用時間的資料不給成長分；READ_OK不代表已補完整歷史'},
    ]
    for (r,col),v in original.items():
        audit.append({'檢查項目':f'H153.6保留原版首頁單元格 R{r}C{col}',
                      '結果':v,'說明':'完整保留在02稽核工作表，首頁改為可讀的摘要'})
    return pd.DataFrame(audit,columns=['檢查項目','結果','說明'])


__all__=['VERSION','clarify_manager_board']
