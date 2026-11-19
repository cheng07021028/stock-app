# -*- coding: utf-8 -*-
"""Append a bounded H172 historical research receipt to *existing* Sheet11 only."""
from __future__ import annotations
from pathlib import Path
from typing import Any
from openpyxl.styles import Font, PatternFill, Alignment
from godpick_h172_closed_loop import load_verified_report


def append_h172_receipt(wb:Any, path:Path, *, max_events:int=25) -> dict:
    tab='11_交易員重點導航'
    if tab not in wb.sheetnames:
        return {'status':'NO_EXISTING_NAV_SHEET','trade_authority_changed':False}
    report=load_verified_report(path)
    if not report:
        return {'status':'NO_SIGNED_H172_RESEARCH_REPORT','trade_authority_changed':False}
    ws=wb[tab]
    start=ws.max_row+3
    ws.cell(start,1,'H172｜H116決策快照 × H117官方行情｜歷史推薦績效核查')
    ws.cell(start,1).fill=PatternFill('solid',fgColor='17324D')
    ws.cell(start,1).font=Font(name='Microsoft JhengHei',color='FFFFFF',bold=True)
    ws.cell(start,2,'截至：'+report['as_of']+'｜純研究回顧，不是當時已知資訊／非可執行買點')
    ws.cell(start+1,1,'官方交易日證據狀態')
    ws.cell(start+1,2,'觀察到的指數交易日 '+str(report['official_index_observed_sessions'])+
            '｜三市場齊全 '+str(report['fully_observed_market_sessions'])+
            '｜獨立交易日曆未認證')
    ws.cell(start+2,1,'H116 符合逐股時點的研究事件')
    ws.cell(start+2,2,report['eligible_research_events'])
    ws.cell(start+3,1,'T+1／T+5／T+10 有效樣本／勝率（僅收盤對收盤）')
    ws.cell(start+3,2,'｜'.join(f"T+{n}: {(report['summary'][f'T+{n}']['evaluated'])}筆 / " +
        ('--' if report['summary'][f'T+{n}']['win_rate_pct'] is None else
            str(report['summary'][f'T+{n}']['win_rate_pct'])+'%') for n in (1,5,10)))
    ws.cell(start+4,1,'完整性提醒')
    ws.cell(start+4,2,'不可用本表修正歷史選股分數、H109/H126或正式交易資格；尚未證實完整交易日曆／除權息調整')
    head=start+6
    headers=['事件ID','股票代號','主導策略','推薦參考交易日','T+1報酬%','T+1狀態',
             'T+5報酬%','T+5狀態','T+10報酬%','T+10狀態']
    for col,title in enumerate(headers,1):
        c=ws.cell(head,col,title)
        c.fill=PatternFill('solid',fgColor='0B6E75')
        c.font=Font(name='Microsoft JhengHei',color='FFFFFF',bold=True)
        c.alignment=Alignment(wrap_text=True,vertical='center')
    ws.row_dimensions[head].height=32
    for pos,event in enumerate(report['results'][:max_events],head+1):
        rt=event['returns']
        row=[event['event_id'],event['code'],event['strategy'],event['signal_date']]
        for horizon in (1,5,10):
            v=rt[f'T+{horizon}']
            row.extend([v['return_pct'] if v['status']=='EVALUATED_PRICE_ONLY' else None,v['status']])
        for col,value in enumerate(row,1):
            ws.cell(pos,col,value)
        ws.row_dimensions[pos].height=25
    return {'status':'H172_VERIFIED_HISTORICAL_RECEIPT_APPENDED','rows':min(len(report['results']),max_events),
            'start_row':start,'source_hash':report['report_sha256'],
            'trade_authority_changed':False}

__all__=['append_h172_receipt']
