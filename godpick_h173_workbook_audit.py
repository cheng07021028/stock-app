# -*- coding: utf-8 -*-
"""H173 existing Sheet11 addition only, never alter original trading sheets."""
from __future__ import annotations
from pathlib import Path
from typing import Any
from openpyxl.styles import Font,PatternFill,Alignment
from godpick_h173_relative_edge import load_relative_report


def append_h173_audit(wb:Any,path:Path,*,max_strategies:int=30) -> dict:
    sheet='11_交易員重點導航'
    if sheet not in wb.sheetnames:
        return {'status':'MISSING_EXISTING_SHEET11','trade_authority_changed':False}
    report=load_relative_report(path)
    if report is None:
        return {'status':'NO_SIGNED_H173_REPORT','trade_authority_changed':False}
    ws=wb[sheet]
    beginning=ws.max_row+3
    ws.cell(beginning,1,'H173｜T+1／T+5／T+10 相對加權指數 × 抗跌實績稽核')
    ws.cell(beginning,1).fill=PatternFill('solid',fgColor='17324D')
    ws.cell(beginning,1).font=Font(name='Microsoft JhengHei',bold=True,color='FFFFFF')
    ws.cell(beginning,2,'截至 '+report['as_of']+'｜收盤對收盤、非實際可交易報酬／非明日預測')
    ws.cell(beginning+1,1,'原推薦結果來源SHA')
    ws.cell(beginning+1,2,report['h172_report_sha256'])
    ws.cell(beginning+2,1,'風控與資料限制')
    ws.cell(beginning+2,2,'只做事後稽核；未含除權息／費用／稅；未認證完整交易日曆；不改H109/H126、不自動調排名')
    head=beginning+4
    titles=['維度','分類','期間','已成熟不同股日','有效不同股日','獨立推薦日','不同股票','有效資料覆蓋%',
            '個股勝率%','跑贏加權指數%','平均超額績效(pp)','超額中位數(pp)',
            '大盤負報酬區間','大盤跌時個股不跌%','大盤跌時跑贏%','Wilson勝率下界%','樣本判定']
    for col,t in enumerate(titles,1):
        c=ws.cell(head,col,t);c.fill=PatternFill('solid',fgColor='0B6E75')
        c.font=Font(name='Microsoft JhengHei',color='FFFFFF',bold=True)
        c.alignment=Alignment(vertical='center',wrap_text=True)
    ws.row_dimensions[head].height=34
    line=head
    sections=[('全部','全部',report['overall'])]
    strategies=report['by_group'].get('strategy',{})
    sections.extend(('主導策略',name,items) for name,items in list(sorted(strategies.items()))[:max_strategies])
    for category,name,items in sections:
        for n in (1,5,10):
            m=items[f'T+{n}'];line+=1
            values=[category,name,f'T+{n}',m['mature_distinct_code_days'],m['evaluated_distinct_code_days'],
                    m['distinct_signal_days'],m['distinct_stock_codes'],m['evaluated_coverage_pct'],
                    m['stock_win_rate_pct'],m['index_outperformance_rate_pct'],m['mean_excess_pct_points'],
                    m['median_excess_pct_points'],m['negative_index_windows'],
                    m['positive_stock_on_negative_index_pct'],m['outperformed_during_negative_index_pct'],
                    m['index_outperformance_wilson_lower_95_pct'],m['evidence_status']]
            for col,v in enumerate(values,1):
                cell=ws.cell(line,col,v)
                if 8<=col<=16 and isinstance(v,(int,float)):
                    cell.number_format='0.00'
            ws.row_dimensions[line].height=22
    return {'status':'H173_VERIFIED_RESEARCH_APPENDED','start_row':beginning,
            'rows':line-head,'report_sha256':report['report_sha256'],
            'trade_authority_changed':False,'original_sheet_count_unchanged':True}

__all__=['append_h173_audit']
