# -*- coding: utf-8 -*-
"""Render the actual Page07 workbook writer with H149 six-pillar rows populated."""
import ast
import io
from pathlib import Path
import pandas as pd
from openpyxl import load_workbook
from godpick_h149_trade_intelligence import build_six_pillar_review
from test_h149_trade_intelligence import raw, lead, DAY


def test_actual_export_has_six_pillars_and_unchanged_h126_sheet():
    path=Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py'
    source=path.read_text('utf-8')
    tree=ast.parse(source)
    names={'_excel_safe_value','_excel_safe_df','_write_df_to_ws_fast_h84'}
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==3
    start=source.index('    sheets = [\n        ("00_主管重點觀察榜", h144_focus,')
    end=source.index('    return output.getvalue()',start)+len('    return output.getvalue()')
    block='''def export_page():
    import io
    from openpyxl import Workbook
    wb=Workbook();wb.remove(wb.active)
    output=io.BytesIO()
    h144_focus=pd.DataFrame(columns=['觀察順位','股票代號','股票名稱','買進資格'])
    h144_focus_meta={'scanned':2100,'diagnosed':2035,'final':0,'received':0,
      'audit_count':120,'authority_count':1,'all_scores_tied':False,
      'scope_issues':['H145母體未證'],'valid_score_rows':0,'score_distinct':0,
      'h126_buyable':0,'warning':'僅供研究'}
    tables={'h126_authority':pd.DataFrame([{'股票代號':'3035','H126能否買進':'否'}])}
    h148_meta={'review_unique':0,'scope_status':'母體未證','zero_formal_diagnosis':'0正式推薦'}
    h1481_meta={'eligible':0,'quarantine':120,'h113_missed_strong_research_count':5}
    h149_opportunities,h149_summary,h149_meta=build_six_pillar_review(
       diagnosis_frame=pd.DataFrame([raw()]),research_frame=pd.DataFrame([lead()]),
       authority_frame=tables['h126_authority'],research_day=DAY)
    _h147_scan_diag={'status':'CHECK'}
    _h145_bridge={'status':'UNPROVEN_SOURCE'}
'''+source[start:end]+'\n'
    env={'pd':pd,'raw':raw,'lead':lead,'DAY':DAY,'build_six_pillar_review':build_six_pillar_review}
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes,type_ignores=[])),str(path),'exec'),env,env)
    exec(compile(block,str(path),'exec'),env,env)
    wb=load_workbook(io.BytesIO(env['export_page']()))
    assert wb.sheetnames==['00_主管重點觀察榜','00_H126主管決策']
    ws,formal=wb.worksheets
    values=[str(ws.cell(r,1).value or '') for r in range(1,ws.max_row+1)]
    assert any('H149｜六維交易研究' in val for val in values)
    assert any('1｜交易機會' in val for val in values)
    assert any(ws.cell(r,2).value=='2352' for r in range(8,ws.max_row+1))
    assert formal['B2'].value=='否'
    assert ws.auto_filter.ref.startswith('A7:')
    wb.close()


def test_h114_existing_history_exposes_dated_close_not_guessed_quote():
    from godpick_h114_parallel_alpha import analyze_history
    days=pd.bdate_range('2026-06-01',periods=90)
    data=pd.DataFrame([{'日期':d.date().isoformat(), '開盤價':100+i*.1,
      '最高價':102+i*.1,'最低價':99+i*.1,'收盤價':101+i*.1,
      '成交量(張)':500+i*2} for i,d in enumerate(days)])
    row=analyze_history(data,asof=days[-1].date())
    assert row['H114K線資料狀態']=='VALID_OHLCV'
    assert row['H114K線證據日']==days[-1].date().isoformat()
    assert row['H114當日收盤價']==round(101+89*.1,3)
    data=data.drop(columns=['成交量(張)'])
    other=analyze_history(data,asof=days[-1].date())
    assert other['H114當日收盤價'] is None
