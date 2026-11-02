# -*- coding: utf-8 -*-
"""Real Page-7 Excel body, two sheets with visible H148.2 evidence appendix."""
import ast
import io
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from godpick_h1482_source_audit import build_source_audit
from test_h1481_quality_gate import row, run, DAY


def _export_real_body(board, authority, trace, repair, details):
    page = Path(__file__).resolve().parents[1] / 'pages' / '7_股神推薦.py'
    source=page.read_text('utf-8')
    tree=ast.parse(source)
    need=['_excel_safe_value','_excel_safe_df','_write_df_to_ws_fast_h84']
    functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in need]
    assert len(functions)==3
    start=source.index('    sheets = [\n        ("00_主管重點觀察榜", h144_focus,')
    end=source.index('    return output.getvalue()',start)+len('    return output.getvalue()')
    body='''def export(board,authority,trace,repair,details):
    from openpyxl import Workbook
    import io
    wb=Workbook()
    wb.remove(wb.active)
    output=io.BytesIO()
    h144_focus=board
    h144_focus_meta={'scanned':10,'diagnosed':len(board),'final':len(board),
         'received':len(board),'audit_count':1,'authority_count':len(authority),
         'all_scores_tied':False,'scope_issues':[],'valid_score_rows':1,
         'score_distinct':1,'h126_buyable':0,'warning':'H1482研究資料'}
    tables={'h126_authority':authority}
    h147_five_gates=pd.DataFrame()
    h148_trace=trace
    h148_meta={'review_unique':len(trace),'scope_status':'局部診斷池',
          'zero_formal_diagnosis':'無正式推薦'}
    h1481_meta={'eligible':len(board),'quarantine':1,'h113_missed_strong_research_count':0}
    h1482_repair=repair
    h1482_details=details
    h1482_meta={'same_day_h114':0,'candidate_not_h113':0}
    _h147_scan_diag={'status':'SOURCE_AUDIT'}
    _h145_bridge={'status':'UNPROVEN_SOURCE'}
'''+source[start:end]+'\n'
    ns={'pd':pd}
    exec(compile(ast.fix_missing_locations(ast.Module(body=functions,type_ignores=[])),str(page),'exec'),ns,ns)
    exec(compile(body,str(page),'exec'),ns,ns)
    return ns['export'](board,authority,trace,repair,details)


def test_h1482_actual_page07_empty_main_keeps_120_audit_rows_not_false_recommendations():
    bad=row('2344',H109決策權威='FADE_VETO',H114K線證據日=None)
    board,isolated,_,trace,_ = run([bad])
    assert board.empty
    audit=pd.DataFrame([bad])
    repair,details,meta=build_source_audit(candidate_frame=audit,audit_frame=audit,
             trace_frame=trace,quarantine_frame=isolated,research_day=DAY)
    authority=pd.DataFrame([{'股票代號':'2344','H126能否買進':'否'}])
    blob=_export_real_body(board,authority,trace,repair,details)
    wb=load_workbook(io.BytesIO(blob))
    assert wb.sheetnames[:2]==['00_主管重點觀察榜','00_H126主管決策'] and len(wb.sheetnames)==12
    main,official=wb.worksheets[:2]
    assert main['B8'].value not in ('2344',2344)
    assert main['A7'].value=='觀察順位' or main['A7'].value=='狀態'
    detail=wb['10_逐股排除明細']
    assert detail['A1'].value=='股票代號'
    assert any(str(detail.cell(r,1).value)=='2344' for r in range(2,detail.max_row+1))
    assert detail.freeze_panes=='A2'
    assert detail.auto_filter.ref==detail.dimensions
    assert '完整隔離原因' in [cell.value for cell in detail[1]]
    assert any(detail.cell(r,14).value is not None for r in range(2,detail.max_row+1))
    assert wb['09_資料來源品質'].max_row>=2
    assert any('否' in str(cell.value) for row in official for cell in row)
    wb.close()
