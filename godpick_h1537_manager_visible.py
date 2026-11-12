# -*- coding: utf-8 -*-
"""H153.7 | Visible named manager research lists and zero-authority explanation.

Only changes workbook presentation. Never changes H109/H126 permissions, scans,
ranking scores, historical records, or any durable settings.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import math
import pandas as pd

VERSION = 'V191-H153.7-MANAGER-VISIBLE'
_HEADERS = ['研究順位','股票代號','股票名稱','研究等級','研究證據分',
            '研究主題','技術證據','基本面／三率','H109風控證據',
            'H126交易資格','研究日期','研究反證／下一步']


def _text(value: Any, max_len: int = 250) -> str:
    if value is None: return ''
    try:
        if pd.isna(value): return ''
    except (TypeError, ValueError):
        pass
    s = str(value).strip()
    if s.lower() in ('nan', 'none', '<na>', 'nat'): return ''
    # Excel formula injection is not appropriate for external research labels.
    if s.startswith(('=', '+', '-', '@')) and not s.replace('.', '', 1).lstrip('+-').isdigit():
        s = "'" + s
    return s[:max_len]


def _code(value: Any) -> str:
    s = _text(value, 32)
    if s.endswith('.0') and s[:-2].isdigit(): s=s[:-2]
    return s.zfill(4) if s.isdigit() and len(s) < 4 else s


def _records(frame: Any) -> list[dict]:
    return frame.to_dict('records') if isinstance(frame, pd.DataFrame) and not frame.empty else []


def _first(record: Mapping, *fields: str) -> str:
    for key in fields:
        val=_text(record.get(key))
        if val and val not in ('未驗證','未認證','待補證','不適用'):
            return val
    return ''


def _technical(record: Mapping) -> str:
    evidence=[]
    for key in ('黃金交叉','箱型型態','H1532布林收斂','H1532OBV量價背離'):
        val=_text(record.get(key),80)
        if val and val not in ('未驗證','未認證','無','否','待補') and not val.startswith(('未驗','待補')):
            evidence.append(f'{key.replace("H1532", "")}:{val}')
    if not evidence:
        val=_first(record,'技術觀察證據','研究成立理由','研究主題')
        return val[:145] or '技術證據待補'
    return '；'.join(evidence)[:145]


def _finance(record: Mapping) -> str:
    parts=[]
    three=_text(record.get('H1534三率三升狀態'),55)
    if three: parts.append('三率:'+three)
    for field,label in (('近四季EPS','EPS'),('本益比','PE'),('營收年增率%','營收YoY')):
        val=_text(record.get(field),25)
        if val: parts.append(label+':'+val)
    return '｜'.join(parts)[:150] if parts else '官方財報／三率尚待核實'


def _named_rows(ranked: Any, watch: Any, research: Any, h126_buyable: int, research_day: Any):
    valid=[];seen=set()
    normal=[r for r in _records(ranked) if _text(r.get('排名等級')).startswith(('A','B'))]
    # Fall back to research sheet for old tests/call paths where ranked was not
    # passed; NEVER fabricate a rank for an unranked research record.
    if not normal:
        normal=[r for r in _records(research)
                if _text(r.get('排名等級')).startswith(('A','B')) and _text(r.get('研究排名'))]
    for r in normal:
        c=_code(r.get('股票代號'))
        if not c or c in seen: continue
        seen.add(c);valid.append(r)
    # Input rank order is already evidence-ranked by H153. Maintain competition
    # ranks and only use code as stable display order, never as new score.
    valid.sort(key=lambda r:(float(_text(r.get('研究排名'))) if _text(r.get('研究排名')).replace('.','',1).isdigit() else math.inf,
                             -float(_text(r.get('研究證據分'))) if _text(r.get('研究證據分')).replace('.','',1).isdigit() else 0,
                             _code(r.get('股票代號'))))
    w=[];seen_w=set()
    for r in _records(watch):
        c=_code(r.get('股票代號'))
        if not c or c in seen or c in seen_w: continue
        seen_w.add(c);w.append(r)
    w.sort(key=lambda r:(float(_text(r.get('核查優先序'))) if _text(r.get('核查優先序')).replace('.','',1).isdigit() else math.inf,
                         _code(r.get('股票代號'))))
    day=_text(research_day,40)
    trade_note='禁止買進｜H126本輪0檔' if int(h126_buyable or 0)==0 else '研究非交易｜須到H126逐股核對'
    normal_rows=[]
    for r in valid:
        normal_rows.append([
            _text(r.get('研究排名'),20), _code(r.get('股票代號')),_text(r.get('股票名稱'),35),
            _text(r.get('排名等級'),65),_text(r.get('研究證據分'),20),
            (_first(r,'研究主題','H153成長與技術主題')+'｜'+_first(r,'H1539主流證據等級'))[:125] or '研究主題待核',
            _technical(r),_finance(r),
            _first(r,'H109風控','H109證據分類','H109決策權威')[:95] or '風控待核',
            trade_note,_first(r,'研究交易日','行情證據日','H114行情日期') or day,
            _first(r,'反證與待補','H1539排序理由','H153來源與反證','尚需驗證與不得買進原因')[:230] or '需複核當日資料及交易資格'])
    watch_rows=[]
    for r in w:
        watch_rows.append([
            'W-'+(_text(r.get('核查優先序'),12) or '待核'),_code(r.get('股票代號')),
            _text(r.get('股票名稱'),35),'W｜非交易補證', '',
            _first(r,'研究分類')[:80] or '需補證研究',
            _technical(r),_finance(r),
            _first(r,'H109風控','H109證據分類')[:95] or '風控待核',
            '禁止買進｜非交易核查',_first(r,'研究交易日','行情證據日') or day,
            _first(r,'反證與待補','技術觀察證據')[:230] or '僅核查，不得視為推薦'])
    return normal_rows,watch_rows


def render_manager_visible(wb:Any, *, ranked:Any=None, watch:Any=None,
                           research:Any=None,h126_buyable:Any=0,
                           research_day:Any=None,h145_status:str='') -> dict:
    """Place named ranked table in visible A:L near top, preserve H148.1 below.

    H126 authority cells are NEVER edited; only empty authority explanation is
    placed beneath the existing original H126 table.
    """
    from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
    from openpyxl.worksheet.table import Table, TableStyleInfo
    from openpyxl.utils.cell import range_boundaries,get_column_letter

    ws=wb['00_主管重點觀察榜']
    authority=wb['00_H126主管決策']
    try: trade=max(0,int(h126_buyable))
    except (TypeError,ValueError): trade=0
    normal_rows,watch_rows=_named_rows(ranked,watch,research,trade,research_day)
    # Preserve everything originally in the H148.1 section and its filter;
    # openpyxl insert_rows does not relocate the filter by itself.
    old_end=ws.max_row
    legacy_head=7  # no visible candidates: keep the exact legacy H146 filter contract
    if normal_rows or watch_rows:
        old_filter=ws.auto_filter.ref or ''
        base_main=9
        watch_title=base_main+max(1,len(normal_rows))+1
        watch_header=watch_title+1
        watch_first=watch_header+1
        audit_note=watch_first+max(1,len(watch_rows))+1
        legacy_head=audit_note+2
        shift=legacy_head-7
        ws.insert_rows(7,amount=shift)
        if old_filter:
            try:
                lo_col,lo_row,hi_col,hi_row=range_boundaries(old_filter)
                ws.auto_filter.ref=f'{get_column_letter(lo_col)}{lo_row+shift}:{get_column_letter(hi_col)}{hi_row+shift}'
            except ValueError:
                ws.auto_filter.ref=f'A{legacy_head}:O{old_end+shift}'
        ws.sheet_view.showGridLines=False
        ws.freeze_panes='D8'  # preserve manager navigation/freeze contract

        ws.cell(7,1,'H153.7｜主管可參考研究順位（非正式買進建議）')
        ws.cell(7,2,f'A／B 具名研究 {len(normal_rows)} 檔｜W 非交易核查 {len(watch_rows)} 檔｜H126 可買進 {trade} 檔')
        ws.row_dimensions[7].height=30
        for idx,label in enumerate(_HEADERS,1): ws.cell(8,idx,label)
        for i,row in enumerate(normal_rows,base_main):
            for col,value in enumerate(row,1): ws.cell(i,col,value)
        if not normal_rows:
            ws.cell(base_main,1,'本輪沒有經驗證的 A/B 具名研究；請核對01表及資料來源，不能硬補名次')
        ws.cell(watch_title,1,'H153.7｜W級非交易補證優先觀察（不等於買進推薦）')
        ws.cell(watch_title,2,f'實際可見不同股票 {len(watch_rows)} 檔；與上方 A/B 代號不重複')
        for idx,label in enumerate(_HEADERS,1):ws.cell(watch_header,idx,label)
        for i,row in enumerate(watch_rows,watch_first):
            for col,value in enumerate(row,1):ws.cell(i,col,value)
        if not watch_rows:ws.cell(watch_first,1,'本輪沒有額外 W 級具名核查股票；不強制湊榜')
        ws.cell(audit_note,1,'原始 H148.1 量價五關研究／舊版稽核完整保留下方')
        ws.cell(audit_note,2,'原始資料未刪除；下方原有 AutoFilter 仍在，必要時可查看 H148.1 證據')
        for row_number,fill in ((7,'164E63'),(watch_title,'78511E'),(audit_note,'334155')):
            for col in range(1,13):
                cell=ws.cell(row_number,col)
                cell.fill=PatternFill('solid',fgColor=fill)
                cell.font=Font(name='Microsoft JhengHei',bold=True,color='FFFFFF',size=11)
                cell.alignment=Alignment(vertical='center',wrap_text=True)
            ws.row_dimensions[row_number].height=32
        for row_number,fill in ((8,'0F766E'),(watch_header,'9A6700')):
            for col in range(1,13):
                cell=ws.cell(row_number,col)
                cell.fill=PatternFill('solid',fgColor=fill)
                cell.font=Font(name='Microsoft JhengHei',bold=True,color='FFFFFF',size=10)
                cell.alignment=Alignment(vertical='center',horizontal='center',wrap_text=True)
            ws.row_dimensions[row_number].height=34
        for first,records in ((base_main,normal_rows),(watch_first,watch_rows)):
            for i,row in enumerate(records,first):
                for col in range(1,13):
                    cell=ws.cell(i,col)
                    cell.alignment=Alignment(vertical='top',wrap_text=col in (4,6,7,8,9,10,12))
                    if i%2==0: cell.fill=PatternFill('solid',fgColor='EFF6FA')
                    if col==10:cell.font=Font(name='Microsoft JhengHei',bold=True,color='9F1239',size=10)
                ws.row_dimensions[i].height=46
        for col,width in {'A':16,'B':15,'C':19,'D':27,'E':13,'F':24,
                          'G':28,'H':30,'I':24,'J':27,'K':17,'L':66}.items():
            ws.column_dimensions[col].width=width
        if normal_rows:
            tab=Table(displayName='H1537ManagerResearch',ref=f'A8:L{base_main+len(normal_rows)-1}')
            tab.tableStyleInfo=TableStyleInfo(name='TableStyleMedium2',showRowStripes=False)
            ws.add_table(tab)
        if watch_rows:
            tab=Table(displayName='H1537VerificationWatch',ref=f'A{watch_header}:L{watch_first+len(watch_rows)-1}')
            tab.tableStyleInfo=TableStyleInfo(name='TableStyleMedium9',showRowStripes=False)
            ws.add_table(tab)
    # Retain original H126 A1:Tn data and authority status. Explanatory note
    # is only appended when authority has NO qualified securities.
    if trade==0:
        note_start=max(4,authority.max_row+2)
        note=[('H153.7｜本輪 H126 正式交易授權','0 檔｜禁止買進；此表無股票資料列是正常的風控結果'),
              ('為何研究有股票、這張表沒有？',
               'A/B/W 均只是研究或補證名單，未通過全部 H126 官方證據與交易授權，不得視為下單訊號'),
              ('目前資料品質／限制',
               f'H145：{_text(h145_status,110) or "來源待核"}；需查核正式輸出完整性與法人／大戶等權威證據'),
              ('請先閱讀','00_主管重點觀察榜：具名研究與W核查；01_具名強勢研究：逐股證據；02：完整稽核')]
        for r,(label,explain) in enumerate(note,note_start):
            authority.cell(r,1,label)
            authority.merge_cells(start_row=r,start_column=2,end_row=r,end_column=11)
            authority.cell(r,2,explain)
            authority.cell(r,1).fill=PatternFill('solid',fgColor='334155')
            authority.cell(r,1).font=Font(name='Microsoft JhengHei',color='FFFFFF',bold=True,size=10)
            authority.cell(r,2).font=Font(name='Microsoft JhengHei',color='9F1239' if r==note_start else '334155',bold=r==note_start,size=11)
            authority.cell(r,2).alignment=Alignment(wrap_text=True,vertical='center')
            authority.row_dimensions[r].height=34
        authority.column_dimensions['A'].width=max(authority.column_dimensions['A'].width or 0,32)
    return {'a_b_rows':len(normal_rows),'w_rows':len(watch_rows),'h126_authorized':trade,
            'main_codes':[r[1] for r in normal_rows], 'watch_codes':[r[1] for r in watch_rows],
            'legacy_header_row':legacy_head,'original_legacy_rows':old_end-6,
            'visible_manager_table':bool(normal_rows or watch_rows)}


def validate_manager_content(wb:Any, result:Mapping, *,research:Any=None):
    """Fail export rather than silently deliver an empty manager list."""
    ws=wb['00_主管重點觀察榜']
    valid=int(result['a_b_rows'])+int(result['w_rows'])
    if valid and not ws.tables:
        raise ValueError('H153.7 主管榜有具名候選，但沒有可篩選的股票明細表')
    codes=list(result['main_codes'])+list(result['watch_codes'])
    if len(codes)!=len(set(codes)):
        raise ValueError('H153.7 主管榜研究與W股票代號重複')
    research_codes={_code(r.get('股票代號')) for r in _records(research)}
    missing=set(codes)-research_codes if research_codes else set()
    if missing:
        raise ValueError('H153.7 主管榜與01具名研究不一致: '+','.join(sorted(missing)[:8]))
    authority=wb['00_H126主管決策']
    headings=[str(c.value or '') for c in authority[1]]
    if 'H126能否買進' not in headings:
        raise ValueError('H153.7 H126權威欄位不一致')
    trade_col=headings.index('H126能否買進')+1
    if int(result['h126_authorized'])==0:
        statuses=[str(authority.cell(row,trade_col).value or '') for row in range(2, min(authority.max_row,3)+1)]
        if not any(('禁止買進' in t or t in {'否','不得買進','不允許'}) for t in statuses):
            raise ValueError('H153.7 正式0檔但H126缺禁止買進標示')
    return True

def validate_exported_cross_tab(wb: Any, result: Mapping) -> bool:
    """Verify ACTUAL cells after all 12 worksheets have been generated."""
    if wb.sheetnames[:2]!=['00_主管重點觀察榜','00_H126主管決策']:
        raise ValueError('H153.7 主管首頁／H126位置被改變')
    if len(wb.sheetnames)!=12:
        raise ValueError('H153.7 主管分類工作表不是12張')
    expected=set(result['main_codes'])|set(result['watch_codes'])
    if not expected:
        # Legacy/no-research export does not have to contain a named stock table.
        # Never invent names merely to pass content acceptance.
        return True
    research=wb['01_具名強勢研究']
    headers=[str(c.value or '') for c in research[1]]
    if '股票代號' not in headers:
        raise ValueError('H153.7 01具名研究缺股票代號')
    col=headers.index('股票代號')+1
    source_codes={_code(research.cell(i,col).value) for i in range(2,research.max_row+1)}
    expected=set(result['main_codes'])|set(result['watch_codes'])
    if expected-source_codes:
        raise ValueError('H153.7 真實匯出00/01股票代號不一致: '+','.join(sorted(expected-source_codes)[:8]))
    manager=wb['00_主管重點觀察榜']
    actual_codes=set()
    for table_name in ('H1537ManagerResearch','H1537VerificationWatch'):
        if table_name not in manager.tables:continue
        from openpyxl.utils.cell import range_boundaries
        _minc,top,_maxc,bottom=range_boundaries(manager.tables[table_name].ref)
        for r in range(top+1,bottom+1):
            code=_code(manager.cell(r,2).value)
            if not code or code in actual_codes:
                raise ValueError('H153.7 主管榜代號空白／重複')
            actual_codes.add(code)
    if actual_codes != expected:
        raise ValueError('H153.7 主管榜有研究股但未實際寫入可見表格')
    if expected and (not manager['A7'].value or '主管可參考研究' not in str(manager['A7'].value)):
        raise ValueError('H153.7 首頁未建立具名排名區')
    return True

__all__=['VERSION','render_manager_visible','validate_manager_content','validate_exported_cross_tab']
