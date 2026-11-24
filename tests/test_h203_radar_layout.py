from io import BytesIO
from copy import deepcopy
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table
from godpick_h175_evidence_rescue import append_h175_excel, HEADERS
from test_h175_evidence_rescue import book, mock_input
from godpick_h175_evidence_rescue import build_h175_audit

def build():
    d,t,a=mock_input()
    a.update(h85_waiting_codes=["2892","3016","0055"],h85_emerging_codes=["3016"])
    return build_h175_audit(diagnosis=d,tables=t,h154_audit=a,study_day="2026-11-19")


def test_names_and_diagnostics_are_separate_roundtrip():
    w=book(); m=w['00_主管重點觀察榜']; m['B1']='摘要完整保留'; m.column_dimensions['B'].width=16
    report=build(); original=deepcopy(report); sheets=w.sheetnames[:]; authority=list(w['00_H126主管決策'].values)
    res=append_h175_excel(w,report); top=res['manager_start_row']; buf=BytesIO(); w.save(buf); buf.seek(0)
    w=load_workbook(buf);m=w['00_主管重點觀察榜']; n=w['11_交易員重點導航']
    assert [m.cell(top+2,c).value for c in range(1,6)] == ['來源序／代號','股票名稱','股票池','H114資料問題','優先查證／說明']
    for i,r in enumerate(report['radar'][:10],top+3):
        assert m.cell(i,2).value == r['股票名稱']
        assert m.cell(i,3).value == r['股票池']
        assert m.cell(i,4).value == r['H114量價狀態']
        assert m.cell(i,5).value == r['先補的資料／禁止誤判']
        assert m.cell(i,2).font.bold and m.cell(i,5).alignment.wrap_text
        assert f'E{i}:G{i}' in m.merged_cells
    assert m.column_dimensions['B'].width==18
    assert m['B1'].value=='摘要完整保留' and 'B1:G1' in m.merged_cells
    assert m['A8'].value=='preserve'
    assert list(w['00_H126主管決策'].values)==authority
    assert w.sheetnames==sheets and report==original
    assert n.cell(res['navigation_start_row']+3,2).value=='股票代號'
    assert n.cell(res['navigation_start_row']+3,3).value=='股票名稱'
    assert n.cell(res['navigation_start_row']+4,3).value==report['radar'][0]['股票名稱']


def test_no_overwrite_of_summary_or_existing_rank_table():
    w=book();m=w.active;m['B1']='摘要';m['C1']='不得覆蓋';m['A7']='順位';m['B7']='代號';m['C7']='名稱';m['B8']='2330';m['C8']='台積電';m.add_table(Table(displayName='OldRank',ref='A7:C8'));m.auto_filter.ref='A7:C8';m.freeze_panes='D8'
    append_h175_excel(w,build())
    assert m['C1'].value=='不得覆蓋' and 'B1:G1' not in m.merged_cells
    assert m['B8'].value=='2330' and m['C8'].value=='台積電'
    assert m.tables['OldRank'].ref=='A7:C8' and m.auto_filter.ref=='A7:C8' and m.freeze_panes=='D8'


def test_empty_radar_no_fake_names():
    w=book();r=build();r['radar']=[];x=append_h175_excel(w,r)
    assert x['radar_rows']==0 and w.active.cell(x['manager_start_row']+3,2).value is None


def test_top10_and_long_names_keep_full_nav():
    w=book();r=build();template=r['radar'][0];r['radar']=[dict(template,股票代號=str(3000+i),股票名稱='測試長名稱'+str(i),原來源順序=i+1) for i in range(12)]
    result=append_h175_excel(w,r); top=result['manager_start_row'];nav=w['11_交易員重點導航'];start=result['navigation_start_row']
    assert w.active.cell(top+12,2).value=='測試長名稱9'
    assert w.active.cell(top+13,2).value is None
    assert nav.cell(start+15,3).value=='測試長名稱11'
