# -*- coding: utf-8 -*-
import json
import subprocess
import sys
from pathlib import Path
from openpyxl import Workbook, load_workbook
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h171_workbook_audit import append_h171_research
from tools.h171_financial_archive import refresh

NOW='2026-11-19T17:32:00+08:00'


def workbook():
    wb=Workbook();src=wb.active;src.title='01_具名強勢研究'
    src.append(['股票代號','股票名稱','市場別','研究分','推薦角色'])
    src.append(['3227','原相','上市',42,'B｜研究參考'])
    src.append(['1477','聚陽','上市',41,'B｜研究參考'])
    nav=wb.create_sheet('11_交易員重點導航');nav['A1']='H169 已核 PE 警告';nav['B1']='保留'
    h126=wb.create_sheet('00_H126主管決策');h126['G2']='禁止買進'
    return wb


def sample():
    return [{'出表日期':'1151119','年度':'115','季別':'3',
             '公司代號':str(3000+i),'基本每股盈餘（元）':'2.56'} for i in range(24)]


class Response:
    def __init__(self, status=200):self.status_code=status
    def json(self):return sample()


def test_h171_archive_403_never_overwrites_original(tmp_path):
    path=tmp_path/'original.json';path.write_text('{"rows":[],"marker":"old"}',encoding='utf-8')
    z=refresh(get=lambda *a,**k:Response(403),observed_at=NOW,output=path)
    assert not z['saved']
    assert json.loads(path.read_text())['marker']=='old'


def test_h171_archive_first_observation_idempotent(tmp_path):
    path=tmp_path/'archive.json'
    z=refresh(get=lambda *a,**k:Response(),observed_at=NOW,output=path)
    assert z['saved'] and z['added_rows']==48
    assert json.loads(path.read_text(encoding='utf-8'))['rows'][0]['available_at']==NOW
    again=refresh(get=lambda *a,**k:Response(),observed_at='2026-11-20T17:32:00+08:00',output=path)
    assert not again['saved'] and again['status']=='NO_NEW_FINANCIAL_OBSERVATIONS'
    assert json.loads(path.read_text(encoding='utf-8'))['rows'][0]['available_at']==NOW


def test_h171_archive_corrupt_old_file_no_overwrite(tmp_path):
    path=tmp_path/'archive.json';path.write_text('{broken',encoding='utf-8')
    z=refresh(get=lambda *a,**k:Response(),observed_at=NOW,output=path)
    assert not z['saved'] and z['status']=='CORRUPT_ARCHIVE_NO_OVERWRITE'
    assert path.read_text(encoding='utf-8')=='{broken'


def test_h171_workbook_preserves_H126_and_H169_excel_and_order():
    wb=workbook();names=wb.sheetnames[:]
    report=append_h171_research(wb,financial_rows=[],research_day='2026-11-19')
    assert report['status']=='H171_RESEARCH_SECTION_APPENDED'
    assert report['research_rows']==2
    assert report['reconstructed_ttm_rows']==0
    assert report['trade_authority_changed'] is False
    assert wb.sheetnames==names
    assert wb['00_H126主管決策']['G2'].value=='禁止買進'
    assert wb['11_交易員重點導航']['A1'].value=='H169 已核 PE 警告'
    assert wb['01_具名強勢研究']['D2'].value==42
    assert wb['01_具名強勢研究']['D3'].value==41


def test_h171_workbook_missing_sheet_no_replacement():
    wb=Workbook();names=wb.sheetnames[:]
    assert append_h171_research(wb,financial_rows=[],research_day='2026-11-19')['status']=='MISSING_EXISTING_SHEETS'
    assert wb.sheetnames==names


def test_h171_workbook_cli_end_to_end(tmp_path):
    src=tmp_path/'old.xlsx';out=tmp_path/'verified.xlsx';f=tmp_path/'financial.json'
    workbook().save(src)
    f.write_text(json.dumps({'rows':[]}),encoding='utf-8')
    command=[sys.executable,str(ROOT/'tools'/'h171_research_cli.py'),
             '--workbook',str(src),'--financial',str(f),
             '--research-day','2026-11-19','--output',str(out)]
    subprocess.run(command,check=True,capture_output=True,text=True)
    assert out.exists() and out.stat().st_size>0
    opened=load_workbook(out,read_only=True)
    assert opened['00_H126主管決策']['G2'].value=='禁止買進'
    assert opened['11_交易員重點導航']['A1'].value=='H169 已核 PE 警告'
    assert len(opened.sheetnames)==3
    assert src.read_bytes()!=out.read_bytes()


def test_h171_workbook_t_plus_from_same_research_day_only():
    wb=workbook()
    perf={'summary':{'T+1':{'evaluated':1,'win_rate_pct':100},
                     'T+5':{'evaluated':0,'win_rate_pct':None}},
          'signals':[{'code':'3227','signal_date':'2026-11-19',
                      'returns':{'T+1':{'status':'EVALUATED_PRICE_ONLY','return_pct':2.5},
                                 'T+5':{'status':'NOT_MATURED','return_pct':None}}}]}
    result=append_h171_research(wb,financial_rows=[],research_day='2026-11-19',performance=perf)
    nav=wb['11_交易員重點導航']
    header=result['start_row']+5
    assert nav.cell(header+1,10).value==2.5
    assert nav.cell(header+1,11).value is None
    assert nav.cell(header+2,10).value is None
    assert nav.cell(result['start_row']+3,2).value.startswith('100% / --')
