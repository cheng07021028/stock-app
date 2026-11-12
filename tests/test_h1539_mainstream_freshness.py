# -*- coding: utf-8 -*-
"""H153.9: real research ranking transparency, no artificial sector leadership."""
import io
from datetime import date
from pathlib import Path
import pandas as pd
from openpyxl import load_workbook
from godpick_h1539_mainstream_freshness import (
    audit_mainstream_repetition,read_local_rotation_snapshots)
from test_h1537_manager_visible import make_workbook
from godpick_h1537_manager_visible import render_manager_visible,validate_manager_content,validate_exported_cross_tab
from godpick_h151_workbook_sections import add_research_section_sheets
from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
from test_h151_excel_split import _writer

DAY='2026-11-11'

def market_inputs():
    rec=[]
    for i in range(45):
        sector='新興族群' if i<7 else '傳統族群'
        code=str(3000+i)
        rec.append({'股票代號':code,'股票名稱':sector+str(i),'類別':sector,
            'H114K線證據日':DAY,'H114K線資料狀態':'VALID_OHLCV',
            'H114K線有效根數':100,'H113當日成交額百萬':300,
            'H114當日成交量張':2000,'H114當前5日漲跌幅%':7 if i<7 else -2,
            'H109決策權威':'UNKNOWN'})
    ranked=pd.DataFrame([
        {'研究排名':1,'股票代號':'3012','股票名稱':'舊名單','產業':'傳統族群',
         '排名等級':'B｜研究證據待補（不得交易）','研究證據分':55,
         '反證與待補':'H126禁止買進','研究主題':'箱型準突破'},
        {'研究排名':2,'股票代號':'3002','股票名稱':'新型態','產業':'新興族群',
         '排名等級':'B｜研究證據待補（不得交易）','研究證據分':45,
         '反證與待補':'H126禁止買進','研究主題':'量縮墊高'},
    ])
    hist=[{'date':'2026-11-09','rows':[{'股票代號':'3012'}]},
          {'date':'2026-11-10','rows':[{'股票代號':'3012'}]}]
    return ranked,pd.DataFrame(rec),hist

def test_peer_leadership_beats_repeat_with_no_confirmation():
    rank,diag,hist=market_inputs()
    got,stats,meta=audit_mainstream_repetition(rank,diag,research_day=DAY,snapshots=hist,
        history_source='既有輪動快照')
    assert got.iloc[0]['股票代號']=='3002'
    assert got.iloc[0]['研究排名']==1
    assert got.iloc[0]['H1539主流證據等級'].startswith('B')
    assert got.iloc[1]['H1539跨日快照次數']==2
    assert got.iloc[1]['H1539主流證據等級'].startswith('U')
    assert got.iloc[1]['研究證據分']==55 # do not rewrite evidence score
    assert '非H153' in got.iloc[1]['H1539跨日快照來源'] or '既有輪動快照' in got.iloc[1]['H1539跨日快照來源']
    assert meta['rotation_days']==2
    assert 'H153.9同輪廣度有效樣本' in stats['檢查項目'].tolist()

def test_h108_and_dated_institutional_are_required_for_highest_grade():
    rank,diag,hist=market_inputs()
    idx=diag.index[diag['股票代號']=='3002'][0]
    for k,v in {'H108族群生命週期':'IGNITION','H108族群前次資料日':'2026-11-10',
                'H108族群資金流變化':2.5,'H108族群廣度變化':3.0,
                'H108內部樣本可信度':60,'H123法人實證狀態':'PROVEN_FLOW',
                'H123法人資料日期':'2026-11-11'}.items():diag.loc[idx,k]=v
    got,_,_=audit_mainstream_repetition(rank,diag,research_day=DAY,snapshots=hist)
    assert got.iloc[0]['H1539主流證據等級'].startswith('A')
    # Future evidence must not become today's mainstream proof.
    diag.loc[idx,'H123法人資料日期']='2026-11-12'
    got,_,_=audit_mainstream_repetition(rank,diag,research_day=DAY)
    assert not got.loc[got['股票代號']=='3002','H1539主流證據等級'].iloc[0].startswith('A')

def test_missing_history_never_means_new_candidate():
    r,d,_=market_inputs()
    got,_,meta=audit_mainstream_repetition(r,d,research_day=DAY,snapshots=None)
    assert got['H1539跨日快照次數'].isna().all()
    assert '缺前輪快照' in got['H1539排序理由'].iloc[0]
    assert meta['rotation_days']==0

def test_old_or_crossday_quotes_cannot_be_mainstream():
    r,d,_=market_inputs()
    d.loc[d['股票代號']=='3002','H114K線證據日']='2026-11-10'
    got,_,_=audit_mainstream_repetition(r,d,research_day=DAY)
    assert got.loc[got['股票代號']=='3002','H1539主流證據等級'].iloc[0].startswith('U')

def test_veto_stays_veto_and_h126_not_claimed():
    r,d,_=market_inputs()
    d.loc[d['股票代號']=='3002','H109決策權威']='FADE_VETO'
    got,_,_=audit_mainstream_repetition(r,d,research_day=DAY)
    assert got.loc[got['股票代號']=='3002','H1539主流證據等級'].iloc[0].startswith('VETO')
    assert all('買進' not in str(x) or '禁止' in str(x) for x in got['H1539主流證據等級'])

def test_same_quality_ties_and_existing_rank_warning():
    r,d,_=market_inputs()
    r.loc[0,'研究證據分']=45
    r.loc[0,'股票代號']='3012'
    d=d[d['股票代號'].isin(['3002','3012'])]
    got,_,_=audit_mainstream_repetition(r,d,research_day=DAY)
    assert list(got['研究排名'])==[1,1]
    assert 'H153原研究排名' in got

def test_corrupt_history_or_missing_file_does_not_fetch_network(tmp_path):
    h,info=read_local_rotation_snapshots(tmp_path)
    assert h==[] and '未找到' in info
    (tmp_path/'godpick_rotation_history.json').write_text('invalid json',encoding='utf8')
    h,info=read_local_rotation_snapshots(tmp_path)
    assert h==[] and '失敗' in info

def test_actual_manager_workbook_shows_mainstream_quality_and_h126_zero():
    r,d,h=market_inputs()
    ranked,_,_=audit_mainstream_repetition(r,d,research_day=DAY,snapshots=h)
    wb,_,watch,_=make_workbook(0,0)
    normal=ranked.copy()
    research=pd.concat([normal],ignore_index=True)
    output=render_manager_visible(wb,ranked=normal,watch=watch,research=research,
        h126_buyable=0,research_day=DAY)
    assert output['a_b_rows']==2
    assert '主流' in str(wb['00_主管重點觀察榜']['F9'].value) or '族群' in str(wb['00_主管重點觀察榜']['F9'].value)
    assert '禁止買進' in wb['00_H126主管決策']['G2'].value
    assert validate_manager_content(wb,output,research=research)
    add_research_section_sheets(wb,writer=_writer,research=research)
    assert validate_exported_cross_tab(wb,output)
    normalize_frozen_sheet_views(wb)
    b=io.BytesIO();wb.save(b)
    file=load_workbook(io.BytesIO(b.getvalue()))
    assert len(file.sheetnames)==12
    assert '主流' in file['01_具名強勢研究']['J2'].value or 'H1539' in ''.join(str(v.value) for v in file['01_具名強勢研究'][1])
    assert file['00_H126主管決策']['G2'].value=='禁止買進'
