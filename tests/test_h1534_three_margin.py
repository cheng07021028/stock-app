# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pytest
from godpick_h115_pit_evidence import append_quarterly_margins, build_h114_evidence, evidence_digest
from godpick_h1534_three_margin import evaluate_three_rises
from tools.h1534_import_margins import parse_rows, TWSE_URL

OFFICIAL = 'https://openapi.twse.com.tw/v1/opendata/t187ap17_L'


def rows(code='2330'):
    # Two consecutive YoY rising quarters: 2026Q2/Q3 vs 2025Q2/Q3.
    seq=[('2025Q2',22.,10.,6.,'2025-08-12T18:00:00+08:00'),
         ('2025Q3',23.,11.,7.,'2025-11-14T18:00:00+08:00'),
         ('2025Q4',24.,12.,8.,'2026-03-10T18:00:00+08:00'),
         ('2026Q1',25.,13.,9.,'2026-05-13T18:00:00+08:00'),
         ('2026Q2',27.,15.,10.,'2026-08-12T18:00:00+08:00'),
         ('2026Q3',30.,18.,12.,'2026-11-09T18:00:00+08:00')]
    return [dict(code=code,year_quarter=q,gross_margin_pct=gm,
          operating_margin_pct=om,net_margin_pct=nm,
          industry_type='GENERAL',statement_basis='SINGLE_QUARTER',
          available_at=stamp,source='TWSE',source_url=OFFICIAL,
          availability_basis='PUBLISHED_VERIFIED')
          for q,gm,om,nm,stamp in seq]


def test_pit_yoy_two_breakout_and_no_future_leak(tmp_path: Path):
    store=tmp_path/'data'/'h115'
    r=append_quarterly_margins(store, rows())
    assert r['inserted']==6
    assert append_quarterly_margins(store, rows())['inserted']==0
    early=build_h114_evidence(tmp_path,'2026-11-08').get('2330',{})
    assert all(x['year_quarter']!='2026Q3' for x in early.get('H1534季度三率歷史',[]))
    assert not evaluate_three_rises(early.get('H1534季度三率歷史'),research_day='2026-11-08')['yoy_two']
    valid=build_h114_evidence(tmp_path,'2026-11-10')['2330']
    res=evaluate_three_rises(valid['H1534季度三率歷史'],research_day='2026-11-10')
    assert res['yoy_two'] and res['breakout'] and res['qoq']
    assert res['quarter']=='2026Q3'
    assert res['gross']==30 and res['operating']==18 and res['net']==12
    assert 'quarterly_margins.jsonl' in evidence_digest(tmp_path)['files']
    assert len(valid['H1534季度三率歷史'])==6


def test_net_profit_not_from_core_and_single_rise_not_certified():
    r=rows()
    r[-1]['operating_margin_pct']=8
    r[-1]['net_margin_pct']=50  # exceptional non-operating boost
    x=evaluate_three_rises(r,research_day='2026-11-10')
    assert not x['yoy_two']
    assert not x['breakout']


def test_missing_yoy_and_stale_not_promoted():
    assert not evaluate_three_rises(rows()[-1:],research_day='2026-11-10')['yoy_two']
    assert not evaluate_three_rises(rows()[:-1],research_day='2026-11-10')['yoy_two']
    assert evaluate_three_rises(rows(),research_day='2026-08-16')['quarter']=='2026Q2'


def test_reject_cumulative_and_fake_sources(tmp_path):
    bad=rows()[-1].copy();bad['statement_basis']='CUMULATIVE'
    with pytest.raises(ValueError):append_quarterly_margins(tmp_path,[bad])
    bad=rows()[-1].copy();bad['source_url']='https://not-official.example/fake'
    with pytest.raises(ValueError):append_quarterly_margins(tmp_path,[bad])
    bad=rows()[-1].copy();bad['gross_margin_pct']=float('nan')
    with pytest.raises(ValueError):append_quarterly_margins(tmp_path,[bad])
    assert not (tmp_path/'quarterly_margins.jsonl').exists()


def test_importer_parses_official_chinese_headers():
    src=[{'公司代號':'2330','年度':'115','季別':'3',
          '毛利率(%)(營業毛利)/(營業收入)':'30.5',
          '營業利益率(%)(營業利益)/(營業收入)':'18.2',
          '稅後純益率(%)(稅後純益)/(營業收入)':'13.1'}]
    x=parse_rows(src,source='TWSE',source_url=TWSE_URL,observed_at='2026-11-10T10:00:00+08:00')
    assert len(x)==1 and x[0]['year_quarter']=='115Q3'
    assert x[0]['availability_basis']=='FIRST_OBSERVED'


def test_rank_three_rises_raises_research_quality_not_h126():
    import pandas as pd
    from test_h153_growth_technical_ranking import diagnostic
    from godpick_h153_growth_setup_rank import build_growth_setup_ranking
    stock=diagnostic('2330',H114量縮墊高='是')
    stock['H114K線證據日']='2026-11-10'
    diag=pd.DataFrame([stock])
    data={'2330':{'H1534季度三率歷史':rows()}}
    ranked,summary,meta=build_growth_setup_ranking(
       diagnosis_frame=diag,research_frame=pd.DataFrame(),research_day='2026-11-10',
       verified_fundamentals=data)
    assert len(ranked)==1
    assert ranked.iloc[0]['H1534三率三升狀態'].startswith('連兩季')
    assert ranked.iloc[0]['H1534毛利營益突破']=='近五季新高'
    assert ranked.iloc[0]['研究主題']=='三率三升獲利突破／技術蓄勢'
    assert ranked.iloc[0]['排名等級'].startswith('A')
    assert '非H126' in ranked.iloc[0]['研究聲明']
    assert summary['檢查項目'].str.contains('H153.4已核連兩季').any()
    nofinance,_,_=build_growth_setup_ranking(
       diagnosis_frame=diag,research_frame=pd.DataFrame(),research_day='2026-11-10')
    assert nofinance.iloc[0]['研究證據分'] < ranked.iloc[0]['研究證據分']
    assert nofinance.iloc[0]['H1534三率三升狀態']=='待補官方單季財報'
    stock['H109決策權威']='FADE_VETO'
    veto,_,_=build_growth_setup_ranking(
       diagnosis_frame=pd.DataFrame([stock]),research_frame=pd.DataFrame(),
       research_day='2026-11-10',verified_fundamentals=data)
    assert veto.iloc[0]['排名等級'].startswith('C')


def test_real_excel_twelve_tabs_has_margins_but_no_h126_override():
    import io
    import pandas as pd
    from openpyxl import Workbook, load_workbook
    from test_h153_growth_technical_ranking import diagnostic
    from test_h151_excel_split import _writer
    from godpick_h151_workbook_sections import add_research_section_sheets, CORE_TABS
    from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
    from godpick_h153_growth_setup_rank import build_growth_setup_ranking,attach_rank_to_research,build_ranked_five_plans
    stock=diagnostic('2330',H114量縮墊高='是');stock['H114K線證據日']='2026-11-10'
    diagnosis=pd.DataFrame([stock])
    ranking,summary,_=build_growth_setup_ranking(diagnosis_frame=diagnosis,
        research_frame=pd.DataFrame(),research_day='2026-11-10',
        verified_fundamentals={'2330':{'H1534季度三率歷史':rows()}})
    research=attach_rank_to_research(pd.DataFrame(),ranking)
    plans=build_ranked_five_plans(ranked=ranking,diagnosis_frame=diagnosis,
         previous_plans=pd.DataFrame(),research_day='2026-11-10')
    wb=Workbook();wb.active.title=CORE_TABS[0];h126=wb.create_sheet(CORE_TABS[1])
    wb[CORE_TABS[0]]['A1']='主管觀察';h126['A1']='股票代號';h126['B1']='資格'
    h126['A2']='2330';h126['B2']='禁止買進'
    add_research_section_sheets(wb,writer=_writer,research=research,opportunities=plans,
                               research_summary=summary)
    normalize_frozen_sheet_views(wb)
    f=io.BytesIO();wb.save(f)
    reloaded=load_workbook(io.BytesIO(f.getvalue()))
    assert len(reloaded.sheetnames)==12
    assert reloaded['00_H126主管決策']['B2'].value=='禁止買進'
    for name in ('01_具名強勢研究','03_五檔研究計畫'):
        w=reloaded[name];heads=[c.value for c in w[1]]
        assert 'H1534三率三升狀態' in heads
        c=heads.index('H1534三率三升狀態')+1
        assert str(w.cell(2,c).value).startswith('連兩季')
    w=reloaded['02_研究召回統計']
    assert any('H153.4已核連兩季' in str(c.value) for row in w for c in row)
