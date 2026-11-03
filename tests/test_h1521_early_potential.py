# -*- coding: utf-8 -*-
"""H152.1: pre-breakout opportunities, not buying authorizations."""
import io
from pathlib import Path
import pandas as pd
from openpyxl import Workbook,load_workbook
from godpick_h1521_early_potential import build_early_potential
from godpick_h149_trade_intelligence import build_six_pillar_review
from godpick_h151_workbook_sections import add_research_section_sheets, CORE_TABS, TABS_COLOR
from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
from test_h151_excel_split import _writer
DAY='2026-11-02'

def candidate(code='2330', **updates):
    r={'股票代號':code,'股票名稱':'測試潛力股','類別':'半導體',
       'H114K線證據日':DAY,'H114K線資料狀態':'VALID_OHLCV','H114K線有效根數':85,
       'H114當日收盤價':100,'H114當前5日漲跌幅%':2.3,'H114當日漲跌幅%':0.3,
       'H114K線量比':0.8,'H114當日成交量張':7000,
       'H113當日成交額百萬':240,'H113二十日均成交額百萬':270,
       'H114波動收斂':'是','H114量縮墊高':'是','H114距20日高點%':4.0,
       'H109決策權威':'UNKNOWN','H123法人實證狀態':'PENDING_EVIDENCE',
       'H124大戶實證狀態':'PENDING_EVIDENCE'}
    r.update(updates);return r

def build(rows, old=None):
    return build_early_potential(diagnosis_frame=pd.DataFrame(rows),
          research_frame=pd.DataFrame(old or []),research_day=DAY, limit=12)

def test_quiet_liquid_prebreakout_is_visible_and_no_formal_trade():
    pot,combined,stat,meta=build([candidate()])
    assert len(pot)==1 and len(combined)==1
    assert pot.iloc[0]['研究層級'].startswith('P1｜')
    assert pot.iloc[0]['量比']==.8
    assert '非H126買進授權' in pot.iloc[0]['潛力研究聲明']
    assert meta['formal_h126_modified'] is False
    assert 'H152.1預發動候選總數' in stat['檢查項目'].tolist()

def test_no_overheated_or_weak_il_liquid_cold_stocks():
    invalid=[{'H114當前5日漲跌幅%':15}, {'H114當日漲跌幅%':8},
             {'H114K線量比':.2}, {'H114當日成交量張':50},
             {'H113二十日均成交額百萬':20}, {'H114距20日高點%':40},
             {'H105發動階段':'ALREADY-IGNITED'},
             {'H114量縮墊高':'否','H114波動收斂':'否'}]
    for change in invalid:
        pot,*_=build([candidate(**change)])
        assert pot.empty,change

def test_missing_date_and_fake_technical_data_are_rejected():
    for change in [{'H114K線證據日':'2026-10-30'}, {'H114K線有效根數':20},
                   {'H114K線資料狀態':'VALID_OHLC_ONLY_VOLUME_UNIT_UNKNOWN'},
                   {'H114當前5日漲跌幅%':None}]:
        pot,*_=build([candidate(**change)]);assert pot.empty

def test_duplicate_ticker_not_cherry_picked():
    pot,_,stats,meta=build([candidate(),candidate(**{'H114當日漲跌幅%':1.5})])
    assert pot.empty and meta['excluded']['重複代號隔離']==1

def test_potential_prioritized_before_old_momentum_without_loss():
    old=[{'研究層級':'A｜同日量價研究已核','股票代號':'3008',
         '股票名稱':'已發動觀察','量比':2}]
    pot,merged,_,_=build([candidate()],old)
    assert merged['股票代號'].tolist()==['2330','3008']
    assert merged.iloc[1]['研究層級']==old[0]['研究層級']

def test_h149_five_stocks_accept_prebreakout_without_faking_price():
    pot,merged,_,_=build([candidate()])
    formal=pd.DataFrame([{'股票代號':'2330','交易資格':'不可買進'}])
    ideas,notes,meta=build_six_pillar_review(
       diagnosis_frame=pd.DataFrame([candidate()]),research_frame=merged,
       authority_frame=formal,research_day=DAY,
       h145_bridge={'same_run_proven':False,'candidate_complete':False})
    assert len(ideas)==1
    assert ideas.iloc[0]['股票代號']=='2330'
    assert ideas.iloc[0]['研究層級'].startswith('P1｜')
    assert '蓄勢尚未確認突破' in ideas.iloc[0]['反證／失效條件']
    assert pd.isna(ideas.iloc[0]['參考風險報酬比'])
    assert ideas.iloc[0]['交易資格'].startswith('研究觀察')

def test_excel_keeps_twelve_tabs_no_h126_override_and_potential_first():
    potential, merged,stat,_=build([candidate()])
    wb=Workbook();wb.active.title=CORE_TABS[0];formal=wb.create_sheet(CORE_TABS[1]);formal['A2']='禁止買進'
    wb[CORE_TABS[0]].freeze_panes='D8'
    add_research_section_sheets(wb,writer=_writer,research=merged,
        research_summary=stat,opportunities=pd.DataFrame([{'股票代號':'2330','交易資格':'研究觀察／非交易'}]))
    normalize_frozen_sheet_views(wb)
    f=io.BytesIO();wb.save(f);re=load_workbook(io.BytesIO(f.getvalue()))
    assert len(re.sheetnames)==12
    assert re[CORE_TABS[1]]['A2'].value=='禁止買進'
    assert re['01_具名強勢研究']['A2'].value=='P1｜量縮墊高與波動收斂'
    for name in re.sheetnames:
        assert re[name].sheet_properties.tabColor.rgb[-6:]==TABS_COLOR[name]

def test_real_page_wires_early_potential_into_research_and_no_new_tab():
    source=(Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py').read_text('utf-8')
    assert 'from godpick_h1521_early_potential import build_early_potential' in source
    assert 'research=locals().get(\'h152_named_research\'' in source
    assert 'h1521_opportunities' in source
    assert "tables['h126_authority']" in source


def test_h114_current_five_day_is_asof_and_never_uses_future_sessions():
    from datetime import date, timedelta
    from godpick_h114_parallel_alpha import analyze_history
    start=date(2026,7,1);dates=[]; current=start
    for i in range(82):
        while current.weekday()>=5:current+=timedelta(days=1)
        c=80+i*.12
        dates.append({'日期':str(current),'開盤價':c-.05,'最高價':c+.1,
            '最低價':c-.1,'收盤價':c,'成交量(張)':2000})
        current+=timedelta(days=1)
    frame=pd.DataFrame(dates)
    end=dates[-4]['日期']
    out=analyze_history(frame,asof=end)
    expected=(dates[-4]['收盤價']/dates[-9]['收盤價']-1)*100
    assert abs(out['H114當前5日漲跌幅%']-expected)<.01
    assert out['H114K線證據日']==end
    assert out['H114當日收盤價']==dates[-4]['收盤價']

def test_explicit_h109_veto_is_deprioritized_not_reinterpreted_as_bullish():
    p1=candidate('2352', **{'H109決策權威':'FADE_VETO'})
    p2=candidate('2603', **{'H109決策權威':'ALLOW'})
    lead,merged,_,_=build([p1,p2])
    assert lead.iloc[0]['股票代號']=='2603'
    assert 'H109:FADE_VETO' in lead.iloc[1]['預發動風險反證']
