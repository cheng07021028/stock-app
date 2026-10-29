# -*- coding: utf-8 -*-
"""H147 adversarial evidence review: synthetic daily facts only."""
from pathlib import Path
import ast
import io
import pandas as pd
from godpick_h147_five_gate_review import evaluate_five_gates, build_five_gate_review
from godpick_h144_focus_board import build_focus_board

ROOT = Path(__file__).resolve().parents[1]


def row(**kw):
    r={
        '股票代號':'2317','股票名稱':'測試樣本','H110研究資訊截止日':'2026-10-28',
        'H114K線證據日':'2026-10-28','H110證據狀態':'OFFICIAL_VERIFIED',
        'H113當日成交額百萬':360,'H113二十日均成交額百萬':250,
        'H113當日成交量張':2200,'H113流動性合格':'是',
        'H114K線量比':1.6,'H105發動階段':'BH1',
        'H105未來發動窗口':'1～5日（待驗）', 'H105技術蓄勢分':75,
        '突破準備分':65,'H108族群生命週期':'IGNITION',
        'H108族群確認分':80,'H124大戶實證狀態':'VERIFIED_RISING',
        'H124大戶變化pp':0.17,'H109決策權威':'RESEARCH_ELIGIBLE',
        '收盤距MA20%':3,'收盤距MA60%':8,
    }
    r.update(kw)
    return r


def test_five_factors_from_dated_synthetic_facts_but_never_trade():
    g=evaluate_five_gates(row())
    assert g['成交量能']=='量能支持'
    assert g['起漲位置']=='預發動條件待驗'
    assert g['強勢族群']=='族群方向支持'
    assert g['大戶鎖碼']=='官方週增已驗'
    assert g['技術結構']=='技術研究支持'
    assert g['H126買進資格'].startswith('未經本模組授權')
    assert g['證據支持項數']==4


def test_missing_market_inputs_never_pass_volume_or_technical():
    g=evaluate_five_gates({'股票代號':'2344','H110研究資訊截止日':'2026-10-28',
                           'H113流動性合格':'是'})
    assert g['成交量能']=='待補證'
    assert g['技術結構']=='待核日期'
    assert g['大戶鎖碼']=='待補證'
    assert g['H126買進資格'].startswith('未經本模組授權')


def test_old_k_line_cannot_be_called_today_expansion():
    g=evaluate_five_gates(row(H114K線證據日='2026-10-27'))
    assert g['成交量能']=='待核日期'
    assert g['技術結構']=='待核日期'


def test_future_data_never_counts_as_volume_or_technical():
    g=evaluate_five_gates(row(H114K線證據日='2026-10-29'))
    assert g['成交量能']=='資料衝突'
    assert g['起漲位置']=='資料衝突'
    assert g['技術結構']=='資料衝突'


def test_h113_failed_but_positive_numbers_are_conflict_not_success():
    g=evaluate_five_gates(row(H113流動性合格='否'))
    assert g['成交量能']=='資料衝突'


def test_low_actual_volume_fails_existing_150m_and_500_lots():
    g=evaluate_five_gates(row(H113當日成交額百萬=100,H113當日成交量張=400))
    assert g['成交量能']=='未過門檻'
    assert '150/150' in g['量能明細']


def test_historical_ignition_is_not_a_buy_signal():
    g=evaluate_five_gates(row(H105發動階段='ALREADY-IGNITED'))
    assert g['起漲位置']=='已發動／防追高'
    assert g['研究分類']=='已發動強勢｜不冒稱起漲'
    assert 'H126' in g['H126買進資格']


def test_fade_veto_remains_even_if_other_four_gates_positive():
    g=evaluate_five_gates(row(H109決策權威='FADE_VETO',H108族群生命週期='FADE'))
    assert g['強勢族群']=='退潮否決／待查來源'
    assert g['H109原權威']=='FADE_VETO'
    assert g['H126買進資格'].startswith('未經本模組授權')


def test_tdcc_proxy_pp_without_h124_proof_must_not_claim_lock():
    g=evaluate_five_gates(row(H124大戶實證狀態='PENDING_EVIDENCE',
         TDCC千張大戶週變化pp=0.3,TDCC大戶資料日期='2026-10-23',TDCC大戶資料狀態='OFFICIAL'))
    assert g['大戶鎖碼']=='數值待H124驗證'


def test_overheated_ma20_flags_risk_without_changing_h126():
    g=evaluate_five_gates(row(**{'收盤距MA20%':15}))
    assert g['技術結構']=='追高風險'


def test_duplicated_codes_not_double_counted_or_promoted():
    df,meta=build_five_gate_review(pd.DataFrame([row(),row(H109決策權威='FADE_VETO')]))
    assert meta['reviewed']==1 and meta['duplicate_codes']==['2317']
    assert df.loc[0,'研究分類'].startswith('同代號重複')
    assert '禁止' in df.loc[0,'H126買進資格']


def test_h147_does_not_alter_h146_formal_sheet_and_research_rank():
    raw=row(**{'H110最終研究優先分':52,'類別':'AI設備'})
    frame,meta=build_focus_board({'audit':pd.DataFrame([raw]),
        'h126_authority':pd.DataFrame([{'股票名稱':'無合格股票','H126能否買進':'禁止買進'}])},
        {'status':'EXPORT_SCOPE_INCOMPLETE','scope_incomplete':True})
    assert frame.loc[0,'H110研究分']==52
    assert frame.loc[0,'買進資格']=='禁止依本榜買進'
    assert '量:量能支持' in frame.loc[0,'下一個必查條件']
    assert '戶:官方週增已驗' in frame.loc[0,'下一個必查條件']
    assert meta['h126_buyable']==0


def test_no_network_no_persistent_writes_no_h126_patch():
    text=(ROOT/'godpick_h147_five_gate_review.py').read_text(encoding='utf-8')
    assert not any(s in text for s in ('requests.get(', 'urlopen(', 'to_csv(', 'to_json(', 'st.session_state', 'os.replace('))
    page=(ROOT/'pages/7_股神推薦.py').read_text('utf-8')
    assert "h147_five_gate_data" in page
    assert "_h147_extra_cols" in page
    assert page.count('("00_H126主管決策", tables') == 1
