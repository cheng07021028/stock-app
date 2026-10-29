# -*- coding: utf-8 -*-
"""H148 synthetic proof, false positives, current-run scope and anti-overreach."""
from pathlib import Path
import pandas as pd
from godpick_h148_strength_provenance import build_strength_trace

SHA='a'*64
DAY='2026-10-28'


def stock(code='2317',**kw):
    x={'股票代號':code,'股票名稱':'僅測試假資料','類別':'AI設備','H110研究資訊截止日':DAY,
       'H114K線證據日':DAY,'H114K線資料狀態':'VALID_OHLCV','H114K線有效根數':80,
       'H114距20日高點%':-0.8,'H114K線量比':1.8,
       'H113流動性合格':'是','H113當日成交額百萬':310,
       'H113二十日均成交額百萬':270,'H113當日成交量張':1350,
       '收盤距MA20%':4,'收盤距MA60%':8,'H105發動階段':'BH1',
       'H108族群生命週期':'IGNITION','H109決策權威':'RESEARCH_ELIGIBLE',
       'H123法人實證狀態':'PROVEN_FLOW','H123來源雜湊':SHA,
       'H123法人逐日有效天數':5,'H123研究實際時間':DAY+'T17:20:00',
       'H124大戶實證狀態':'VERIFIED_RISING','H124大戶來源雜湊':SHA,
       'H124大戶變化pp':0.15}
    x.update(kw)
    return x


def run(*,cand=None,audit=None,final=None,authority=None,report=None,bridge=None,day=DAY):
    c=cand if cand is not None else [stock()]
    return build_strength_trace(candidate_frame=pd.DataFrame(c),
        audit_frame=pd.DataFrame(audit if audit is not None else c),
        result_frame=pd.DataFrame(final if final is not None else c),
        authority_frame=pd.DataFrame(authority if authority is not None else [
            {'股票代號':'2317','H126能否買進':'否'}]),
        scan_report=report if report is not None else {'total_count':1651,'candidate_diagnosis_count':len(c),'final_candidate_count':1},
        h145_bridge=bridge if bridge is not None else {'candidate_complete':True,'same_run_proven':True},
        research_day=day)


def test_true_same_run_qualified_volume_breakout_is_research_only():
    df,groups,meta=run()
    assert meta['proven_diagnosis_pool']
    assert df.loc[0,'H148研究分類']=='突破確認研究／非買點'
    assert '集保級距' in df.loc[0,'H148大戶週證']
    assert '五日來源雜湊' in df.loc[0,'H148法人證據']
    assert '不允許' in df.loc[0,'H148非交易聲明']
    assert meta['counts']['已核實研究突破']==1
    assert groups.loc[0,'量價突破研究數']==1
    assert meta['h126_modified'] is False


def test_existing_ignited_is_trend_not_future_blackhorse():
    df,_,meta=run(cand=[stock(H105發動階段='ALREADY-IGNITED')])
    assert df.loc[0,'H148研究分類']=='已發動健康續攻研究／防追高'
    assert df.loc[0,'H148突破階段']=='已發動'
    assert meta['counts']['健康續攻研究']==1


def test_h108_rotation_requires_prior_measured_breadth_and_flow():
    evidence={'H108族群前次資料日':'2026-10-27','H108族群資金流變化':2.8,
              'H108族群廣度變化':5,'H108內部樣本可信度':85}
    df,groups,meta=run(cand=[stock(**evidence)])
    assert '雙指標支持' in df.loc[0,'H148族群證據']
    assert groups.loc[0,'H108雙指標支持數']==1
    assert meta['counts']['族群雙指標支持']==1
    assert '獨立驗證' in df.loc[0,'H148族群證據']


def test_h108_future_prev_day_and_missing_flow_are_not_sector_evidence():
    row=stock(**{'H108族群前次資料日':'2026-10-29','H108族群資金流變化':3,
                 'H108族群廣度變化':4,'H108內部樣本可信度':90})
    df,_,_=run(cand=[row])
    assert '僅標籤' in df.loc[0,'H148族群證據']
    row['H108族群前次資料日']='2026-10-27'
    row.pop('H108族群資金流變化')
    df,_,_=run(cand=[row])
    assert '僅標籤' in df.loc[0,'H148族群證據']


def test_fade_veto_stays_even_with_countertrend_internal_metrics():
    row=stock(H109決策權威='FADE_VETO',H108族群生命週期='FADE',
              H108族群前次資料日='2026-10-27',H108族群資金流變化=2,
              H108族群廣度變化=3,H108內部樣本可信度=70)
    df,_,_=run(cand=[row])
    assert 'FADE否決保留' in df.loc[0,'H148族群證據']
    assert '矛盾待查' in df.loc[0,'H148族群證據']


def test_candidate_only_breakout_cannot_pose_as_full_h113_audited():
    df,_,meta=run(audit=[])
    assert '待H113稽核' in df.loc[0,'H148研究分類']
    assert meta['counts']['已核實研究突破']==0
    assert meta['counts']['H113稽核漏接']==1


def test_old_k_line_is_not_current_breakout():
    df,_,meta=run(cand=[stock(H114K線證據日='2026-10-27')])
    assert df.loc[0,'H148研究分類']=='行情或交易日待核'
    assert meta['counts']['已核實研究突破']==0


def test_future_quote_must_fail_closed():
    df,_,meta=run(cand=[stock(H114K線證據日='2026-10-29')])
    assert df.loc[0,'H148量價狀態']=='未來資料衝突'
    assert meta['counts']['已核實研究突破']==0


def test_not_enough_candles_cannot_claim_usable_ohlcv():
    df,_,_=run(cand=[stock(H114K線有效根數=42)])
    assert '不足60' in df.loc[0,'H148量價狀態']
    assert df.loc[0,'H148研究分類']=='行情或交易日待核'


def test_missing_volume_units_dont_give_strength():
    df,_,_=run(cand=[stock(H114K線資料狀態='VALID_OHLC_ONLY_VOLUME_UNIT_UNKNOWN')])
    assert df.loc[0,'H148研究分類']=='行情或交易日待核'


def test_incomplete_funnel_is_not_proven_market_coverage():
    df,_,meta=run(report={'total_count':0,'candidate_diagnosis_count':1,'final_candidate_count':1})
    assert not meta['proven_diagnosis_pool']
    assert not meta['funnel_geometry_valid']
    assert '母體不完整' in meta['zero_formal_diagnosis']
    assert meta['counts']['已核實研究突破']==1  # row evidence doesn't imply market coverage


def test_reboot_missing_matching_run_identifier_does_not_claim_complete():
    _,_,meta=run(bridge={'candidate_complete':True,'same_run_proven':False})
    assert not meta['proven_diagnosis_pool']


def test_tdcc_string_without_real_two_week_proof_is_not_locking():
    df,_,_=run(cand=[stock(H124大戶來源雜湊='',H124大戶變化pp=.3)])
    assert '宣稱' in df.loc[0,'H148大戶週證']
    assert '鎖單' not in df.loc[0,'H148大戶週證']


def test_institution_5days_without_verified_hash_is_not_proven():
    df,_,_=run(cand=[stock(H123來源雜湊='bogus')])
    assert '宣稱' in df.loc[0,'H148法人證據']


def test_fade_veto_not_bypassed_by_positive_strength():
    df,_,meta=run(cand=[stock(H109決策權威='FADE_VETO',H108族群生命週期='FADE')])
    assert 'FADE' in df.loc[0,'H148族群證據']
    assert meta['counts']['FADE否決']==1
    assert '不允許' in df.loc[0,'H148非交易聲明']


def test_duplicate_conflicting_rows_quarantined_and_not_complete():
    a=stock();b=stock(H114K線量比=1.9)
    df,_,meta=run(cand=[a,b],report={'total_count':2,'candidate_diagnosis_count':2,'final_candidate_count':1})
    assert len(df)==1
    assert df.loc[0,'H148研究分類']=='來源矛盾／禁止推論'
    assert meta['duplicate_codes']==['2317']
    assert not meta['proven_diagnosis_pool']


def test_asof_mismatch_between_input_and_audit_is_conflict():
    cand=stock();audit=stock(H110研究資訊截止日='2026-10-27')
    df,_,_=run(cand=[cand],audit=[audit])
    assert df.loc[0,'H148研究分類']=='來源矛盾／禁止推論'


def test_no_input_no_stock_invention_and_no_trade_granted():
    df,groups,meta=run(cand=[],audit=[],final=[],authority=[],report={})
    assert df.empty and groups.empty and meta['review_unique']==0
    assert meta['counts'].get('H126原表可買',0)==0
    assert '不可歸因' in meta['zero_formal_diagnosis']


def test_h126_original_positive_is_only_reported_not_granted():
    authority=[{'股票代號':'2317','H126能否買進':'是','H126權威層別':'A'}]
    df,_,meta=run(authority=authority)
    assert meta['counts']['H126原表可買']==1
    assert '原表允許' in df.loc[0,'H148非交易聲明']
    assert meta['h126_modified'] is False


def test_file_is_read_only_and_manager_wiring_has_two_sheets():
    root=Path(__file__).resolve().parents[1]
    code=(root/'godpick_h148_strength_provenance.py').read_text('utf8')
    assert not any(x in code for x in ('requests.get(', 'urlopen(', 'to_csv(', 'to_json(', 'os.replace(', 'st.session_state'))
    page=(root/'pages/7_股神推薦.py').read_text('utf8')
    assert "_k('h148_strength_trace')" in page
    assert 'H148｜' in page
    assert page.count('("00_主管重點觀察榜", h144_focus,')==1
    assert page.count('("00_H126主管決策", tables')==1
