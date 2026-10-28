import pandas as pd
from godpick_h144_scope_challenge import audit_h144

def _tabs():
    return {'h126_health':pd.DataFrame([
        {'項目':'原候選去重數','數值':1},
        {'項目':'禁止新倉／隔離觀察','數值':1},
    ])}

def _run(**kwargs):
    inputs=dict(scan_report={'total_count':1718,'candidate_diagnosis_count':1665,'final_candidate_count':8},
     candidate_frame=pd.DataFrame([{'股票代號':'2344'}]), result_frame=pd.DataFrame([{'股票代號':'2344'}]),
     sector_frame=pd.DataFrame([{'類別':'資訊服務業','股票數':53,'類股平均漲幅':3.13,'族群樣本可信度':91.4},
       {'類別':'樣本少','股票數':2,'類股平均漲幅':5.0,'族群樣本可信度':28.5}]),
     tables=_tabs(),core_source='h97-bounded-export-recovery',research_day='2026-10-28')
    inputs.update(kwargs)
    return audit_h144(**inputs)

def test_actual_oct28_scope_mismatch_and_no_fake_buy():
    s, funnel, shadow, blind, counter=_run()
    assert s['scope_incomplete'] and s['status']=='EXPORT_SCOPE_INCOMPLETE'
    assert [funnel.iloc[i,1] for i in (0,1,2,3)]==[1718,1665,8,1]
    assert int((shadow['正式買進']=='否').sum())==len(shadow)
    assert len(shadow)==1 and shadow.loc[0,'名稱或產業']=='資訊服務業'
    assert not blind.empty and not counter.empty

def test_full_scope_not_flagged_without_weak_core():
    c=pd.DataFrame([{'股票代號':'%04d'%i} for i in range(1000,1100)])
    state,*_=_run(scan_report={'total_count':100,'candidate_diagnosis_count':100,'final_candidate_count':10},
                   candidate_frame=c,core_source='session-core')
    assert state['status']=='SCOPE_RECONCILED_FOR_DISPLAY'
    assert not state['scope_incomplete']

def test_sample_less_than_20_is_not_overclaimed():
    state,*_=_run(scan_report={'total_count':1,'candidate_diagnosis_count':1,'final_candidate_count':1},
                  core_source='h97-bounded-export-recovery')
    assert state['scope_incomplete'] is False

def test_invalid_scan_counts_fail_closed():
    state,*_=_run(scan_report={'total_count':100,'candidate_diagnosis_count':500,'final_candidate_count':1})
    assert state['scope_incomplete']

def test_no_market_data_not_claimed_ready():
    s,*_=_run(scan_report={},core_source='session-core')
    assert s['status']=='SCOPE_NOT_PROVEN'

def test_ignited_missing_date_not_claimed_same_day_strength():
    t=_tabs();t['ignited_watch']=pd.DataFrame([{'股票代號':'2344','股票名稱':'華邦電','當日漲幅%':3.5},
    {'股票代號':'9999','股票名稱':'假設','當日漲幅%':-1}])
    s,f,shadow,*_=_run(tables=t,sector_frame=pd.DataFrame())
    assert len(shadow)==1 and shadow.loc[0,'來源類型']=='個股待核日期'
    assert shadow.loc[0,'正式買進']=='否'

def test_ignited_confirmed_same_day_only():
    t=_tabs();t['ignited_watch']=pd.DataFrame([{'股票代號':'2344','股票名稱':'華邦電','當日漲幅%':3.5,'資料交易日':'2026-10-28'},
                                                {'股票代號':'2317','股票名稱':'鴻海','當日漲幅%':2.5,'資料交易日':'2026-10-27'}])
    _,_,s,*_=_run(tables=t,sector_frame=pd.DataFrame())
    assert list(s['來源類型'])==['個股已發動觀察','個股待核日期']

def test_rejects_unknown_code_and_nonfinite_prices():
    t=_tabs();t['ignited_watch']=pd.DataFrame([{'股票代號':'ABCD','當日漲幅%':10},
                                                {'股票代號':'2317','當日漲幅%':float('nan')},
                                                {'股票代號':'2330','當日漲幅%':float('inf')}])
    _,_,s,*_=_run(tables=t,sector_frame=pd.DataFrame())
    assert s.empty

def test_does_not_promote_old_legacy_scores():
    t=_tabs();t['legacy_priority_overview']=pd.DataFrame([{'股票代號':'2330','H102動態優先分':99.9}])
    _,_,s,*_=_run(tables=t,sector_frame=pd.DataFrame(),result_frame=pd.DataFrame())
    assert s.empty

def test_audit_deterministic():
    a=_run(); b=_run()
    for i in range(1,5):pd.testing.assert_frame_equal(a[i],b[i])
