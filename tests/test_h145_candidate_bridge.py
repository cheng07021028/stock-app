# -*- coding: utf-8 -*-
"""H145 fail-closed export candidate recovery from the same scan only."""
import pandas as pd
from pathlib import Path
from godpick_h145_core_bridge import (restore_export_pool, bounded_with_final_coverage,
                                      audit_final_coverage)


def pool(n=1665):
    return pd.DataFrame({'股票代號': [str(i) for i in range(1000,1000+n)],
                         'H110最終研究優先分':list(range(n))})


def params(**kwargs):
    p=dict(scan_report={'candidate_diagnosis_count':1665,'final_candidate_count':8},
           context_run_id='same-run-20261028',scan_run_id='same-run-20261028',
           decision_signature='scan-fingerprint',active_signature='scan-fingerprint')
    p.update(kwargs)
    return p


def test_oct28_recover_1665_from_current_session_not_one_row():
    source=pool(); selected,meta=restore_export_pool(source.iloc[:1],source,source.iloc[:8],**params())
    assert len(selected)==1665 and meta['recovered'] and meta['same_run_proven']
    assert meta['candidate_complete'] and meta['status']=='RECOVERED_FULL_SCAN_SAME_RUN'


def test_oct28_can_recover_pool_but_eight_final_codes_are_not_invented():
    source=pool(); selected,meta=restore_export_pool(source.iloc[:1],source,source.iloc[:1],**params())
    assert len(selected)==1665
    assert not meta['candidate_complete']
    assert meta['status']=='BLOCKED_FINAL_RESULT_COUNT_GAP'


def test_no_matching_run_id_does_not_claim_full_scope():
    source=pool(); selected,meta=restore_export_pool(source.iloc[:1],source,source.iloc[:8],
                                                     **params(scan_run_id='older-run'))
    assert len(selected)==1 and not meta['candidate_complete']


def test_no_matching_scan_signature_does_not_resurrect_old_cached_pool():
    source=pool(); selected,meta=restore_export_pool(source.iloc[:1],source,source.iloc[:8],
                                                     **params(active_signature='old-scan'))
    assert len(selected)==1 and meta['status']=='BLOCKED_PARTIAL_EXPORT_NO_TRUSTED_RECOVERY'


def test_supplied_full_scan_needs_matching_run_id():
    source=pool(); selected,meta=restore_export_pool(source,source.iloc[:1],source.iloc[:8],
                                                     **params(context_run_id=''))
    assert len(selected)==1665 and not meta['candidate_complete']
    assert meta['status']=='BLOCKED_FULL_SCAN_RUN_ID_UNVERIFIED'


def test_complete_supplied_current_run_is_accepted_without_cache():
    source=pool(); selected,meta=restore_export_pool(source,source.iloc[:1],source.iloc[:8],**params())
    assert len(selected)==1665 and meta['candidate_complete'] and not meta['recovered']


def test_final_candidate_code_missing_is_blocked():
    source=pool(); final=source.iloc[:7].copy();final.loc[len(final)]={'股票代號':'9999','H110最終研究優先分':70}
    _,meta=restore_export_pool(source.iloc[:1],source,final,**params())
    assert not meta['candidate_complete'] and meta['missing_final_codes']==['9999']


def test_bounded_selected_input_preserves_all_eight_known_final_stocks():
    source=pool(); final=source.tail(8)
    selected,meta=bounded_with_final_coverage(source,final,selector=lambda x:x.head(240),limit=240)
    assert len(selected)==240 and meta['pinned']==8
    assert set(final['股票代號']).issubset(set(selected['股票代號']))
    assert list(selected.head(8)['股票代號'])==list(final['股票代號'])


def test_bounded_never_manufactures_stock_not_present_in_full_pool():
    source=pool();final=pd.DataFrame([{'股票代號':'9999'}])
    selected,meta=bounded_with_final_coverage(source,final,selector=lambda x:x.head(240),limit=240)
    assert '9999' not in set(selected['股票代號'])
    assert meta['status']=='FINAL_CODE_NOT_IN_FULL_POOL'


def test_score_not_overridden_when_pinning():
    source=pool();final=source.tail(8)
    selected,_=bounded_with_final_coverage(source,final,selector=lambda x:x.head(240),limit=240)
    expected=source.set_index('股票代號')['H110最終研究優先分']
    assert all(row['H110最終研究優先分']==expected[row['股票代號']] for _,row in selected.iterrows())


def test_missing_audit_final_stocks_cannot_be_called_complete():
    out=audit_final_coverage(pool(3),pool(8));assert out['observed']==3 and not out['complete']
    assert len(out['missing'])==5


def test_page_stays_only_two_sheets_and_h109_h126_unchanged():
    text=(Path(__file__).resolve().parents[1]/'pages/7_股神推薦.py').read_text('utf-8')
    assert 'from godpick_h145_core_bridge import restore_export_pool' in text
    assert 'st.session_state[_k("h145_candidate_bridge")]' in text
    assert 'priority_final=final_candidates' in text
    section=text[text.index('    sheets = [\n',text.index('# H144: verify scan->H97')):]
    assert section.split('    for name, frame, fallback in sheets:',1)[0].count('("00_')==2
    assert '"00_H126主管決策", tables' in section
    assert 'H144_EXPORT_SCOPE_INCOMPLETE' in text
