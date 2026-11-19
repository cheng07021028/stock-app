# -*- coding: utf-8 -*-
"""H170 offline verification: stages, PIT EPS, existing H145/H126 safety."""
from __future__ import annotations
import ast,json,sys
from pathlib import Path
import pandas as pd
from openpyxl import Workbook
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h170_stage_reconcile import build_stage_contract,validate_stage_contract
from godpick_h145_core_bridge import restore_export_pool
from godpick_h170_eps_evidence import parse_official_eps_ytd,combine_sources,load_snapshot,asof_read
from tools.h170_refresh_eps_ytd import refresh

RUN="gprun_h170";SIG="scan-seq-170"
def frame(*codes,reason=""):
    return pd.DataFrame([{"股票代號":code,"硬否決原因":reason} for code in codes])
def stage(pre,post,mother=None):
    mother=mother if mother is not None else frame("3227","3293","1477")
    return build_stage_contract(scanner_final=pre,governed_final=post,
                                governed_universe=mother,run_id=RUN,
                                scan_signature=SIG)
def verify(contract,post,**kwargs):
    base=dict(expected_pre=2,actual_final=post,context_run_id=RUN,
              scan_run_id=RUN,decision_signature=SIG,active_signature=SIG)
    base.update(kwargs)
    return validate_stage_contract(contract,**base)

def test_h170_stage_two_becomes_one_proven_only_same_run():
    pre=frame("3227","3293")
    post=frame("3293")
    mother=frame("3227","3293")
    proof=stage(pre,post,mother)
    audit=verify(proof,post)
    assert audit["stage_verified"] and audit["removed_codes"]==["3227"]
    assert audit["result"]=="RECONCILED_POST_GOVERNANCE_RESELECTION"
    src,bridge=restore_export_pool(mother,mother,post,
        scan_report={"candidate_diagnosis_count":2,"final_candidate_count":2,
                     "h170_stage_contract":proof},
        context_run_id=RUN,scan_run_id=RUN,
        decision_signature=SIG,active_signature=SIG)
    assert len(src)==2
    assert bridge["candidate_complete"]
    assert bridge["status"]=="RECONCILED_POST_GOVERNANCE_RESELECTION"
    assert bridge["trade_authority_changed"] is False

def test_h170_no_stage_proof_keeps_blocked():
    pre=frame("3227","3293");post=frame("3293");mother=frame("3227","3293")
    for proof in ({},stage(pre,post)):
        _,result=restore_export_pool(mother,mother,post,
            scan_report={"candidate_diagnosis_count":2,
                         "final_candidate_count":2,"h170_stage_contract":proof},
            context_run_id="other" if proof else RUN,scan_run_id=RUN,
            decision_signature=SIG,active_signature=SIG)
        assert result["status"]=="BLOCKED_FINAL_RESULT_COUNT_GAP"
        assert not result["candidate_complete"]

def test_h170_stage_signature_mismatch_rejected():
    a=stage(frame("3227","3293"),frame("3227"))
    assert not verify(a,frame("3227"),active_signature="future-run")["stage_verified"]

def test_h170_stage_disappearing_from_universe_rejected():
    a=stage(frame("3227","3293"),frame("3293"),frame("3293"))
    assert not a["same_run_stage_reselection_proven"]
    assert not verify(a,frame("3293"))["stage_verified"]

def test_h170_duplicate_stage_final_cannot_become_green():
    dup=frame("3293","3293")
    a=stage(frame("3227","3293"),dup)
    assert not a["same_run_stage_reselection_proven"]
    assert not verify(a,dup)["stage_verified"]

def test_h170_no_mutation_of_H109_or_H126_frames():
    source=frame("3227","3293",reason="股神真實風控")
    frozen=source.copy(deep=True)
    pre=source.copy()
    post=source.iloc[:1].copy()
    _=stage(pre,post,source)
    pd.testing.assert_frame_equal(source,frozen)

def test_page7_records_stage_from_actual_later_governance():
    page=(ROOT/"pages"/"7_股神推薦.py").read_text("utf-8")
    ast.parse(page)
    assert "_h170_scanner_final = final_df.copy(deep=False)" in page
    assert 'scan_report["h170_stage_contract"]=build_stage_contract(' in page
    assert page.index("_h170_scanner_final = final_df.copy") < page.index(
        'scan_report["h170_stage_contract"]=build_stage_contract(')
    assert "governed_universe=governed_candidate_df" in page

NOW="2026-11-19T22:30:00+08:00"
def eps_records(market="TWSE",n=26):
    return [{"出表日期":"1151119","年度":"115","季別":"3",
             "公司代號":str(3000+i),"基本每股盈餘（元）":"2.86",
             "公司名稱":"示例_"+str(i)} for i in range(n)]

def test_official_eps_current_quarter_YTD_not_TTM():
    rows,meta=parse_official_eps_ytd(eps_records(),market="TWSE",observed_at=NOW)
    assert meta["usable_eps_ytd_rows"]==26
    assert rows[0]["year_quarter"]=="2026Q3"
    assert rows[0]["eps_ytd"]==2.86
    assert rows[0]["basis"]=="CUMULATIVE_YTD_NOT_SINGLE_QUARTER"
    assert rows[0]["eps_ttm"] is None
    assert not meta["ttm_eps_certified"]

def test_official_eps_partial_market_cannot_be_all_verified():
    z=combine_sources({"TWSE":eps_records()},observed_at=NOW)
    assert z["status"]=="EPS_YTD_PARTIAL"
    assert z["sources"]["TPEx"]["status"]=="NOT_AVAILABLE"
    assert not z["ttm_eps_certified"]
    assert not z["official_low_pe_high_eps_certified"]

def test_official_eps_negative_ytd_can_be_evidence_not_high_eps():
    sample=eps_records(n=1);sample[0]["基本每股盈餘（元）"]="-0.75"
    z,_=parse_official_eps_ytd(sample,market="TWSE",observed_at=NOW)
    assert z[0]["eps_ytd"]==-0.75
    assert z[0]["eps_ttm"] is None

def test_official_eps_future_report_is_not_accepted():
    sample=eps_records(n=1);sample[0]["出表日期"]="1151120"
    z,_=parse_official_eps_ytd(sample,market="TWSE",observed_at=NOW)
    assert z==[]

def test_official_eps_same_day_not_backfilled_into_earlier_scan():
    rows,_=parse_official_eps_ytd(eps_records(n=1),market="TWSE",observed_at=NOW)
    assert asof_read(rows[0],study_day="2026-11-19")=="OBSERVED_SAME_OR_LATER_DAY_NOT_PIT"
    assert asof_read(rows[0],study_day="2026-11-20")=="OFFICIAL_YTD_PIT_ONLY_NOT_TTM"

def test_official_eps_bad_403_never_replaces_authority(tmp_path):
    p=tmp_path/"eps.json";p.write_text('{"safe":true}',encoding="utf-8")
    class R:status_code=403
    x=refresh(get=lambda *a,**k:R(),observed_at=NOW,output=p)
    assert not x["saved"]
    assert p.read_text("utf-8")=='{"safe":true}'

def test_official_eps_snapshot_from_one_source_records_partial(tmp_path):
    p=tmp_path/"eps.json"
    class R:
        status_code=200
        def json(self): return eps_records()
    z=refresh(get=lambda *a,**k:R(),observed_at=NOW,output=p)
    assert z["saved"]
    s=json.loads(p.read_text("utf-8"))
    assert s["status"]=="EPS_YTD_BOTH_MARKETS"
    assert not s["ttm_eps_certified"]

def test_h169_existing_excel_sheet_count_and_pe_eps_warning():
    from godpick_h169_pe_watch import append_pe_audit
    wb=Workbook();ws=wb.active;ws.title="01_具名強勢研究"
    ws.append(["股票代號","股票名稱","市場別"])
    ws.append(["3227","原相","上市"])
    nav=wb.create_sheet("11_交易員重點導航")
    nav["A1"]="EXISTING H157"
    existing=wb.sheetnames[:]
    result=append_pe_audit(wb,research_day="2026-11-19")
    assert wb.sheetnames==existing
    assert nav["A1"].value=="EXISTING H157"
    assert result["eps_ttm_verified"] is False
    assert "累計EPS" in str(nav.cell(result["start_row"]+3,2).value)
