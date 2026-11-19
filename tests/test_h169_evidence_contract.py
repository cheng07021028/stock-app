# -*- coding: utf-8 -*-
"""H169 offline tests: no trading permission, honest same-run/PE/volume evidence."""
from __future__ import annotations

from datetime import date,timedelta
from pathlib import Path
import json
import sys

import pandas as pd
from openpyxl import Workbook

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h145_core_bridge import restore_export_pool
from godpick_h114_parallel_alpha import analyze_history
from godpick_h169_pe_watch import (
    parse_pe_rows, merge_pe_observations, validate_observation_for_research,
    load_pe_snapshot, append_pe_audit,
)
from tools.h169_refresh_pe_watch import refresh

OBS="2026-11-18T22:15:00+08:00"

def bars(n=100,*, bad_lot_at=None):
    base=date(2026,5,1)
    out=[]
    for i in range(n):
        price=65 + i * .16
        out.append({"日期":(base+timedelta(days=i)).isoformat(),
                    "開盤價":price,"最高價":price+1,
                    "最低價":price-.9,"收盤價":price+.25,
                    "成交量(張)":0 if i==bad_lot_at else 2300+i*5})
    return pd.DataFrame(out)

def test_h114_valid_sixty_and_h1532_valid_eightyfive():
    p=bars()
    x=analyze_history(p,asof=p.iloc[-1]["日期"])
    assert x["H114K線資料狀態"]=="VALID_OHLCV"
    assert x["H169_H114量價契約"]=="60根成交量張數均有效"
    assert x["H169_H1532完整型態契約"]=="85根完整驗證"

def test_h114_zero_volume_in_last_sixty_fails_closed():
    p=bars(bad_lot_at=77)
    x=analyze_history(p,asof=p.iloc[-1]["日期"])
    assert x["H114K線資料狀態"]=="INVALID_OHLCV_VOLUME"
    assert x["H114黃金交叉"]=="未驗證"
    assert "非正值" in x["H169_H114量價契約"]
    assert x["H1532ADX"] is None
    assert "未達85根" in x["H169_H1532完整型態契約"]

def test_h114_sixty_valid_not_equal_h1532_eightyfive():
    p=bars(bad_lot_at=20)
    x=analyze_history(p,asof=p.iloc[-1]["日期"])
    assert x["H114K線資料狀態"]=="VALID_OHLCV"
    assert "未達85根" in x["H169_H1532完整型態契約"]
    assert x["H1532ADX"] is None

def test_h145_two_expected_one_actual_remains_blocked():
    pool=pd.DataFrame([{"股票代號":"3227"},{"股票代號":"3293"}])
    final=pool.iloc[:1].copy()
    got,meta=restore_export_pool(pool,pool,final,scan_report={
        "candidate_diagnosis_count":2,"final_candidate_count":2,
    },context_run_id="same",scan_run_id="same",
        decision_signature="sig",active_signature="sig")
    assert got.shape[0]==2
    assert meta["status"]=="BLOCKED_FINAL_RESULT_COUNT_GAP"
    assert not meta["candidate_complete"]
    check=meta["final_reconciliation"]
    assert check["scanner_expected_count"]==2
    assert check["actual_unique_4digit_codes"]==1
    assert check["unaccounted_unique_gap"]==1
    assert check["can_authorize_trade"] is False
    assert not check["exact_missing_code_known"]

def test_h145_duplicate_final_names_are_not_two_authorized_codes():
    pool=pd.DataFrame([{"股票代號":"3227"},{"股票代號":"3293"}])
    final=pd.DataFrame([{"股票代號":"3227"},{"股票代號":"3227"}])
    _,meta=restore_export_pool(pool,pool,final,scan_report={
        "candidate_diagnosis_count":2,"final_candidate_count":2,
    },context_run_id="same",scan_run_id="same",
      decision_signature="sig",active_signature="sig")
    assert meta["status"]=="BLOCKED_FINAL_RESULT_COUNT_GAP"
    check=meta["final_reconciliation"]
    assert check["duplicated_or_invalid_final_rows"]==1
    assert "去重" in check["reason"]

def sample_market(market="TWSE",n=25,day="1151118"):
    return [{
        "Date":day,"Code":str(3100+i),"Name":"測試_"+str(i),
        "PEratio":"12.50","PBratio":"1.20","DividendYield":"3.00",
    } for i in range(n)]

def test_pe_only_twse_real_schema_no_fake_eps():
    rows,summary=parse_pe_rows(sample_market(),market="TWSE",observed_at=OBS)
    assert summary["verified_pe_rows"]==25
    assert summary["status"]=="PE_ONLY_SOURCE_VERIFIED"
    assert rows[0]["asof_date"]=="2026-11-18"
    assert rows[0]["source_url"].startswith("https://openapi.twse.com.tw")
    assert rows[0]["eps_ttm"] is None
    assert rows[0]["eps_status"]=="NOT_IN_THIS_FEED"

def test_pe_only_partial_tpex_never_fully_green():
    p=merge_pe_observations({"TWSE":sample_market()},observed_at=OBS)
    assert p["status"]=="PE_ONLY_PARTIAL"
    assert p["sources"]["TPEx"]["status"]=="SOURCE_MISSING_OR_UNAVAILABLE"
    assert not p["eps_verified"] and not p["h126_trade_permission_changed"]

def test_future_date_and_bad_pe_not_accepted():
    v=sample_market(n=1)
    v[0]["Date"]="1151119"
    v[0]["PEratio"]="-"
    x,summary=parse_pe_rows(v,market="TWSE",observed_at=OBS)
    assert not x and summary["verified_pe_rows"]==0

def test_intraday_same_day_unknown_scan_timestamp_blocks_backfill():
    item=merge_pe_observations({"TWSE":sample_market()},observed_at=OBS)["pe_only"][0]
    assert "不得回填" in validate_observation_for_research(item,research_day="2026-11-18")
    assert "可作PE研究" in validate_observation_for_research(item,research_day="2026-11-19")

def test_missing_file_has_honest_status(tmp_path):
    x=load_pe_snapshot(tmp_path/"missing.json")
    assert x["status"]=="PE_ONLY_NOT_COLLECTED"

def test_refresh_never_writes_empty_or_403(tmp_path):
    class Resp:
        status_code=403
    def get(*a,**k):return Resp()
    path=tmp_path/"pe_only.json"
    path.write_text('{"old":"preserve"}',encoding="utf-8")
    x=refresh(get=get,observed_at=OBS,output=path)
    assert not x["saved"]
    assert path.read_text(encoding="utf-8")=='{"old":"preserve"}'

def test_refresh_saves_pe_only_without_eps(tmp_path):
    class Resp:
        status_code=200
        def json(self): return sample_market()
    def get(*a,**k):return Resp()
    path=tmp_path/"pe_only.json"
    x=refresh(get=get,observed_at=OBS,output=path)
    assert x["saved"]
    assert x["status"]=="PE_ONLY_BOTH_MARKETS"
    s=json.loads(path.read_text(encoding="utf-8"))
    assert len(s["pe_only"])==50 and not s["eps_verified"]

def test_workbook_pe_append_preserves_old_cells_and_tabs(tmp_path):
    w=Workbook()
    src=w.active;src.title="01_具名強勢研究"
    src.append(["股票代號","股票名稱","市場別"])
    src.append(["3227","原相","上市"])
    nav=w.create_sheet("11_交易員重點導航")
    nav["A1"]="H157 舊版內容"
    nav["B5"]="H126 原限制"
    orig=list(w.sheetnames)
    row=append_pe_audit(w,research_day="2026-11-18")
    assert w.sheetnames==orig
    assert nav["A1"].value=="H157 舊版內容"
    assert nav["B5"].value=="H126 原限制"
    assert row["checked_codes"]==1
    assert nav.cell(row["start_row"],1).value.startswith("H169")
    assert "禁止買進" in str(nav.cell(row["start_row"]+6,11).value)

def test_h169_forensics_visible_in_original_sheet02_summary():
    from godpick_h1536_manager_clarity import clarify_manager_board
    wb=Workbook();ws=wb.active;ws.title="00_主管重點觀察榜"
    x=clarify_manager_board(wb,h145_bridge={
        "status":"BLOCKED_FINAL_RESULT_COUNT_GAP",
        "expected_final":2,
        "final_reconciliation":{
            "scanner_expected_count":2,
            "actual_final_rows":1,
            "actual_unique_4digit_codes":1,
            "unaccounted_unique_gap":1,
            "reason":"正式候選結果在輸出前已減少",
        },
    },research_day="2026-11-18")
    names=x["檢查項目"].tolist()
    assert "H169 H145正式輸出筆數斷點" in names
    value=x.loc[x["檢查項目"]=="H169 H145正式輸出筆數斷點","結果"].iloc[0]
    assert "掃描預期2" in value and "差額1" in value
    assert "H169上市／上櫃官方PE單獨來源" in names


def test_page7_ranking_not_changed_by_pe_watch():
    src=(ROOT/"godpick_h157_trader_audit.py").read_text(encoding="utf-8")
    assert "append_pe_audit(wb, research_day=research_day)" in src
    assert src.index("append_h165_review(")<src.index("append_pe_audit(")
    assert "H126仍為唯一權威" in (ROOT/"godpick_h169_pe_watch.py").read_text(encoding="utf-8")
