# -*- coding: utf-8 -*-
from __future__ import annotations
import json,sys,copy
from datetime import date,datetime,timedelta
from pathlib import Path
import pytest
from openpyxl import Workbook
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h116_daily_feedback import make_decision_snapshot
from godpick_h117_official_market_closure import make_official_day_manifest
from godpick_h172_closed_loop import build_research_report,verify_research_report,load_verified_report
from godpick_h172_workbook_audit import append_h172_receipt
from tools.h172_closed_loop_cli import run


def data(n=12):
    days=[];d=date(2026,11,2)
    while len(days)<n:
        if d.weekday()<5: days.append(d.isoformat())
        d+=timedelta(days=1)
    rows={'TWSE':{},'TPEX':{},'TAIEX':{}}
    for i,day in enumerate(days):
        ts=day+'T19:00:00+08:00'
        for src,code in [('TWSE','3227'),('TPEX','3293'),('TAIEX','BENCHMARK')]:
            c=100+i if src=='TWSE' else 150+i if src=='TPEX' else 20000+i
            item={'code':code,'date':day,'close':c,'exchange':src}
            rows[src][day]=make_official_day_manifest(
                {'source':src,'day':day,'source_url':'https://official.example.invalid/'+src,
                 'rows':[item]},collected_at=ts)
    core={'run_id':'h172_001','research':[{'股票代號':'3227','股票名稱':'原相',
          'H114主導策略':'BREAKOUT','H114入榜身分':'B',
          'latest_evidence_at':days[0]+'T14:30:00+08:00'}]}
    snap=make_decision_snapshot(core,run_id='h172_001',
            decided_at=days[0]+'T16:00:00+08:00',market_day=days[0])
    return [snap],rows,days


def build(s=None,d=None,days=None):
    if s is None:s,d,days=data()
    return build_research_report(s,d,evaluated_at=days[-1]+'T20:00:00+08:00')


def test_h172_valid_signed_events_price_only_and_source_receipt():
    z=build()
    assert z['eligible_research_events']==1
    assert z['summary']['T+1']['evaluated']==1
    assert z['summary']['T+5']['evaluated']==1
    assert z['summary']['T+10']['evaluated']==1
    assert z['results'][0]['returns']['T+1']['return_pct']==1.0
    assert z['results'][0]['returns']['T+10']['return_pct']==10.0
    assert z['results'][0]['entry_basis']=='MARKET_DAY_CLOSE_PRICE_ONLY_NOT_EXECUTABLE'
    assert z['independent_holiday_calendar_verified'] is False
    assert z['trade_authority_changed'] is False and z['legacy_godpick_records_modified'] is False
    assert verify_research_report(z)


def test_h172_partial_market_day_is_not_silently_removed():
    s,d,days=data();del d['TPEX'][days[1]]
    z=build(s,d,days)
    assert days[1] in z['partial_official_market_sessions']
    assert z['official_index_observed_sessions']==12
    assert z['results'][0]['returns']['T+1']['status']=='MISSING_OR_CONFLICTING_EXIT_CLOSE'
    assert z['results'][0]['returns']['T+5']['status']=='EVALUATED_PRICE_ONLY'


def test_h172_h117_tampered_price_manifest_fail_closed():
    s,d,days=data();d['TWSE'][days[1]]['rows'][0]['close']=999
    with pytest.raises(ValueError,match='integrity'):build(s,d,days)


def test_h172_h116_tampered_strategy_fail_closed():
    s,d,days=data();s[0]['decisions'][0]['strategy']='tampered'
    with pytest.raises(ValueError,match='H116'):build(s,d,days)


def test_h172_h116_missing_pit_evidence_not_counted():
    s,d,days=data()
    c={'run_id':'nopit','research':[{'股票代號':'3227','股票名稱':'原相',
                                    'H114主導策略':'BREAKOUT'}]}
    s=[make_decision_snapshot(c,run_id='nopit',decided_at=days[0]+'T16:00:00+08:00',market_day=days[0])]
    z=build(s,d,days)
    assert z['eligible_research_events']==0
    assert z['excluded_events'][0]['status']=='UNVERIFIED_ASOF_EVIDENCE'
    assert z['summary']['T+1']['evaluated']==0


def test_h172_future_quotes_not_used_for_earlier_cutoff():
    s,d,days=data()
    z=build_research_report(s,d,evaluated_at=days[1]+'T17:00:00+08:00')
    assert z['summary']['T+1']['evaluated']==0
    assert z['results'][0]['returns']['T+1']['status']=='NOT_MATURED'


def test_h172_duplicate_stock_code_cross_exchange_fail_closed():
    s,d,days=data()
    d['TPEX'][days[0]]['rows'][0]['code']='3227'
    from godpick_h117_official_market_closure import _json_bytes,_sha
    m=d['TPEX'][days[0]];m['content_sha256']=_sha(_json_bytes({k:v for k,v in m.items() if k!='content_sha256'}))
    with pytest.raises(ValueError,match='cross-market duplicate'):build(s,d,days)


def test_h172_report_sha_catches_mutation():
    z=build();z['summary']['T+1']['win_rate_pct']=0
    assert not verify_research_report(z)


def test_h172_no_data_never_overwrites_existing(tmp_path):
    output=tmp_path/'keep.json';output.write_text('KEEP',encoding='utf-8')
    z=run(tmp_path/'no_decisions',tmp_path/'no_daily',output)
    assert z['saved'] is False and output.read_text()=='KEEP'


def test_h172_cli_signed_local_readback_and_idempotent(tmp_path):
    s,d,days=data()
    from godpick_h117_official_market_closure import _json_bytes
    import gzip
    de=tmp_path/'decisions';de.mkdir()
    (de/'snapshot.json').write_text(json.dumps(s[0],ensure_ascii=False),encoding='utf-8')
    daily=tmp_path/'daily'
    for source,items in d.items():
        (daily/source).mkdir(parents=True)
        for day,m in items.items():
            (daily/source/(day+'.json.gz')).write_bytes(gzip.compress(_json_bytes(m),mtime=0))
    out=tmp_path/'H172_performance_research.json'
    x=run(de,daily,out)
    assert x['status']=='LOCAL_WRITTEN_READBACK_VERIFIED_NOT_YET_GITHUB_PERMANENT'
    assert x['saved'] and load_verified_report(out)['eligible_research_events']==1
    contents=out.read_bytes();y=run(de,daily,out)
    assert y['status']=='ALREADY_VERIFIED_IDENTICAL' and not y['saved']
    assert out.read_bytes()==contents
    # CLI is a separate sidecar; old godpick_records.json is not imported or written.


def test_h172_excel_append_existing_sheet_only(tmp_path):
    p=tmp_path/'report.json';p.write_text(json.dumps(build(),ensure_ascii=False),encoding='utf-8')
    w=Workbook();w.active.title='11_交易員重點導航';w.active['A1']='KEEP'
    h126=w.create_sheet('00_H126主管決策');h126['G2']='禁止買進'
    names=w.sheetnames[:]
    result=append_h172_receipt(w,p)
    assert result['status']=='H172_VERIFIED_HISTORICAL_RECEIPT_APPENDED'
    assert w.sheetnames==names
    assert w.active['A1'].value=='KEEP'
    assert h126['G2'].value=='禁止買進'


def test_h172_excel_does_not_add_sheets_without_report(tmp_path):
    w=Workbook();w.active.title='11_交易員重點導航'
    assert append_h172_receipt(w,tmp_path/'missing.json')['status']=='NO_SIGNED_H172_RESEARCH_REPORT'
    assert len(w.sheetnames)==1


def test_h172_page7_h169_hook_preserves_h126_original():
    source=(ROOT/'godpick_h169_pe_watch.py').read_text('utf-8')
    assert source.count('H171 research-only')==1
    assert source.count('H172: append verified retrospective')==1
    assert source.index('H171 research-only')<source.index('H172: append verified retrospective')
    assert '"h172_audit": _h172_result' in source
    import ast;ast.parse(source)


def test_h172_nightly_keeps_original_h122_workflow():
    s=(ROOT/'.github/workflows/h117_official_ohlcv_feedback.yml').read_text('utf-8')
    assert s.count('H172 retrospective T+N evidence')==1
    assert 'python tools/h122_incident_cli.py collect-safe market_eod' in s
    assert 'python tools/h116_closed_loop_cli.py review' in s
    assert 'H122: approved official data or audited incident health' in s
