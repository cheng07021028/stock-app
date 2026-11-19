# -*- coding: utf-8 -*-
"""H173 signed-source, calendar, attribution and fail-closed tests."""
from __future__ import annotations
import copy,gzip,json,sys
from datetime import date,timedelta
from pathlib import Path
import pytest
from openpyxl import Workbook
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h116_daily_feedback import make_decision_snapshot
from godpick_h117_official_market_closure import make_official_day_manifest,_json_bytes,_sha
from godpick_h172_closed_loop import build_research_report,_digest as h172_digest
from godpick_h173_relative_edge import build_relative_report,verify_relative_report,load_relative_report
from godpick_h173_workbook_audit import append_h173_audit
from tools.h173_relative_edge_cli import run


def sample(days_n=14,signals_n=1):
    trading=[];current=date(2026,9,1)
    while len(trading)<days_n:
        if current.weekday()<5:trading.append(current.isoformat())
        current+=timedelta(days=1)
    quote={'TWSE':{},'TPEX':{},'TAIEX':{}}
    codes=[str(3001+i) for i in range(10)]
    for ix,day in enumerate(trading):
        ts=day+'T19:00:00+08:00'
        for source in ('TWSE','TPEX','TAIEX'):
            if source=='TAIEX':
                close=20000+20*ix
                bars=[{'code':'BENCHMARK','date':day,'close':close,'exchange':'TAIEX'}]
            elif source=='TWSE':
                bars=[{'code':code,'date':day,'close':100+ix,'exchange':'TWSE'} for code in codes]
            else:
                bars=[{'code':'3293','date':day,'close':120+ix,'exchange':'TPEX'}]
            quote[source][day]=make_official_day_manifest({'source':source,'day':day,
                    'source_url':'https://official.invalid/'+source,'rows':bars},collected_at=ts)
    snapshots=[]
    for i in range(signals_n):
        d=trading[i]
        code=codes[i % len(codes)]
        core={'run_id':'h173run'+str(i),'research':[{'股票代號':code,'股票名稱':'測試股票',
            'H114主導策略':'主流突破','H114入榜身分':'B',
            'latest_evidence_at':d+'T14:00:00+08:00'}]}
        snapshots.append(make_decision_snapshot(core,run_id='h173run'+str(i),
            decided_at=d+'T16:00:00+08:00',market_day=d))
    r=build_research_report(snapshots,quote,evaluated_at=trading[-1]+'T20:00:00+08:00')
    return r,quote,trading


def test_h173_basic_index_excess_and_decline_definition():
    r,d,days=sample()
    z=build_relative_report(r,d)
    assert z['research_events']==1
    line=z['outcomes'][0]['relative']['T+1']
    assert line['status']=='EVALUATED_RELATIVE_PRICE_ONLY'
    assert line['index_return_pct']==0.1
    assert line['stock_return_pct']==1
    assert line['excess_return_pct']==0.9 and line['beat_index']
    assert not line['index_fell'] and not line['stock_held_flat_or_up']
    assert z['overall']['T+10']['evaluated_distinct_code_days']==1
    assert z['overall']['T+1']['evidence_status']=='INSUFFICIENT_INDEPENDENT_EVIDENCE'
    assert z['h109_h126_trade_authority_changed'] is False
    assert z['ranking_change_applied'] is False
    assert verify_relative_report(z)


def test_h173_long_sample_meets_review_only_not_autotrading():
    r,d,days=sample(48,35)
    z=build_relative_report(r,d)
    for n in (1,5,10):
        metric=z['by_group']['strategy']['主流突破'][f'T+{n}']
        assert metric['evaluated_distinct_code_days']==35
        assert metric['distinct_stock_codes']==10
        assert metric['distinct_signal_days']==35
        assert metric['evidence_status']=='PRELIMINARY_REVIEW_ALLOWED_NO_AUTO_RANK'
        assert 0<=metric['index_outperformance_wilson_lower_95_pct']<=100
    assert not z['ranking_change_proposed'] and not z['ranking_change_applied']
    assert not z['independent_holiday_calendar_verified']


def test_h173_report_modified_fails_integrity():
    r,d,_=sample();z=build_relative_report(r,d)
    z['outcomes'][0]['relative']['T+1']['excess_return_pct']=999
    assert not verify_relative_report(z)


def test_h173_h172_modified_fails_sha_integrity():
    r,d,_=sample();r['results'][0]['strategy']='bogus'
    with pytest.raises(ValueError,match='SHA'):build_relative_report(r,d)


def test_h173_signed_h172_stock_return_mismatch_rejected_even_if_resigned():
    r,d,_=sample()
    r['results'][0]['returns']['T+1']['return_pct']=999
    r['report_sha256']=h172_digest({k:v for k,v in r.items() if k!='report_sha256'})
    with pytest.raises(ValueError,match='return conflicts'):build_relative_report(r,d)


def test_h173_source_manifest_mutation_fails():
    r,d,days=sample();d['TWSE'][days[1]]['rows'][0]['close']=999
    with pytest.raises(ValueError,match='hash/source/date'):build_relative_report(r,d)


def test_h173_manifest_changes_even_with_valid_new_signature_fail_provenance():
    r,d,days=sample()
    m=d['TAIEX'][days[1]];m['rows'][0]['close']+=99
    m['content_sha256']=_sha(_json_bytes({k:v for k,v in m.items() if k!='content_sha256'}))
    with pytest.raises(ValueError,match='manifest list differs'):build_relative_report(r,d)


def test_h173_cannot_relabel_future_price_as_mature():
    r,d,days=sample()
    r['results'][0]['returns']['T+10']['target_session']='2050-01-01'
    r['report_sha256']=h172_digest({k:v for k,v in r.items() if k!='report_sha256'})
    with pytest.raises(ValueError,match='target session'):build_relative_report(r,d)


def test_h173_missing_stock_quote_stays_not_evaluated():
    r,d,days=sample()
    # Remove entire stock entry from day 2 before building signed H172
    del d['TWSE'][days[1]]['rows'][0]
    m=d['TWSE'][days[1]]
    m['content_sha256']=_sha(_json_bytes({k:v for k,v in m.items() if k!='content_sha256'}))
    # Need rebuild H172 to match H117; cannot reuse stale receipt
    snapshots=[make_decision_snapshot({'run_id':'rnew','research':[{'股票代號':'3001',
      'H114主導策略':'breakout','latest_evidence_at':days[0]+'T14:00:00+08:00'}]},
      run_id='rnew',decided_at=days[0]+'T16:00:00+08:00',market_day=days[0])]
    original=build_research_report(snapshots,d,evaluated_at=days[-1]+'T20:00:00+08:00')
    z=build_relative_report(original,d)
    assert z['outcomes'][0]['relative']['T+1']['status']=='STOCK_NOT_EVALUATED'
    assert z['overall']['T+1']['evaluated_distinct_code_days']==0


def test_h173_negative_index_resilience_not_confused_with_alpha():
    r,d,days=sample()
    # Rebuild everything with negative benchmark for T+1 and stock -1%, still beats -2%
    d['TWSE'][days[1]]['rows'][0]['close']=99
    d['TAIEX'][days[1]]['rows'][0]['close']=19600
    for src in ('TWSE','TAIEX'):
        m=d[src][days[1]];m['content_sha256']=_sha(_json_bytes({k:v for k,v in m.items() if k!='content_sha256'}))
    snaps=[make_decision_snapshot({'run_id':'rfall','research':[{'股票代號':'3001',
     'H114主導策略':'抗跌','latest_evidence_at':days[0]+'T14:00:00+08:00'}]},
      run_id='rfall',decided_at=days[0]+'T16:00:00+08:00',market_day=days[0])]
    r=build_research_report(snaps,d,evaluated_at=days[-1]+'T20:00:00+08:00')
    v=build_relative_report(r,d)['outcomes'][0]['relative']['T+1']
    assert v['stock_return_pct']==-1 and v['index_return_pct']==-2
    assert v['excess_return_pct']==1 and v['beat_index']
    assert v['index_fell'] and not v['stock_held_flat_or_up']
    assert v['outperformed_during_index_decline']


def test_h173_workbook_retains_original_and_h126(tmp_path):
    r,d,_=sample();result=build_relative_report(r,d)
    p=tmp_path/'signed.json';p.write_text(json.dumps(result,ensure_ascii=False),encoding='utf-8')
    w=Workbook();w.active.title='11_交易員重點導航';w.active['A1']='EXISTING CONTENT'
    x=w.create_sheet('00_H126主管決策');x['G2']='禁止買進'
    old=w.sheetnames[:]
    z=append_h173_audit(w,p)
    assert z['status']=='H173_VERIFIED_RESEARCH_APPENDED' and z['rows']>=3
    assert w.sheetnames==old and w.worksheets[0]['A1'].value=='EXISTING CONTENT'
    assert x['G2'].value=='禁止買進'


def test_h173_cli_skips_without_receipt_preserves_existing(tmp_path):
    out=tmp_path/'out.json';out.write_text('PROTECTED',encoding='utf-8')
    z=run(tmp_path/'no-h172',tmp_path/'no-data',out)
    assert not z['saved'] and out.read_text('utf-8')=='PROTECTED'


def test_h173_cli_idempotent_and_verified(tmp_path):
    r,d,days=sample()
    p=tmp_path/'h172.json';p.write_text(json.dumps(r,ensure_ascii=False),encoding='utf-8')
    data=tmp_path/'daily'
    for src,part in d.items():
        (data/src).mkdir(parents=True)
        for day,manifest in part.items():
            (data/src/(day+'.json.gz')).write_bytes(gzip.compress(_json_bytes(manifest),mtime=0))
    out=tmp_path/'result.json'
    first=run(p,data,out)
    assert first['saved'] and first['status']=='LOCAL_VERIFIED_NOT_YET_REMOTE_PERMANENT'
    z=load_relative_report(out)
    assert z and z['h172_report_sha256']==r['report_sha256']
    original=out.read_bytes()
    second=run(p,data,out)
    assert not second['saved'] and second['status']=='ALREADY_VERIFIED_IDENTICAL'
    assert out.read_bytes()==original


def test_h173_workflow_valid_and_protected_publish():
    s=(ROOT/'.github/workflows/h117_official_ohlcv_feedback.yml').read_text('utf-8')
    assert s.count('id: h173')==1
    assert 'H173 / H172 SHA mismatch; do not publish' in s
    assert 'steps.h173.outcome' in s and "'success'" in s
    assert 'python tools/h172_closed_loop_cli.py' in s
    assert 'python tools/h122_incident_cli.py collect-safe market_eod' in s
    import yaml
    y=yaml.safe_load(s)
    steps=y['jobs']['operations']['steps']
    assert next(t for t in steps if t.get('id')=='h173')['run'].count('python tools/h173_relative_edge_cli.py')==1
