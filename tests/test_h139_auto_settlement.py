# -*- coding: utf-8 -*-
"""H139 isolated synthetic tests. NOT evidence of real portfolio returns."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock
import base64
import json

import pandas as pd
import pytest

from godpick_h134_decision_vault import _json_bytes
from godpick_h136_auto_hub import make_artifacts
from godpick_h139_settlement import (build_settlement, load_verified_h136,
                                     read_remote_settlement, SCHEMA)
from tools.h139_ops import recovery_once, settle_once, _choose_market

DAY = date(2026, 10, 20)
CODES = ('2330', '2317', '2454', '2382', '2308')
AUDIT = {'schema':'H137-audit-1', 'asof_tw': DAY.isoformat(),
         'status':'INTERNAL_CHECK_PASS_NOT_PIT', 'findings': []}


def dataset():
    days = [x.date() for x in pd.bdate_range(end=DAY.isoformat(), periods=70)]
    sig = pd.DataFrame([{'股票代號': c, '訊號研究日': str(d), 'H126權威層別':'B｜待驗證',
                         'H126最後研究分': 90-i, 'H109決策權威':'RESEARCH_ELIGIBLE'}
                        for d in days[1:-1] for i,c in enumerate(CODES)])
    stock = pd.DataFrame([{'股票代號':c,'日期':str(d),'收盤價':round((100+i*9)*(1+0.001*j),4)}
                          for j,d in enumerate(days) for i,c in enumerate(CODES)])
    bench = pd.DataFrame([{'日期':str(d),'收盤價':10000+5*j} for j,d in enumerate(days)])
    manifest = {'schema':'H136-1','asof':DAY.isoformat(),'status':'RESEARCH_DATA_AVAILABLE',
                'latest_stock_date':DAY.isoformat(),'latest_benchmark_date':DAY.isoformat(),
                'signal_days': len(days)-2}
    return {'signals':sig, 'stock':stock, 'benchmark':bench}, manifest


def fixture_tree(root: Path):
    frames, manifest = dataset()
    artifacts = make_artifacts(frames, manifest)
    base=root/'data'/'h136';base.mkdir(parents=True,exist_ok=True)
    for name, data in artifacts.items(): (base/name).write_bytes(data)
    (base/'H136_health.json').write_bytes(_json_bytes({'schema':'H136-health-1',
        'run_day_tw':DAY.isoformat(),'status':'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY'}))
    p=root/'data'/'h137'/'H137_audit.json';p.parent.mkdir(parents=True,exist_ok=True)
    p.write_bytes(_json_bytes(AUDIT))
    return frames, artifacts


def test_complete_cohorts_settle_without_trade_claim():
    f, m = dataset()
    out=build_settlement(asof=DAY,frames=f,manifest=m,audit=AUDIT)
    assert out['status']=='RESEARCH_MATURED_OBSERVATIONS'
    assert out['trade_permission']=='UNCHANGED' and out['automatic_weights']=='DISABLED'
    for key in ('TOP3','TOP5'):
        for h in ('T+1','T+3','T+5'):
            item=out['strategies'][key]['horizons'][h]
            assert item['newly_settled_research_days']==1
            assert item['coverage_pct']==100
            assert item['mean_research_alpha_pp'] is not None
            assert item['allow_auto_weight_update'] is False


def test_missing_stock_does_not_select_winners():
    f,m=dataset()
    f['stock']=f['stock'][~((f['stock']['股票代號']=='2308')&(f['stock']['日期']==str(DAY)))].copy()
    out=build_settlement(asof=DAY,frames=f,manifest=m,audit=AUDIT)
    assert out['strategies']['TOP5']['horizons']['T+1']['newly_quarantined_days']==1
    assert out['strategies']['TOP5']['horizons']['T+1']['newly_settled_research_days']==0
    assert out['strategies']['TOP3']['horizons']['T+1']['newly_settled_research_days']==1


def test_all_missing_current_day_price_quarantined():
    f,m=dataset();f['stock']=f['stock'][f['stock']['日期']!=DAY.isoformat()]
    out=build_settlement(asof=DAY,frames=f,manifest=m,audit=AUDIT)
    assert out['status']=='RESEARCH_SETTLED_NO_NEW_MATURITY'
    assert out['strategies']['TOP3']['horizons']['T+1']['newly_quarantined_days']==1

@pytest.mark.parametrize('change',[
    {'status':'NOT_READY'}, {'asof':'2026-10-19'},
    {'latest_benchmark_date':'2026-10-19'}, {'latest_stock_date':'2026-10-19'}
])
def test_stale_or_unverified_manifest_blocks(change):
    f,m=dataset();m.update(change)
    out=build_settlement(asof=DAY,frames=f,manifest=m,audit=AUDIT)
    assert out['status']=='BLOCKED_NO_VERIFIED_RESEARCH_DATA'


def test_h137_block_prevents_settlement():
    f,m=dataset()
    out=build_settlement(asof=DAY,frames=f,manifest=m,
                         audit={**AUDIT,'status':'BLOCKED_REQUIRES_ATTENTION'})
    assert not out['strategies']


def test_invalid_stock_source_blocks():
    f,m=dataset();f['stock'].loc[0,'收盤價'] = -100
    # Entire price input invalid if key has conflicting or impossible quote.
    f['stock']=pd.concat([f['stock'], f['stock'].iloc[[0]].assign(收盤價=5000)], ignore_index=True)
    out=build_settlement(asof=DAY,frames=f,manifest=m,audit=AUDIT)
    assert out['status']=='BLOCKED_NO_VERIFIED_RESEARCH_DATA'


def test_fresh_validated_local_cache_round_trip():
    with TemporaryDirectory() as tmp:
        root=Path(tmp);fixture_tree(root)
        frame, manifest=load_verified_h136(root,asof=DAY)
        assert len(frame['signals'])>100 and manifest['schema']=='H136-1'
        report=settle_once(runtime_root=root,asof=DAY)
        assert report['status']=='RESEARCH_MATURED_OBSERVATIONS'
        assert json.loads((root/'data/h139/H139_settlement.json').read_bytes())['schema']==SCHEMA


def test_missing_health_blocks():
    with TemporaryDirectory() as tmp:
        root=Path(tmp);fixture_tree(root)
        (root/'data/h136/H136_health.json').unlink()
        with pytest.raises(ValueError,match='HEALTH'):
            load_verified_h136(root,asof=DAY)
        assert settle_once(runtime_root=root,asof=DAY)['status']=='BLOCKED_NO_VERIFIED_RESEARCH_DATA'


def test_stale_health_blocks():
    with TemporaryDirectory() as tmp:
        root=Path(tmp);fixture_tree(root)
        (root/'data/h136/H136_health.json').write_bytes(_json_bytes({'schema':'H136-health-1',
            'run_day_tw':'2026-10-19','status':'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY'}))
        with pytest.raises(ValueError,match='NOT_READY'):
            load_verified_h136(root,asof=DAY)


def test_tampered_sha_blocks_and_no_false_settlement():
    with TemporaryDirectory() as tmp:
        root=Path(tmp);fixture_tree(root)
        with (root/'data/h136/H136_stock.csv').open('ab') as f:f.write(b'evil')
        with pytest.raises(ValueError,match='CACHE_REJECTED'):
            load_verified_h136(root,asof=DAY)
        assert settle_once(runtime_root=root,asof=DAY)['status']=='BLOCKED_NO_VERIFIED_RESEARCH_DATA'


def test_symlink_health_denied():
    with TemporaryDirectory() as tmp:
        root=Path(tmp);fixture_tree(root)
        p=root/'data/h136/H136_health.json';p.unlink();p.symlink_to(root/'data/h136/H136_manifest.json')
        with pytest.raises(ValueError,match='UNSAFE'):
            load_verified_h136(root,asof=DAY)


def test_recovery_skips_ready_without_rebuild():
    with TemporaryDirectory() as tmp:
        root=Path(tmp);fixture_tree(root)
        fn=Mock(side_effect=RuntimeError('must not run'))
        r=recovery_once(source_root=root,runtime_root=root,asof=DAY,sync_fn=fn)
        assert r['attempts']==0 and r['status']=='ALREADY_READY_NO_RETRY'
        fn.assert_not_called()


def test_recovery_attempts_exactly_once_on_missing():
    with TemporaryDirectory() as tmp:
        root=Path(tmp)
        fn=Mock(return_value={'status':'NOT_READY'})
        r=recovery_once(source_root=root,runtime_root=root,asof=DAY,sync_fn=fn)
        assert r['attempts']==1 and fn.call_count==1
        assert r['status']=='RETRY_FAILED_STILL_NOT_READY'


def test_recovery_success_after_mock_refresh():
    with TemporaryDirectory() as tmp:
        root=Path(tmp)
        def fake_sync(**kwargs):
            fixture_tree(kwargs['output_root'])
        r=recovery_once(source_root=root,runtime_root=root,asof=DAY,sync_fn=fake_sync)
        assert r['attempts']==1 and r['status']=='RECOVERED_INTERNAL_SHA_EOD_CURRENT'


def test_recovery_does_not_promote_old_prices():
    with TemporaryDirectory() as tmp:
        root=Path(tmp)
        def fake_sync(**kwargs):
            fixture_tree(kwargs['output_root'])
            p=root/'data/h136/H136_health.json'
            p.write_bytes(_json_bytes({'schema':'H136-health-1','run_day_tw':'2026-10-19',
                                       'status':'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY'}))
        r=recovery_once(source_root=root,runtime_root=root,asof=DAY,sync_fn=fake_sync)
        assert r['status']=='RETRY_FAILED_STILL_NOT_READY'


def test_choose_source_prefers_current_complete_h117():
    with TemporaryDirectory() as a, TemporaryDirectory() as b:
        first,second=Path(a),Path(b)
        for src in ('TWSE','TPEX','TAIEX'):
            p=second/'data/h117/daily'/src/(str(DAY)+'.json.gz')
            p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'placeholder')
        assert _choose_market(first,second,DAY)==second
        # Presence is not a genuine origin claim: downstream parser must verify manifest.


def test_no_audit_never_calculates():
    with TemporaryDirectory() as tmp:
        root=Path(tmp);fixture_tree(root)
        (root/'data/h137/H137_audit.json').unlink()
        assert settle_once(runtime_root=root,asof=DAY)['status']=='BLOCKED_NO_VERIFIED_RESEARCH_DATA'


class FakeResponse:
    def __init__(self,status,obj=None): self.status_code,self.obj=status,obj
    def json(self): return self.obj

class FakeGitHub:
    def __init__(self, payload): self.payload=payload
    def get(self, url, **kw):
        return FakeResponse(200,{'encoding':'base64',
                                 'content':base64.b64encode(self.payload).decode()})

CFG={'owner':'offline','repo':'stock','branch':'runtime-data','token':'OFFLINE_FAKE'}

def test_remote_canonical_verified():
    f,m=dataset();out=build_settlement(asof=DAY,frames=f,manifest=m,audit=AUDIT)
    assert read_remote_settlement(config=CFG,transport=FakeGitHub(_json_bytes(out)),asof=DAY)['schema']==SCHEMA


def test_remote_noncanonical_rejected():
    f,m=dataset();out=build_settlement(asof=DAY,frames=f,manifest=m,audit=AUDIT)
    invalid=json.dumps(out,ensure_ascii=False).encode()
    assert read_remote_settlement(config=CFG,transport=FakeGitHub(invalid),asof=DAY)['status']=='REMOTE_INVALID_RECEIPT'


def test_remote_stale_rejected():
    f,m=dataset();out=build_settlement(asof=DAY,frames=f,manifest=m,audit=AUDIT)
    assert read_remote_settlement(config=CFG,transport=FakeGitHub(_json_bytes(out)),asof=date(2026,10,29))['status']=='OUTDATED_RECEIPT'


def test_missing_token_not_attempt_network():
    assert read_remote_settlement(config={**CFG,'token':''})['status']=='NOT_CONFIGURED'


def test_extreme_return_quarantined():
    f,m=dataset()
    f['stock'].loc[(f['stock']['股票代號']=='2330') & (f['stock']['日期']==str(DAY)),'收盤價']=147.0
    report=build_settlement(asof=DAY,frames=f,manifest=m,audit=AUDIT)
    assert report['strategies']['TOP3']['horizons']['T+1']['newly_quarantined_days']==1


def test_static_policy_does_not_auto_promote():
    f,m=dataset();report=build_settlement(asof=DAY,frames=f,manifest=m,audit=AUDIT)
    for pol in report['strategies'].values():
        for item in pol['horizons'].values():
            assert item['drift_advice'] in ('INSUFFICIENT_INDEPENDENT_DAYS_OR_COVERAGE',
                                           'ADVERSE_SHIFT_RESEARCH_REVIEW_ONLY',
                                           'DESCRIPTIVE_NO_CONFIRMED_ADVERSE_SHIFT')
            assert item['allow_auto_weight_update'] is False

def test_staged_retry_does_not_overwrite_old_cache_on_failure():
    with TemporaryDirectory() as tmp:
        root=Path(tmp)
        fixture_tree(root)
        p=root/'data/h136/H136_health.json'
        p.write_bytes(_json_bytes({'schema':'H136-health-1','run_day_tw':'2026-10-19',
                                    'status':'NOT_READY_OLD_DATA_PRESERVED'}))
        before={f.name:f.read_bytes() for f in (root/'data/h136').iterdir()}
        def failed_stage(**kwargs):
            fixture_tree(kwargs['output_root'])
            (kwargs['output_root']/'data/h136/H136_stock.csv').write_bytes(b'CORRUPT')
        result=recovery_once(source_root=root,runtime_root=root,asof=DAY,sync_fn=failed_stage)
        after={f.name:f.read_bytes() for f in (root/'data/h136').iterdir()}
        assert result['status']=='RETRY_FAILED_STILL_NOT_READY'
        assert before==after


from godpick_h139_settlement import read_remote_recovery

def test_recovery_remote_receipt_roundtrip():
    receipt={'schema':'H139-recovery-1','asof_tw':str(DAY),
             'attempts':0,'status':'ALREADY_READY_NO_RETRY'}
    assert read_remote_recovery(config=CFG,transport=FakeGitHub(_json_bytes(receipt)),asof=DAY)['attempts']==0


def test_recovery_remote_fake_success_rejected():
    receipt={'schema':'H139-recovery-1','asof_tw':str(DAY),'attempts':99,'status':'REMOTE_CONFIRMED'}
    assert read_remote_recovery(config=CFG,transport=FakeGitHub(_json_bytes(receipt)),asof=DAY)['status']=='REMOTE_INVALID_RECEIPT'


def test_recovery_remote_stale_rejected():
    receipt={'schema':'H139-recovery-1','asof_tw':str(DAY),'attempts':1,'status':'RETRY_FAILED_STILL_NOT_READY'}
    assert read_remote_recovery(config=CFG,transport=FakeGitHub(_json_bytes(receipt)),asof=date(2026,10,29))['status']=='OUTDATED_RECEIPT'
