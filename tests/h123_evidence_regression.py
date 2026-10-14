# -*- coding: utf-8 -*-
"""Deterministic synthetic tests: do not claim real official API success."""
from __future__ import annotations
import gzip,hashlib,json,os,sys,tempfile,unittest
from datetime import datetime,timedelta
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h123_flow_provenance import (build_twse_evidence,save_evidence,evidence_by_day,
                                          assess_flow,verify_evidence,enforce_h113)
from godpick_h123_runtime_bridge import load_context,official_fields_if_proven,apply_strict_h113
from godpick_h123_strategy_attribution import review_quality
from godpick_h117_official_market_closure import make_official_day_manifest
from godpick_h116_daily_feedback import make_decision_snapshot

FIELDS=['證券代號','外陸資買賣超股數(不含外資自營商)','投信買賣超股數','三大法人買賣超股數']
DAYS=['2026-09-21','2026-09-22','2026-09-23','2026-09-24','2026-09-25']

def raw(day, foreign=1500, trust=500):
    return {'stat':'OK','date':day.replace('-',''),'fields':FIELDS.copy(),
            'data':[['1234',str(foreign),str(trust),str(foreign+trust+100)]]}

def ev(day, foreign=1500,trust=500):
    return build_twse_evidence(raw(day,foreign,trust),trade_day=day,
           collected_at='2026-10-14T14:00:00+08:00')

class H123(unittest.TestCase):
    def test_twse_real_columns_shares(self):
        e=ev(DAYS[0]);self.assertEqual(e['stocks']['1234']['foreign_shares'],1500)
        self.assertTrue(verify_evidence(e));self.assertEqual(e['source'],'TWSE')
    def test_reject_bad_field_and_wrong_date(self):
        x=raw(DAYS[0]);x['fields'][1]='foreign lots'
        with self.assertRaises(ValueError):build_twse_evidence(x,trade_day=DAYS[0],collected_at='2026-10-14T14:00:00+08:00')
        with self.assertRaises(ValueError):build_twse_evidence(raw(DAYS[0]),trade_day=DAYS[1],collected_at='2026-10-14T14:00:00+08:00')
    def test_reject_fake_source_and_uncertain_capture_time(self):
        with self.assertRaises(ValueError):build_twse_evidence(raw(DAYS[0]),trade_day=DAYS[0],
            collected_at='2026-10-14T14:00:00+08:00',source_url='https://evil.test/fund/T86')
        with self.assertRaises(ValueError):build_twse_evidence(raw(DAYS[0]),trade_day=DAYS[0],
            collected_at='2026-10-14')
    def test_immutable_dedup_and_conflict(self):
        with tempfile.TemporaryDirectory() as td:
            e=ev(DAYS[0]);p=save_evidence(Path(td),e)
            self.assertEqual(p,save_evidence(Path(td),ev(DAYS[0])))
            altered=ev(DAYS[0],foreign=2222);save_evidence(Path(td),altered)
            with self.assertRaises(ValueError):evidence_by_day(Path(td))
    def test_no_future_data_backtest(self):
        ar={d:ev(d) for d in DAYS}
        r=assess_flow(code='1234',market='上市',evidence_days=ar,
            info_cutoff='2026-10-13T20:00:00+08:00',official_sessions=DAYS)
        self.assertEqual(r['H123法人實證狀態'],'PENDING_EVIDENCE')
        self.assertIn('晚於推薦時間',r['H123法人實證原因'])
    def test_five_official_sessions_and_streak(self):
        ar={d:ev(d) for d in DAYS}
        r=assess_flow(code='1234',market='上市',evidence_days=ar,
            info_cutoff='2026-10-14T19:00:00+08:00',official_sessions=DAYS)
        self.assertEqual(r['H123法人實證狀態'],'PROVEN_FLOW')
        self.assertEqual(r['H123外資連買天數'],5)
        self.assertEqual(sum(x['net'] for x in r['H123外資逐日股數']),7500)
        self.assertEqual(official_fields_if_proven(r)['外資近5日買賣超_官方'],7500)
        self.assertEqual(len(r['H123來源雜湊']),64)
    def test_missing_day_and_wrong_market_fail_closed(self):
        ar={d:ev(d) for d in DAYS};ar.pop(DAYS[2])
        result=assess_flow(code='1234',market='上市',evidence_days=ar,
            info_cutoff='2026-10-14T19:00:00+08:00',official_sessions=DAYS)
        self.assertEqual(result['H123法人實證狀態'],'PENDING_EVIDENCE')
        self.assertEqual(assess_flow(code='1234',market='上櫃',evidence_days=ar,
            info_cutoff='2026-10-14T19:00:00+08:00',official_sessions=DAYS)['H123法人連買實證'],'否')
    def test_negative_flow_not_a_buy(self):
        ar={d:ev(d,foreign=-10000,trust=-100) for d in DAYS}
        r=assess_flow(code='1234',market='上市',evidence_days=ar,
            info_cutoff='2026-10-14T19:00:00+08:00',official_sessions=DAYS)
        self.assertEqual(r['H123法人實證狀態'],'VERIFIED_BUT_NOT_BUYING')
        self.assertEqual(r['H123法人連買實證'],'否')
    def test_strict_cannot_grant_h113(self):
        base={'H113入榜資格':'QUALIFIED','H113法人連買合格':'是','H113缺失原因':''}
        row={'H123法人連買實證':'否'}
        x=enforce_h113(row,base)
        self.assertEqual(x['H113入榜資格'],'WATCH_ONLY')
        y=apply_strict_h113(dict(base),{'H123法人連買實證':'是'},mode='STRICT')
        self.assertEqual(y['H123最終主榜准入'],'H113_AND_H109_STILL_REQUIRED')
        self.assertEqual(y['H113入榜資格'],'QUALIFIED')
    def test_missing_weekday_not_assumed_holiday(self):
        missed=['2026-09-21','2026-09-22','2026-09-24','2026-09-25','2026-09-28']
        ar={d:ev(d) for d in missed}
        r=assess_flow(code='1234',market='上市',evidence_days=ar,
           info_cutoff='2026-10-14T19:00:00+08:00',official_sessions=missed)
        self.assertEqual(r['H123法人實證狀態'],'PENDING_EVIDENCE')
        self.assertIn('休市',r['H123法人實證原因'])

    def test_session_union_catches_partial(self):
        # Official quote daily manifests: last five union days, one source absent.
        with tempfile.TemporaryDirectory() as td:
            base=Path(td)
            for day in DAYS:
                for src in ('TWSE','TPEX','TAIEX'):
                    if day==DAYS[-1] and src=='TPEX':continue
                    m=make_official_day_manifest({'source':src,'day':day,
                       'source_url':'https://official.example/quotes','rows':[],'skipped':0},
                       collected_at='2026-10-14T18:30:00+08:00')
                    p=base/'data/h117/daily'/src/(day+'.json.gz');p.parent.mkdir(parents=True,exist_ok=True)
                    with gzip.open(p,'wt',encoding='utf-8') as f:json.dump(m,f)
            os.environ['GODPICK_H123_MODE']='STRICT'
            try:ctx=load_context(base)
            finally:os.environ.pop('GODPICK_H123_MODE',None)
            self.assertEqual(ctx['status'],'INCOMPLETE_OFFICIAL_DAY_CALENDAR')
    def test_strategy_quality_requires_h120_proofs(self):
        snapshot=make_decision_snapshot({'run_id':'run_test_1','version':'h114','research':[
           {'股票代號':'1234','H114主導策略':'H114_VOLUME','H123法人實證狀態':'PROVEN_FLOW',
             'latest_evidence_at':'2026-10-13T17:00:00+08:00'}]},
           run_id='run_test_1',decided_at='2026-10-13T20:00:00+08:00',market_day='2026-10-13')
        details=[{'decision_date':'2026-10-13','code':'1234','strategy':'H114_VOLUME','horizon':'1',
                  'data_status':'OK','return_basis':'PRICE_ONLY_NO_ACTION','net_return_pct':'2.0','alpha_pp':'1.2'}]
        raw=b'example';manifest={'h118_quality_policy':'STRICT','h120_policy':'MANUAL_RAW_PROOF_ENFORCED',
                                'h120_proof_accepted_days':1,'h120_details_csv_sha256':hashlib.sha256(raw).hexdigest()}
        result=review_quality(snapshots=[snapshot],details=details,source_manifest=manifest,details_bytes=raw)
        self.assertEqual(result['status'],'EVIDENCE_COHORTS_RESEARCH_ONLY')
        self.assertEqual(result['groups'][0]['stock_day_samples'],1)
        wrong=review_quality(snapshots=[snapshot],details=details,source_manifest=manifest,details_bytes=b'changed')
        self.assertEqual(wrong['status'],'DETAILS_HASH_MISMATCH')

if __name__=='__main__':unittest.main()
