# -*- coding: utf-8 -*-
import copy,json,os,types,unittest
from unittest.mock import patch
from datetime import datetime
import test_h250r7_tracking as baseline
from test_h250r7_tracking import r, row, archive, DAYS, NOW

# R7-compatible proof versions and independent R8 provider evidence.
def provider(raw_start=100,raw_end=119,adj_start=90,adj_end=119,symbol='2330.TW'):
    return {'symbol':symbol,'bars':{DAYS[0]:(raw_start,adj_start),DAYS[-1]:(raw_end,adj_end)},'sha256':'a'*64}

def pending_row(i='A',**kwargs):
    return row(i,**dict({'績效行情日期':'2026-11-03'},**kwargs))

class FullBacklog(unittest.TestCase):
    def test_legacy_default_migration(self):
        for kind,cfg in zip(r.TASKS,({'max_records':5000},{'max_rows':300},{'max_rows':6})):
            self.assertEqual(r.archive_limit(cfg,kind),5000)
    def test_explicit_cap(self):
        self.assertEqual(r.archive_limit({'archive_max_rows':120},r.TASKS[0]),120)
        self.assertEqual(r.archive_limit({'archive_max_rows':9000},r.TASKS[0]),5000)
        self.assertEqual(r.archive_limit({'max_rows':1},r.TASKS[1]),1)
    def test_2385_all_three_full_scan(self):
        rows=[row(str(i)) for i in range(2385)];a=archive()
        for kind in r.TASKS:
            p,s,g=r.build_extended_plan(rows,a,kind,5000,{'adjusted_max_symbols':0})
            self.assertEqual(len(p),2385);self.assertEqual(s['DEFERRED'],0);self.assertFalse(g)
            self.assertEqual(set(p),{str(i) for i in range(2385)})
            self.assertFalse(any('推薦後1日%' in d for d in p.values()))
    def test_full_idempotence(self):
        a=archive();rows=[row(str(i)) for i in range(25)]
        for kind in r.TASKS:
            p,s,g=r.build_extended_plan(rows,a,kind,5000,{'adjusted_max_symbols':0})
            rows=[dict(x,**p.get(x['record_id'],{})) for x in rows]
            p2,s2,g2=r.build_extended_plan(rows,a,kind,5000,{'adjusted_max_symbols':0})
            self.assertFalse(p2)
    def test_identity_breakdown(self):
        rows=[row('X',推薦日期=''),row('Y',市場別=''),row('Z'),row('Z')]
        p,s,g=r.build_extended_plan(rows,archive(),r.TASKS[1],5000,{})
        self.assertFalse(p);self.assertEqual(s['IDENTITY_PENDING'],4)
        self.assertEqual(g['MISSING_OR_INVALID_RECOMMEND_DATE'],1);self.assertEqual(g['UNKNOWN_MARKET'],1);self.assertEqual(g['DUPLICATE_RECORD_ID'],2)
    def test_calendar_breakdown(self):
        p,s,g=r.build_extended_plan([row('A',推薦日期='2026-10-01'),row('B',推薦日期='2026-11-07')],archive(),r.TASKS[2],5000,{})
        self.assertFalse(p);self.assertEqual(g['MISSING_RECOMMEND_MONTH:2026-10'],1);self.assertEqual(g['RECOMMEND_DATE_NOT_VERIFIED_SESSION'],1)
    def test_quote_breakdown(self):
        p,s,g=r.build_extended_plan([row()],archive(missing=DAYS[0]),r.TASKS[2],5000,{})
        self.assertEqual(g['MISSING_ANCHOR_QUOTE:TWSE:2026-11'],1);self.assertFalse(p)

class AdjustedEvidence(unittest.TestCase):
    def test_corroborated_formula_and_separate_fields(self):
        x=pending_row();a=archive();ident=r.identity(x,NOW);d,state=r.adjusted_delta(x,a,ident,provider())
        self.assertEqual(state,'ADJUSTED_CORROBORATED');self.assertEqual(d[r.ADJ_VALUE],32.22)
        self.assertNotIn('目前損益幅%',d);self.assertTrue(r.adjusted_proof_valid(dict(x,**d),a,ident))
    def test_mismatched_endpoints(self):
        x=pending_row();a=archive();d,s=r.adjusted_delta(x,a,r.identity(x,NOW),provider(raw_end=118))
        self.assertFalse(d);self.assertEqual(s,'OFFICIAL_RAW_MISMATCH')
    def test_wrong_symbol(self):
        x=pending_row();a=archive();d,s=r.adjusted_delta(x,a,r.identity(x,NOW),provider(symbol='2330.TWO'))
        self.assertFalse(d);self.assertEqual(s,'PROVIDER_IDENTITY_MISMATCH')
    def test_zero_adjustment_rejected(self):
        x=pending_row();a=archive();d,s=r.adjusted_delta(x,a,r.identity(x,NOW),provider(adj_start=0))
        self.assertFalse(d);self.assertEqual(s,'INVALID_ADJUSTMENT')
    def test_tampered_proof(self):
        x=pending_row();a=archive();ident=r.identity(x,NOW);d,_=r.adjusted_delta(x,a,ident,provider())
        y=dict(x,**d);y[r.ADJ_VALUE]=999
        self.assertFalse(r.adjusted_proof_valid(y,a,ident))
    def test_one_request_per_market_code_and_no_refetch(self):
        calls=[]
        def fetch(*args):calls.append(args);return provider()
        a=archive();rows=[pending_row(str(i)) for i in range(30)]
        p,s,g=r.build_extended_plan(rows,a,r.TASKS[0],5000,{},fetch)
        self.assertEqual(len(calls),1);self.assertEqual(s['ADJUSTED_CORROBORATED'],30);self.assertEqual(len(p),30)
        rows=[dict(x,**p[x['record_id']]) for x in rows]
        p,s,g=r.build_extended_plan(rows,a,r.TASKS[0],5000,{},fetch)
        self.assertFalse(p);self.assertEqual(len(calls),1)
    def test_fetch_failure_keeps_raw_and_warning(self):
        def fail(*args):raise ValueError('HTTP_429')
        p,s,g=r.build_extended_plan([pending_row()],archive(),r.TASKS[0],5000,{},fail)
        self.assertIn(r.RAW_NOW,p['A']);self.assertNotIn(r.ADJ_PROOF,p['A']);self.assertEqual(s['ADJUSTED_PENDING'],1);self.assertEqual(g['ADJUSTED_FETCH_HTTP_429'],1)
    def test_cap_zero_no_fetch(self):
        def fail(*args):self.fail('network must not run')
        p,s,g=r.build_extended_plan([pending_row()],archive(),r.TASKS[0],5000,{'adjusted_max_symbols':0},fail)
        self.assertEqual(s['ADJUSTED_PROVIDER_DEFERRED'],1);self.assertEqual(s['ADJUSTED_PENDING'],1)
    def test_actual_http_parser(self):
        ts=[int(datetime.fromisoformat(d).replace(tzinfo=r.TZ).timestamp())+9*3600 for d in (DAYS[0],DAYS[-1])]
        payload={'chart':{'result':[{'meta':{'symbol':'2330.TW','currency':'TWD'},'timestamp':ts,'indicators':{'quote':[{'close':[100,119]}],'adjclose':[{'adjclose':[90,119]}]}}]}}
        def resp(obj):return types.SimpleNamespace(status_code=200,content=json.dumps(obj).encode(),json=lambda:obj)
        with patch('requests.get',return_value=resp(payload)) as get:
            out=r.fetch_adjusted('TWSE','2330',DAYS[0],DAYS[-1]);self.assertEqual(out['bars'][DAYS[0]],(100,90));self.assertTrue(get.call_args.args[0].endswith('2330.TW'))
        broken=copy.deepcopy(payload);del broken['chart']['result'][0]['indicators']['adjclose']
        with patch('requests.get',return_value=resp(broken)):
            with self.assertRaisesRegex(ValueError,'EXPLICIT_ADJUSTED_ARRAY_MISSING'):r.fetch_adjusted('TWSE','2330',DAYS[0],DAYS[-1])
        wrong=copy.deepcopy(payload);wrong['chart']['result'][0]['meta']['symbol']='2330.TWO'
        with patch('requests.get',return_value=resp(wrong)):
            with self.assertRaisesRegex(ValueError,'SYMBOL_OR_CURRENCY_MISMATCH'):r.fetch_adjusted('TWSE','2330',DAYS[0],DAYS[-1])
    def test_real_commit_contract(self):
        obj=baseline.Authority();obj.setUp();obj.rows=[pending_row()]
        with patch.object(r,'fetch_adjusted',return_value=provider()):out=obj.run_task(r.TASKS[0])
        self.assertTrue(out['ok']);self.assertFalse(out['warning']);self.assertEqual(out['details']['confirmed'],1);self.assertEqual(out['details']['diagnostics']['ADJUSTED_NEW_CONFIRMED'],1)
        self.assertEqual(obj.commit_calls[0][3][0]['目前損益幅%'],20)
    def test_remote_fail_never_confirmed(self):
        obj=baseline.Authority();obj.setUp();obj.rows=[pending_row()];obj.report.github_ok=False
        with patch.object(r,'fetch_adjusted',return_value=provider()):out=obj.run_task(r.TASKS[0])
        self.assertFalse(out['ok']);self.assertEqual(out['details']['confirmed'],0);self.assertEqual(out['details']['diagnostics']['ADJUSTED_NEW_CONFIRMED'],0)

if __name__=='__main__':unittest.main()
