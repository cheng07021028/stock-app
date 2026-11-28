import copy,json,sys,unittest,types
from pathlib import Path
from datetime import timedelta
from unittest.mock import patch
import pandas as pd
import test_h250r7_tracking as t
import test_h250r8_tracking as v8
import test_h250r9_evidence as v9
import godpick_h250r7_tracking as r
import godpick_h250r10_recovery as x
from tools.h250r10_gap_repair import targets,recover
from godpick_h250r6_monitor import safe_tracking_message

class Recovery(unittest.TestCase):
    def test_all_three_replay_and_no_duplicate(self):
        a=v9.market_archive(other=True);rows=[t.row(str(i)) for i in range(50)]
        for k in r.TASKS:
            p,s,g=x.plan(rows,a,k,5000,{'adjusted_max_symbols':0});self.assertEqual(s['METRICS_PLANNED'],50);self.assertEqual(s['DIAGNOSTICS_PLANNED'],50)
            rows=[dict(z,**p[z['record_id']]) for z in rows];self.assertFalse(x.plan(rows,a,k,5000,{'adjusted_max_symbols':0})[0]);self.assertEqual(rows[0]['市場別'],'上市')
    def test_identity_gap_metadata_not_fake_date(self):
        a=t.archive();row=t.row(推薦日期='');p,s,g=x.plan([row],a,r.TASKS[1],5000,{});self.assertEqual(set(p['A']),{x.GAP_FIELDS[1]});self.assertEqual(s['METRICS_PLANNED'],0);self.assertEqual(s['IDENTITY_PENDING'],1)
        row.update(p['A']);self.assertFalse(x.plan([row],a,r.TASKS[1],5000,{})[0]);self.assertEqual(row['推薦日期'],'')
    def test_duplicate_or_missing_id_never_patched(self):
        p,s,g=x.plan([t.row(),t.row(),t.row('')],t.archive(),r.TASKS[1],5000,{});self.assertFalse(p);self.assertEqual(s['IDENTITY_PENDING'],3)
    def test_exact_gap_day_queue(self):
        a=t.archive(missing=t.DAYS[5]);a.markets={'TWSE':{d:1 for d in t.DAYS},'TPEX':{}};row=t.row();p,s,g=x.plan([row],a,r.TASKS[2],5000,{});row.update(p['A']);self.assertEqual(x.queue_from_rows([row]),[('TWSE',t.DAYS[5],'2330','STOCK_QUOTE_MISSING')]);self.assertNotIn(r.HIT_PROOF,p['A'])
    def test_official_queue_dedup_and_filter(self):
        a=t.archive(missing=t.DAYS[5]);good=dict(market='TWSE',day=t.DAYS[5],code='2330');items=[good,good,dict(good,day=t.DAYS[0]),dict(good,day='2026-11-07'),dict(good,market='bad')];self.assertEqual(targets(items,a),[('TWSE',t.DAYS[5],['2330'])])
    def test_dry_run_never_fetches(self):
        a=t.archive(missing=t.DAYS[5]);a.markets={'TWSE':{d:1 for d in t.DAYS}};q=[dict(market='TWSE',day=t.DAYS[5],code='2330')]
        with patch.object(r,'load_archive',return_value=a),patch('tools.h250r10_gap_repair.ar._get',side_effect=AssertionError):
            result=recover(q,Path('/nonexistent'),apply=False)
        self.assertEqual(result['requests'],0);self.assertEqual(result['targets'][0]['status'],'QUEUED')
    def test_local_official_addition_only_after_readback(self):
        a=t.archive(missing=t.DAYS[5]);a.markets={'TWSE':{d:1 for d in t.DAYS}};q=[dict(market='TWSE',day=t.DAYS[5],code='2330')]
        with patch.object(r,'load_archive',return_value=a),patch('tools.h250r10_gap_repair.ar._get',return_value={}),patch('tools.h250r10_gap_repair.ar.save_supplement',return_value=2) as save,patch('tools.h250r10_gap_repair.ar.quote_rows',return_value={'2330':{}}):
            result=recover(q,Path('/nonexistent'),apply=True,http=object(),now=t.NOW,pause=0)
        self.assertEqual(result['targets'][0]['status'],'TARGET_QUOTES_VERIFIED');self.assertFalse(result['remote_publication']);save.assert_called_once()
    def test_market_conflict_not_fabricated(self):
        a=v9.market_archive(other=True,both=True);p,s,g=x.plan([t.row()],a,r.TASKS[0],5000,{'adjusted_max_symbols':0});self.assertEqual(s['METRICS_PLANNED'],0);self.assertEqual(json.loads(p['A'][x.GAP_FIELDS[0]])['reasons'],['AMBIGUOUS_OFFICIAL_MARKET'])
    def test_strict_proof_not_legacy_label(self):
        a=t.archive();p,s,g=x.plan([t.row()],a,r.TASKS[0],5000,{'adjusted_max_symbols':0});self.assertEqual(s['ADJUSTED_PENDING'],1);self.assertEqual(s['PAGE08_CURRENT_REUSED'],1);self.assertNotIn(r.ADJ_PROOF,p['A'])
    def test_good_provider_and_replay(self):
        a=t.archive();calls=[]
        def fetch(*args):calls.append(args);return v8.provider()
        rows=[t.row(str(i)) for i in range(30)];p,s,g=x.plan(rows,a,r.TASKS[0],5000,{},fetch);self.assertEqual(s['ADJUSTED_CORROBORATED'],30);self.assertEqual(s['ADJUSTED_PENDING'],0);self.assertEqual(len(calls),1)
        rows=[dict(z,**p[z['record_id']]) for z in rows];self.assertFalse(x.plan(rows,a,r.TASKS[0],5000,{},fetch)[0]);self.assertEqual(len(calls),1)
    def test_failed_source_cooldown_and_fairness(self):
        a=t.archive();a.bars['TWSE','2340']=a.bars['TWSE','2330'];a.quotes['TWSE','2340']=a.quotes['TWSE','2330'];rows=[t.row(),t.row('B',股票代號='2340')];calls=[]
        def fail(m,c,*args):calls.append(c);raise ValueError('HTTP_429')
        for n in range(2):
            p,s,g=x.plan(rows,a,r.TASKS[0],5000,{'adjusted_max_symbols':1},fail);rows=[dict(z,**p.get(z['record_id'],{})) for z in rows]
        self.assertEqual(calls,['2330','2340']);p,s,g=x.plan(rows,a,r.TASKS[0],5000,{},fail);self.assertEqual(s['ADJUSTED_COOLDOWN'],2);self.assertEqual(calls,['2330','2340']);self.assertEqual(s['ADJUSTED_PENDING'],2)
        rows=[dict(z,**p.get(z['record_id'],{})) for z in rows];self.assertFalse(x.plan(rows,a,r.TASKS[0],5000,{},fail)[0])
        a.cal.now+=timedelta(hours=7);x.plan(rows,a,r.TASKS[0],5000,{'adjusted_max_symbols':1},fail);self.assertEqual(calls[-1],'2330')
    def test_preserve_old_proof_on_provider_mismatch(self):
        a=t.archive();row=t.row(**{r.ADJ_PROOF:'old evidence',r.ADJ_VALUE:9});p,s,g=x.plan([row],a,r.TASKS[0],5000,{},lambda *a:v8.provider(raw_end=118));self.assertEqual(s['ADJUSTED_PENDING'],1);self.assertNotIn(r.ADJ_PROOF,p['A']);self.assertNotIn(r.ADJ_VALUE,p['A']);self.assertEqual(s['ADJUSTED_ATTEMPT_FAILED'],1)
    def test_remapped_adjustment_keeps_identity(self):
        a=v9.market_archive(other=True);row=t.row();p,s,g=x.plan([row],a,r.TASKS[0],5000,{},lambda *args:v8.provider(symbol='2330.TWO'));self.assertEqual(s['ADJUSTED_CORROBORATED'],1);self.assertNotIn('市場別',p['A']);self.assertIn('H250R9_行情身分證據',p['A'])
    def test_task_remote_fail_no_confirm(self):
        obj=t.Authority();obj.setUp();obj.report.github_ok=False
        with patch.dict(sys.modules,{'godpick_h241_github_records':obj.h,'godpick_persistence_service':obj.p,'godpick_auto_update_tasks':obj.tasks}),patch.object(r,'load_archive',return_value=t.archive()):out=r.task(r.TASKS[1],{},r10=True)
        self.assertFalse(out['ok']);self.assertEqual(out['details']['confirmed'],0);self.assertEqual(out['details']['diagnostics']['DIAGNOSTICS_CONFIRMED'],0);self.assertEqual(out['details']['execution_outcome'],'REMOTE_UNCONFIRMED')
    def test_warning_separate_work_outcome(self):
        obj=t.Authority();obj.setUp();obj.rows=[t.row(推薦日期='')]
        with patch.dict(sys.modules,{'godpick_h241_github_records':obj.h,'godpick_persistence_service':obj.p,'godpick_auto_update_tasks':obj.tasks}),patch.object(r,'load_archive',return_value=t.archive()):out=r.task(r.TASKS[1],{},r10=True)
        self.assertTrue(out['ok']);self.assertTrue(out['warning']);self.assertFalse(out['retry_pending']);self.assertEqual(out['details']['confirmed'],0);self.assertEqual(out['details']['diagnostics']['DIAGNOSTICS_CONFIRMED'],1);self.assertEqual(out['details']['execution_outcome'],'REMOTE_CONFIRMED');self.assertEqual(out['details']['evidence_outcome'],'EVIDENCE_PENDING')
    def test_diagnostics_ui_and_sanitizer(self):
        p,s,g=x.plan([t.row(推薦日期='')],t.archive(),r.TASKS[1],5000,{});row=dict(t.row(推薦日期=''),**p['A']);df=x.diagnostics_table(pd.DataFrame([row]));self.assertEqual(df.iloc[1]['原始身分待證'],1);self.assertEqual(df.iloc[0]['未鑑識'],1);msg=safe_tracking_message(r.TASKS[0],'R10 推薦後績效｜08權威 2385 筆／永久確認 10 筆／WORK_VERIFIED 1、ADJUSTED_COOLDOWN 3 https://secret');self.assertIn('還原冷卻 3',msg);self.assertNotIn('secret',msg)

    def test_rate_limit_stops_after_wave(self):
        a=t.archive();rows=[];calls=[]
        for n in range(20):
            c=str(3000+n);a.bars['TWSE',c]=a.bars['TWSE','2330'];a.quotes['TWSE',c]=a.quotes['TWSE','2330'];rows.append(t.row(str(n),股票代號=c))
        def fail(m,c,*args):calls.append(c);raise ValueError('HTTP_429')
        p,s,g=x.plan(rows,a,r.TASKS[0],5000,{},fail);self.assertEqual(len(calls),8);self.assertEqual(s['ADJUSTED_RATE_LIMITED'],1);self.assertEqual(s['ADJUSTED_PROVIDER_DEFERRED'],12)
    def test_completed_warning_is_not_execution_failure_or_success(self):
        import test_h250r6_monitor as m
        obj=m.Test();b=obj.batch();b['validation']['job_results'][0]['status']='WARNING';b['validation']['all_success']=False;b['summary'].update(success=2,warning=1)
        a=m.model(obj.snap(status='completed',conclusion='failure'),{'last_force_enabled_batch':b},m.P,m.L)
        self.assertTrue(a['quality_pending']);self.assertFalse(a['all_ok']);self.assertIn('嚴格批次驗收未通過',a['title'])
        b['summary']['warning']=2;self.assertFalse(m.model(obj.snap(status='completed',conclusion='failure'),{'last_force_enabled_batch':b},m.P,m.L)['quality_pending'])
    def test_new_metadata_protected_by_legacy_merge(self):
        if 'streamlit' not in sys.modules:sys.modules['streamlit']=types.SimpleNamespace(secrets={})
        import godpick_h241_github_records as h
        old=t.row(**{x.ATTEMPT:'prior',x.GAP_FIELDS[0]:'proof'});new=h._merge(old,dict(old,**{x.ATTEMPT:'changed',x.GAP_FIELDS[0]:''}));self.assertEqual(new[x.ATTEMPT],'prior');self.assertEqual(new[x.GAP_FIELDS[0]],'proof')

if __name__=='__main__':unittest.main()
