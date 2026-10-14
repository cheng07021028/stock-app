# -*- coding: utf-8 -*-
"""H124 synthetic/offline assertions (NOT production TPEx/TDCC API acceptance)."""
from __future__ import annotations
import csv,io,os,sys,tempfile,unittest,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h124_ownership_flow import (build_tpex,build_tdcc,verify_capture,
    save_capture,load_captures,assess_tpex,assess_tdcc)
from godpick_h124_runtime_bridge import apply_strict_h113,official_fields_if_proven
from tools.apply_h124_fullmarket_hook import transform,snapshot

DAYS=['2026-09-21','2026-09-22','2026-09-23','2026-09-24','2026-09-25']
OBS='2026-09-25T19:00:00+08:00'
TPEX_URL='https://www.tpex.org.tw/openapi/v1/official_test'

def rows(day,foreign=2500,trust=1000):
    return [{'資料日期':day.replace('-',''),'證券代號':'1234',
          '外資及陸資買賣超股數(不含外資自營商)':str(foreign),
          '投信買賣超股數':str(trust),
          '三大法人買賣超股數合計':str(foreign+trust+5)}]

def tpex_ev(day,foreign=2500,trust=1000):
    return build_tpex(rows(day,foreign,trust),trade_day=day,collected_at=OBS,source_url=TPEX_URL)

def weekly(day, top_percent=12.00):
    # Make total 10,000,000 shares; top bracket ratio matches shares exactly.
    top=int(top_percent/100*10000000)
    entries=[{'資料日期':day.replace('-',''),'證券代號':'1234','持股分級':k,
              '人數':'20','股數':str(n),'占集保庫存數比例%':str(v)}
             for k,n,v in [(15,top,top_percent),(17,10000000,100.0)]]
    data=io.StringIO();w=csv.DictWriter(data,fieldnames=list(entries[0]));w.writeheader();w.writerows(entries)
    return data.getvalue().encode('utf-8-sig')

def tdcc_ev(day,pct=12):
    return build_tdcc(weekly(day,pct),collected_at=OBS)

class H124(unittest.TestCase):
    def test_tpex_shares_and_report_date(self):
        ev=tpex_ev(DAYS[0]);self.assertTrue(verify_capture(ev))
        self.assertEqual(ev['stocks']['1234']['foreign_shares'],2500)
        with self.assertRaises(ValueError):build_tpex(rows(DAYS[0]),trade_day=DAYS[1],collected_at=OBS,source_url=TPEX_URL)

    def test_tpex_missing_or_ambiguous_columns_fail(self):
        inp=rows(DAYS[0]);inp[0].pop('資料日期')
        with self.assertRaises(ValueError):build_tpex(inp,trade_day=DAYS[0],collected_at=OBS,source_url=TPEX_URL)
        inp=rows(DAYS[0]);inp[0].pop('投信買賣超股數');inp[0]['投信張數']='2'
        with self.assertRaises(ValueError):build_tpex(inp,trade_day=DAYS[0],collected_at=OBS,source_url=TPEX_URL)

    def test_tpex_nonofficial_url_rejected(self):
        with self.assertRaises(ValueError):build_tpex(rows(DAYS[0]),trade_day=DAYS[0],collected_at=OBS,source_url='https://evil.example/openapi/')

    def test_tpex_five_consecutive_and_negative(self):
        good={day:tpex_ev(day) for day in DAYS}
        out=assess_tpex(code='1234',evidence=good,sessions=DAYS,cutoff='2026-09-25T20:00:00+08:00')
        self.assertEqual(out['H123法人實證狀態'],'PROVEN_FLOW')
        self.assertEqual(out['H123外資連買天數'],5)
        sell={day:tpex_ev(day,-10000,-500) for day in DAYS}
        self.assertEqual(assess_tpex(code='1234',evidence=sell,sessions=DAYS,cutoff='2026-09-25T20:00:00+08:00')['H123法人實證狀態'],'VERIFIED_BUT_NOT_BUYING')

    def test_tpex_missing_day_and_backfill(self):
        good={day:tpex_ev(day) for day in DAYS};good.pop(DAYS[2])
        self.assertEqual(assess_tpex(code='1234',evidence=good,sessions=DAYS,cutoff='2026-09-25T20:00:00+08:00')['H123法人實證狀態'],'PENDING_EVIDENCE')
        old={day:tpex_ev(day) for day in DAYS}
        self.assertEqual(assess_tpex(code='1234',evidence=old,sessions=DAYS,cutoff='2026-09-25T18:00:00+08:00')['H123法人實證狀態'],'PENDING_EVIDENCE')

    def test_tdcc_level_15_and_17_not_16(self):
        ev=tdcc_ev('2026-09-18',12)
        self.assertTrue(verify_capture(ev));self.assertEqual(ev['stocks']['1234']['bracket15_pct'],12)
        self.assertEqual(ev['stocks']['1234']['over_1000_lots_bracket_shares'],1200000)

    def test_tdcc_two_weeks_and_delta(self):
        e={'2026-09-18':tdcc_ev('2026-09-18',12),
           '2026-09-25':tdcc_ev('2026-09-25',12.10)}
        result=assess_tdcc(code='1234',evidence=e,cutoff='2026-09-25T20:00:00+08:00')
        self.assertEqual(result['H124大戶實證狀態'],'VERIFIED_RISING')
        self.assertAlmostEqual(result['H124大戶變化pp'],0.1)
        result=assess_tdcc(code='1234',evidence={'2026-09-25':e['2026-09-25']},cutoff='2026-09-25T20:00:00+08:00')
        self.assertEqual(result['H124大戶實證狀態'],'PENDING_EVIDENCE')

    def test_tdcc_decrease_and_stale(self):
        e={'2026-09-18':tdcc_ev('2026-09-18',12.1),
           '2026-09-25':tdcc_ev('2026-09-25',12.0)}
        self.assertEqual(assess_tdcc(code='1234',evidence=e,cutoff='2026-09-25T20:00:00+08:00')['H124大戶實證狀態'],'VERIFIED_NOT_RISING')
        self.assertEqual(assess_tdcc(code='1234',evidence=e,cutoff='2026-10-20T20:00:00+08:00')['H124大戶實證狀態'],'PENDING_EVIDENCE')

    def test_immutable_roundtrip_and_revision_conflict(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);x=tdcc_ev('2026-09-18',12);path=save_capture(root,x)
            self.assertEqual(path,save_capture(root,x))
            self.assertEqual(len(load_captures(root,'TDCC')),1)
            y=tdcc_ev('2026-09-18',12.5);save_capture(root,y)
            with self.assertRaises(ValueError):load_captures(root,'TDCC')

    def test_sha_modified_block(self):
        ev=tdcc_ev('2026-09-18');ev['stocks']['1234']['bracket15_pct']=88
        self.assertFalse(verify_capture(ev))

    def test_strict_can_only_veto(self):
        base={'H113入榜資格':'QUALIFIED','H113大戶鎖碼合格':'是','H113缺失原因':''}
        out=apply_strict_h113(dict(base),{'H123法人連買實證':'是','H124大戶實證狀態':'PENDING_EVIDENCE'},mode='STRICT')
        self.assertEqual(out['H113入榜資格'],'WATCH_ONLY');self.assertEqual(out['H124主榜決策'],'VETO_TDCC_WEEKLY_UNVERIFIED_OR_NOT_RISING')
        out=apply_strict_h113(dict(base),{'H123法人連買實證':'是','H124大戶實證狀態':'VERIFIED_RISING'},mode='STRICT')
        self.assertEqual(out['H113入榜資格'],'QUALIFIED')
        out=apply_strict_h113(dict(base),{'H123法人連買實證':'否','H124大戶實證狀態':'VERIFIED_RISING'},mode='STRICT')
        self.assertEqual(out['H113入榜資格'],'WATCH_ONLY')

    def test_safe_source_patcher_idempotent(self):
        text="from godpick_h123_runtime_bridge import (load_context as h123_load_context, review as h123_review)\n"
        self.assertEqual(transform(transform(text)),transform(text))
        old="FIELDS = ('H123法人實證狀態','H114主導策略')\n"
        self.assertEqual(snapshot(snapshot(old)),snapshot(old))

if __name__=='__main__':unittest.main()
