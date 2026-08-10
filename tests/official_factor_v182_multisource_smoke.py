# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import tempfile
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import official_factor_service as ofs


def assert_true(cond, msg):
    if not cond:
        raise AssertionError(msg)


def main():
    calls=[]
    def fake_get_json(url, params=None):
        calls.append((url, params))
        if url == ofs.TWSE_BWIBBU_ALL:
            return [
                {"Date":"1150807","Code":"1101","Name":"台泥","PEratio":"20","DividendYield":"3.2","PBratio":"1.1"},
                {"Date":"1150807","Code":"1102","Name":"亞泥","PEratio":"","DividendYield":"7.0","PBratio":"0.7"},
            ]
        if url == ofs.TWSE_DAILY_CLOSE_ALL:
            return [
                {"Date":"1150807","Code":"1101","ClosingPrice":"40"},
                {"Date":"1150807","Code":"1102","ClosingPrice":"45"},
            ]
        if url == ofs.TPEX_PERATIO_OPENAPI:
            return [
                {"Date":"1150807","SecuritiesCompanyCode":"6488","PriceEarningRatio":"25","DividendYield":"1.5","PriceBookRatio":"5.0"},
                {"Date":"1150807","SecuritiesCompanyCode":"8299","PriceEarningRatio":"","DividendYield":"2.0","PriceBookRatio":"4.0"},
            ]
        if url == ofs.TPEX_DAILY_CLOSE_OPENAPI:
            return [
                {"SecuritiesCompanyCode":"6488","Close":"500"},
                {"SecuritiesCompanyCode":"8299","Close":"900"},
            ]
        if url == ofs.TWSE_MONTHLY_REVENUE_L:
            return [{
                "公司代號":"1101","資料年月":"11507",
                "營業收入-當月營收":"1200","營業收入-上月營收":"1000","營業收入-去年當月營收":"800",
                "累計營業收入-當月累計營收":"7000","累計營業收入-去年累計營收":"5600",
            }]
        if url == ofs.TPEX_MONTHLY_REVENUE_O:
            return [{
                "公司代號":"6488","資料年月":"11507",
                "營業收入-當月營收":"2400","營業收入-上月營收":"2000","營業收入-去年當月營收":"1600",
                "累計營業收入-當月累計營收":"14000","累計營業收入-去年累計營收":"11200",
            }]
        if url == ofs.TPEX_3INSTI_OPENAPI:
            return [{
                "Date":"1150807","SecuritiesCompanyCode":"6488",
                "Foreign Investors include Mainland Area Investors-Difference":"1000",
                "SecuritiesInvestmentTrustCompanies-Difference":"200",
                "Dealers-Difference":"-100","TotalDifference":"1100",
            }]
        raise AssertionError(f"unexpected URL (legacy path should not be called while OpenAPI succeeds): {url}")

    orig_json = ofs._get_json
    orig_hist = ofs.INSTITUTIONAL_HISTORY_FILE
    try:
        ofs._get_json = fake_get_json
        with tempfile.TemporaryDirectory() as td:
            ofs.INSTITUTIONAL_HISTORY_FILE = Path(td) / "inst_history.json"

            twse_val, msg1 = ofs.fetch_twse_bwibbu_all()
            assert_true(len(twse_val)==2, f"TWSE valuation rows: {len(twse_val)}")
            eps1101 = float(twse_val.loc[twse_val['股票代號'].eq('1101'),'估算EPS'].iloc[0])
            assert_true(abs(eps1101-2.0)<1e-9, f"TWSE estimated EPS wrong: {eps1101}")
            # loss-making / blank PER still counts valuation if PBR or yield is present
            score1102 = ofs._calc_scores(twse_val.loc[twse_val['股票代號'].eq('1102')].iloc[0].to_dict())
            assert_true(score1102['官方資料完整度']==25, f"PBR/yield valuation completeness wrong: {score1102}")

            otc_val, msg2 = ofs.fetch_tpex_valuation()
            assert_true(len(otc_val)==2, f"TPEx valuation rows: {len(otc_val)}")
            eps6488=float(otc_val.loc[otc_val['股票代號'].eq('6488'),'估算EPS'].iloc[0])
            assert_true(abs(eps6488-20.0)<1e-9, f"TPEx estimated EPS wrong: {eps6488}")

            rev, msg3 = ofs.fetch_monthly_revenue()
            assert_true(set(rev['股票代號'])=={'1101','6488'}, f"revenue coverage wrong: {rev['股票代號'].tolist()}")
            r1101=rev.loc[rev['股票代號'].eq('1101')].iloc[0]
            assert_true(abs(float(r1101['月營收MoM%'])-20.0)<1e-9, f"MoM wrong: {r1101['月營收MoM%']}")
            assert_true(abs(float(r1101['月營收YoY%'])-50.0)<1e-9, f"YoY wrong: {r1101['月營收YoY%']}")

            inst, msg4 = ofs.fetch_tpex_institutional(days=5)
            assert_true(len(inst)==1 and inst.iloc[0]['股票代號']=='6488', f"TPEx inst missing: {inst}")
            assert_true(int(inst.iloc[0]['三大法人近1日合計'])==1100, f"TPEx inst total wrong: {inst.iloc[0].to_dict()}")
            assert_true('OPENAPI' in str(inst.iloc[0].get('法人資料源','')).upper(), "source not marked")

            # second day snapshot: verify true rolling accumulation, not copying one-day into five-day
            ofs._save_institutional_daily_snapshot('上櫃','20260806',[{"股票代號":"6488","foreign":500,"trust":100,"dealer":0,"total":600}])
            roll=ofs._aggregate_institutional_history('上櫃',days=5)
            row=roll.loc[roll['股票代號'].eq('6488')].iloc[0]
            assert_true(int(row['三大法人近1日合計'])==1100, f"1d rolling wrong: {row.to_dict()}")
            assert_true(int(row['三大法人近5日合計'])==1700, f"5d rolling wrong: {row.to_dict()}")

        called_urls=[x[0] for x in calls]
        assert_true(ofs.TPEX_PERATIO_LEGACY not in called_urls, "legacy peratio called despite OpenAPI success")
        assert_true(ofs.TPEX_3ITRADE_LEGACY not in called_urls, "legacy institutional called despite OpenAPI success")

        # Failure mode: a current TPEx API outage must not trigger repeated known-404 legacy routes by default.
        fail_calls=[]
        def fail_current(url, params=None):
            fail_calls.append(url)
            if url in {ofs.TPEX_PERATIO_OPENAPI, ofs.TPEX_3INSTI_OPENAPI, ofs.TPEX_MONTHLY_REVENUE_O}:
                raise RuntimeError("simulated current API outage")
            if url == ofs.TWSE_MONTHLY_REVENUE_L:
                return []
            raise AssertionError(f"obsolete legacy route unexpectedly called: {url}")
        ofs._get_json=fail_current
        old_env=__import__('os').environ.pop('OFFICIAL_FACTOR_ENABLE_LEGACY_ENDPOINTS', None)
        try:
            vfail, vmsg=ofs.fetch_tpex_valuation()
            ifail, imsg=ofs.fetch_tpex_institutional(days=5)
            rfail, rmsg=ofs.fetch_monthly_revenue()
        finally:
            if old_env is not None:
                __import__('os').environ['OFFICIAL_FACTOR_ENABLE_LEGACY_ENDPOINTS']=old_env
            ofs._get_json=fake_get_json
        assert_true(ofs.TPEX_PERATIO_LEGACY not in fail_calls, f"legacy peratio called on outage: {fail_calls}")
        assert_true(ofs.TPEX_3ITRADE_LEGACY not in fail_calls, f"legacy inst called on outage: {fail_calls}")
        assert_true('跳過' in vmsg and ('FinMind' in vmsg or '快取' in vmsg), f"valuation fallback diagnosis missing: {vmsg}")
        assert_true('跳過' in imsg and ('FinMind' in imsg or '快取' in imsg), f"inst fallback diagnosis missing: {imsg}")
        assert_true('legacy MOPS' in rmsg, f"revenue fallback diagnosis missing: {rmsg}")

        # Security: query string/tokens must never reach diagnostics.
        secret='SECRET_TOKEN_ABC123'
        orig_token=ofs._finmind_token
        ofs._finmind_token=lambda: secret
        try:
            red=ofs._compact_error(RuntimeError(f"400 for https://api.finmindtrade.com/api/v4/data?dataset=X&token={secret}&data_id=1101"))
            assert_true(secret not in red, f"token leaked in diagnostics: {red}")
            assert_true('query-redacted' in red or 'REDACTED' in red, f"query not redacted: {red}")
        finally:
            ofs._finmind_token=orig_token

        # FinMind request must use Authorization header, never params[token].
        captured={}
        class Resp:
            status_code=200
            content=b'{}'
            text='{"status":200,"data":[]}'
            def raise_for_status(self): return None
            def json(self): return {"status":200,"data":[]}
        orig_req=ofs.requests.get
        orig_token=ofs._finmind_token
        orig_consume=ofs._consume_request
        ofs._finmind_token=lambda: secret
        ofs._consume_request=lambda label='': 1.0
        def fake_req(url, params=None, headers=None, timeout=None, **kwargs):
            captured.update(url=url,params=dict(params or {}),headers=dict(headers or {}),timeout=timeout)
            return Resp()
        ofs.requests.get=fake_req
        try:
            ofs._finmind_get('TaiwanStockPER','2026-08-01','2026-08-07',data_id='1101')
        finally:
            ofs.requests.get=orig_req; ofs._finmind_token=orig_token; ofs._consume_request=orig_consume
        assert_true('token' not in captured['params'], f"token still in query params: {captured}")
        assert_true(captured['headers'].get('Authorization')==f'Bearer {secret}', "Bearer header missing")

        # FinMind monthly revenue math: revenue_month/year are identifiers, not growth percentages.
        fm=ofs._finmind_revenue_frame([
            {"stock_id":"1101","date":"2025-07-10","revenue_year":2025,"revenue_month":7,"revenue":800},
            {"stock_id":"1101","date":"2026-06-10","revenue_year":2026,"revenue_month":6,"revenue":1000},
            {"stock_id":"1101","date":"2026-07-10","revenue_year":2026,"revenue_month":7,"revenue":1200},
        ])
        f=fm.iloc[0]
        assert_true(abs(float(f['月營收MoM%'])-20.0)<1e-9, f"FinMind MoM math wrong: {f.to_dict()}")
        assert_true(abs(float(f['月營收YoY%'])-50.0)<1e-9, f"FinMind YoY math wrong: {f.to_dict()}")

        print('PASS official_factor_v182_multisource_smoke')
        print('TWSE:',msg1)
        print('TPEX VAL:',msg2)
        print('REVENUE:',msg3)
        print('TPEX INST:',msg4)
    finally:
        ofs._get_json=orig_json
        ofs.INSTITUTIONAL_HISTORY_FILE=orig_hist

if __name__=='__main__':
    main()
