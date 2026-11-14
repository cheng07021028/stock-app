# -*- coding: utf-8 -*-
"""H158 official TAIFEX parsing; entirely synthetic no external requests."""
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import godpick_h158_taifex_openapi as api

NOW = datetime(2026, 11, 14, 10, 55, tzinfo=ZoneInfo("Asia/Taipei"))


class Resp:
    def __init__(self, payload=None, csv_text=None):
        self.payload = payload
        self.content = csv_text.encode("utf-8") if csv_text is not None else b""
        self.text = csv_text or ""
    def json(self):
        if self.payload is None:
            raise ValueError("TAIFEX emitted CSV")
        return self.payload
    def raise_for_status(self):
        return None


def _pcr(d="20261113"):
    return [{"Date": d, "PutVolume": "473795", "CallVolume": "480228", "PutOI": "61300", "CallOI": "106148"}]


def _fut(d="20261113"):
    return [{"Date": d, "ContractCode": "臺股期貨", "Item": "外資及陸資", "OpenInterest(Net)": "-24390"},
            {"Date": d, "ContractCode": "小型臺指期貨", "Item": "外資及陸資", "OpenInterest(Net)": "999999"}]


def test_verified_json_source_both_dates_and_units():
    urls = []
    def get(url, **kw):
        urls.append(url)
        return Resp(_pcr() if url.endswith("PutCallRatio") else _fut())
    result, diag = api.fetch_taifex_openapi(now=NOW, http_get=get)
    assert len(urls) == 2 and api.complete(result, NOW), diag
    assert result["外資臺指期未平倉淨口數"] == -24390
    assert abs(result["PCR成交量比%"] - 98.66) < 0.1
    assert abs(result["PCR未平倉量比%"] - 57.75) < 0.1


def test_csv_degraded_provider_is_parsed_safely():
    csv_pcr = "Date,PutVolume,CallVolume,PutOI,CallOI\n20261113,473795,480228,61300,106148\n"
    csv_fut = 'Date,ContractCode,Item,OpenInterest(Net)\n20261113,臺股期貨,外資及陸資,-24390\n'
    def get(url, **kw):
        return Resp(csv_text=csv_pcr if url.endswith("PutCallRatio") else csv_fut)
    result, _ = api.fetch_taifex_openapi(now=NOW, http_get=get)
    assert api.complete(result, NOW)


def test_stale_and_future_sources_not_complete():
    assert not api.parse_pcr(_pcr("20261012"), NOW)
    assert not api.parse_futures(_fut("20261116"), NOW)
    assert not api.complete({"TAIFEX_PCR資料日期":"2026-11-13","TAIFEX期貨資料日期":"2026-11-13","來源":"UNVERIFIED","PCR成交量比%":99,"PCR未平倉量比%":70,"外資臺指期未平倉淨口數":-4000}, NOW)


def test_wrong_contract_or_unknown_field_never_guessed():
    rows = [{"Date":"20261113","ContractCode":"小型臺指期貨","Item":"外資及陸資","OpenInterest(Net)":"999"},
            {"Date":"20261113","ContractCode":"臺股期貨","Item":"投信","OpenInterest(Net)":"123"}]
    assert not api.parse_futures(rows, NOW)
    rows.append({"Date":"20261113","ContractCode":"臺股期貨","Item":"外資及陸資","OpenInterest(Short)":"10000"})
    assert not api.parse_futures(rows, NOW)


def test_http_fail_or_partial_pcr_not_green():
    def get(url, **kw):
        if url.endswith("PutCallRatio"):
            raise TimeoutError("mock offline")
        return Resp(_fut())
    result, diag = api.fetch_taifex_openapi(now=NOW, http_get=get)
    assert result.get("外資臺指期未平倉淨口數") == -24390
    assert not api.complete(result, NOW)
    assert "失敗" in " ".join(diag)


def test_zero_oi_is_legitimate_number():
    rows = _fut()
    rows[0]["OpenInterest(Net)"] = "0"
    assert api.parse_futures(rows, NOW)["外資臺指期未平倉淨口數"] == 0


def test_bad_units_do_not_become_zero():
    rows = _fut()
    rows[0]["OpenInterest(Net)"] = "--"
    assert not api.parse_futures(rows, NOW)
