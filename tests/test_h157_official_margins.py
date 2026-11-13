# -*- coding: utf-8 -*-
"""H157: reproduce the 2026-11-12 official margins false-green regression."""
from datetime import datetime
from pathlib import Path
import math
import pytest
from godpick_h115_pit_evidence import append_quarterly_margins, TZ_TW
from tools.h1534_import_margins import parse_rows, TWSE_URL


def official(code='2330', gross='30.5', op='18.2', net='13.1', yr='115', q='3'):
    return {'公司代號': code, '年度': yr, '季別': q,
      '毛利率(%)(營業毛利)/(營業收入)': gross,
      '營業利益率(%)(營業利益)/(營業收入)': op,
      '稅後純益率(%)(稅後純益)/(營業收入)': net}


def parse(rows):
    return parse_rows(rows,source='TWSE',source_url=TWSE_URL,
        observed_at=datetime.now(TZ_TW).isoformat(timespec='seconds'))


def test_one_bogus_margin_does_not_abort_official_batch(tmp_path):
    rows=[official(),official('2331',gross='9999.99'),
          official('2332',op='NaN'),official('2333',net='inf'),
          official('2334',gross='-100.01'), official('2335',q='5'),
          official('2336',gross='25.1',op='0',net='-13.8')]
    accepted=parse(rows)
    assert {i['code'] for i in accepted}=={'2330','2336'}
    status=append_quarterly_margins(tmp_path,accepted)
    assert status['received']==2 and status['inserted']==2
    assert append_quarterly_margins(tmp_path,accepted)['inserted']==0
    assert (tmp_path/'quarterly_margins.jsonl').exists()


def test_all_invalid_returns_empty_no_fabricated_evidence(tmp_path):
    values=parse([official('2331',gross='9999'),official('2332',net='NaN'),
                  official('2333',yr='130',q='4'), official('2334',gross='bad')])
    assert values==[]
    assert not (tmp_path/'quarterly_margins.jsonl').exists()


def test_good_zero_and_negative_margins_are_allowed():
    r=parse([official(gross='0',op='-10.1',net='-7.5')])
    assert len(r)==1 and r[0]['operating_margin_pct']==-10.1


def test_workflow_red_if_both_sources_failed():
    text=Path('.github/workflows/h1534_official_margins.yml').read_text()
    assert "if: steps.twse.outcome != 'success' && steps.tpex.outcome != 'success'" in text
    body=text.split('Explicitly fail if no official margins were imported')[1]
    assert 'exit 1' in body.split('- name: Warn on partial official market coverage')[0]
    assert 'Warn on partial official market coverage' in text


def test_independently_validated_first_observed_only():
    rows=parse([official()]); assert rows and rows[0]['availability_basis']=='FIRST_OBSERVED'
    assert rows[0]['statement_basis']=='SINGLE_QUARTER'
