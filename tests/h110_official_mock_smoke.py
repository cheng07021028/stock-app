# -*- coding: utf-8 -*-
"""Offline mock of a genuine TWSE current-period disclosure; no network."""
import io
import datetime as dt
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from godpick_h109_authority_governor import official_snapshot, join_official
from godpick_h110_evidence_integrity import audit, research_asof

now=dt.datetime.now(ZoneInfo('Asia/Taipei'))
if now.hour < 15:
    print('H110_OFFICIAL_MOCK_SMOKE SKIP (before local close)')
else:
    official_month=(now.replace(day=1)-dt.timedelta(days=1))
    p=f"{official_month.year-1911:03d}{official_month.month:02d}"
    today_roc=f"{now.year-1911:03d}{now.month:02d}{now.day:02d}"
    rows=[{'公司代號':f'{2500+i}', '出表日期':today_roc,'資料年月':p,
           '營業收入-去年同月增減(%)':'+25.5','營業收入-上月比較增減(%)':'3.2'} for i in range(56)]
    class Res(io.BytesIO):
        def __enter__(self): return self
        def __exit__(self,*a): self.close()
    def fake_urlopen(*a, **kw): return Res(json.dumps(rows,ensure_ascii=False).encode('utf-8'))
    with tempfile.TemporaryDirectory() as d:
        os.environ['GODPICK_H109_OFFICIAL_FETCH']='1'
        with patch('godpick_h109_authority_governor.urlopen', fake_urlopen):
            data=official_snapshot(Path(d),now.date())
        assert len(data)==56,len(data)
        rec=data['2500']
        assert rec['H109營收出表日期']==str(now.date()),rec
        merged=join_official({'營收資料日期':str(official_month.year)+f'{official_month.month:02d}'},rec)
        assert merged['月營收YoY%_官方']==25.5
        check=audit({**merged,'H109最終研究優先分':80},market_day=now.date(),info_asof=now.date(),asof_mode='LIVE_EXPLICIT')
        assert check['H110營收來源驗證']=='TWSE_公告日期已核對',check
    os.environ['GODPICK_H110_SCAN_MODE']='AUTO'
    assert research_asof(str(now.date()))[2]
    os.environ['GODPICK_H110_SCAN_MODE']='SAFE'
    assert not research_asof(str(now.date()-dt.timedelta(days=3)))[2]
    print('H110_OFFICIAL_MOCK_SMOKE PASS (56 company payload, publish-day, same-month official override)')
