# -*- coding: utf-8 -*-
import os,sys,json,datetime as dt,tempfile
from pathlib import Path
import pandas as pd
os.environ['GODPICK_H109_OFFICIAL_FETCH']='0'
os.environ['GODPICK_H109_RISK_HISTORY']='1'
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import godpick_h109_authority_governor as h
with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp)
    p=root/'godpick_h109_quarantine_history.json'
    p.write_text(json.dumps({'2059':{'first_day':'2026-10-07','observed_days':['2026-10-07'],'last_shock_day':'2026-10-07'}}))
    for day,expected in [(8,'COOLDOWN_QUARANTINE'),(9,'COOLDOWN_QUARANTINE'),(12,'RESEARCH_ELIGIBLE')]:
        d=dt.date(2026,10,day)
        df=pd.DataFrame([{'股票代號':'2059','H109決策權威':'RESEARCH_ELIGIBLE','H109最終研究優先分':98.}])
        tables={'research':df}
        result=h.apply_quarantine_continuity(tables,root,d,persist=False)
        # re-create journal as if previous trading date had been persisted in live mode
        if expected=='COOLDOWN_QUARANTINE':
            hist=json.loads(p.read_text()); days=hist['2059']['observed_days']; days.append(str(d))
            p.write_text(json.dumps(hist))
        assert tables['research'].iloc[0]['H109決策權威']==expected, tables
    assert result['history']==0

# Mock official network to verify parser and strict as-of behavior with a real cache.
os.environ["GODPICK_H109_OFFICIAL_FETCH"] = "1"
from unittest.mock import patch
from io import BytesIO
class FakeResp(BytesIO):
    def __enter__(self): return self
    def __exit__(self,*args): self.close()
with tempfile.TemporaryDirectory() as tmp:
    day=dt.datetime.now(h.ZoneInfo('Asia/Taipei')).date()
    if dt.datetime.now(h.ZoneInfo('Asia/Taipei')).hour>=15:
        raw=json.dumps([{'公司代號':str(2000+i),'資料年月':f'{day.year-1911}{day.month-1:02d}' if day.month>1 else f'{day.year-1912}12',
                         '營業收入-去年同月增減(%)':'25.1','營業收入-上月比較增減(%)':'6.3'} for i in range(60)]).encode()
        calls=[]
        def response(*args,**kw): calls.append(1); return FakeResp(raw)
        with patch.object(h,'urlopen',side_effect=response):
            a=h.official_snapshot(Path(tmp),day)
            b=h.official_snapshot(Path(tmp),day)
        assert len(a)==60 and a==b and len(calls)==1,(len(a),len(calls))
        assert h.official_snapshot(Path(tmp),day-dt.timedelta(days=1))=={}
    news_payload=[{'公司代號':'2330','發言日期':f'{day.year-1911}{day.month:02d}{day.day:02d}', '事實發生日':'1150101', '主旨 ':'公告重大訊息'}]
    if dt.datetime.now(h.ZoneInfo('Asia/Taipei')).hour>=15:
        with patch.object(h,'urlopen',return_value=FakeResp(json.dumps(news_payload).encode())):
            news=h.official_announcements(Path(tmp),day)
        assert '2330' in news and news['2330']['H109重大訊息狀態'].startswith('已取得公告')
        assert h.official_announcements(Path(tmp),day-dt.timedelta(days=1))=={}
print('H109_SOURCE_COOLDOWN_SMOKE PASS (3 distinct market days, official cache, as-of protection, published-day evidence)')
