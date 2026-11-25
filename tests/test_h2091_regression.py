from datetime import datetime, timedelta, date
from pathlib import Path
import io
import pytest
import godpick_h209_rolling_backtest as h
from test_h209_rolling_backtest import NOW,DAYS,FIELDS,source,event,signal,Evidence


def test_official_twse_header_roc_date_and_legacy_archive(tmp_path):
    day=DAYS[2];r=source(day,[event('115年11月25日')]);r['fields']=['資料日期',*FIELDS[1:]];r['title']='115年11月25日 至 115年11月25日 除權除息計算結果表'
    v=h.parse_actions(r,'TWSE',day,NOW,source_url=h.LEGACY_TWSE)
    assert v['rows'][0]['cash']==5
    h.ar._write(h.action_path(tmp_path,'TWSE',day),h.ar._seal(v));assert h.read_actions(tmp_path,'TWSE',day,NOW)[0]['1000']['cash']==5


def test_tpex_named_cash_and_conflict():
    r=source(DAYS[2],[event(DAYS[2])+[5]]);r['fields']=[*FIELDS[:-2],'權值加息值','權或息','現金股利']
    assert h.parse_actions(r,'TPEX',DAYS[2],NOW)['rows'][0]['cash']==5
    r['data'][0][-1]=3
    assert h.parse_actions(r,'TPEX',DAYS[2],NOW)['rows'][0]['cash'] is None


def test_empty_and_intraday_range_rejected():
    r=source(DAYS[2]);r.pop('total')
    with pytest.raises(ValueError,match='zero count'):h.parse_actions(r,'TWSE',DAYS[2],NOW)
    r=source(DAYS[2]);r['title']=DAYS[1]+' 至 '+DAYS[2]
    with pytest.raises(ValueError,match='invalid range'):h.parse_actions(r,'TWSE',DAYS[1],NOW.replace(hour=15),start=DAYS[1],end=DAYS[2])


def test_calendar_refresh_after_close(tmp_path):
    d=DAYS[2];root=tmp_path/'data/h190';raw={'stat':'OK','fields':['日期','收盤指數'],'data':[[d,30000]]}
    a=h.ar.parse_calendar(raw,d[:7],observed_at=d+'T15:00:00+08:00');b=h.ar.parse_calendar(raw,d[:7],observed_at=d+'T18:25:00+08:00')
    assert h.ar.save_calendar(root,a);assert not h.perf.Market(tmp_path,NOW).today_confirmed(d)
    assert h.ar.save_calendar(root,b);assert h.perf.Market(tmp_path,NOW).today_confirmed(d)
    assert not h.ar.save_calendar(root,a);assert not h.ar.save_calendar(root,b)
    assert h.perf.Market(tmp_path,NOW).today_confirmed(d)


def test_no_signals_and_pending_not_zero_win(tmp_path):
    r=h.run(tmp_path,NOW);assert r['pending']=={'尚無前瞻研究封存':1};assert r['revision']=='V191-H209.1' and r['summary']==[]


def test_oos_strict_cutoff_and_new_metrics(monkeypatch):
    dates=[(date(2026,7,1)+timedelta(days=i*2)).isoformat() for i in range(30)]
    class M:
        def span(self,d,n):return [d,(date.fromisoformat(d)+timedelta(days=1)).isoformat()] if n==1 else []
    def tr(s,ds,e):return {'status':'VALID_CASH_MODEL','net_return':.01,'trade_mdd':-.02,'resolved_at':ds[-1]+'T22:00:00+08:00','raw_return':.015,'cash_dividend':0}
    monkeypatch.setattr(h,'trade',tr)
    _,summary,_,folds,_=h.evaluate([signal(d) for d in dates],M(),None,NOW)
    assert summary[0]['中位數淨報酬']==.01 and summary[0]['同樣本原始均值']==.015
    assert folds[0]['樣本外累積淨報酬']==pytest.approx(1.01**10-1) and folds[0]['樣本外勝窗率']==1
    def equal_cut(s,ds,e):
        r=tr(s,ds,e)
        if s['signal_day']==dates[19]:r['resolved_at']=dates[20]+'T22:00:00+08:00'
        return r
    monkeypatch.setattr(h,'trade',equal_cut)
    *_,folds,_=h.evaluate([signal(d) for d in dates],M(),None,NOW)
    assert folds[0]['樣本外累積淨報酬'] is None and folds[0]['訓練有效窗']==19


def test_excel_percent_numeric():
    import openpyxl
    r={'summary':[{'勝率':.5}],'folds':[],'cohorts':[],'detail':[]}
    w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));c=w['策略績效']['A2'];assert c.value==.5 and '%' in c.number_format
