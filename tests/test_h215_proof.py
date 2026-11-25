from datetime import datetime,timedelta
from types import SimpleNamespace
import io,pytest
import godpick_h215_proof as h
import godpick_h209_rolling_backtest as b
from test_h209_rolling_backtest import source,DAYS,NOW
N=datetime.fromisoformat('2026-11-25T23:35:00+08:00')
@pytest.mark.parametrize('market',['TWSE','TPEX'])
def test_dated_status(market):
    rows=h.parse_status([{'Code':'1001','StartDate':'1151123','EndDate':'1151125','Reason':'停牌'}],market,N)
    gaps=[{'市場':market,'股票代號':'1001','缺日期':'2026-11-23、2026-11-24、2026-11-25、2026-11-26'}]
    assert [x['狀態'] for x in h.classify_gaps(gaps,{market:rows})]==['ANNOUNCEMENT_BOUNDARY','DATED_HALT_INTERVAL_REVIEW','ANNOUNCEMENT_BOUNDARY','NO_STATUS_EVIDENCE']
    assert h.classify_gaps(gaps,{})[0]['狀態']=='NO_STATUS_EVIDENCE'
@pytest.mark.parametrize('raw',[{},[{'Code':'1001'}],[{'Code':'1001','StartDate':'1151125','EndDate':'1151123'}],[{'Code':'1001','StockCode':'1002','StartDate':'1151123'}]])
def test_reject_bad_status(raw):
    with pytest.raises(ValueError):h.parse_status(raw,'TWSE',N)
def test_empty_future_and_open_end():
    assert h.parse_status([],'TWSE',N)==[]
    assert h.parse_status([{'Code':'1001','StartDate':'1151126'}],'TWSE',N)==[]
    r=h.parse_status([{'Code':'1001','StartDate':'1151123'}],'TWSE',N)
    assert h.classify_gaps([{'市場':'TWSE','股票代號':'1001','缺日期':'2026-11-24'}],{'TWSE':r})[0]['狀態']=='OPEN_END_REVIEW'
def market(days=('2026-11-25',),obs=N-timedelta(minutes=5)):
    return SimpleNamespace(days=days,cal_observed={'2026-11':obs})
def q(ok=True):return {'session':'2026-11-25','checks':[{'環節':'H194驗收日研究','狀態':'PASS' if ok else 'PENDING'},{'環節':'成熟含成本淨績效','狀態':'PENDING'}],'counts':{'valid_rows':0},'errors':[]}
U={'status':'EVIDENCE_PENDING'}
def test_daily_pass_not_mature():
    r=h.gate(q(),U,N,market());assert r['daily_status']=='PASS_DAILY' and r['mature_status']=='PENDING' and r['daily_total']==1
@pytest.mark.parametrize('now,expected',[('2026-11-25T17:00:00+08:00','PENDING_DAILY'),('2026-11-25T22:00:00+08:00','PENDING_DAILY'),('2026-11-25T23:35:00+08:00','FAIL_DAILY')])
def test_daily_deadline(now,expected):
    n=datetime.fromisoformat(now);assert h.gate(q(False),U,n,market(obs=n-timedelta(minutes=1)))['daily_status']==expected
def test_holiday_and_unconfirmed_day():
    assert h.gate(q(False),U,N,market(days=()))['daily_status']=='NON_SESSION'
    assert h.gate(q(False),U,N,market(days=(),obs=N-timedelta(days=1)))['daily_status']=='FAIL_DAILY'
    assert h.gate(q(),U,N,market(),['bad archive'])['daily_status']=='FAIL_DAILY'
def test_status_archive(tmp_path):
    class Resp:
        def raise_for_status(self):pass
        def json(self):return [{'Code':'1001','StartDate':'1151123','EndDate':'1151125'}]
    class HTTP:
        def get(self,*a,**kw):return Resp()
    out=h.collect(tmp_path,N,HTTP());assert all(x['rows']==1 for x in out.values())
    assert not h.load_status(tmp_path,N-timedelta(seconds=1))[0]
    assert len(h.load_status(tmp_path,N)[0])==2
    p=next((tmp_path/h.REL/'TWSE').glob('*.gz'));v=h.ar._read(p);v['rows']=[];h.ar._write(p,h.ar._seal(v));assert h.load_status(tmp_path,N)[1]
def test_compact_range_and_diagnostic():
    r=source(DAYS[2]);r['title']='1151125 至 1151125 除權除息計算結果表';assert b.parse_actions(r,'TPEX',DAYS[2],NOW)['rows']==[]
    r['title']='1151124 至 1151124';r['secret']='do not retain'
    with pytest.raises(ValueError):b.parse_actions(r,'TPEX',DAYS[2],NOW)
    d=b.response_diagnostic(r);assert d['root']['metadata']['title']==r['title'] and 'do not retain' not in str(d)
def test_readonly_and_excel(tmp_path):
    r=h.run(tmp_path,N);assert r['gate']['daily_status']=='FAIL_DAILY';assert not (tmp_path/'data').exists()
    import openpyxl
    w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==7
