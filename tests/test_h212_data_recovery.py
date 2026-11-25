from datetime import datetime,timedelta
from pathlib import Path
import io,json,pytest
import godpick_h212_data_recovery as h
import godpick_h190_market_archive as ar
import godpick_h194_performance as perf
import godpick_h209_rolling_backtest as back
import godpick_h211_counterevidence as ce
from test_h190_market_archive import raw,calendar,DAY,NOW

OBS=datetime.fromisoformat(NOW);LATER=OBS+timedelta(hours=1)

def seed(tmp,market='TWSE'):
    p=tmp/'data/h190';ar.save_calendar(p,calendar([DAY]));ar.save_day(p,ar.parse_daily(raw(market=market),market,DAY,observed_at=NOW));return p

@pytest.mark.parametrize('market',['TWSE','TPEX'])
def test_additive_and_first_observation(tmp_path,market):
    p=seed(tmp_path,market);before=(p/'daily'/market/(DAY+'.json.gz')).read_bytes();rr=raw(market=market,n=ar.MIN_ROWS[market]+1);new=rr['tables'][0]['data'][-1][0];assert ar.save_supplement(p,market,DAY,rr,LATER.isoformat())==1
    assert (p/'daily'/market/(DAY+'.json.gz')).read_bytes()==before
    q=ar.quote_rows(p,market,DAY,OBS);assert new not in q
    q=ar.quote_rows(p,market,DAY,LATER);assert q[new][1]==LATER and q[next(iter(q))][1]==OBS
    assert ar.save_supplement(p,market,DAY,rr,LATER.isoformat())==0
    assert len(ar._index(p,asof=LATER)[2][market,new])==1 and (market,new) not in ar._index(p,asof=OBS)[2]
    m=perf.Market(tmp_path,LATER);assert m.quotes(market,DAY)[new]==100
    assert back.Evidence(tmp_path,m,LATER).quote(market,DAY,new)[1]==LATER

@pytest.mark.parametrize('kind',['conflict','deleted','bad_date','duplicate','future'])
def test_bad_supplement_refused(tmp_path,kind):
    p=seed(tmp_path);r=raw(n=ar.MIN_ROWS['TWSE']+1);d=r['tables'][0]['data'];obs=LATER
    if kind=='conflict':d[0][7]='100.5'
    elif kind=='deleted':d.pop(0)
    elif kind=='bad_date':r['date']='20261122'
    elif kind=='duplicate':d.append(d[0])
    elif kind=='future':obs=OBS-timedelta(days=1)
    with pytest.raises(ValueError):ar.save_supplement(p,'TWSE',DAY,r,obs.isoformat())
    assert not ar.supplemental_path(p,'TWSE',DAY).exists()


def test_same_report_no_new_rows(tmp_path):
    p=seed(tmp_path);assert ar.save_supplement(p,'TWSE',DAY,raw(),LATER.isoformat())==0
    assert not ar.supplemental_path(p,'TWSE',DAY).exists()


def test_zero_volume_not_added(tmp_path):
    p=seed(tmp_path);r=raw(n=ar.MIN_ROWS['TWSE']+1);r['tables'][0]['data'][-1][2]='0'
    assert ar.save_supplement(p,'TWSE',DAY,r,LATER.isoformat())==0


def test_tampered_supplement_is_error(tmp_path):
    p=seed(tmp_path);ar.save_supplement(p,'TWSE',DAY,raw(n=ar.MIN_ROWS['TWSE']+1),LATER.isoformat());f=ar.supplemental_path(p,'TWSE',DAY);v=ar._read(f);v['base_sha256']='bad';ar._write(f,ar._seal(v));ar._invalidate(p)
    assert ar._index(p,asof=LATER)[3]


def test_repair_budget_round_robin_and_cooldown(tmp_path,monkeypatch):
    p=seed(tmp_path);seed(tmp_path,'TPEX');q=[{'市場':m,'交易日':DAY,'缺檔數':n} for m,n in [('TWSE',5),('TPEX',9)]]
    monkeypatch.setattr(h,'gaps',lambda *a,**kw:{'day_queue':q,'coverage':{},'archive_errors':[]})
    calls=[]
    def get(http,url,params,timeout):
        m=next(k for k,v in ar.URLS.items() if v==url);calls.append(m);return raw(market=m)
    monkeypatch.setattr(ar,'_get',get)
    r=h.repair(tmp_path,LATER,budget=2,http=object(),pause=0);assert r['requests']==2 and calls==['TWSE','TPEX'] and r['new_valid_rows']==0
    r=h.repair(tmp_path,LATER+timedelta(days=1),budget=2,http=object(),pause=0);assert r['requests']==0


def test_repair_adds_and_reports(tmp_path,monkeypatch):
    p=seed(tmp_path);q=[{'市場':'TWSE','交易日':DAY,'缺檔數':1}];monkeypatch.setattr(h,'gaps',lambda *a,**kw:{'day_queue':q,'coverage':{},'archive_errors':[]});monkeypatch.setattr(ar,'_get',lambda *a,**kw:raw(n=ar.MIN_ROWS['TWSE']+1))
    r=h.repair(tmp_path,LATER,budget=1,http=object(),pause=0);assert r['new_valid_rows']==1 and r['tried'][0]['status']=='ADDED' and ar._read(tmp_path/h.REL)['new_valid_rows']==1


def test_financial_only_evidence_not_missing():
    r={'技術日':'','技術研究池':'','H198資料日期':DAY,'H198階段':'其他型態','H211技術證據':{'流動性通過':True,'收盤':100,'MA20':99,'MA20前5日':98,'5日漲跌%':1,'20日漲跌%':2,'量比':1}}
    assert ce.tech_ok(r,DAY) and not ce.tech_ok(r,'2026-11-24')
    r['技術研究池']='主流強勢';assert not ce.tech_ok(r,DAY)

@pytest.mark.parametrize('status,raw_,net,expect', [('VALID_CASH_MODEL',.01,-.001,'原價非負但含成本淨損（模型差額）'),('VALID_CASH_MODEL',-.03,-.04,'原始價格下跌且淨損'),('VALID_CASH_MODEL',-.02,.01,'非虧損'),('除權息涵蓋待補證',.01,None,'不可歸因／除權息涵蓋待補證')])
def test_attribution(status,raw_,net,expect):
    d,s=h.attribution([{'市場':'TWSE','策略':'主流','期間':5,'status':status,'raw_return':raw_,'net_return':net}]);assert d[0]['結果分類']==expect and s[0]['策略列數']==1


def test_empty_readonly_no_write(tmp_path):
    r=h.run(tmp_path,LATER);assert r['status']=='DATA_PENDING' and r['counts']['valid_rows']==0 and not r['leaders'] and not (tmp_path/'data').exists()
    import openpyxl
    w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==12 and '03_逐股缺日' in w.sheetnames
