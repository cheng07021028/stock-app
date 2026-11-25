from datetime import datetime,date,timedelta
from pathlib import Path
import io,copy,pytest
import godpick_h209_rolling_backtest as h
NOW=datetime.fromisoformat('2026-11-25T23:30:00+08:00')
DAYS=['2026-11-23','2026-11-24','2026-11-25']
FIELDS=['除權息日期','股票代號','股票名稱','除權息前收盤價','除權息參考價','權值+息值','權/息']
def source(day,rows=()):return {'stat':'OK','title':day+' 至 '+day+' 除權除息計算結果表','fields':FIELDS,'data':list(rows),'total':len(rows)}
def event(day,prev=100,ref=95,value=5,kind='息'):return [day,'1000','A',prev,ref,value,kind]
def signal(day=DAYS[0]):return {'market':'TWSE','code':'1000','name':'=BAD','signal_day':day,'created_at':day+'T22:00:00+08:00','base_close':100.,'strategies':['技術突破'],'source':'H194','authority':'RESEARCH_ONLY'}
class Evidence:
    def __init__(self,prices=(100,100,95),events=None,missing=None,locked=False):self.prices=prices;self.ev=events or {};self.missing=missing;self.locked=locked
    def quote(self,m,d,c):
        i=DAYS.index(d);v=self.prices[i];op=100 if i==1 else v;hi=max(v,op)+1;lo=min(v,op)-1
        if self.locked and i==1:hi=lo=v=op
        return ['1000',op,hi,lo,v,300000,30000000,100],datetime.fromisoformat(d+'T21:00:00+08:00')
    def action(self,m,d,c):return self.ev.get(d),None if self.missing==d else NOW

def test_cash_dividend():
    a=h.parse_actions(source(DAYS[2],[event(DAYS[2])]),'TWSE',DAYS[2],NOW)['rows'][0];assert a['cash']==5
    r=h.trade(signal(),DAYS,Evidence(events={DAYS[2]:a}));assert r['status']=='VALID_CASH_MODEL';assert r['cash_dividend']==5000;assert r['reference_return']==pytest.approx(0);assert r['raw_return']==pytest.approx(-.05);assert -.02<r['net_return']<0
@pytest.mark.parametrize('market',['TWSE','TPEX'])
def test_dated_empty(market):assert h.parse_actions(source(DAYS[2]),market,DAYS[2],NOW)['rows']==[]
@pytest.mark.parametrize('bad',[{}, {'stat':'OK','data':[]},{'stat':'OK','fields':FIELDS,'data':[]},{**source(DAYS[2]),'total':1},source(DAYS[1]),source(DAYS[2],[event(DAYS[1])]),source(DAYS[2],[event(DAYS[2]),event(DAYS[2])])])
def test_unverified_actions(bad):
    with pytest.raises(ValueError):h.parse_actions(bad,'TWSE',DAYS[2],NOW)
def test_complex_rights():
    a=h.parse_actions(source(DAYS[2],[event(DAYS[2],ref=50,value=50,kind='權')]),'TWSE',DAYS[2],NOW)['rows'][0]
    r=h.trade(signal(),DAYS,Evidence((100,100,50),{DAYS[2]:a}));assert r['status']=='複雜權息／僅參考價代理' and r['reference_return']==pytest.approx(0) and 'net_return' not in r

def test_entry_day_no_cash():
    a=h.parse_actions(source(DAYS[1],[event(DAYS[1])]),'TWSE',DAYS[1],NOW)['rows'][0]
    r=h.trade(signal(),DAYS[:2],Evidence(events={DAYS[1]:a}));assert r['status']=='VALID_CASH_MODEL' and r['cash_dividend']==0

def test_gaps_and_conflict():
    assert h.trade(signal(),DAYS,Evidence(missing=DAYS[2]))['status']=='除權息涵蓋待補證'
    assert h.trade(signal(),DAYS,Evidence(locked=True))['status']=='單一價格／成交待核'
    s=signal();s['base_close']=90;assert h.trade(s,DAYS,Evidence())['status']=='基準價衝突'
    ev={'previous_close':90,'reference':85,'cash':5,'kind':'息'};assert h.trade(signal(),DAYS,Evidence(events={DAYS[2]:ev}))['status']=='權息參考價／前收衝突'

def test_cost_and_drawdown():
    r=h.trade(signal(),DAYS,Evidence((100,100,100)));assert r['net_return']<0 and r['trade_mdd']==pytest.approx(r['net_return']);assert h.drawdown([1,1.2,0.9,1.3])==pytest.approx(-.25)

def test_read_sealed_action(tmp_path):
    v=h.parse_actions(source(DAYS[2]),'TWSE',DAYS[2],NOW);p=h.action_path(tmp_path,'TWSE',DAYS[2]);h.ar._write(p,h.ar._seal(v));assert h.read_actions(tmp_path,'TWSE',DAYS[2],NOW)[0]=={}
    v['source_url']='https://invalid.example';h.ar._write(p,h.ar._seal(v))
    with pytest.raises(ValueError):h.read_actions(tmp_path,'TWSE',DAYS[2],NOW)

def test_empty_run_and_excel(tmp_path):
    r=h.run(tmp_path,NOW);assert r['status']=='DATA_PENDING' and not r['valid_rows'] and not (tmp_path/'data/h209').exists()
    import openpyxl
    w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==5

class Market:
    def span(self,d,n):return DAYS[:2] if n==1 else []

def test_cohort_no_survivorship(monkeypatch):
    a,b=signal(),signal();b['code']='1001'
    def tr(s,ds,e):return {'status':'VALID_CASH_MODEL','net_return':.1,'trade_mdd':-.01,'resolved_at':NOW.isoformat()} if s['code']=='1000' else {'status':'行情缺口'}
    monkeypatch.setattr(h,'trade',tr)
    rows,summary,cohorts,folds,_=h.evaluate([a,b],Market(),None,NOW)
    assert summary[0]['覆蓋率']==.5 and summary[0]['期末再平衡淨報酬'] is None and cohorts[0]['淨報酬'] is None
    import openpyxl
    r={'summary':summary,'folds':folds,'cohorts':cohorts,'detail':rows}
    w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert w['逐筆研究']['D2'].data_type!='f'

def test_walk_forward_no_lookahead(monkeypatch):
    dates=[(date(2026,7,1)+timedelta(days=i*2)).isoformat() for i in range(40)]
    class M:
        def span(self,d,n):return [d,(date.fromisoformat(d)+timedelta(days=1)).isoformat()] if n==1 else []
    def tr(s,ds,e):return {'status':'VALID_CASH_MODEL','net_return':.01,'trade_mdd':-.02,'resolved_at':ds[-1]+'T22:00:00+08:00'}
    monkeypatch.setattr(h,'trade',tr)
    _,summary,_,folds,_=h.evaluate([signal(d) for d in dates],M(),None,NOW)
    assert len(folds)==2 and all(f['狀態']=='FIXED_RULE_OOS' for f in folds) and summary[0]['期末再平衡淨報酬']==pytest.approx(1.01**40-1)
    def late(s,ds,e):return {**tr(s,ds,e),'resolved_at':NOW.isoformat()}
    monkeypatch.setattr(h,'trade',late)
    *_,folds,_=h.evaluate([signal(d) for d in dates],M(),None,NOW);assert all(f['樣本外均值'] is None for f in folds)

def test_collect_retry_and_no_overwrite(tmp_path):
    class Http:
        def __init__(self):self.calls=[]
        def get(self,url,params,timeout):
            self.calls.append((url,params));d=h.ar._day(params['endDate']);start=h.ar._day(params['startDate'])
            class R:
                def raise_for_status(self):pass
                def json(self):return {**source(d),'title':start+' 至 '+d+' 除權除息計算結果表'}
            return R()
    class M:days=DAYS
    x=Http();r=h.collect_actions(tmp_path,M(),NOW,[],budget=2,http=x);assert r['saved']==6 and len(x.calls)==2
    r=h.collect_actions(tmp_path,M(),NOW,[],budget=24,http=x);assert r['saved']==0 and len(x.calls)==2
    assert h.collect_actions(tmp_path,M(),NOW,[],budget=24,http=x)['saved']==0


def test_range_explicit_coverage():
    raw={**source(DAYS[2],[event(DAYS[2])]),'title':DAYS[0]+' 至 '+DAYS[2]+' 除權除息計算結果表'}
    assert h.parse_actions(raw,'TWSE',DAYS[1],NOW,start=DAYS[0],end=DAYS[2])['rows']==[]
    assert h.parse_actions(raw,'TWSE',DAYS[2],NOW,start=DAYS[0],end=DAYS[2])['rows'][0]['cash']==5
    with pytest.raises(ValueError):h.parse_actions(raw,'TWSE',DAYS[2],NOW,start=DAYS[1],end=DAYS[2])


def test_capture_recall_only_same_evening(tmp_path,monkeypatch):
    import godpick_h206_market_gap as gap
    import godpick_h208_recall_review as recall
    day=DAYS[2]
    class M:
        def today_confirmed(self,d):return d==day
        def quotes(self,m,d):return {'1000':100}
    monkeypatch.setattr(gap,'build',lambda *a,**k:{'day':day})
    monkeypatch.setattr(recall,'build',lambda *a,**k:{'day':day,'status':'RESEARCH_READY','rows':[{'市場':'TWSE','股票代號':'1000','股票名稱':'A','收盤':100,'召回池':recall.POOLS[0],'H198階段':'蓄勢待突破'}]})
    assert h.capture_recall(tmp_path,M(),NOW)=='H208封存 1 檔'
    ss=h.read_signals(tmp_path,NOW);assert len(ss)==1 and len(ss[0]['strategies'])==2
    path=tmp_path/'data/h209/signals'/(day+'.json.gz');before=path.read_bytes();assert h.capture_recall(tmp_path,M(),NOW)=='當日已封存' and path.read_bytes()==before
    assert h.capture_recall(tmp_path,M(),NOW+timedelta(days=1))=='等待當日官方盤後證據'


def test_daily_fallback_budget(tmp_path):
    class Http:
        def __init__(self):self.n=0
        def get(self,url,params,timeout):
            self.n+=1;start=h.ar._day(params['startDate']);end=h.ar._day(params['endDate'])
            class R:
                def raise_for_status(self):pass
                def json(self):return {} if start!=end else source(end)
            return R()
    class M:days=DAYS
    x=Http();r=h.collect_actions(tmp_path,M(),NOW,[],budget=2,http=x);assert r['saved']==1 and r['attempted']==2 and x.n==2
