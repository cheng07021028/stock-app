from datetime import datetime,date,timedelta
import io
import pytest
import godpick_h197_strategy_review as h
p=h.p
NOW=datetime.fromisoformat('2026-11-24T23:00:00+08:00')
class Market:
    errors=[]
    def __init__(self,days,quotes):self.days=days;self.q=quotes
    def span(self,d,n):
        i=self.days.index(d);v=self.days[i:i+n+1];return v if len(v)==n+1 else []
    def quotes(self,m,d):return self.q.get((m,d),{})
    def index(self,d):return None

def sample(edge=.02,missing=False,late=False):
    # Simulated calendar, not actual market sessions.
    days=[(date(2026,1,1)+timedelta(days=i)).isoformat() for i in range(190)]
    ss={};oo={};q={}
    for i in range(0,180,6):
        d,t=days[i],days[i+5]
        for j in range(8):
            code=str(1000+j);s={'market':'TWSE','signal_day':d,'code':code,'tier':p.TIERS[1],'created_at':d+'T22:00:00+08:00','base_close':100.,'authority':'RESEARCH_ONLY','tech_pools':'主流強勢' if j<4 else '技術突破','financial_pools':''};ss[p.sid(s)]=s
            v=100*(1+edge) if j<4 else 100.;q.setdefault(('TWSE',d),{})[code]=100.;q.setdefault(('TWSE',t),{})[code]=v
            o={'target_day':t,'horizon':5,'target_close':v,'raw_return':v/100-1,'signal_hash':p.digest(s),'resolved_at':('2026-11-24' if late else t)+'T22:00:00+08:00'}
            if not (missing and j==0 and i==0):oo[(*p.sid(s),5)]=o
    return ss,oo,Market(days,q)

def get(r,strategy='主流強勢',market='TWSE'):return next(x for x in r['evaluation'] if x['策略']==strategy and x['市場']==market)
def test_empty(tmp_path):
    r=h.run(tmp_path,NOW);assert r['status']=='DATA_PENDING' and len(r['evaluation'])==12 and len(r['summary'])==60 and not r['weights_changed'];assert not (tmp_path/'data').exists()
@pytest.mark.parametrize('edge,state',[(.02,'研究優先複核候選'),(-.02,'建議暫緩提升研究權重／人工複核'),(0,'優勢未確認／維持觀察')])
def test_train_hold(edge,state):
    s,o,m=sample(edge);r=h.evaluate(s,o,m,NOW);a=get(r);assert a['評估狀態']==state and a['訓練_日數']==20 and a['留出_日數']==10;assert a['留出_官方指數有效日']==0;assert not r['trade_authority_changed']
def test_missing_coverage():
    s,o,m=sample(missing=True);a=get(h.evaluate(s,o,m,NOW));assert a['評估狀態']!='研究優先複核候選' and a['訓練_日數']==19

def test_late_data_no_lookahead():
    s,o,m=sample(late=True);assert get(h.evaluate(s,o,m,NOW))['評估狀態']=='成熟覆蓋不足／繼續補證'
def test_target_date_and_hash():
    s,o,m=sample();o[next(iter(o))]['target_day']='2026-01-03';o[list(o)[1]]['signal_hash']='bad';a=get(h.evaluate(s,o,m,NOW));assert a['訓練_日數']==19

def test_archive_error():
    s,o,m=sample();m.errors=['bad archive'];assert get(h.evaluate(s,o,m,NOW))['評估狀態']=='行情驗證異常／禁止評估'
def test_no_comparator():
    s,o,m=sample();s={k:v for k,v in s.items() if v['tech_pools']=='主流強勢'};r=h.evaluate(s,o,m,NOW);assert r['status']=='DATA_PENDING'
def test_membership_exact():
    assert h.memberships({'tech_pools':'主流強勢、技術突破、主流強勢','financial_pools':'單月營收觀察、月增待年增補證'})=={'主流強勢','技術突破'}
    assert h.memberships({'tech_pools':'','financial_pools':'連兩季三率三升、低PE高TTM EPS'})=={h.STRATEGIES[-1]}
def test_overlap():
    assert len(h.independent([{'研究日':'01','目標日':'05'},{'研究日':'03','目標日':'07'},{'研究日':'05','目標日':'09'},{'研究日':'06','目標日':'10'}]))==2

def test_export(tmp_path):
    import openpyxl
    w=openpyxl.load_workbook(io.BytesIO(h.excel(h.run(tmp_path,NOW))));assert len(w.sheetnames)==4
