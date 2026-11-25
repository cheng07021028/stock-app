from datetime import datetime,date,timedelta
import io,copy
import pytest
import godpick_h210_strategy_competition as h

REG=h.REGIMES[0]
BASE=date(2025,1,1)
NOW=datetime.fromisoformat('2026-11-25T23:00:00+08:00')
def dt(i):return (BASE+timedelta(days=i*7)).isoformat()
def window(i,a=.04,b=-.02,c=.01):
    d=dt(i);end=(date.fromisoformat(d)+timedelta(days=5)).isoformat()
    return {'day':d,'end':end,'decision':d+'T21:00:00+08:00','regime':REG,'strategies':{s:{'return':v,'observed':end+'T21:00:00+08:00','candidates':3,'valid':3,'unique':3} for s,v in [('A',a),('B',b),('C',c)]}}
def sample(n=40):return [window(i) for i in range(n)]
def detail(rows,n=5):
    out=[]
    for w in rows:
        for s,v in w['strategies'].items():
            for j in range(3):
                out.append({'市場':'TWSE','期間':n,'研究日':w['day'],'出場日':w['end'],'決策時間':w['decision'],'策略':s,'股票代號':str(1000+j),'status':'VALID_CASH_MODEL','net_return':v['return'],'resolved_at':v['observed']})
    return out
class Evidence:
    def regime(self,m,d,cut):return {'市場':m,'研究日':d,'狀態':REG}
class Market:
    errors=[]
    def __init__(self,rows):
        self.days=sorted({w['day'] for w in rows}|{w['end'] for w in rows});self.cal={d[:7]:[] for d in self.days};self.cal['2026-11']=[];self.cal_observed={'2026-11':NOW}
        # Use a recent holiday-confirmed calendar with no more than a few subsequent sessions.
        self.days.append('2026-11-24');self.cal.update({(date(2025,1,1)+timedelta(days=i)).isoformat()[:7]:[] for i in range(700)})
    def today_confirmed(self,d):return True

def test_two_folds_positive_negative():
    fs,ss=h.compete(sample());assert len(fs)==2 and all(f['champion']=='A' and f['challenger']=='C' and f['status']=='OOS_COMPLETE' for f in fs)
    assert all(s['positive'] for s in ss if s['strategy']=='A');assert all(s['negative'] for s in ss if s['strategy']=='B')

def test_train_only_holdout_cannot_choose_champion():
    r=sample(30)
    for w in r[20:]:w['strategies']['A']['return']=-.1;w['strategies']['B']['return']=.2
    f,s=h.compete(r);assert f[0]['champion']=='A';assert not next(x for x in s if x['strategy']=='A')['positive']

def test_future_observation_not_training():
    r=sample(30);r[0]['strategies']['A']['observed']=r[20]['decision'];f,s=h.compete(r);assert 'A' not in f[0]['eligible']

def test_missing_holdout_no_survivor_bias():
    r=sample(30);r[21]['strategies']['A']['return']=None;f,s=h.compete(r);assert f[0]['matched_hold']==9 and not s and f[0]['status']=='留出覆蓋不足'

def test_incomplete_strategy_window():
    d=detail(sample(1));d[0]['status']='除權息涵蓋待補證';w=h.windows(d,Evidence(),NOW)['TWSE',5][0];assert w['strategies']['A']['return'] is None;assert w['strategies']['B']['return']==pytest.approx(-.02)

def test_duplicate_no_double_count():
    d=detail(sample(1));d.append(copy.deepcopy(d[0]));w=h.windows(d,Evidence(),NOW)['TWSE',5][0];assert w['strategies']['A']['return'] is None

def test_minimum_stocks():
    d=detail(sample(1))[1:];w=h.windows(d,Evidence(),NOW)['TWSE',5][0];assert w['strategies']['A']['return'] is None

def test_common_windows_before_regime_filter():
    d=detail(sample(2));x=copy.deepcopy(d[0]);x['研究日']='2025-01-02';x['決策時間']='2025-01-02T21:00:00+08:00';x['出場日']='2025-01-07';d.append(x)
    assert len(h.windows(d,Evidence(),NOW)['TWSE',5])==2

def test_review_gate_no_weight_change():
    rows=sample();r=h.assess(detail(rows),Market(rows),Evidence(),NOW)
    x={v['策略']:v for v in r['recommendations']};assert r['review_count']==2 and x['A']['狀態'].startswith('冠軍') and x['B']['狀態'].startswith('建議暫緩')
    assert all(v['研究權重倍率']==1. for v in x.values()) and not r['trade_authority_changed'] and not r['production_weights_changed']

@pytest.mark.parametrize('condition',['one_fold','missing_calendar','changed_regime','archive_error','other_horizon','stale','different_peer','late_prior','tail_missing'])
def test_fail_closed(condition):
    rows=sample(30 if condition=='one_fold' else 41 if condition=='tail_missing' else 40);m=Market(rows);e=Evidence();n=1 if condition=='other_horizon' else 5
    if condition=='missing_calendar':m.cal_observed={}
    if condition=='archive_error':m.errors=['bad archive']
    if condition=='changed_regime':
        class E(Evidence):
            def regime(self,m,d,cut):return {'市場':m,'研究日':d,'狀態':h.REGIMES[1] if cut==NOW else REG}
        e=E()
    if condition=='stale':m.days=sorted(set(m.days)|{(date.fromisoformat(rows[-1]['end'])+timedelta(days=i)).isoformat() for i in range(1,40)})
    if condition=='different_peer':rows[0]['strategies'].pop('C')
    if condition=='late_prior':rows[29]['strategies']['A']['observed']=rows[30]['decision']
    d=detail(rows,n)
    if condition=='tail_missing':d[-1]['status']='行情缺口'
    assert h.assess(d,m,e,NOW)['status']=='BASELINE_ONLY'

def test_tail_drift_veto_champion():
    rows=sample(43)
    for w in rows[-3:]:w['strategies']['A']['return']=-.12
    r=h.assess(detail(rows),Market(rows),Evidence(),NOW)
    assert next(v for v in r['recommendations'] if v['策略']=='A')['狀態']=='近期衰退警示／不淘汰'

def test_unknown_market_regime_coverage():
    rows=sample(43)
    class E(Evidence):
        def regime(self,m,d,cut):return {'市場':m,'研究日':d,'狀態':'待補證' if d in [w['day'] for w in rows[:3]] else REG}
    assert h.assess(detail(rows),Market(rows),E(),NOW)['status']=='BASELINE_ONLY'

def test_no_data_read_only_excel(tmp_path):
    r=h.run(tmp_path,NOW);assert r['status']=='BASELINE_ONLY' and not list(tmp_path.iterdir())
    import openpyxl
    w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==7

def test_excel_percent_and_formula_safety():
    rows=sample();r=h.assess(detail(rows),Market(rows),Evidence(),NOW);r['upstream']={};r['recommendations'][0]['策略']='=bad'
    import openpyxl
    w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));ws=w['策略競爭'];cols={v.value:v.column for v in ws[1]};assert ws.cell(2,cols['策略']).data_type=='s';assert ws.cell(2,cols['最近留出相對差']).number_format.startswith('0.00%')
