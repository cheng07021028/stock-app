from datetime import timedelta
import hashlib
import pytest
import godpick_h232_eps_comparability as h
import godpick_h231_company_history as c
import godpick_h222_financial_history as history
import godpick_h192_financial_growth as f
from test_h231_company_history import fixture,master,NOW,Q,M

def sample(p=Q,shares=1000,numerator=None):
    eps=2*(p%4+1);numerator=eps*shares if numerator is None else numerator
    return fixture(p).replace('</table>',f'<tr><td>基本每股盈餘之分子(仟元)</td><td>{numerator}</td><td>0</td><td>1</td><td>1</td></tr><tr><td>普通股加權平均流通在外股數(仟股)</td><td>{shares}</td><td>0</td><td>1</td><td>1</td></tr></table>')

def persist(root,p=Q,shares=1000,numerator=None,t=NOW):
    html=sample(p,shares,numerator);row=c.parse(html,'2330',p,t)
    history._persist(c.path_for(root,'TWSE','2330',p),{'version':c.VERSION,'url':c.URL,'market':'TWSE','code':'2330','period':p,'observed_at':t.isoformat(),'html':html,'raw_sha256':hashlib.sha256(html.encode()).hexdigest(),'row':row})

def result(root,t=NOW):
    d,e=f.load_inputs(root);assert not e
    l,_,_=f.build_ledgers(d,t)
    return f.financial_profile({},l['margin','TWSE','2330'],l['ytd','TWSE','2330'],{},M,Q,'')

def test_extract_exact():
    assert h.extract(sample(),Q,6)=={'numerator_twd':6000000,'shares':1000000,'reported_eps':6}

@pytest.mark.parametrize('old,new',[('普通股加權平均流通在外股數(仟股)','普通股股本(仟元)'),('基本每股盈餘之分子(仟元)','本期淨利(仟元)'),('普通股加權平均流通在外股數(仟股)','普通股加權平均流通在外股數'),('普通股加權平均流通在外股數(仟股)','稀釋加權平均股數(仟股)')])
def test_no_inferred_denominator(old,new):
    assert h.extract(sample().replace(old,new),Q,6) is None

def test_stable_series(tmp_path):
    master(tmp_path)
    for p in range(Q-7,Q+1):persist(tmp_path,p)
    r=result(tmp_path);assert r['EPS可比性']==h.CONSISTENT and r['EPS同分母TTM']==8 and r['EPS分母核實期數']==5
    assert '待核' in r['EPS可比性'] and '尚未' in r['EPS權益事件']

def test_changed_denominator(tmp_path):
    master(tmp_path)
    for p in range(Q-7,Q+1):persist(tmp_path,p,shares=2000 if p==Q else 1000)
    r=result(tmp_path);assert r['TTM EPS重建']==8 and r['EPS可比性']==h.CHANGED and r['EPS同分母TTM'] is None

def test_numerator_mismatch(tmp_path):
    master(tmp_path)
    for p in range(Q-7,Q+1):persist(tmp_path,p,numerator=999 if p==Q else None)
    r=result(tmp_path);assert r['EPS可比性']==h.CONFLICT and r['EPS同分母TTM'] is None

def test_future_observation(tmp_path):
    master(tmp_path)
    for p in range(Q-7,Q+1):persist(tmp_path,p,t=NOW+timedelta(hours=1) if p==Q else NOW)
    r=result(tmp_path);assert r['EPS同分母TTM'] is None

def test_mixed_source():
    a={p:{'source_url':'a' if p!=Q else 'b','eps_ytd':2*(p%4+1),'h232_eps_basis':{'numerator_twd':2*(p%4+1)*1000,'shares':1000,'reported_eps':2*(p%4+1)}} for p in range(Q-4,Q+1)}
    assert h.audit(a,Q,8)['EPS可比性']==h.CONFLICT

def test_growth_subset():
    r={'EPS可比性':h.CONSISTENT,'EPS同分母TTM':8,'連兩季三率年增':True,'連3月成長':True,'獲利率為正':True,'EPS季度':'2026Q3','三率季度':'2026Q3'}
    assert h.high_growth(r)
    for k,v in [('EPS可比性',h.MISSING),('連兩季三率年增',False),('EPS同分母TTM',4.99),('三率季度','2026Q2')]:assert not h.high_growth({**r,k:v})

def test_report_keeps_old_sheets(tmp_path):
    from openpyxl import load_workbook
    import io
    master(tmp_path)
    for p in range(Q-7,Q+1):persist(tmp_path,p)
    a=history.inspect(tmp_path,NOW); assert a['stocks'][0]['EPS同分母TTM']==8
    assert not a['h232_candidates'] # actual revenue missing
    w=load_workbook(io.BytesIO(history.excel(a)))
    assert w.sheetnames[:5]==['01_母體驗收','02_高成長財報複核','03_逐股缺期','04_回補日誌','05_異常'] and '07_H232同分母成長' in w.sheetnames
