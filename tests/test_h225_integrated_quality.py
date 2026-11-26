from datetime import datetime
from types import SimpleNamespace
from openpyxl import Workbook,load_workbook
import io
import pytest
import godpick_h225_integrated_quality as h
NOW=datetime.fromisoformat('2026-11-26T19:00:00+08:00');DAY='2026-11-25'
class Market:
    errors=[];days=[DAY,'2026-11-26']
    def quotes(self,m,d):return {'1234':100,'5678':100}
    def today_confirmed(self,d):return True
    def span(self,d,n):return self.days if n==1 else []
K=('TWSE','1234')
def records():
    base={'市場':'TWSE','股票代號':'1234','股票名稱':'示例','營收年月':'2026-10','營收年增%':20,'TTM EPS重建':6,'獲利突破':True}
    g={**base,'原始收盤':100,'連兩季三率三升':True,'籌碼分組':h.rotation.FULL,'法人來源雜湊':'a','TDCC來源雜湊':'b'}
    r={**base,'H220財報交叉一致':True,'H220原始收盤':100,'H224分層':h.resonance.ORDER[0],'連兩季三率年增':True,'籌碼分組':h.rotation.FULL,'法人來源雜湊':'a','TDCC來源雜湊':'b'}
    a={**base,'精簡技術入選':True,'官方原始收盤價':100,'H211反證':'未觸發本規則（非無風險）'}
    return {'H195':{K:a},'H220':{K:g},'H223':{K:base},'H224':{K:r}}
def test_merge():
    all,short,c=h.consolidate(records(),Market(),DAY);assert len(all)==len(short)==1 and short[0]['研究分層']=='財技籌碼共振' and short[0]['來源']=='H195、H220、H223、H224'
@pytest.mark.parametrize('source,field,value',[('H220','原始收盤',101),('H224','法人來源雜湊','other'),('H223','TTM EPS重建',5),('H195','H211反證','風險'),('H195','股票名稱','不同')])
def test_conflict(source,field,value):
    x=records();x[source][K][field]=value;all,short,_=h.consolidate(x,Market(),DAY);assert not short and all[0]['研究分層']=='證據衝突／反證待核'
def test_no_inferred_cross():
    x=records();del x['H220'];all,short,_=h.consolidate(x,Market(),DAY);assert not short

def test_financial_separate():
    x=records();all,short,_=h.consolidate({'H223':x['H223']},Market(),DAY);assert short[0]['研究分層']=='財報成長複核'
def test_gate_counts():
    assert h.gate('t',100,95,'x',at_least=True)['狀態']=='PASS';assert h.gate('t',95,95,'x',ready=False)['狀態']=='PENDING';assert h.gate('t',0,0,'x')['狀態']=='PENDING';assert h.gate('t',100,95,'x',at_least=True)['有效']==100

def histories():
    x=records();v={'day':DAY,'created_at':DAY+'T22:55:00+08:00','rows':[x['H224'][K]]};g={'day':DAY,'created_at':DAY+'T21:35:00+08:00','rows':[x['H220'][K]]};return v,g

def test_maturity(monkeypatch):
    v,g=histories();monkeypatch.setattr(h.back,'trade',lambda *args:{'status':'VALID_CASH_MODEL','net_return':.03,'resolved_at':NOW.isoformat()});d,s=h.mature([v],Market(),None,NOW,[g]);assert len(d)==5 and d[0]['淨報酬']==.03 and s[0]['平均淨報酬']==.03

def test_unproven_early(monkeypatch):
    v,g=histories();monkeypatch.setattr(h.back,'trade',lambda *args:pytest.fail('must not trade without evidence'));d,s=h.mature([v],Market(),None,NOW,[]);assert d[0]['狀態']=='H220原始交集待重驗' and s[0]['平均淨報酬'] is None and s[0]['到期']==1

def test_financial_only_not_trade(monkeypatch):
    v,g=histories();v['rows'][0]['H220財報交叉一致']=False;monkeypatch.setattr(h.back,'trade',lambda *args:pytest.fail('financial only'));d,s=h.mature([v],Market(),None,NOW,[g]);assert not d[0]['可模擬交易'] and s[0]['平均淨報酬'] is None

def test_intraday(monkeypatch):
    v,g=histories();monkeypatch.setattr(h.back,'trade',lambda *args:pytest.fail('intraday'));d,s=h.mature([v],Market(),None,NOW.replace(hour=12),[g]);assert all(x['出場日'] is None for x in d)

def test_manager_original_kept(monkeypatch,tmp_path):
    monkeypatch.setattr(h.p,'Market',lambda *a:Market());monkeypatch.setattr(h,'snapshots',lambda *a:(DAY,records(),[],[],[]));w=Workbook();w.active.title='00_H126主管決策';w.active['A1']='原授權';c={'day':DAY,'market_day':DAY,'same_run_verified':True,'complete_scope':True,'h126_buyable':0};ws=h.append_manager(w,tmp_path,DAY,c,NOW);assert w.sheetnames[0]=='00_H225主管總覽' and ws['B3'].value==0 and w['00_H126主管決策']['A1'].value=='原授權' and ws['B8'].value=='示例';buf=io.BytesIO();w.save(buf);assert load_workbook(buf).sheetnames==w.sheetnames

def test_manager_unverified(monkeypatch,tmp_path):
    monkeypatch.setattr(h.p,'Market',lambda *a:Market());monkeypatch.setattr(h,'snapshots',lambda *a:(DAY,records(),[],[],[]));w=Workbook();ws=h.append_manager(w,tmp_path,DAY,{'h126_buyable':0},NOW);assert ws['B3'].value=='未核實';w=Workbook();ws=h.append_manager(w,tmp_path,'2026-11-24',{},NOW);assert ws['B3'].value=='未核實' and ws['A8'].value.startswith('無同日')
