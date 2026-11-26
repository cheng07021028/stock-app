from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
import io,copy
import openpyxl,pytest
import godpick_h220_growth_rotation as h
D='2026-11-25';P='2026-11-24';T='2026-11-26';NOW=datetime.fromisoformat(D+'T21:35:00+08:00');LATER=datetime.fromisoformat(T+'T21:35:00+08:00')
def tech_rows(n=100,valid=100,market='TWSE'):
    return [{'市場':market,'股票代號':str(1000+i),'股票名稱':'test','產業':'半導體' if i<10 else '其他電子','資料日期':D,**({'H220產業量價':{'當日成交額':200 if i<10 else 100,'前20日均成交額':100,'5日漲跌%':5 if i<10 else -1}} if i<valid else {})} for i in range(n)]
def tech(rows):
    return {'asof':D,'diagnostic':rows,'calendar_fresh':True,'archive_errors':[],'status':'PARTIAL_RESEARCH','coverage':{m:{'valid':sum(r['市場']==m and 'H220產業量價' in r for r in rows),'total':sum(r['市場']==m for r in rows)} for m in h.ar.URLS}}
def sector(rs,m='TWSE'):return next(x for x in rs if x['市場']==m and x['產業']=='半導體')
def test_market_split():
    t=tech(tech_rows()+tech_rows(valid=76,market='TPEX'));rs,c=h.sectors(t);assert c['TWSE']['達標'] and not c['TPEX']['達標'];assert sector(rs)['狀態']=='主流活躍' and sector(rs,'TPEX')['狀態']=='待補證'
@pytest.mark.parametrize('v,ready',[(94,False),(95,True),(100,True)])
def test_exact_coverage(v,ready):assert h.sectors(tech(tech_rows(valid=v)))[1]['TWSE']['達標']==ready

def test_reported_count_conflict():
    t=tech(tech_rows());t['coverage']['TWSE']['valid']=99;assert not h.sectors(t)[1]['TWSE']['達標']

def test_duplicate_identity():
    rows=tech_rows();rows.append(rows[0]);with_error=tech(rows)
    with pytest.raises(ValueError):h.sectors(with_error)

def test_sector_inflow_not_claimed():
    r=sector(h.sectors(tech(tech_rows()))[0]);assert r['成交額活躍比']==2 and '非資金淨流入' in r['口徑']

def test_rotation_exact_constituents():
    r=sector(h.sectors(tech(tech_rows()))[0]);old={**r,'狀態':'已核未達主流','成交額活躍比':.8};h.rotation([r],[{'day':P,'sectors':[old]}],P);assert r['輪動狀態']=='新轉活躍' and r['活躍比變化']==pytest.approx(1.2)
    old['有效成分指紋']='changed';h.rotation([r],[{'day':P,'sectors':[old]}],P);assert r['輪動狀態']=='前窗／成分待核' and r['活躍比變化'] is None

def test_rotation_no_prior():
    r=sector(h.sectors(tech(tech_rows()))[0]);h.rotation([r],[],P);assert r['輪動狀態']=='前窗／成分待核'

@pytest.mark.parametrize('f,d,expected',[('PROVEN_FLOW','VERIFIED_RISING',h.FULL),('VERIFIED_BUT_NOT_BUYING','VERIFIED_RISING',h.CONTROL),('PROVEN_FLOW','VERIFIED_NOT_RISING',h.CONTROL),('PENDING_EVIDENCE','VERIFIED_RISING',h.UNKNOWN)])
def test_chips_separate_unknown(monkeypatch,f,d,expected):
    monkeypatch.setattr(h.flow,'assess_flow',lambda **k:{'H123法人實證狀態':f});monkeypatch.setattr(h.own,'assess_tdcc',lambda **k:{'H124大戶實證狀態':d});assert h.chips('TWSE','1001',{'TWSE':{},'TDCC':{}},[],NOW)['籌碼分組']==expected

def test_tpex_never_twse(monkeypatch):
    def bad(**k):raise AssertionError('TWSE not allowed')
    monkeypatch.setattr(h.flow,'assess_flow',bad);monkeypatch.setattr(h.own,'assess_tpex',lambda **k:{'H123法人實證狀態':'PROVEN_FLOW'});monkeypatch.setattr(h.own,'assess_tdcc',lambda **k:{'H124大戶實證狀態':'VERIFIED_RISING'});assert h.chips('TPEX','1001',{'TPEX':{},'TDCC':{}},[],NOW)['籌碼分組']==h.FULL

def candidate(c='1001',group=h.FULL):return {'市場':'TWSE','股票代號':c,'股票名稱':'=SUM(1,2)','資料日':D,'原始收盤':100,'籌碼分組':group}
def current(rows=None):return {'day':D,'financial_day':D,'coverage':{'TWSE':{'達標':True}},'rows':[candidate()] if rows is None else rows,'sectors':[],'audit':[],'source_errors':[],'financial_errors':[]}
def market():return SimpleNamespace(errors=[],days=[D,T],today_confirmed=lambda day:day==D,span=lambda day,n:[D,T] if n==1 else [])
def test_capture_and_immutable(tmp_path):
    m=market();assert h.snapshot(tmp_path,NOW,current(),m)=='CAPTURED';file=tmp_path/h.REL/(D+'.json.gz');before=file.read_bytes();assert h.snapshot(tmp_path,NOW,current([candidate('1002')]),m)=='ALREADY_CAPTURED';assert file.read_bytes()==before and h.read_history(tmp_path,NOW)[0]['rows'][0]['股票代號']=='1001'

def test_empty_and_bad_day_capture(tmp_path):
    m=market();r=current([]);r['financial_errors']=['missing'];assert h.snapshot(tmp_path,NOW,r,m)=='CAPTURE_PENDING';r['financial_errors']=[];assert h.snapshot(tmp_path,NOW,r,m)=='CAPTURED_NO_MATCH'
    other=tmp_path/'other';assert h.snapshot(other,LATER,current(),m)=='CAPTURE_PENDING';assert not other.exists()

def test_before_window(tmp_path):assert h.snapshot(tmp_path,datetime.fromisoformat(D+'T17:00:00+08:00'),current(),market())=='CAPTURE_PENDING'

def test_history_future_not_visible(tmp_path):
    h.snapshot(tmp_path,NOW,current(),market());assert h.read_history(tmp_path,datetime.fromisoformat(D+'T20:00:00+08:00'))==[]

def test_feedback_unknown_excluded_and_full_required(monkeypatch):
    rows=[candidate(str(1001+i),h.FULL if i<3 else h.CONTROL if i<6 else h.UNKNOWN) for i in range(7)]
    hist=[{'day':D,'created_at':NOW.isoformat(),'rows':rows}]
    def trade(s,ds,e):return {'status':'VALID_CASH_MODEL','net_return':.03 if int(s['code'])<=1003 else .01,'resolved_at':LATER.isoformat()}
    monkeypatch.setattr(h.b,'trade',trade);d,p,r=h.feedback(hist,market(),None,LATER);assert len(d)==35 and len(p)==1 and p[0]['籌碼雙證據相對差']==pytest.approx(.02) and p[0]['籌碼待補']==1 and r[0]['狀態']=='BASELINE_ONLY'
    monkeypatch.setattr(h.b,'trade',lambda s,ds,e: {'status':'缺權息'} if s['code']=='1001' else trade(s,ds,e));_,p,_=h.feedback(hist,market(),None,LATER);assert p[0]['籌碼雙證據相對差'] is None

def test_feedback_before_close(monkeypatch):
    monkeypatch.setattr(h.b,'trade',lambda *a:pytest.fail('future trade'))
    d,p,_=h.feedback([{'day':D,'created_at':NOW.isoformat(),'rows':[candidate()]}],market(),None,datetime.fromisoformat(T+'T10:00:00+08:00'));assert len(d)==5 and not p and all(x['淨報酬'] is None for x in d)

def test_manager_excel_safe(tmp_path,monkeypatch):
    h.snapshot(tmp_path,NOW,current(),market());monkeypatch.setattr(h.p,'Market',lambda *a:market());w=openpyxl.Workbook();s=h.append_manager(w,tmp_path,D,NOW);assert s['C2'].data_type=='s';s=h.append_manager(openpyxl.Workbook(),tmp_path,P,NOW);assert '未取得同日' in s['A2'].value

def test_source_fingerprint(tmp_path):
    a=h.source_fingerprint(tmp_path);p=tmp_path/'stock_master_cache.json';p.write_text('[]');assert h.source_fingerprint(tmp_path)!=a

def test_empty_run_and_workbook(tmp_path):
    r=h.run(tmp_path,NOW);assert r['counts']['候選']==0 and r['weight_multiplier']==1.0 and not list(tmp_path.iterdir());w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==10

def test_screen_strict_positive_and_counterevidence(tmp_path,monkeypatch):
    rows=tech_rows();r=rows[0];r.update({'研究池':'技術突破','H198階段':'初期突破待確認','H198資料日期':D,'H198失效參考':99,'H198確認條件':'守箱頂','H211技術證據':{'收盤':100,'MA20':98,'MA20前5日':97,'5日漲跌%':5,'20日漲跌%':8,'量比':1.8,'流動性通過':True}})
    f={'market_day':D,'profiles':[{'市場':'TWSE','股票代號':'1000','營收新鮮':True,'研究池':'營收成長、獲利突破、低PE高TTM EPS','PE同交易日':True,'官方PE':10,'TTM EPS重建':10,'營收年增%':25}]}
    m=market();m.quotes=lambda m,d:{'1000':100}
    monkeypatch.setattr(h,'chips',lambda *a:{'籌碼分組':h.FULL});z=h.screen(tech(rows),f,m,tmp_path,NOW);assert len(z['rows'])==1 and z['rows'][0]['低PE高EPS成立'] and z['rows'][0]['籌碼分組']==h.FULL
    r['H198階段']='突破反證與追高警示';z=h.screen(tech(rows),f,m,tmp_path,NOW);assert not z['rows']
    r['H198階段']='初期突破待確認';f['profiles'][0]['營收新鮮']=False;assert not h.screen(tech(rows),f,m,tmp_path,NOW)['rows']
