from datetime import datetime,timedelta
import io,pytest
from openpyxl import Workbook,load_workbook
import godpick_h195_command_center as h
from test_h194_performance import setup_market,NOW

def candidate(code='1000',tier=h.TIERS[0],market='TWSE'):
    return {'市場':market,'股票代號':code,'股票名稱':'=1+1','研究分級':tier,'研究順序':1,'技術研究池':'技術突破' if tier in h.TIERS[:2] else '', '財報研究池':'營收成長' if tier!=h.TIERS[1] else '', '技術日':'2026-11-23','營收年增%':20,'財報缺口':'待查','交易狀態':'禁止買進'}
def joint(rows,day='2026-11-23'):
    return {'asof':NOW.isoformat(),'market_day':day,'same_session':bool(day),'pools':{t:[r for r in rows if r['研究分級']==t] for t in h.TIERS},'audit':rows,'archive_errors':[]}
def write(root,b):h.ar._write(root/h.REL,h.ar._seal(b))
def test_missing(tmp_path):assert h.load(tmp_path,NOW)[1]=='MISSING'
def test_financial_preserved_no_tech(tmp_path):
    r=h.build(joint([candidate(tier=h.TIERS[0]),candidate('1001',h.TIERS[2]),candidate('1002',h.TIERS[3])],''),{},tmp_path,NOW,0)
    assert len(r['rows'])==2 and r['counts'][h.TIERS[0]]==0 and r['counts'][h.TIERS[3]]==1
    assert all(x['交易狀態']=='僅研究／未授權' and x['上漲機率%'] is None for x in r['rows'])
def test_match_frozen(tmp_path):
    market=setup_market(tmp_path); r=joint([candidate()]);model=h.perf.models({}, {},NOW);model['TWSE|'+h.TIERS[0]]['probabilities']=[.1,.2,.7]
    body,_=h.perf.freeze(r,market,model,NOW);h.perf.immutable(tmp_path/'data/h194/signals/2026-11-23.json.gz',body)
    b=h.build(r,{},tmp_path,NOW,0);x=b['rows'][0];assert x['上漲機率%']==70 and x['收盤日']=='2026-11-23' and x['隔日預測日']=='2026-11-23'
    r['pools'][h.TIERS[0]][0]['技術研究池']='低位階蓄勢';b=h.build(r,{},tmp_path,NOW,0);assert b['rows'][0]['上漲機率%'] is None and '不一致' in b['rows'][0]['預測狀態']
def test_no_cross_market(tmp_path):
    setup_market(tmp_path);r=h.build(joint([candidate(market='TPEX')]),{},tmp_path,NOW,0);assert r['rows'][0]['官方原始收盤價'] is None
@pytest.mark.parametrize('offset,status',[(timedelta(minutes=1),'CURRENT'),(timedelta(hours=10),'CURRENT'),(timedelta(hours=22),'STALE'),(timedelta(minutes=-1),'INVALID')])
def test_fresh(tmp_path,offset,status):
    b=h.build(joint([candidate()]),{},tmp_path,NOW,0);write(tmp_path,b);assert h.load(tmp_path,NOW+offset)[1]==status

def test_manager_preserves_existing(tmp_path):
    b=h.build(joint([candidate()]),{},tmp_path,NOW,0);write(tmp_path,b);w=Workbook();w.active.title='00_主管重點觀察榜';w.active['A1']='existing';w.create_sheet('00_H126主管決策')['A1']='禁止買進'
    ws=h.append_manager(w,tmp_path,'2026-11-23',NOW);assert w.sheetnames==['00_主管重點觀察榜','00_H126主管決策','H195_聯合決策研究'];assert w.worksheets[1]['A1'].value=='禁止買進';assert ws['E8'].data_type!='f' and ws['E8'].value=="'=1+1";assert ws['Y8'].value=='僅研究／未授權'
    wb=load_workbook(io.BytesIO(h.workbook(tmp_path,NOW)));assert len(wb.sheetnames)==5

def test_mismatched_run_date(tmp_path):
    b=h.build(joint([candidate()]),{},tmp_path,NOW,0);write(tmp_path,b);w=Workbook();ws=h.append_manager(w,tmp_path,'2026-11-20',NOW);assert ws['A8'].value.startswith('待確認')
def test_bad_ledger_does_not_hide_research(tmp_path):
    p=tmp_path/'data/h194/signals/a.json.gz';p.parent.mkdir(parents=True);p.write_bytes(b'bad');b=h.build(joint([candidate()]),{},tmp_path,NOW,0);assert b['errors'] and len(b['rows'])==1 and b['rows'][0]['上漲機率%'] is None

def test_duplicate_rejected(tmp_path):
    with pytest.raises(ValueError):h.build(joint([candidate(),candidate(tier=h.TIERS[2])]),{},tmp_path,NOW,0)
def test_publish_read_no_authority_mutation(tmp_path,monkeypatch):
    p=tmp_path/'godpick_records.json';p.write_text('sentinel');monkeypatch.setattr(h.joint,'run',lambda *a,**kw:(joint([candidate(tier=h.TIERS[2])],''),{}));b=h.publish(tmp_path,NOW);assert h.load(tmp_path,NOW)[0]==b and p.read_text()=='sentinel';assert len(b['reviews'])==1

def test_integrity(tmp_path):
    b=h.build(joint([candidate()]),{},tmp_path,NOW,0);write(tmp_path,b);p=tmp_path/h.REL;p.write_bytes(b'broken');assert h.load(tmp_path,NOW)[1]=='INVALID'

def test_snapshot_cache_refresh(tmp_path):
    b=h.build(joint([candidate()]),{},tmp_path,NOW,0);write(tmp_path,b);first=h.cache_token(tmp_path,NOW);b['asof']=(NOW+timedelta(minutes=1)).isoformat();write(tmp_path,b);assert h.cache_token(tmp_path,NOW+timedelta(minutes=1))!=first
