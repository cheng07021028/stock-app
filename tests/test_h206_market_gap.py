from datetime import datetime
from pathlib import Path
import io,json
import pytest
from openpyxl import Workbook,load_workbook
import godpick_h206_market_gap as h
from test_h190_market_archive import raw
NOW=datetime.fromisoformat('2026-11-24T23:50:00+08:00');D='2026-11-24';P='2026-11-23'

def setup(root,missing=False):
    ar=h.ar;p=root/'data/h190';ar.save_calendar(p,ar.parse_calendar({'stat':'OK','fields':['日期','收盤指數'],'data':[[d,30000] for d in (P,D)]},'2026-11',observed_at=NOW.isoformat()));master=[]
    for m,n in ar.MIN_ROWS.items():
        for d in (P,D):
            if missing and m=='TPEX' and d==D:continue
            r=raw(d,m);rr=r['tables'][0]['data'];
            for i,row in enumerate(rr):
                row[2]='300000';row[3]='30000000'
                if d==D and i<25:row[5:8]=['110','99',str(104+i%5)]
                if d==P:master.append({'code':row[0],'name':'=test','market':m,'official_industry':'測試'})
            ar.save_day(p,ar.parse_daily(r,m,d,observed_at=d+'T21:00:00+08:00'))
    master.append({'code':'0050','name':'ETF','market':'TWSE'});(root/'stock_master_cache.json').write_text(json.dumps(master),'utf-8')

def test_real_archive_and_two_markets(tmp_path):
    setup(tmp_path);b=h.build(tmp_path,NOW);assert b['status']=='MARKET_READY' and len(b['rows'])==40 and b['day']==D and b['previous_day']==P;assert b['coverage']['TWSE']['雙日覆蓋率%']==100;assert all(r['股票代號']!='0050' for r in b['rows']);assert b['prior_research']['TWSE']['命中比例%'] is None

def test_missing_market(tmp_path):
    setup(tmp_path,True);b=h.build(tmp_path,NOW);assert b['status']=='PARTIAL_OR_STALE' and len(b['rows'])==20 and 'TPEX' not in b['coverage']

def test_prior_hits(tmp_path,monkeypatch):
    setup(tmp_path);r=h.build(tmp_path,NOW)['rows'][0];k=(P,r['市場'],r['股票代號']);monkeypatch.setattr(h.perf,'read_ledger',lambda root:({k:{'created_at':P+'T22:00:00+08:00','tier':'技術研究','base_close':100}},{}));b=h.build(tmp_path,NOW);assert b['prior_research'][r['市場']]['前日研究命中']==1;assert b['prior_research'][r['市場']]['可比較強勢股']==20

def test_prior_price_conflict(tmp_path,monkeypatch):
    setup(tmp_path);r=h.build(tmp_path,NOW)['rows'][0];k=(P,r['市場'],r['股票代號']);monkeypatch.setattr(h.perf,'read_ledger',lambda root:({k:{'created_at':P+'T22:00:00+08:00','tier':'技術研究','base_close':99}},{}));b=h.build(tmp_path,NOW);assert b['rows'][0]['前日研究對帳'].startswith('基準價衝突')

def inputs():
    b={'day':D,'rows':[{'市場':'TWSE','股票代號':str(c),'股票名稱':'=test'} for c in (1000,1001,1002,1003)]};p={'run_identity':'MATCHED','run_id':'x','scan_run_id':'x','h126_buyable':1,'ranked_codes':['1001'],'watch_codes':['1002']};g={'candidate_complete':True,'same_run_proven':True,'audit_final_coverage':{'complete':True}};a=[{'股票代號':'1000','市場別':'上市','H126能否買進':'是'}];t=[{'股票代號':'1003','H148交易日':D,'H148漏網位置':'H113已稽核／未入最終作戰','H148補證優先':'行情日期/歷史K','H148風控':'FADE_VETO'}];return b,dict(research_day=D,parity=p,bridge=g,authority=a,trace=t)

def test_same_run_separate_pools():
    b,k=inputs();c=h.compare_export(b,**k);assert c['same_run_verified'] and c['complete_scope'] and c['h126_buyable']==1;assert [r['本輪對帳'] for r in c['rows']]==['H126原表有授權（仍依原表）','已入A/B研究（非授權）','已入觀察（非授權）','未入本輪研究／逐股複核'];assert c['rows'][3]['H109原風控']=='FADE_VETO'

@pytest.mark.parametrize('field,value',[('research_day',P),('research_day',D+', '+P),('authority',[]),('authority',[{'股票代號':'1000','市場別':'上市','H126能否買進':'禁止買進'}]),('parity',{})])
def test_unverified_run(field,value):
    b,k=inputs();k[field]=value;c=h.compare_export(b,**k);assert c['h126_buyable'] is None and not c['same_run_verified'] and all(r['本輪對帳']=='本輪身分／日期／授權數未核實' for r in c['rows'])

def test_partial_not_miss():
    b,k=inputs();k['bridge']['candidate_complete']=False;c=h.compare_export(b,**k);assert c['rows'][3]['本輪對帳']=='局部母體／不可判定漏選'

def test_zero_placeholder():
    b,k=inputs();k['authority']=[{'股票代號':'','H126權威層別':'無合格推薦','H126能否買進':'禁止買進'}];k['parity']['h126_buyable']=0;assert h.compare_export(b,**k)['h126_buyable']==0

def test_market_identity():
    b,k=inputs();k['authority'][0]['市場別']='上櫃';c=h.compare_export(b,**k);assert c['rows'][0]['本輪對帳']!='H126原表有授權（仍依原表）'

def test_excel_additive_and_safe():
    b,k=inputs();w=Workbook();w.active.title='00_H126主管決策';w.active['A1']='禁止買進';h.append_sheet(w,b,h.compare_export(b,**k));out=io.BytesIO();w.save(out);out.seek(0);x=load_workbook(out);assert x['00_H126主管決策']['A1'].value=='禁止買進';assert x[h.SHEET]['C7'].value=="'=test" and x[h.SHEET]['C7'].data_type!='f'

def test_publish_load(tmp_path):
    setup(tmp_path);b=h.publish(tmp_path,NOW);assert h.load(tmp_path,NOW)==b and not (tmp_path/'godpick_records.json').exists()

def test_future_snapshot(tmp_path):
    setup(tmp_path);h.publish(tmp_path,NOW);assert h.load(tmp_path,datetime.fromisoformat('2026-11-23T23:00:00+08:00'))=={}

def test_invalid_old_archive(tmp_path):
    setup(tmp_path);p=tmp_path/'data/h190/daily/TWSE'/f'{D}.json.gz';p.write_bytes(b'bad');b=h.build(tmp_path,NOW);assert not b['rows'] and b['errors']

def test_joint_same_day_not_prediction(tmp_path):
    from godpick_h195_command_center import VERSION,REL
    setup(tmp_path);r=h.build(tmp_path,NOW)['rows'][0]
    j={'version':VERSION,'trade_authority_changed':False,'asof':NOW.isoformat(),'expected_session':D,'market_day':D,'technical_current':True,'rows':[{'市場':r['市場'],'股票代號':r['股票代號'],'研究分級':'技術研究'}],'counts':{'技術研究':1}}
    h.ar._write(tmp_path/REL,h.ar._seal(j));b=h.build(tmp_path,NOW);assert b['joint_research']['count']==1 and b['rows'][0]['同日H195研究對帳']=='已入同日聯合研究（非預測）';assert b['rows'][0]['前日研究對帳']=='前日無可核對研究封存'
    j['expected_session']=P;h.ar._write(tmp_path/REL,h.ar._seal(j));assert h.build(tmp_path,NOW)['rows'][0]['同日H195研究對帳']=='待同日聯合研究快照'
