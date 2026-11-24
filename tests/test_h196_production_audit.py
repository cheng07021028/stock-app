from datetime import datetime
import json
from pathlib import Path
import pytest
import godpick_h190_market_archive as ar
from tools.h196_production_audit import audit
from test_h190_market_archive import HTTP, Reply, raw, NOW

class TpexRequiresType(HTTP):
    def get(self,url,params,**kw):
        if url==ar.URLS['TPEX'] and params.get('type')!='EW':
            r=raw(ar._day(params['date']),'TPEX');r['tables'][0]['data']=[];return Reply(r)
        return super().get(url,params,**kw)

def test_actual_request_parameters_and_recovery(tmp_path):
    a=ar.collect(tmp_path,http=TpexRequiresType(fail=ar.URLS['TPEX']),now=datetime.fromisoformat(NOW),budget=3,pause=0)
    assert a['market_files_present']['TWSE']>0 and a['market_files_present']['TPEX']==0
    http=TpexRequiresType();b=ar.collect(tmp_path,http=http,now=datetime.fromisoformat(NOW),budget=4,pause=0)
    assert b['market_files_present']['TPEX']>0 and not b['errors']
    assert all(p['type']=='EW' for u,p in http.calls if u==ar.URLS['TPEX'])
    assert all(ar._read(p)['session_scope']=='TPEX_ROUND_LOT_REPORT_EW' for p in (tmp_path/'daily/TPEX').glob('*.gz'))

def test_empty_repo_does_not_pass(tmp_path):
    r=audit(tmp_path,datetime.fromisoformat(NOW));assert r['status']=='BLOCKED'
    assert 'H195_DEPLOYMENT_MISSING' in r['issues'] and 'H194_NO_PROSPECTIVE_SIGNALS' in r['issues']
    assert 'TWSE_COVERAGE_BELOW_95_PERCENT' in r['issues'] and 'TPEX_COVERAGE_BELOW_95_PERCENT' in r['issues']
    assert r['trade_authority_changed'] is False and not (tmp_path/'data').exists()

def test_audit_one_market_partial_not_success(tmp_path):
    (tmp_path/'stock_master_cache.json').write_text(json.dumps([{'code':'1000','market':'TWSE'},{'code':'5000','market':'TPEX'}]))
    ar.collect(tmp_path/'data/h190',http=HTTP(fail=ar.URLS['TPEX']),now=datetime.fromisoformat(NOW),budget=4,pause=0)
    r=audit(tmp_path,datetime.fromisoformat(NOW));assert r['status']=='BLOCKED'
    assert r['coverage60']['TPEX']['complete_60']==0 and r['coverage60']['TPEX']['master_codes']==1
    assert r['coverage60']['TPEX']['missing_60_codes']==['5000']
    assert r['market_archive']['market_verified_sessions']['TWSE']>0

def test_h196_install_guard(tmp_path,monkeypatch):
    from tools import h196_preflight as p
    root=tmp_path/'root';payload=tmp_path/'payload';root.mkdir();payload.mkdir()
    monkeypatch.setattr(p,'BASE',{'old.py':p.gitsha(b'old'),'new.py':None});monkeypatch.setattr(p,'DEPENDENCIES',{})
    (root/'old.py').write_bytes(b'old');(payload/'old.py').write_bytes(b'new');(payload/'new.py').write_bytes(b'added')
    (payload/'H196_SHA256SUMS.txt').write_text(p.sha(b'new')+'  old.py\n'+p.sha(b'added')+'  new.py\n')
    assert not p.check(root,payload)[2];backup=p.apply(root,payload);assert (backup/'old.py').read_bytes()==b'old'
    assert p.apply(root,payload) is None
    (root/'old.py').write_bytes(b'unknown');assert p.check(root,payload)[2]
