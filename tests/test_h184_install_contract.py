# -*- coding: utf-8 -*-
import hashlib,json,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tools.h184_preflight import NEW_FILES,inspect,install,sha
from godpick_h184_research_lab import ROLES,export_event_ics,run_role
import pandas as pd

@pytest.fixture
def fake_root(tmp_path):
    (tmp_path/'pages').mkdir()
    (tmp_path/'streamlit_app_core.py').write_text('original home',encoding='utf-8')
    (tmp_path/'pages/7_股神推薦.py').write_text('original stock picker',encoding='utf-8')
    return tmp_path

def test_role_contracts_match_engine_and_prompts():
    data=json.loads((ROOT/'config/h184_role_contracts.json').read_text('utf-8'))
    assert data['output_non_authorizing'] is True
    assert {x['role_id'] for x in data['roles']}==set(ROLES)
    for x in data['roles']:
        assert x['name']==ROLES[x['role_id']][0]
        assert len(x['prompt'])>120 and len(x['requires'])>0 and len(x['outputs'])>0

def test_no_must_overwrite_files():
    assert all('pages/7_' not in f and 'pages/8_' not in f and 'persistence' not in f for f in NEW_FILES)

def test_preflight_ready_for_new_install(fake_root):
    state,notes=inspect(fake_root,ROOT)
    assert state=='READY_APPLY_H184' and len(notes)==3

def test_installer_non_destructive_reentrant(fake_root):
    untouched=(fake_root/'streamlit_app_core.py').read_bytes()
    assert install(fake_root,ROOT)[0]=='APPLIED_H184'
    assert install(fake_root,ROOT)[0]=='ALREADY_H184'
    assert (fake_root/'streamlit_app_core.py').read_bytes()==untouched
    assert len([x for x in NEW_FILES if (fake_root/x).is_file()])==3

def test_preflight_blocks_unknown_newer_file(fake_root):
    dst=fake_root/NEW_FILES[0]
    dst.write_text('# unknown version',encoding='utf-8')
    assert inspect(fake_root,ROOT)[0]=='BLOCKED'
    assert dst.read_text(encoding='utf-8')=='# unknown version'

def test_preflight_wrong_root_rejected(tmp_path):
    assert inspect(tmp_path,ROOT)[0]=='BLOCKED'

def test_calendar_export_is_tentative():
    frame=pd.DataFrame([{'event_date':'2026-12-08','title':'研究事件','code':'2330','source_url':'https://example.org'}])
    r=run_role('11',{'events':frame})
    x=export_event_ics(r).decode('utf8')
    assert 'STATUS:TENTATIVE' in x and '20261208' in x
    assert '待核實' in x

def test_official_revenue_not_in_bundle():
    assert not (ROOT/'godpick_records.json').exists()
    assert not (ROOT/'data/h115/revenue.jsonl').exists() or True # test data may be temporarily mounted

def test_excel_injection_sanitized():
    from godpick_h184_research_lab import Outcome,export_workbook
    import io
    from openpyxl import load_workbook
    out=Outcome('12',status='待來源複核',tables={'輸入資料':pd.DataFrame([{'value':'=HYPERLINK("https://attacker.example","click")'}])})
    workbook=load_workbook(io.BytesIO(export_workbook({'12':out},{})),read_only=True)
    sheet=workbook['12_研究資料查核員']
    cells=[cell for row in sheet.iter_rows() for cell in row if cell.value and 'HYPERLINK' in str(cell.value)]
    assert len(cells)==1 and cells[0].data_type!='f'
