# -*- coding: utf-8 -*-
"""H160: same-run rank stability and honest H111 settings persistence."""
from __future__ import annotations
import base64, json, sys, tempfile
from pathlib import Path
import unittest
from openpyxl import Workbook, load_workbook
from openpyxl.utils.cell import range_boundaries
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h1537_manager_visible import render_manager_visible
from godpick_h154_export_parity import verify_final_xlsx,reconcile_run
from godpick_h111_scan_settings_authority import load_scan_settings,save_scan_settings,_config

class FakeResponse:
    def __init__(self,status,data=None):self.status_code=status;self.data=data or {}
    def json(self):return self.data

class FakeGitHub:
    def __init__(self,document=None,put_status=201):
        self.doc=document;self.put_status=put_status;self.calls=[];self.sha='sha0';self.last_headers={}
    def get(self,url,**kw):
        self.calls.append(('get',kw));self.last_headers=kw.get('headers',{})
        if not self.doc:return FakeResponse(404)
        return FakeResponse(200,{'sha':self.sha,'content':base64.b64encode(json.dumps(self.doc,ensure_ascii=False).encode()).decode('ascii')})
    def put(self,url,**kw):
        self.calls.append(('put',kw))
        if self.put_status not in (200,201):return FakeResponse(self.put_status)
        self.doc=json.loads(base64.b64decode(kw['json']['content']))
        self.sha='newsha'
        return FakeResponse(self.put_status)

TOKEN={'GITHUB_TOKEN':'test','GITHUB_REPO_OWNER':'demo','GITHUB_REPO_NAME':'stock-app','GITHUB_REPO_BRANCH':'main'}

class TestH160(unittest.TestCase):
    def _wb(self):
        w=Workbook();m=w.active;m.title='00_主管重點觀察榜'
        m['A1']='主管';m['B4']='研究';m['B5']='風控';m.auto_filter.ref='A7:O10'
        a=w.create_sheet('00_H126主管決策');a['G2']='禁止買進'
        w.create_sheet('01_具名強勢研究');w.create_sheet('02_研究召回統計');w.create_sheet('03_五檔研究計畫')
        return w
    def _dataset(self):
        rows=[]
        for n in range(1,12):
            # First 9 positions have distinct ranks; tie 10/11 would be reordered
            # by the original raw-score secondary sort, not by H154 score.
            rows.append({'股票代號':str(2800+n),'股票名稱':f'測試{n}',
               '排名等級':'B｜研究','研究排名':n if n<=10 else 10,
               '研究證據分':60-n if n<=10 else 99,
               'H154研究排序分':200-n if n<=10 else 180,
               'H154順位調整說明':'已核查非交易'})
        return pd.DataFrame(rows)
    def _render(self):
        r=self._dataset();w=self._wb()
        render_manager_visible(w,ranked=r,research=r,watch=pd.DataFrame(),h126_buyable=0,research_day='2026-11-13')
        sheet=w['01_具名強勢研究'];columns=['研究排名','排名等級','研究證據分','股票代號','股票名稱','H154研究排序分']
        for i,c in enumerate(columns,1):sheet.cell(1,i,c)
        for j,record in enumerate(r.to_dict('records'),2):
            for i,c in enumerate(columns,1):sheet.cell(j,i,record[c])
        plan=w['03_五檔研究計畫'];plan['A1']='股票代號'
        for j,code in enumerate(r['股票代號'].head(5),2):plan.cell(j,1,code)
        w['02_研究召回統計']['A1']='檢查項目';w['02_研究召回統計']['A2']='H154正式推薦唯一來源H126'
        audit={'ranked_codes':list(r['股票代號']),'h126_buyable':0}
        return w,r,audit
    def test_original_bug_reproduces_after_eighth_stock(self):
        w,r,audit=self._render()
        self.assertEqual(w['00_主管重點觀察榜']['B9'].value,'2801')
        self.assertEqual(w['00_主管重點觀察榜']['B18'].value,'2810')
        self.assertEqual(w['00_主管重點觀察榜']['B19'].value,'2811')
        self.assertTrue(verify_final_xlsx(w,audit))
    def test_excel_save_and_reload(self):
        w,r,audit=self._render()
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'test.xlsx';w.save(p);z=load_workbook(p)
            self.assertTrue(verify_final_xlsx(z,audit))
    def test_real_mismatch_still_fails_closed(self):
        w,r,audit=self._render()
        w['00_主管重點觀察榜']['B19']='9999'
        with self.assertRaisesRegex(ValueError,'首次差異第11檔'):
            verify_final_xlsx(w,audit)
    def test_h126_gate_still_fails_closed(self):
        w,r,audit=self._render();w['00_H126主管決策']['G2']='開放交易'
        with self.assertRaisesRegex(ValueError,'H126禁止買進'):
            verify_final_xlsx(w,audit)
    def test_forced_runtime_data_branch(self):
        self.assertEqual(_config(TOKEN)['branch'],'runtime-data')
        self.assertEqual(_config({**TOKEN,'GITHUB_RUNTIME_DATA_BRANCH':'runtime-data'})['branch'],'runtime-data')
    def test_anonymous_public_read_is_verified_but_no_write(self):
        remote={'schema':'stockapp.h111.scan-settings.v1','revision':'rev68','settings':{'selected_categories':['全部']+[f'產業{i}' for i in range(67)]}}
        fake=FakeGitHub(remote)
        with tempfile.TemporaryDirectory() as t:
            res=load_scan_settings(project_root=t,legacy_settings={},provided_secrets={'GITHUB_TOKEN':'','GITHUB_REPO_OWNER':'demo','GITHUB_REPO_NAME':'stock-app'},transport=fake)
            self.assertTrue(res.durable_verified)
            self.assertEqual(len(res.settings['selected_categories']),68)
            self.assertNotIn('Authorization',fake.last_headers)
            saved=save_scan_settings(res.settings,project_root=t,provided_secrets={'GITHUB_TOKEN':'','GITHUB_REPO_OWNER':'demo','GITHUB_REPO_NAME':'stock-app'},transport=fake)
            self.assertFalse(saved.ok);self.assertIn('Contents: Read and write',saved.message)
            self.assertEqual(len([x for x in fake.calls if x[0]=='put']),0)
    def test_actions_only_token_cannot_claim_settings_saved(self):
        fake=FakeGitHub(put_status=403)
        with tempfile.TemporaryDirectory() as t:
            s=save_scan_settings({'selected_categories':['全部','IC設計']},project_root=t,provided_secrets=TOKEN,transport=fake)
            self.assertFalse(s.ok);self.assertFalse(s.durable_verified)
            self.assertIn('HTTP 403',s.message);self.assertIn('Contents: Read and write',s.message)
            self.assertEqual(len([x for x in fake.calls if x[0]=='put']),1)
    def test_roundtrip_after_enabling_contents_permission(self):
        fake=FakeGitHub()
        with tempfile.TemporaryDirectory() as t, tempfile.TemporaryDirectory() as t2:
            data={'selected_categories':['全部']+[f'產業{i}' for i in range(67)],'min_total_score':83}
            a=save_scan_settings(data,project_root=t,provided_secrets=TOKEN,transport=fake)
            self.assertTrue(a.ok and a.durable_verified)
            b=load_scan_settings(project_root=t2,legacy_settings={},provided_secrets=TOKEN,transport=fake)
            self.assertEqual(b.settings,data)
            self.assertTrue(b.durable_verified)
    def test_concurrent_409_conflict_retries_without_faking_success(self):
        fake=FakeGitHub(put_status=409)
        with tempfile.TemporaryDirectory() as t:
            saved=save_scan_settings({'selected_categories':['全部']},project_root=t,provided_secrets=TOKEN,transport=fake)
            self.assertFalse(saved.ok);self.assertFalse(saved.durable_verified)
            self.assertEqual(len([x for x in fake.calls if x[0]=='put']),3)

if __name__=='__main__':unittest.main(verbosity=2)
