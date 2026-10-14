# -*- coding: utf-8 -*-
"""H125 category persistence and patch safety regression."""
from __future__ import annotations
import base64, copy, json, sys, tempfile, unittest, ast
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h125_multiselect_authority import preserve_selected_categories, category_roundtrip_state
from h125_support.godpick_h111_scan_settings_authority import save_scan_settings,load_scan_settings
from tools.apply_h125_multiselect_authority_fix import decode_escaped_name,rewrite,main

class Response:
    def __init__(self, status_code, data=None): self.status_code=status_code;self.data=data or {}
    def json(self):return self.data
class MockGitHub:
    def __init__(self):self.document=None;self.sha='';self.put_count=0;self.error=None
    def get(self,*a,**kw):
        if self.error:return Response(self.error)
        if self.document is None:return Response(404)
        return Response(200,{'sha':self.sha,'content':base64.b64encode(json.dumps(self.document,ensure_ascii=False).encode()).decode('ascii')})
    def put(self,*a,**kw):
        if self.error:return Response(self.error)
        self.put_count+=1;self.document=json.loads(base64.b64decode(kw['json']['content']))
        self.sha='sha'+str(self.put_count);return Response(201)
CFG={'GITHUB_TOKEN':'test-token','GITHUB_REPO_OWNER':'demo','GITHUB_REPO_NAME':'stock-app','GITHUB_REPO_BRANCH':'main'}

class H125Tests(unittest.TestCase):
    def test_missing_master_sectors_are_never_dropped(self):
        saved=['AI伺服器','光學鏡頭','半導體設備材料','被動元件']
        sel,opt=preserve_selected_categories(saved,['全部','半導體業'])
        self.assertEqual(sel,saved)
        self.assertTrue(all(s in opt for s in saved))
        self.assertEqual(opt[0],'全部')
    def test_reset_and_dedup_no_phantom_categories(self):
        self.assertEqual(preserve_selected_categories([],['全部'])[0],['全部'])
        self.assertEqual(preserve_selected_categories(['光通訊','光通訊',''],['全部'])[0],['光通訊'])
        self.assertEqual(category_roundtrip_state({'selected_categories':['PCB載板']},['全部'])['selected_categories'],['PCB載板'])
    def test_h111_full_remote_reboot_verify_after_master_change(self):
        transport=MockGitHub()
        selected=['IC設計','PCB載板','光通訊','散熱','記憶體','高速傳輸']
        source={'selected_categories':selected,'min_total_score':85.0,'recommend_mode':'飆股模式','risk_strictness':'標準','pick_strategy':'結合版'}
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            saved=save_scan_settings(source,project_root=a,transport=transport,provided_secrets=CFG)
            self.assertTrue(saved.ok and saved.durable_verified)
            cold=load_scan_settings(project_root=b,legacy_settings={'selected_categories':['全部']},transport=transport,provided_secrets=CFG)
            self.assertTrue(cold.durable_verified)
            restored=category_roundtrip_state(cold.settings, ['全部','被動元件'])
            self.assertEqual(restored['selected_categories'],selected)
            self.assertEqual(transport.document['settings']['selected_categories'],selected)
    def test_no_remote_success_on_403(self):
        t=MockGitHub();t.error=403
        with tempfile.TemporaryDirectory() as d:
            got=save_scan_settings({'selected_categories':['PCB載板']},project_root=d,transport=t,provided_secrets=CFG)
            self.assertFalse(got.ok);self.assertFalse(got.durable_verified)
    def test_escaped_pages_decode_exactly(self):
        self.assertEqual(decode_escaped_name('7_#U80a1#U795e#U63a8#U85a6.py'),'7_股神推薦.py')
        self.assertEqual(decode_escaped_name('8_#U80a1#U795e#U63a8#U85a6#U7d00#U9304.py'),'8_股神推薦紀錄.py')
    def test_code_patch_preserves_complete_baseline_and_is_idempotent(self):
        p=ROOT/'pages'/'7_股神推薦.py'
        if not p.exists():self.skipTest('Deploy with original Page7 for integrated fixture')
        before=p.read_text(encoding='utf-8-sig')
        rewritten=rewrite(before,ROOT/'h125_support')
        self.assertEqual(rewritten,rewrite(rewritten,ROOT/'h125_support'))
        ast.parse(rewritten)
        self.assertIn('H125_SELECTED_CATEGORIES_AUTHORITY',rewritten)
        self.assertIn('H111_VERIFIED_SCAN_SETTINGS_AUTHORITY',rewritten)
        self.assertIn('st.form_submit_button("開始推薦"',rewritten)
        self.assertIn('"選擇類型（可多選）"',rewritten)
    def test_canonical_collision_stops_without_touching_files(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);p=root/'pages';p.mkdir()
            (p/'7_股神推薦.py').write_text('x=1',encoding='utf-8')
            (p/'7_#U80a1#U795e#U63a8#U85a6.py').write_text('x=2',encoding='utf-8')
            (root/'h125_support').mkdir();(root/'h125_support'/'godpick_h111_scan_settings_authority.py').write_text('x=1')
            (root/'godpick_h125_multiselect_authority.py').write_text('x=1')
            with self.assertRaisesRegex(RuntimeError,'重複'):
                main(['--project-root',str(root),'--check-only'])
            self.assertEqual((p/'7_股神推薦.py').read_text(), 'x=1')

if __name__=='__main__':unittest.main(verbosity=2)
