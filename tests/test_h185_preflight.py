# -*- coding: utf-8 -*-
from pathlib import Path
import importlib.util
import shutil
import sys
import pytest
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('preflight185',ROOT/'tools/h185_preflight.py')
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)

@pytest.fixture
def root(tmp_path):
 (tmp_path/'pages').mkdir()
 (tmp_path/'requirements.txt').write_text('streamlit==1.59.2',encoding='utf-8')
 (tmp_path/'pages/7_股神推薦.py').write_text('# existing core preserved',encoding='utf-8')
 return tmp_path

def test_install_from_base_new_h184(root):
 assert mod.verify(root)['status']=='READY_APPLY_H185'
 assert mod.apply(root)['status']=='APPLIED_H185'
 assert mod.verify(root)['status']=='ALREADY_H185'
 assert (root/'pages/7_股神推薦.py').read_text()=='# existing core preserved'
 assert not (root/'data').exists()
 assert not (root/'godpick_records.json').exists()

def test_install_repeated_idempotent(root):
 mod.apply(root)
 before=(root/'pages/28_十二研究員決策研究中心.py').read_bytes()
 assert mod.apply(root)['status']=='ALREADY_H185'
 assert (root/'pages/28_十二研究員決策研究中心.py').read_bytes()==before

def test_upgrade_original_h184_page(root):
 p=root/'pages/28_十二研究員決策研究中心.py'
 old=ROOT/'tests/fixtures/h184_page_original.py'
 if not old.exists():pytest.skip('H184 source not mounted')
 shutil.copy2(old,p)
 assert mod.verify(root)['status']=='READY_APPLY_H185'
 r=mod.apply(root)
 assert r['status']=='APPLIED_H185' and r['backup']
 assert (Path(r['backup'])/'pages/28_十二研究員決策研究中心.py').read_bytes()==old.read_bytes()

def test_reject_unknown_page(root):
 p=root/'pages/28_十二研究員決策研究中心.py';p.write_text('# unknown future version')
 assert mod.verify(root)['status']=='BLOCKED_UNKNOWN_VERSION'
 assert mod.apply(root)['status']=='BLOCKED_UNKNOWN_VERSION'
 assert p.read_text()=='# unknown future version'

def test_reject_unknown_new_file(root):
 p=root/'godpick_h185_auto_sources.py';p.write_text('# perhaps a newer upgrade')
 assert mod.verify(root)['status']=='BLOCKED_UNKNOWN_VERSION'

def test_reject_wrong_project(tmp_path):
 assert mod.verify(tmp_path)['status']=='BLOCKED_NOT_STOCKAPP'
