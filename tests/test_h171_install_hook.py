# -*- coding: utf-8 -*-
import ast
import hashlib
import sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tools.h171_install_excel_hook import patched,git_blob,install

MINI='''# -*- coding: utf-8 -*-
import json
from pathlib import Path
ROOT=Path('.')
def append_pe_audit(wb):
    tab='11_交易員重點導航'
    start=1
    seen={'3227'}
    found=1
    snap={'status':'EXISTING'}
    eps_snap={'status':'EXISTING','eps_ytd_rows':[]}
    study='2026-11-19'
    return {"sheet": tab, "start_row": start, "checked_codes": len(seen),
            "with_official_pe_only": found, "eps_independently_verified": 0,
            "snapshot_status": snap.get("status"),
            "eps_ytd_status": eps_snap.get("status"),
            "eps_ttm_verified": False}
'''


def test_h171_installer_sha_precheck_unknown_revision_rejects():
    with pytest.raises(ValueError,match='H169'):
        patched(MINI.encode('utf-8'))


def test_h171_installer_exact_version_matching_dry_patch_parses():
    original=MINI.encode('utf-8')
    new=patched(original,allowed_sha=git_blob(original))
    assert original != new
    assert 'h171_audit' in new.decode('utf-8')
    assert new.count(b'H171 research-only')==1
    ast.parse(new.decode('utf-8'))


def test_h171_installer_does_not_apply_twice():
    raw=MINI.encode('utf-8')
    new=patched(raw,allowed_sha=git_blob(raw))
    with pytest.raises(ValueError,match='H169'):
        patched(new,allowed_sha=git_blob(raw))


def test_h171_installer_unknown_file_untouched(tmp_path):
    p=tmp_path/'godpick_h169_pe_watch.py';p.write_text(MINI,encoding='utf-8')
    before=p.read_bytes()
    z=install(p,apply=True)
    assert z['status']=='PRECHECK_FAILED'
    assert not z['changed'] and p.read_bytes()==before
    assert not list(tmp_path.glob('*.bak'))
