import sys,zipfile
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from h250r3_build_overlay import REQUIRED,verify_zip

def sample(tmp_path,*,leak=False,missing=False):
    out=tmp_path/'test.zip'
    files={n:b'# sample\n' for n in REQUIRED}
    files['godpick_column_manager.py']=(
        'def managed_dataframe():\n    return _h250r2_native()\n'
        'def managed_data_editor():\n    return _h250r2_native()\n'
        if not leak else
        'def managed_dataframe():\n    return _h250r2_native(_godpick_bypass=True)\n'
        'def managed_data_editor():\n    return _h250r2_native()\n').encode()
    files['pages/10_推薦清單.py']=b'# _h250r2_formal_run mask_formal overlay_verified_history\n'
    files['godpick_auto_update_tasks.py']='# H250R2 正式N日績效 run_authority\n'.encode()
    files['godpick_h241_github_records.py']=b'# preserve_verified(old,out)\n'
    if missing:files.pop('godpick_column_manager.py')
    with zipfile.ZipFile(out,'w') as z:
        for n,b in files.items():z.writestr(n,b)
    return out

def test_overlay_receipt(tmp_path):
    r=verify_zip(sample(tmp_path));assert r['status']=='OVERLAY_VERIFIED_NOT_DEPLOYED';assert REQUIRED<=set(r['files'])

def test_overlay_rejects_unpatched_manager(tmp_path):
    with pytest.raises(ValueError,match='unpatched managed table'):verify_zip(sample(tmp_path,leak=True))

def test_overlay_rejects_missing_production(tmp_path):
    with pytest.raises(ValueError,match='missing production'):verify_zip(sample(tmp_path,missing=True))
