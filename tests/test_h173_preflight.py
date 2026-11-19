# -*- coding: utf-8 -*-
import zipfile
from pathlib import Path
from tools.h173_preflight import preflight,EXACT
ROOT=Path(__file__).resolve().parents[1]


def test_h173_exact_local_root_already_installed():
    r=preflight(ROOT)
    assert r['status']=='ALREADY_H173_INSTALLED' and not r['safe_to_overlay']


def test_h173_exact_uploaded_h170_base_ready():
    z=Path('/mnt/data/stock-app-main(3).zip')
    if not z.is_file():return
    import tempfile
    with tempfile.TemporaryDirectory() as d,zipfile.ZipFile(z) as archive:
        for item in EXACT:
            target=Path(d)/item
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes(archive.read('stock-app-main/'+item))
        r=preflight(Path(d))
        assert r['status']=='READY_H173_CUMULATIVE_OVERLAY',r


def test_h173_missing_file_and_unknown_modified_base_block(tmp_path):
    assert preflight(tmp_path)['status']=='BLOCK_UNKNOWN_OR_MISSING_BASE'
    for name in EXACT:
        p=tmp_path/name;p.parent.mkdir(parents=True,exist_ok=True)
        p.write_bytes((ROOT/name).read_bytes())
    (tmp_path/'godpick_h169_pe_watch.py').write_text('unknown newer code',encoding='utf-8')
    assert not preflight(tmp_path)['safe_to_overlay']


def test_h173_mixed_h170_workflow_with_h172_modification_blocks(tmp_path):
    for name in EXACT:
        p=tmp_path/name;p.parent.mkdir(parents=True,exist_ok=True)
        p.write_bytes((ROOT/name).read_bytes())
    original=Path('/mnt/data/stock-app-main(3).zip')
    if not original.is_file():return
    with zipfile.ZipFile(original) as z:
        (tmp_path/'.github/workflows/h117_official_ohlcv_feedback.yml').write_bytes(
            z.read('stock-app-main/.github/workflows/h117_official_ohlcv_feedback.yml'))
    assert preflight(tmp_path)['status']=='BLOCK_MIXED_OR_PARTIAL_INSTALL'
