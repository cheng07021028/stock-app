# -*- coding: utf-8 -*-
from pathlib import Path
from tools.h172_preflight import preflight,EXPECTED

ROOT=Path(__file__).resolve().parents[1]


def test_h172_preflight_exact_cumulative_current_root():
    z=preflight(ROOT)
    assert not z['safe_to_overlay'] and z['status']=='ALREADY_H173_FILES'


def test_h172_preflight_detects_modified_production_base(tmp_path):
    for name in EXPECTED:
        (tmp_path/name).parent.mkdir(parents=True,exist_ok=True)
        (tmp_path/name).write_bytes((ROOT/name).read_bytes())
    (tmp_path/'godpick_h169_pe_watch.py').write_text('invalid',encoding='utf-8')
    result=preflight(tmp_path)
    assert not result['safe_to_overlay']
    assert 'godpick_h169_pe_watch.py' in result['mismatched_files']


def test_h172_preflight_missing_dependency_fails_closed(tmp_path):
    assert not preflight(tmp_path)['safe_to_overlay']


def test_h172_preflight_blocks_partial_installation(tmp_path):
    for name in EXPECTED:
        (tmp_path/name).parent.mkdir(parents=True,exist_ok=True)
        (tmp_path/name).write_bytes((ROOT/name).read_bytes())
    original=(ROOT/'godpick_h169_pe_watch.py.h171.bak').read_bytes()
    (tmp_path/'godpick_h169_pe_watch.py').write_bytes(original)
    assert preflight(tmp_path)['status']=='BLOCK_PARTIALLY_INSTALLED_H173'
