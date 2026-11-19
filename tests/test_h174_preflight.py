# -*- coding: utf-8 -*-
"""Portable H174 preflight tests; no dependency on the user's source ZIP."""
from pathlib import Path
import hashlib
from types import SimpleNamespace
import pytest
from tools import h174_preflight

ROOT = Path(__file__).resolve().parents[1]


def _copy_hook(root):
    path=root/'pages'/'7_股神推薦.py'
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text('add_research_section_sheets as _h151_section_sheets\n'
                    'verify_final_xlsx as _h154_verify\n',encoding='utf-8')


def _pretend_original(tmp_path,monkeypatch):
    # Digest mapping tests preflight state transitions without bundling an
    # outdated full production file alongside this two-file patch.
    old_hash = hashlib.sha256
    mapping={}
    for name, value in h174_preflight.BASE_SHA256.items():
        data=('test-only-original:'+name).encode('utf-8')
        (tmp_path/name).write_bytes(data)
        mapping[data]=value
    monkeypatch.setattr(h174_preflight.hashlib,'sha256',
        lambda data: SimpleNamespace(hexdigest=lambda:mapping[data]) if data in mapping else old_hash(data))
    _copy_hook(tmp_path)


def test_h174_installed_exactly_on_working_copy():
    assert h174_preflight.verify(ROOT)['status']=='ALREADY_H174_INSTALLED'


def test_h174_unmodified_original_is_recognized(tmp_path,monkeypatch):
    _pretend_original(tmp_path,monkeypatch)
    assert h174_preflight.verify(tmp_path)['status']=='READY_TO_APPLY_H174'


def test_h174_unknown_file_is_blocked(tmp_path):
    for name in h174_preflight.BASE_SHA256:
        (tmp_path/name).write_bytes((ROOT/name).read_bytes())
    (tmp_path/'godpick_h151_workbook_sections.py').write_text('altered',encoding='utf-8')
    _copy_hook(tmp_path)
    assert h174_preflight.verify(tmp_path)['status']=='BLOCKED_UNKNOWN_VERSION'


def test_h174_partial_overlay_is_blocked(tmp_path,monkeypatch):
    _pretend_original(tmp_path,monkeypatch)
    (tmp_path/'godpick_h151_workbook_sections.py').write_bytes(
        (ROOT/'godpick_h151_workbook_sections.py').read_bytes())
    assert h174_preflight.verify(tmp_path)['status']=='BLOCKED_PARTIAL_INSTALL'


def test_h174_missing_export_page_blocks(tmp_path):
    for name in h174_preflight.BASE_SHA256:
        (tmp_path/name).write_bytes((ROOT/name).read_bytes())
    assert h174_preflight.verify(tmp_path)['status']=='BLOCKED_EXPORT_HOOK_NOT_FOUND'
