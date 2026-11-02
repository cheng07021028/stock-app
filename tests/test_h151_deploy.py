# -*- coding: utf-8 -*-
from pathlib import Path
from tools.h151_verify_deploy import verify


def test_h151_full_dependency_chain():
    ok, notes = verify(Path(__file__).resolve().parents[1])
    assert ok, notes
