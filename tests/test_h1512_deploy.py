# -*- coding: utf-8 -*-
from pathlib import Path
from tools.h1512_verify_deploy import verify


def test_h1512_deploy_integrity():
    ok, notes = verify(Path(__file__).resolve().parents[1])
    assert ok, notes
