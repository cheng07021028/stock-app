# -*- coding: utf-8 -*-
from pathlib import Path
from tools.h1511_verify_deploy import verify


def test_h1511_tab_color_cumulative_deploy_verification():
    ok, notes = verify(Path(__file__).resolve().parents[1])
    assert ok, notes
