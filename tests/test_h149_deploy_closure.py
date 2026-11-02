from pathlib import Path
import shutil
import sys
from tempfile import TemporaryDirectory
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from h149_verify_deploy import verify,MODULES


def test_deploy_all_dependencies_and_h149_hooks():
    ok,report=verify(ROOT)
    assert ok,report


def test_preflight_catches_h1482_and_h149_missing():
    with TemporaryDirectory() as d:
        dest=Path(d);(dest/'pages').mkdir()
        shutil.copyfile(ROOT/'pages'/'7_股神推薦.py',dest/'pages'/'7_股神推薦.py')
        for mod in MODULES:
            if mod not in ('godpick_h1482_source_audit','godpick_h149_trade_intelligence'):
                shutil.copyfile(ROOT/(mod+'.py'),dest/(mod+'.py'))
        ok,notes=verify(dest)
        assert not ok
        assert any('MISSING: godpick_h1482_source_audit.py' in n for n in notes)
        assert any('MISSING: godpick_h149_trade_intelligence.py' in n for n in notes)
