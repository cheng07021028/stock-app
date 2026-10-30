"""Regression: H148.3 deployed alone must FAIL preflight; H148.3.1 must PASS."""
from pathlib import Path
from tempfile import TemporaryDirectory
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from h14831_verify_deploy import CUMULATIVE_MODULES, verify


def test_real_cumulative_package_dependencies_present():
    ok, messages = verify(ROOT)
    assert ok, messages
    assert any('PASS' in m for m in messages)


def test_checker_catches_exact_h1482_runtime_missing_module():
    with TemporaryDirectory() as d:
        root = Path(d)
        (root / "pages").mkdir()
        shutil.copy2(ROOT / 'pages' / '7_股神推薦.py', root / 'pages')
        for m in CUMULATIVE_MODULES:
            if m != 'godpick_h1482_source_audit':
                shutil.copy2(ROOT / (m + '.py'), root)
        ok, messages = verify(root)
        assert not ok
        assert 'MISSING: godpick_h1482_source_audit.py' in messages
        shutil.copy2(ROOT / 'godpick_h1482_source_audit.py', root)
        ok, messages = verify(root)
        assert ok, messages


def test_page07_h1482_excel_append_import_present():
    code = (ROOT / 'pages' / '7_股神推薦.py').read_text(encoding='utf-8')
    assert 'from godpick_h1482_source_audit import build_source_audit' in code
    assert 'from godpick_h1482_source_audit import append_source_audit_to_sheet' in code
