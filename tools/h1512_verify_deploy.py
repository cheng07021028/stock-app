# -*- coding: utf-8 -*-
"""V191-H151.2 dependency and Excel sheet-view patch verifier."""
from pathlib import Path


def verify(root: Path):
    root = Path(root)
    notes = []
    for rel in (
        "godpick_h151_workbook_sections.py",
        "godpick_h1512_excel_integrity.py",
        "pages/7_股神推薦.py",
        "tools/h1511_verify_deploy.py",
        "tests/test_h1512_excel_view_integrity.py",
    ):
        if not (root / rel).is_file():
            notes.append("MISSING " + rel)
    if notes:
        return False, notes
    page = (root / "pages/7_股神推薦.py").read_text("utf-8")
    integrity = (root / "godpick_h1512_excel_integrity.py").read_text("utf-8")
    adapter = (root / "godpick_h151_workbook_sections.py").read_text("utf-8")
    if '_h1512_normalize_views(wb)\n    wb.save(output)' not in page:
        notes.append("H151.2 missing immediate before-save view normalization")
    if 'view.selection = [Selection()]' not in integrity:
        notes.append("H151.2 missing duplicate selection clearing")
    if 'TABS_COLOR' not in adapter or 'CATEGORY_TABS' not in adapter:
        notes.append("H151.1 colors or H151 category worksheets missing")
    if '"00_H126主管決策"' not in page and "'00_H126主管決策'" not in page:
        notes.append("H126 authority sheet missing")
    return not notes, notes or ["PASS H151.2 Excel frozen selections; H151/H151.1 retained"]


if __name__ == '__main__':
    import argparse
    arg = argparse.ArgumentParser()
    arg.add_argument('--root', default='.')
    opts = arg.parse_args()
    ok, notes = verify(Path(opts.root))
    for entry in notes:
        print(entry)
    raise SystemExit(0 if ok else 1)
