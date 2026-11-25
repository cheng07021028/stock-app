from pathlib import Path
import tempfile
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import godpick_h204_sidebar_navigation as nav


def test_title_map_keeps_new_research_modules_numbered():
    assert nav.PAGE_TITLE_MAP[29] == '29｜主流趨勢與潛力黑馬'
    assert nav.PAGE_TITLE_MAP[30] == '30｜財報成長與合理估值'
    assert nav.PAGE_TITLE_MAP[31] == '31｜十二研究員聯合決策'
    assert nav.PAGE_TITLE_MAP[32] == '32｜隔日預測與AI績效學習'
    assert nav.PAGE_TITLE_MAP[33] == '33｜超級AI股神決策總指揮中心'


def test_page_no_from_name():
    assert nav._page_no_from_name('29_主流趨勢與潛力黑馬.py') == 29
    assert nav._page_no_from_name('nope.py') is None


def test_discover_prefers_utf8_name_over_encoded_duplicate(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        pages = Path(tmp)
        (pages / '7_#U80a1#U795e#U63a8#U85a6.py').write_text('# old', 'utf-8')
        (pages / '7_股神推薦.py').write_text('# new', 'utf-8')
        (pages / '29_主流趨勢與潛力黑馬.py').write_text('# x', 'utf-8')
        monkeypatch.setattr(nav, '_pages_dir', lambda: pages)
        entries = nav.discover_page_entries()
        assert [e.page_no for e in entries] == [7, 29]
        seven = [e for e in entries if e.page_no == 7][0]
        assert seven.path.name == '7_股神推薦.py'
        assert seven.title == '07｜股神推薦'
