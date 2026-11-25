# -*- coding: utf-8 -*-
"""H205 deterministic navigation contract; API doubles, no network/browser."""
from pathlib import Path
import sys
from types import SimpleNamespace
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import godpick_h204_sidebar_navigation as nav

EXPECTED = (0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,98)

@pytest.fixture
def catalog(tmp_path, monkeypatch):
    for no in EXPECTED:
        filename = f'{no}_'+ nav.PAGE_TITLE_MAP[no].split('｜',1)[1].replace(' ', '_')+'.py'
        (tmp_path / filename).write_text('# fixture', 'utf-8')
    monkeypatch.setattr(nav, '_pages_dir', lambda: tmp_path)
    return tmp_path

class FakeStreamlit:
    def __init__(self, selected=None, fail=None):
        self.selected = selected
        self.fail = fail
        self.registered = []
        self.runs = []
    def Page(self, source, **kw):
        return SimpleNamespace(source=source, **kw)
    def navigation(self, pages, **kwargs):
        self.registered.append((pages, kwargs))
        def run():
            self.runs.append(self.selected)
            if self.fail:
                raise self.fail
            if self.selected == 'home':
                next(iter(pages.values()))[0].source()
        return SimpleNamespace(run=run)

def test_group_definition_partitions_all_live_module_numbers():
    numbers = [no for _, group in nav.GROUPS for no in group]
    assert len(numbers) == len(set(numbers)) == 34
    assert set(numbers) == set(EXPECTED) == set(nav.PAGE_TITLE_MAP)
    assert 13 not in numbers

def test_groups_preserve_every_page_and_sort(catalog):
    entries = nav.discover_page_entries()
    groups = nav.group_page_entries(list(reversed(entries)))
    assert list(groups) == [label for label, _ in nav.GROUPS]
    assert len(entries) == 34
    assert set(e.path for e in entries) == {e.path for g in groups.values() for e in g}
    for group in groups.values():
        assert [e.page_no for e in group] == sorted(e.page_no for e in group)
    assert [len(g) for g in groups.values()] == [7,6,13,8]

def test_grouped_menu_contains_home_and_all_pages(catalog, monkeypatch):
    fake = FakeStreamlit(); monkeypatch.setattr(nav, 'st', fake)
    def home(): pass
    groups = nav.build_grouped_pages(home)
    pages = [p for g in groups.values() for p in g]
    assert len(pages) == 35
    assert pages[0].source is home and pages[0].default is True
    assert pages[0].title == '首頁'
    assert len({p.source for p in pages[1:]}) == 34
    for p in pages[1:]:
        assert Path(p.source).exists()
        assert not hasattr(p, 'url_path')

def test_flat_compatibility_and_grouped_same_routes(catalog, monkeypatch):
    monkeypatch.setattr(nav, 'st', FakeStreamlit())
    def home(): pass
    flat = nav.build_streamlit_pages(home)
    grouped = [p for g in nav.build_grouped_pages(home).values() for p in g]
    assert {p.source:p.title for p in flat} == {p.source:p.title for p in grouped}

def test_future_numbered_module_is_visible(catalog):
    (catalog/'34_新增測試.py').write_text('# fixture','utf-8')
    grouped = nav.group_page_entries(nav.discover_page_entries())
    assert [e.title for e in grouped[nav.FUTURE_GROUP]] == ['34｜新增測試']

def test_missing_known_page_not_fabricated(catalog):
    (catalog/'7_股神推薦.py').unlink()
    assert 7 not in {e.page_no for e in nav.discover_page_entries()}

def test_encoded_duplicate_not_selected_or_deleted(catalog):
    old = catalog/'7_#U80a1#U795e#U63a8#U85a6.py'
    old.write_text('# old','utf-8')
    assert [e.path.name for e in nav.discover_page_entries() if e.page_no == 7] == ['7_股神推薦.py']
    assert old.exists()

def test_empty_catalog_keeps_only_home(tmp_path, monkeypatch):
    monkeypatch.setattr(nav, '_pages_dir', lambda: tmp_path)
    monkeypatch.setattr(nav, 'st', FakeStreamlit())
    result = nav.build_grouped_pages(lambda: None)
    assert sum(map(len,result.values())) == 1

def test_native_navigation_runs_once(catalog, monkeypatch):
    fake = FakeStreamlit('module7')
    monkeypatch.setattr(nav, 'st', fake)
    assert nav.run_numbered_navigation(lambda: pytest.fail('unexpected home')) is True
    assert len(fake.registered) == 1 and fake.runs == ['module7']
    assert fake.registered[0][1] == {'position':'sidebar','expanded':True}

def test_selected_home_runs_once(catalog, monkeypatch):
    fake = FakeStreamlit('home'); calls = []
    monkeypatch.setattr(nav,'st',fake)
    assert nav.run_numbered_navigation(lambda: calls.append(1)) is True
    assert calls == [1]

def test_page_error_visible_no_home_fallback(catalog, monkeypatch):
    fake = FakeStreamlit('module7', ValueError('page failed')); calls = []
    monkeypatch.setattr(nav,'st',fake)
    with pytest.raises(ValueError, match='page failed'):
        nav.run_numbered_navigation(lambda: calls.append(1))
    assert calls == [] and len(fake.registered) == 1 and len(fake.runs) == 1

def test_navigation_registration_failure_visible(catalog, monkeypatch):
    fake = FakeStreamlit()
    def bad(*args,**kw): raise RuntimeError('registration failed')
    fake.navigation = bad
    monkeypatch.setattr(nav,'st',fake)
    with pytest.raises(RuntimeError, match='registration failed'):
        nav.run_numbered_navigation(lambda: pytest.fail('unexpected home'))
    assert fake.runs == []

def test_missing_api_legacy_fallback(monkeypatch):
    monkeypatch.setattr(nav,'st',None)
    assert nav.run_numbered_navigation(lambda: pytest.fail('caller owns fallback')) is False
