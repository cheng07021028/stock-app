"""CSS/browser fixture regression; no claim of live Streamlit DOM testing."""
import ast
from pathlib import Path
from html import escape
import pytest
import godpick_h204_sidebar_navigation as nav

ROOT = Path(__file__).resolve().parents[1]

def theme_css():
    tree = ast.parse((ROOT/'utils.py').read_text('utf-8-sig'))
    fn = next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='inject_pro_theme')
    return next(x.value for x in ast.walk(fn) if isinstance(x,ast.Constant) and isinstance(x.value,str) and '<style>' in x.value)

def test_no_position_based_css():
    assert 'nth-of-type' not in theme_css()
    assert 'nth-of-type' not in (ROOT/'pages/14_股神權重校正.py').read_text('utf-8-sig')
    assert 'nth-of-type' not in nav.SIDEBAR_STYLE
    assert 'padding-left: 3.25rem' not in theme_css()

def test_css_injected_once_before_page(monkeypatch):
    calls=[]
    class Fake:
        def Page(self, path, **kw): return path
        def navigation(self, pages, **kw):
            calls.append('navigation')
            class Selected:
                def run(self): calls.append('run')
            return Selected()
        def html(self, css):
            assert css == nav.SIDEBAR_STYLE
            calls.append('style')
    monkeypatch.setattr(nav,'st',Fake())
    monkeypatch.setattr(nav,'discover_page_entries',lambda:[])
    assert nav.run_numbered_navigation(lambda:None)
    assert calls==['navigation','style','run']

@pytest.fixture(scope='module')
def browser():
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        b=p.chromium.launch(executable_path='/usr/bin/chromium',headless=True,args=['--no-sandbox'])
        yield b
        b.close()

@pytest.mark.parametrize('with_theme',[False,True])
def test_browser_grouped_sidebar(browser,with_theme):
    groups=[]
    for label,nums in nav.GROUPS:
        rows=['<li><a data-testid="stSidebarNavLink" aria-current="page"><span>🏠 首頁</span></a></li>'] if not groups else []
        for n in nums:
            rows.append(f'<li><a data-testid="stSidebarNavLink"><span data-testid="stSidebarNavLinkText">{escape(nav.PAGE_TITLE_MAP[n])}</span></a></li>')
        groups.append(f'<div class="group">{escape(label)}<ul>'+''.join(rows)+'</ul></div>')
    default='''<style>section{width:286px}ul{list-style:none;margin:0;padding:0}a{display:flex;width:100%;box-sizing:border-box;height:36px;overflow:hidden;padding-left:50px}a span{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-size:18px;font-family:Arial,sans-serif}body{margin:0}</style>'''
    page=browser.new_page(viewport={'width':500,'height':2200})
    try:
        page.set_content(default+nav.SIDEBAR_STYLE+(theme_css() if with_theme else '')+'<section data-testid="stSidebar"><div data-testid="stSidebarNav">'+''.join(groups)+'</div></section>')
        links=page.locator('a[data-testid="stSidebarNavLink"]')
        assert links.count()==35
        actual=links.all_text_contents()
        assert actual[0]=='🏠 首頁'
        assert actual[1:]==[nav.PAGE_TITLE_MAP[n] for _,ns in nav.GROUPS for n in ns]
        state=links.evaluate_all('els=>els.map(e=>({before:getComputedStyle(e,"::before").content, spanBefore:getComputedStyle(e.querySelector("span"),"::before").content, padding:getComputedStyle(e).paddingLeft, wrap:getComputedStyle(e.querySelector("span")).whiteSpace, clipped:e.querySelector("span").scrollWidth>e.querySelector("span").clientWidth}))')
        assert all(s['before']=='none' and s['spanBefore']=='none' and s['wrap']=='normal' and not s['clipped'] for s in state)
        assert all(float(s['padding'].removesuffix('px'))<20 for s in state)
    finally:
        page.close()
