import inspect
import sys
from pathlib import Path
from types import SimpleNamespace
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h250r3_table_guard import install, VERSION


def fixture():
    calls=[]
    def native(data, *, height=1, key=None):
        calls.append((data,height,key));return data,height,key
    return SimpleNamespace(dataframe=native,data_editor=native),calls,native


def full_on(st):
    for kind in ('dataframe','data_editor'):
        native=getattr(st,kind)
        setattr(st,'_godpick_original_'+kind+'_v105',native)
        def patched(data, _native=native, **kw):
            bypass=kw.pop('_godpick_bypass',False)
            if not bypass:data='managed:'+data
            return _native(data,**kw)
        setattr(st,kind,patched)
    st._godpick_table_patch_v105=True


def full_off(st):
    for kind in ('dataframe','data_editor'):
        setattr(st,kind,getattr(st,'_godpick_original_'+kind+'_v105'))
    st._godpick_table_patch_v105=False


def light_on(st):
    native=st.data_editor
    st._godpick_original_data_editor_v116_light=native
    def patched(data,**kw):
        if not kw.pop('_godpick_bypass',False):data='light:'+data
        return native(data,**kw)
    st.data_editor=patched
    st._godpick_light_checkbox_patch_v116=True


def light_off(st):
    st.data_editor=st._godpick_original_data_editor_v116_light
    st._godpick_light_checkbox_patch_v116=False


@pytest.mark.parametrize('kind',['dataframe','data_editor'])
@pytest.mark.parametrize('bypass',[True,False,None])
def test_native_exact_call(kind,bypass):
    st,calls,native=fixture();assert install(st)==VERSION
    assert getattr(st,kind)('row',height=620,key='k',_godpick_bypass=bypass)==('row',620,'k')
    assert calls==[('row',620,'k')]
    assert inspect.signature(getattr(st,kind))==inspect.signature(native)


def test_unknown_kw_still_fails():
    st,*_=fixture();install(st)
    with pytest.raises(TypeError,match='unknown'):st.dataframe('x',unknown=True)


def test_other_exception_not_masked():
    def boom(data):raise RuntimeError('genuine renderer error')
    st=SimpleNamespace(dataframe=boom,data_editor=boom);install(st)
    with pytest.raises(RuntimeError,match='genuine'):st.data_editor('x',_godpick_bypass=True)


def test_rerun_no_double_wrapping():
    st,*_=fixture();install(st);a,b=st.dataframe,st.data_editor
    for _ in range(10):install(st)
    assert st.dataframe is a and st.data_editor is b


@pytest.mark.parametrize('kind',['dataframe','data_editor'])
def test_full_mode_preserves_bypass_and_normal_management(kind):
    st,*_=fixture();install(st);full_on(st);a=getattr(st,kind);install(st)
    assert getattr(st,kind) is a
    assert a('x',_godpick_bypass=True)[0]=='x'
    assert a('x')[0]=='managed:x'
    full_off(st);assert getattr(st,kind)('x',_godpick_bypass=True)[0]=='x'


def test_light_mode_and_uninstall():
    st,*_=fixture();install(st);light_on(st);a=st.data_editor;install(st)
    assert st.data_editor is a
    assert a('x',_godpick_bypass=True)[0]=='x';assert a('x')[0]=='light:x'
    assert st.dataframe('x',_godpick_bypass=True)[0]=='x'
    light_off(st);assert st.data_editor('x',_godpick_bypass=True)[0]=='x'


def test_late_install_full():
    st,*_=fixture();full_on(st);install(st)
    assert st.dataframe('x',_godpick_bypass=True)[0]=='x'
    full_off(st);assert st.dataframe('x',_godpick_bypass=True)[0]=='x'


def test_late_install_light():
    st,*_=fixture();light_on(st);install(st)
    assert st.data_editor('x',_godpick_bypass=True)[0]=='x'
    light_off(st);assert st.data_editor('x',_godpick_bypass=True)[0]=='x'


def test_heavy_light_full_transition():
    st,*_=fixture();install(st)
    for _ in range(4):
        full_on(st);install(st);full_off(st);light_on(st);install(st)
        assert st.dataframe('x',_godpick_bypass=True)[0]=='x'
        assert st.data_editor('x',_godpick_bypass=True)[0]=='x'
        light_off(st)


def test_real_streamlit_native():
    st=pytest.importorskip('streamlit');import pandas as pd
    # Native bare-mode call; CI installs the exact production version.
    before=(st.dataframe,st.data_editor)
    try:
        install(st);df=pd.DataFrame({'股票代號':['2330'],'勾選':[False]})
        st.dataframe(df, _godpick_bypass=True)
        assert st.data_editor(df,_godpick_bypass=True).equals(df)
    finally:
        st.dataframe,st.data_editor=before
