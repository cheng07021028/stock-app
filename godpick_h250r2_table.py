"""Call native Streamlit renderer without leaking private routing kwargs."""
def render_native(st,kind,data,**kwargs):
    kwargs.pop('_godpick_bypass',None)
    if kind not in ('dataframe','data_editor'):raise ValueError('invalid table kind')
    renderer=getattr(st,kind)
    if getattr(st,'_godpick_table_patch_v105',False):
        renderer=getattr(st,'_godpick_original_'+kind+'_v105',renderer)
    if kind=='data_editor' and getattr(st,'_godpick_light_checkbox_patch_v116',False):
        renderer=getattr(st,'_godpick_original_data_editor_v116_light',renderer)
    return renderer(data,**kwargs)
