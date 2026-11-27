import BUILD_H250R2 as b
def test_patch_table_no_internal_kw():
    s='''def managed_dataframe():\n    kwargs["_godpick_bypass"] = True\n    return st.dataframe(show, **kwargs)\ndef managed_data_editor():\n    if empty:\n        kwargs["_godpick_bypass"] = True\n        return st.data_editor(df, **kwargs)\n    kwargs["_godpick_bypass"] = True\n    return st.data_editor(show, **kwargs)\n# v112：重型頁面防卡設定\n'''
    out=b.patch_table(s);compile(out,'test','exec');assert '_godpick_bypass' not in out;assert out.count('_h250r2_native(st,')==3

def test_worker_function_patch():
    s='def task_recommend_list_n_day(cfg=None):\n    return 1\n\ndef other():\n    return 2\n';out=b.patch_task(s);compile(out,'test','exec');assert 'def other():\n    return 2' in out;assert 'run_authority' in out
