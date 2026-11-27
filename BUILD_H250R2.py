# -*- coding: utf-8 -*-
"""Exact audited source overlay, no project or runtime-data mutations."""
import ast, hashlib, json, argparse, zipfile, tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
PAGE='pages/10_推薦清單.py';TABLE='godpick_column_manager.py';TASK='godpick_auto_update_tasks.py';HISTORY='godpick_h241_github_records.py'
BASE={PAGE:'2e19a1b2786d650676f60c56c2bfd5bc049c360e',TABLE:'0312660b2d47816983a12d5bbdb2bb6015b17508',TASK:'1cfe096d955a82ab7e8aff0cf3715eb82cfd8610',HISTORY:'bd7276b9507ddb94557fbc536a76303f7fba7d2d'}

def blob(b):return hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()
def digest(b):return hashlib.sha256(b).hexdigest()
def change(s,a,b):
    if s.count(a)!=1:raise ValueError('missing or ambiguous anchor: '+repr(a[:120]))
    return s.replace(a,b,1)
def replace_function(s,name,fn):
    nodes=[x for x in ast.parse(s).body if isinstance(x,ast.FunctionDef) and x.name==name]
    if len(nodes)!=1:raise ValueError('function not unique '+name)
    n=nodes[0];a=s.splitlines(keepends=True);return ''.join(a[:n.lineno-1])+fn.rstrip()+'\n'+''.join(a[n.end_lineno:])

def patch_table(s):
    # Three private bypass writes exist only inside explicit managed renderers.
    start=s.index('def managed_dataframe(');end=s.index('# v112：重型頁面防卡設定',start)
    old=s[start:end];new=old.replace('    kwargs["_godpick_bypass"] = True\n','').replace('        kwargs["_godpick_bypass"] = True\n','')
    # Remove indentation residue by line filtering instead of substring substitution.
    new=''.join(l for l in old.splitlines(keepends=True) if l.strip()!='kwargs["_godpick_bypass"] = True')
    if old.count('kwargs["_godpick_bypass"] = True')!=3:raise ValueError('managed bypass anchor count')
    new=new.replace('st.dataframe(show, **kwargs)','_h250r2_native(st, "dataframe", show, **kwargs)').replace('st.data_editor(df, **kwargs)','_h250r2_native(st, "data_editor", df, **kwargs)').replace('st.data_editor(show, **kwargs)','_h250r2_native(st, "data_editor", show, **kwargs)')
    return s[:start]+'from godpick_h250r2_table import render_native as _h250r2_native\n\n'+new+s[end:]

PAGE_HELPER='''def _h250r2_formal_run(max_rows, force=False):
    from godpick_h250r2_nday import run_authority
    with st.spinner("H250-R2｜核實官方交易日、原始行情及08歷史權威；不重跑推薦..."):
        try:
            ok, summary, msgs = run_authority(max_rows=max_rows, force=force)
        except Exception as exc:
            ok, summary, msgs = False, {"status":"ERROR","success":0}, [type(exc).__name__]
        st.session_state[_k("h250r2_formal_receipt")] = (ok, summary, msgs)
        if ok and summary.get("success",0):
            _load_records_cached(force=True)
    st.rerun()


def _h250r2_formal_receipt():
    receipt=st.session_state.get(_k("h250r2_formal_receipt"))
    if not receipt:return
    ok, summary, msgs=receipt
    message=f"H250-R2｜{summary.get('status')}｜已遠端驗證保存 {summary.get('success',0)} 筆｜本輪仍待補 {summary.get('remaining',0)} 筆"
    (st.success if ok and summary.get('success',0) else st.info if ok else st.warning)(message)
    st.caption("診斷："+str(summary.get('diagnostics',{})))
    for msg in msgs:st.caption(str(msg))


'''

def patch_page(s):
    s=change(s,'def _load_records_cached(force: bool = False) -> pd.DataFrame:',PAGE_HELPER+'def _load_records_cached(force: bool = False) -> pd.DataFrame:')
    s=change(s,'    return x[GODPICK_RECORD_COLUMNS].copy()', '''    from godpick_h250r2_nday import PROOF
    if PROOF not in x.columns:x[PROOF]=None
    return x[list(dict.fromkeys([*GODPICK_RECORD_COLUMNS,PROOF]))].copy()''')
    # Keep the historical proof even if a later same-ID current snapshot has stale numeric values.
    s=change(s,'            merged = merged.drop_duplicates(subset=["_merge_key"], keep="last").drop(columns=["_merge_key"], errors="ignore")','''            merged = merged.drop_duplicates(subset=["_merge_key"], keep="last").drop(columns=["_merge_key"], errors="ignore")
            from godpick_h250r2_nday import overlay_verified_history
            historical = [f for f in frames if "資料來源" in f and not f.empty and str(f.iloc[0]["資料來源"]) == "股神推薦紀錄"]
            if historical:merged = overlay_verified_history(merged, pd.concat(historical,ignore_index=True))''')
    # Mask only statistics copy, not raw display/export or authority fields.
    s=change(s,'    current_perf_cols = ["目前損益幅%", "損益幅%", "即時追蹤報酬%", "目前追蹤報酬%", "實際報酬%", "平均目前績效%"]','''    from godpick_h250r2_nday import mask_formal
    x=mask_formal(x)
    st.caption("H250-R2：正式N日勝率只計入有逐期官方原始價格證據的資料；舊數值保留供核查，非含息或扣費後交易績效。")
    current_perf_cols = ["目前損益幅%", "損益幅%", "即時追蹤報酬%", "目前追蹤報酬%", "實際報酬%", "平均目前績效%"]''')
    s=change(s,'        avg20 = pd.to_numeric(filtered_df.get("推薦後20日%"), errors="coerce").dropna().mean() if not filtered_df.empty and "推薦後20日%" in filtered_df.columns else None','''        from godpick_h250r2_nday import series as _h250r2_series
        avg20 = _h250r2_series(filtered_df,"推薦後20日%").dropna().mean() if not filtered_df.empty else None''')
    a='        if st.button("📅 正式N日績效回補", use_container_width=True):';b='        if st.button("🎯 更新隔日命中追蹤", use_container_width=True):'
    if s.count(a)!=1 or s.count(b)!=1:raise ValueError('sidebar anchor')
    i=s.index(a);j=s.index(b,i);s=s[:i]+a+'\n            _h250r2_formal_run(int(batch_n))\n\n'+s[j:]
    a='    try:\n        formal_candidates_v99 = sum(1 for _, _r in df.iterrows() if _row_needs_formal_n_day_update_v98(dict(_r)))';b='    b1, b2, b3 = st.columns([1.2, 1.2, 2])'
    if s.count(a)!=1 or s.count(b)!=1:raise ValueError('main count anchor')
    i=s.index(a);j=s.index(b,i);s=s[:i]+'''    from godpick_h250r2_nday import diagnose as _h250r2_diagnose
    try:
        _diagnostics = _h250r2_diagnose(df)
        formal_candidates_v99 = int(_diagnostics.get("DUE",0))
        st.info(f"H250-R2｜已到期待核 {formal_candidates_v99} 筆／未滿期 {_diagnostics.get('NOT_DUE',0)}／日曆待補 {_diagnostics.get('CALENDAR_GAP',0)}／身分待補 {_diagnostics.get('IDENTITY_PENDING',0)}／五期已驗證 {_diagnostics.get('VERIFIED',0)}。已到期仍須核對官方個股行情，並非保證可回補。")
    except Exception as exc:
        formal_candidates_v99 = 0
        st.warning("官方交易日曆無法核實："+type(exc).__name__)
    _h250r2_formal_receipt()

'''+s[j:]
    s=change(s,'        st.caption("v99：此按鈕只回補 10_推薦清單既有資料並寫回 GitHub/Firestore，不會重跑股神推薦。")','        st.caption("H250-R2：只更新08已核實既有ID的正式N日績效，10以同ID讀取；不改寫當輪清單／07快照，不重跑推薦。")')
    a='    if run_formal_backfill_main_v99 or run_formal_backfill_force_v99:';b='    # <<< V99_MAIN_FORMAL_BACKFILL_BUTTON'
    if s.count(a)!=1 or s.count(b)!=1:raise ValueError('main run anchor')
    i=s.index(a);j=s.index(b,i);s=s[:i]+a+'\n        _h250r2_formal_run(int(st.session_state.get(_k("perf_update_batch_size"),80)), force=run_formal_backfill_force_v99)\n'+s[j:]
    s=change(s,'    detail_show_df = _format_show_df(filtered_show_df[existing_cols])','''    from godpick_h250r2_nday import display_status
    detail_show_df = _format_show_df(filtered_show_df[existing_cols])
    detail_show_df["正式N日驗證狀態"] = display_status(filtered_show_df).to_numpy()''')
    s=change(s,'        _ensure_record_columns(df).to_excel(writer, sheet_name="推薦清單", index=False)','''        from godpick_h250r2_nday import display_status
        out = _ensure_record_columns(df)
        out["正式N日驗證狀態"] = display_status(out)
        out.to_excel(writer, sheet_name="推薦清單", index=False)''')
    s=change(s,'data=filtered_df.loc[:, ~filtered_df.columns.duplicated()].copy().to_csv(index=False, encoding="utf-8-sig"),','data=filtered_df.loc[:, ~filtered_df.columns.duplicated()].copy().assign(正式N日驗證狀態=display_status(filtered_df)).to_csv(index=False, encoding="utf-8-sig"),')
    return s

TASK_BODY='''def task_recommend_list_n_day(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        from godpick_h250r2_nday import run_authority
        ok, summary, msgs=run_authority(root=BASE_DIR,max_rows=int(cfg.get("max_rows",300) or 300))
        d=summary.get("diagnostics",{})
        pending=bool(summary.get("remaining",0) or d.get("IDENTITY_PENDING",0) or d.get("CALENDAR_GAP",0))
        result=_report(ok,f"H250R2 正式N日績效：永久確認 {summary.get('success',0)}／待補 {summary.get('remaining',0)}｜{summary.get('status')}｜診斷 {d}",details={"summary":summary,"sync_messages":msgs},changed_files=["godpick_records.json"] if summary.get("success") else [],started=t0,warning=bool(ok and pending))
        result["retry_pending"]=bool(ok and summary.get("retryable_remaining",0))
        return result
    except Exception as exc:
        return _report(False,f"H250R2 正式N日績效回補失敗：{type(exc).__name__}",started=t0)
'''

def patch_task(s):return replace_function(s,'task_recommend_list_n_day',TASK_BODY)

def patch_history(s):
    return change(s,"    out['record_id']=old['record_id']", "    from godpick_h250r2_nday import preserve_verified\n    out=preserve_verified(old,out)\n    out['record_id']=old['record_id']")

def read(root,p):
    q=root/p
    if not q.is_file() or q.is_symlink() or not q.resolve().is_relative_to(root.resolve()):raise ValueError('missing or unsafe file '+p)
    return q.read_bytes()

def generate(root,out):
    # Reuse exact reverse verification of H245/H246 changes to task only.
    from previous_task_contract import recover_task
    payload={};manifest_path=root/'h250_release_manifest.json';m=None
    if manifest_path.exists():
        m=json.loads(read(root,'h250_release_manifest.json'))
        if m.get('version')!='V191-H250' or not isinstance(m.get('files'),dict):raise ValueError('unknown H250 manifest')
        # Do not endorse a broken prior release.
        for p,ident in m['files'].items():
            b=read(root,p)
            if not any(digest(v)==ident['sha256'] and blob(v)==ident['git_blob'] for v in (b,b.replace(b'\r\n',b'\n'))):raise ValueError('prior manifest mismatch '+p)
    for p,fun in [(PAGE,patch_page),(TABLE,patch_table),(TASK,patch_task),(HISTORY,patch_history)]:
        raw=read(root,p)
        if blob(raw)!=BASE[p]:raw=raw.replace(b'\r\n',b'\n')
        s=raw.decode('utf-8-sig')
        if blob(raw)!=BASE[p]:
            if p!=TASK or not recover_task(raw):raise ValueError('unrecognized exact source '+p+' '+blob(raw))
        payload[p]=fun(s).encode()
    for p in ('godpick_h250r2_nday.py','godpick_h250r2_table.py','tests/test_h250r2.py'):
        data=(ROOT/p).read_bytes()
        if (root/p).exists() and read(root,p)!=data:raise ValueError('existing helper differs '+p)
        payload[p]=data
    for p,b in payload.items():compile(b,p,'exec')
    if m is not None:
        m['revision']='R2'
        for p,b in payload.items():m['files'][p]={'sha256':digest(b),'git_blob':blob(b)}
        payload['h250_release_manifest.json']=json.dumps(m,ensure_ascii=False,indent=2,sort_keys=True).encode()+b'\n'
    payload['H250R2_OVERLAY_SHA256SUMS.txt']=''.join(f'{digest(v)}  {p}\n' for p,v in sorted(payload.items())).encode()
    if out.exists():raise ValueError('output already exists')
    with tempfile.NamedTemporaryFile(dir=out.parent,suffix='.tmp',delete=False) as f:tmp=Path(f.name)
    try:
        with zipfile.ZipFile(tmp,'w',zipfile.ZIP_DEFLATED) as z:
            for p,b in sorted(payload.items()):z.writestr(p,b)
        with zipfile.ZipFile(tmp) as z:
            if z.testzip():raise ValueError('ZIP CRC failure')
        tmp.replace(out)
    finally:tmp.unlink(missing_ok=True)
    print('[PASS] audited source, compile and ZIP checks; changed files:',len(payload),'\n[OUTPUT]',out)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('project',type=Path);p.add_argument('--out',type=Path,default=ROOT/'StockApp_V191_H250R2_Overlay.zip');a=p.parse_args()
    try:generate(a.project.resolve(),a.out.resolve())
    except Exception as e:raise SystemExit('[BLOCKED] '+str(e))
