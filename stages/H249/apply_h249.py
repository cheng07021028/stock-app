# -*- coding: utf-8 -*-
"""H249 exact-source overlay; requires installed H248, no project writes."""
from pathlib import Path
import argparse, hashlib, json, tempfile, zipfile
import h248_baseline as h248
import h244_baseline as h244
ROOT=Path(__file__).resolve().parent
SCHED=h248.SCHED;WORKER=h248.WORKER;PAGE=h248.PAGE
WF='.github/workflows/godpick_auto_scheduler_v191.yml'
PATCHES={SCHED:[('                    result=_execute_one(job,job_cfg,{**cfg,"_h248_single_attempt":not force_all_enabled})','''                    from godpick_h249_queue import guarded_execute
                    result=guarded_execute(job,slot,force_all_enabled,batch_id,status,
                        lambda: _execute_one(job,job_cfg,{**cfg,"_h248_single_attempt":not force_all_enabled}))''')],WORKER:[('def run_worker_dispatch(manual_job: str):','def _run_worker_dispatch_original(manual_job: str):'),('if __name__ == "__main__":','''def run_worker_dispatch(manual_job: str):
    from godpick_h249_queue import worker_dispatch
    return worker_dispatch(str(manual_job or "").strip(),_run_worker_dispatch_original)


if __name__ == "__main__":''')],PAGE:[('    from godpick_scheduler_wakeup_service import dispatch_scheduler_wakeup as dispatch_auto_scheduler_wakeup','    from godpick_h249_queue import dispatch_page17 as dispatch_auto_scheduler_wakeup'),('            st.markdown("#### H248｜智慧診斷與選擇性補跑")','''            st.markdown("#### H249｜永久派工佇列與中斷復原")
            with st.expander("遠端永久派工回執（最近兩個台灣日期）", expanded=False):
                from godpick_h249_queue import queue_view
                _h249_cache=st.session_state.get("_h249_queue_cache") or {}
                if st.button("更新永久派工清單",key="h249_refresh_queue") or time.time()-float(_h249_cache.get("at",0))>=60:
                    try:
                        _h249_rows,_h249_error=queue_view(str(st.secrets.get("GITHUB_TOKEN", "") or ""),str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028") or "cheng07021028"),str(st.secrets.get("GITHUB_REPO_NAME", "stock-app") or "stock-app"))
                    except Exception as _h249_exc:
                        _h249_rows,_h249_error=[],type(_h249_exc).__name__
                    _h249_cache={"at":time.time(),"rows":_h249_rows,"error":_h249_error}
                    st.session_state["_h249_queue_cache"]=_h249_cache
                if _h249_cache.get("error"):
                    st.warning(_h249_cache["error"])
                elif _h249_cache.get("rows"):
                    st.dataframe(pd.DataFrame(_h249_cache["rows"]),use_container_width=True,hide_index=True)
                else:
                    st.caption("最近兩日無已確認的永久派工紀錄。")
                st.caption("QUEUED：已永久受理；CLAIMED：已認領；REVIEW_REQUIRED：先前工作已開始，須核對副作用，禁止自動重播；EXECUTION_RECORDED：等待遠端驗證；VERIFIED：本批次遠端工作回執已核對；INCOMPLETE：未通過；EXPIRED：逾期未執行。")
                st.caption("正式時段工作維持優先；例行Worker空閒時接手一筆永久派工。已開始但結果不明的推薦寫入不會被盲目重播。")
            st.markdown("#### H248｜智慧診斷與選擇性補跑")''')],WF:[('permissions:\n  contents: write','permissions:\n  contents: write\n  actions: read # H249 verify exact previous attempt terminal state'),('      - name: H243 terminal wake receipt (separate from job success)','''      - name: H249 finalize durable request from exact remote batch evidence
        id: h249_finalize
        if: always() && steps.checkout.outcome == 'success' && steps.deps.outcome == 'success'
        continue-on-error: true
        env:
          H249_VERIFY: ${{ steps.verify.outcome }}
        run: python godpick_h249_queue.py

      - name: H243 terminal wake receipt (separate from job success)'''),('        run: python tools/h159_manual_verdict.py','''        run: python tools/h159_manual_verdict.py

      - name: H249 explicit durable dispatch verdict (routine wakes unaffected)
        if: ${{ always() && startsWith(github.event.inputs.wakeup_source || '', 'h249_') && steps.h249_finalize.outcome != 'success' }}
        run: |
          echo '::error::H249 exact durable request receipt not verified'
          exit 1''')]}

def replace(s,old,new,name):return h248.replace_once(s,old,new,name)
def blob(b):return h248.blob(b)
def source(root,name):
    raw=(root/name).read_bytes();s=raw.replace(b'\r\n',b'\n').decode('utf-8-sig')
    if name==WF:
        if h244.BASE[name] in (blob(raw),blob(s.encode())):return s
        # Reverse only known H244 changes to verify, not to generate output.
        prior=s
        for old,new in [('name: GodPick V191 Central Auto Scheduler\n','name: GodPick V191 Central Auto Scheduler\nrun-name: "Central | ${{ inputs.manual_job || \'due_only\' }} | ${{ inputs.wakeup_source || github.event_name }}"\n'),('  group: godpick-v191-central-auto-scheduler\n  cancel-in-progress: false','  group: godpick-v191-central-auto-scheduler\n  cancel-in-progress: false\n  queue: max # H244: preserve pending manual and routine requests, still serialize writes'),('echo "- wake source: ${{ github.event.inputs.wakeup_source || github.event_name }}"','echo "- wake source: $GODPICK_WAKEUP_SOURCE"')]:
            prior=replace(prior,new,old,name)
        if blob(prior.encode())==h244.BASE[name]:return s
        raise ValueError('Workflow differs from audited base or exact H244')
    prior=s
    for old,new in reversed(h248.PATCHES[name]):prior=replace(prior,new,old,name)
    if h248.BASE[name] in (blob(prior.encode()),blob(b'\xef\xbb\xbf'+prior.encode())):return s
    if name==PAGE:
        for old,new in reversed(json.loads((ROOT/'h244_page17_anchors.json').read_text('utf-8'))):prior=replace(prior,new,old,name)
        if h248.BASE[name] in (blob(prior.encode()),blob(b'\xef\xbb\xbf'+prior.encode())):return s
    raise ValueError(name+': differs from exact installed H248 source')
def transform(name,s):
    for old,new in PATCHES[name]:s=replace(s,old,new,name)
    if name.endswith('.py'):compile(s,name,'exec')
    return s

def create(root,out):
    for p in ('godpick_h245_list_authority.py','godpick_h246_seal_consistency.py','godpick_h247_source_health.py','godpick_h248_retry_policy.py'):
        if not (root/p).is_file():raise ValueError('Missing installed predecessor '+p)
    expected='644dcaae929911cc4012d94bcf3be4d8d724e7d5a57898f101bc65ea944f37e4'
    if hashlib.sha256((root/'godpick_h248_retry_policy.py').read_bytes().replace(b'\r\n',b'\n')).hexdigest()!=expected:raise ValueError('H248 policy differs from delivered version')
    if (root/'godpick_h249_queue.py').exists():raise ValueError('H249 already exists; do not overwrite')
    # H248 verdict and dispatch truth also must be actual integrated runtime versions.
    for p in (h248.VERDICT,h248.H244):source(root,p)
    payload={p:transform(p,source(root,p)).encode() for p in PATCHES}
    for p in ('godpick_h249_queue.py','tests/test_h249_queue.py','H249_README.txt','H249_TEST_RESULTS.txt'):
        payload[p]=(ROOT/p).read_bytes()
        if p.endswith('.py'):compile(payload[p],p,'exec')
    payload['H249_SHA256SUMS.txt']=''.join(f'{hashlib.sha256(v).hexdigest()}  {p}\n' for p,v in payload.items()).encode()
    if out.exists():raise ValueError('Output exists; rename old overlay')
    with tempfile.NamedTemporaryFile(dir=out.parent,suffix='.zip',delete=False) as f:tmp=Path(f.name)
    try:
        with zipfile.ZipFile(tmp,'w',zipfile.ZIP_DEFLATED) as z:
            for p,v in payload.items():z.writestr(p,v)
        with zipfile.ZipFile(tmp) as z:
            if z.testzip():raise ValueError('ZIP integrity failed')
        tmp.replace(out)
    finally:tmp.unlink(missing_ok=True)
    print('[PASS] Exact H248 baseline; '+str(len(PATCHES))+' modified sources; overlay: '+str(out))

def main():
    ap=argparse.ArgumentParser();ap.add_argument('project',type=Path);a=ap.parse_args()
    try:create(a.project.resolve(),ROOT/'StockApp_V191_H249_Overlay.zip')
    except Exception as exc:raise SystemExit('[BLOCKED] '+str(exc))
if __name__=='__main__':main()
