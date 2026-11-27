# -*- coding: utf-8 -*-
"""Generate exact-source H244 overlay; no GitHub push, no data/secret mutations."""
from pathlib import Path
import argparse, hashlib, sys, zipfile, tempfile

BASE = {
 '.github/workflows/godpick_auto_scheduler_v191.yml':'5750ba95ff2821610581c7882eaaab2fd139f5f7',
 '.github/workflows/h195_daily_command.yml':'3e08ec7844ea223c640799801ac8cb331dae690b',
 '.github/workflows/h220_review.yml':'5a702a677531e976d8f56b157445a23532aebe1f',
 'godpick_scheduler_wakeup_service.py':'5938c7e20e21937b6fcd831ef63134501964cc9f',
 'godpick_h168_worker_live.py':'2a26a598285d087385bb6abc8336c7cb8eae886c',
 'pages/17_系統健康檢查.py':'1ee0f4b64fab8460f56ddb3940772f025eaa03f8',
 'tests/test_h168_worker_live.py':'06a53fcf7f54f1cf5b0f29bbb20f63ffec0bbce5',
}

def blob_hash(b):
    return hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()


def replace(s, old, new):
    if s.count(old) != 1:
        raise ValueError(f'定位點不唯一或缺失：{old[:120]!r} ({s.count(old)})')
    return s.replace(old,new,1)


def patch(path, s):
    R=lambda old,new: replace(s,old,new)
    if path.endswith('godpick_auto_scheduler_v191.yml'):
        s=R('name: GodPick V191 Central Auto Scheduler\n','name: GodPick V191 Central Auto Scheduler\nrun-name: "Central | ${{ inputs.manual_job || \'due_only\' }} | ${{ inputs.wakeup_source || github.event_name }}"\n')
        s=R('  group: godpick-v191-central-auto-scheduler\n  cancel-in-progress: false','  group: godpick-v191-central-auto-scheduler\n  cancel-in-progress: false\n  queue: max # H244: preserve pending manual and routine requests, still serialize writes')
        # Do not interpolate user-controlled dispatch input directly into a shell script.
        s=R('echo "- wake source: ${{ github.event.inputs.wakeup_source || github.event_name }}"','echo "- wake source: $GODPICK_WAKEUP_SOURCE"')
    elif path.endswith(('h195_daily_command.yml','h220_review.yml')):
        old="      - run: python -m pip install 'pandas==2.3.3' 'numpy>=1.26,<3' 'requests>=2.31,<3' 'openpyxl>=3.1.2,<4'"
        s=R(old,old+" 'lxml>=5.4,<7' 'beautifulsoup4>=4.12,<5' 'html5lib>=1.1,<2'\n      - run: python -c \"import lxml.etree, bs4, html5lib\"")
    elif path=='godpick_scheduler_wakeup_service.py':
        s=R('    http_post: Callable[..., Any] | None = None,','    http_post: Callable[..., Any] | None = None,\n    receipt: dict | None = None,')
        s=R('    token = str(token or "").strip()','    if receipt is not None:\n        receipt.clear()\n    token = str(token or "").strip()')
        s=R('"X-GitHub-Api-Version": "2022-11-28",','"X-GitHub-Api-Version": "2026-03-10" if receipt is not None else "2022-11-28",')
        s=R('        if code == 204:\n            if manual_job:', '''        if code in (200, 204):
            run_id = ""
            if code == 200:
                try:
                    candidate = str(response.json().get("workflow_run_id") or "")
                    run_id = candidate if candidate.isdigit() else ""
                except Exception:
                    pass
            if receipt is not None:
                receipt.update({"run_id": run_id, "http_status": code})
            if run_id:
                return True, f"已受理 {'手動工作 ' + manual_job if manual_job else '到期工作'}｜Run {run_id}；尚未完成驗收。"
            if manual_job:''')
        s=R('return False, f"Page17 自動補跑喚醒失敗：{type(exc).__name__}: {exc}"','return False, f"Page17 自動補跑喚醒失敗：{type(exc).__name__}；遠端是否受理未知，請先查 Actions，勿直接重送。"')
    elif path=='godpick_h168_worker_live.py':
        s=R('params={"per_page": 20}', 'params={"per_page": 100}')
        s=R('for raw in body["workflow_runs"][:20]:','for raw in body["workflow_runs"][:100]:')
        s=R('                "event": str(raw.get("event") or "")[:40],','                "event": str(raw.get("event") or "")[:40],\n                "display_title": str(raw.get("display_title") or "")[:180],')
        s=R('safe["runs"] = runs[:20]','safe["runs"] = runs[:100]')
    elif path=='tests/test_h168_worker_live.py':
        s=R('assert fake.calls[0][1]["params"]["per_page"] == 20','assert fake.calls[0][1]["params"]["per_page"] == 100')
    elif path.startswith('pages/'):
        s=R('# H168: independent read-only Actions truth while the worker\'s local', 'from godpick_h244_dispatch_truth import read_run as _h244_read_run, describe as _h244_describe, taipei_now as _h244_now, ACTIVE as _H244_ACTIVE, mode_label as _h244_mode\n\n@st.cache_data(ttl=25, show_spinner=False)\ndef _h244_cached_run(token, owner, repo, run_id):\n    return _h244_read_run(token=token, owner=owner, repo=repo, run_id=run_id)\n\n# H168: independent read-only Actions truth while the worker\'s local')
        s=R('"事件": x.get("event"), "排隊/執行開始UTC": x.get("created_at_utc"),','"事件": x.get("event"), "批次模式": x.get("display_title"), "排隊/執行開始UTC": x.get("created_at_utc"),')
        s=R('"GitHub 並行群組只保留有限待處理項目，取消不代表業務工作完成。"','"取消不代表業務工作完成；H244 採最多100筆等待的序列佇列。"')
        s=R('''                _ok, _msg = dispatch_auto_scheduler_wakeup(
                    token=_token, owner=_owner, repo=_repo, branch=_branch,
                    wakeup_source=wake_source, manual_job=manual_job,
                )
                st.session_state["h159_worker_dispatch"] = {
                    "ok": bool(_ok), "manual_job": manual_job,
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"), "message": str(_msg),
                }''','''                _receipts = st.session_state.setdefault("h244_receipts", {})
                _old = _receipts.get(manual_job or "due_only", {})
                if _old.get("ok") and _old.get("run_id"):
                    _run = _h244_read_run(token=_token, owner=_owner, repo=_repo, run_id=_old["run_id"])
                    if _run.get("available") and _run.get("status") in _H244_ACTIVE:
                        st.info(f"本模式已有 Run {_old['run_id']} 排隊／執行中，不重複派送。")
                        return
                _receipt = {}
                _ok, _msg = dispatch_auto_scheduler_wakeup(
                    token=_token, owner=_owner, repo=_repo, branch=_branch,
                    wakeup_source=wake_source, manual_job=manual_job, receipt=_receipt,
                )
                _record = {"ok": bool(_ok), "manual_job": manual_job,
                           "time": _h244_now(), "message": str(_msg), **_receipt}
                _receipts[manual_job or "due_only"] = _record
                st.session_state["h159_worker_dispatch"] = _record''')
        s=R('st.info(str(_msg) + "｜僅確認派送成功，不代表14項已完成；請以下方遠端批次履歷和最後訊息為準。")','st.info(str(_msg) + "｜僅確認派送，請看下方本筆 Run ID 專區；不是完成回報。")')
        s=R('''            st.caption("最近 Worker 派送：" + str(_h159_last_dispatch.get("time")) + "｜" + (
                "已受理，待遠端確認" if _h159_last_dispatch.get("ok") else "派送失敗"
            ))''','''            st.caption("最近 Worker 派送：" + str(_h159_last_dispatch.get("time")) + "｜" + _h244_mode(_h159_last_dispatch.get("manual_job") or "") + "｜Run " + str(_h159_last_dispatch.get("run_id") or "未取得"))''')
        s=R('''            st.markdown("#### 即時排程狀態 / 成功失敗訊息")''','''            st.markdown("#### H244｜本次人工派送（依 Run ID，不混入例行排程）")
            for _receipt in st.session_state.get("h244_receipts", {}).values():
                try:
                    _rid = _receipt.get("run_id")
                    _token = str(st.secrets.get("GITHUB_TOKEN", "") or "").strip()
                    _owner = str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028") or "cheng07021028").strip()
                    _repo = str(st.secrets.get("GITHUB_REPO_NAME", "stock-app") or "stock-app").strip()
                    _run = _h244_cached_run(_token, _owner, _repo, _rid) if _rid else {}
                    _level, _detail = _h244_describe(_receipt, _run, _live_status)
                    getattr(st, _level)(_detail)
                    if _run.get("url"):
                        st.markdown(f"[查看本筆 GitHub Run {_rid}]({_run['url']})")
                except Exception as _exc:
                    st.warning("本筆 Run 查詢未確認：" + type(_exc).__name__)
            _latest_manual = (_live_status.get("last_manual_batch") or {}) if isinstance(_live_status, dict) else {}
            if _latest_manual:
                _ms = _latest_manual.get("summary") or {}
                st.caption(f"遠端最近手動批次（重啟後仍可查）：{_latest_manual.get('batch_id')}｜{_latest_manual.get('batch_mode')}｜成功 {_ms.get('success',0)}／警示 {_ms.get('warning',0)}／失敗 {_ms.get('failed',0)}／阻擋 {_ms.get('blocked',0)}")
            st.markdown("#### 即時排程狀態 / 成功失敗訊息（全域，非本次人工派送）")''')
        s=R('''            _ok, _msg = dispatch_auto_scheduler_wakeup(
                token=_token, owner=_owner, repo=_repo, branch=_branch,
                wakeup_source="page17_h156_" + _manual_job,
                manual_job=_manual_job,
            )
            st.session_state["h156_last_dispatch"] = {
                "ok": bool(_ok), "mode": _manual_job,
                "time": time.strftime("%Y-%m-%d %H:%M:%S"), "message": str(_msg),
            }''','''            _receipts = st.session_state.setdefault("h244_receipts", {})
            _old = _receipts.get(_manual_job, {})
            if _old.get("ok") and _old.get("run_id"):
                _run = _h244_read_run(token=_token, owner=_owner, repo=_repo, run_id=_old["run_id"])
                if _run.get("available") and _run.get("status") in _H244_ACTIVE:
                    st.info(f"本模式已有 Run {_old['run_id']} 排隊／執行中，不重複派送。")
                    return
            _receipt = {}
            _ok, _msg = dispatch_auto_scheduler_wakeup(
                token=_token, owner=_owner, repo=_repo, branch=_branch,
                wakeup_source="page17_h156_" + _manual_job,
                manual_job=_manual_job, receipt=_receipt,
            )
            _record = {"ok": bool(_ok), "mode": _manual_job, "manual_job": _manual_job,
                       "time": _h244_now(), "message": str(_msg), **_receipt}
            _receipts[_manual_job] = _record
            st.session_state["h156_last_dispatch"] = _record''')
        s=R('st.caption(f"本頁最近派送：{_dispatch_state.get(\'time\')}｜{_dispatch_state.get(\'mode\')}｜{\'已受理，待 worker 執行\' if _dispatch_state.get(\'ok\') else \'派送失敗\'}")','st.caption(f"本頁最近派送：{_dispatch_state.get(\'time\')}｜{_dispatch_state.get(\'mode\')}｜Run {_dispatch_state.get(\'run_id\') or \'未取得\'}｜{\'已受理，請看 H244 本次人工派送\' if _dispatch_state.get(\'ok\') else \'派送失敗\'}")')
    else: raise ValueError(path)
    return s


def build(root, output):
    """Verify every source byte before making a single output; never modify root."""
    changes={}
    for path, sha in BASE.items():
        raw=(root/path).read_bytes()
        # Git checkout on Windows may convert CRLF. Verify canonical blob bytes.
        if blob_hash(raw)!=sha:
            raw=raw.replace(b'\r\n',b'\n')
        if blob_hash(raw)!=sha:
            raise ValueError(f'版本不符 {path}，預期 {sha}，實際 {blob_hash(raw)}；停止，沒有覆蓋任何原始檔。')
        src=raw.decode('utf-8')
        new=patch(path,src)
        if path.endswith('.py'): compile(new,path,'exec')
        changes[path]=new.encode('utf-8')
    helper=Path(__file__).with_name('godpick_h244_dispatch_truth.py').read_bytes()
    compile(helper,'godpick_h244_dispatch_truth.py','exec')
    changes['godpick_h244_dispatch_truth.py']=helper
    if (root/'godpick_h244_dispatch_truth.py').exists():
        raise ValueError('H244 helper 已存在，停止避免覆蓋新版。')
    if output.exists(): raise FileExistsError(f'輸出已存在：{output}')
    output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=output.parent,suffix='.zip',delete=False) as tmp: temp=Path(tmp.name)
    try:
        with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED) as z:
            for path,b in changes.items(): z.writestr(path,b)
        with zipfile.ZipFile(temp) as z:
            if z.testzip() is not None: raise ValueError('ZIP integrity failure')
        temp.replace(output)
    finally:
        temp.unlink(missing_ok=True)
    return list(changes)


def main():
    p=argparse.ArgumentParser(description='H244 exact-source overlay generator; original project remains untouched')
    p.add_argument('project',type=Path,help='GitHub main 原始專案解壓目錄')
    p.add_argument('--out',type=Path,default=Path.cwd()/'StockApp_V191_H244_Overlay.zip')
    a=p.parse_args()
    try:
        files=build(a.project.resolve(),a.out.resolve())
        print('[PASS] 原始版本雜湊、定位點、Python compile、ZIP integrity 通過')
        for f in files: print(f)
        print('[OUTPUT]',a.out.resolve())
    except Exception as e:
        print('[BLOCKED]',e); return 1
    return 0

if __name__=='__main__': sys.exit(main())
