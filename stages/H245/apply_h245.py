# -*- coding: utf-8 -*-
"""Create an exact-source H245 overlay; never write to project or runtime-data."""
from pathlib import Path
import argparse, ast, hashlib, zipfile, json

ROOT = Path(__file__).resolve().parent
TARGET = 'godpick_auto_update_tasks.py'
BASE = '1cfe096d955a82ab7e8aff0cf3715eb82cfd8610'
PATCHES = [
('''    latest = latest if isinstance(latest, dict) else {}
    rows = _extract_named_rows_v191_h7(latest)''','''    latest = latest if isinstance(latest, dict) else {}
    # H245: an empty current list is not permission to resurrect historical rows.
    # Validate publication pointers before even considering the legacy H7 fallback.
    if payload == []:
        from godpick_h245_list_authority import audit_empty_list, PREFIX
        from godpick_persistence_service import (
            read_github_json, ensure_records_local_authority_current,
            records_authority_verification_h240,
        )
        def _h245_verified_history():
            history, _, _ = ensure_records_local_authority_current()
            return history if records_authority_verification_h240().get("status") == "VERIFIED" else None
        audit = audit_empty_list(payload, latest, read_github_json, _h245_verified_history)
        msgs.append(PREFIX + audit["code"] + "｜" + audit["detail"] + f"｜07快照 {audit['snapshot_rows']} 筆")
        return pd.DataFrame(), msgs
    rows = _extract_named_rows_v191_h7(latest)'''),
('''def _h243_identity_guard(df, msgs, started):
    from godpick_h243_tracking_evidence import eligible''','''def _h243_identity_guard(df, msgs, started):
    from godpick_h245_list_authority import PREFIX
    authority = next((m for m in reversed(msgs) if isinstance(m, str) and m.startswith(PREFIX)), "")
    if df.empty and authority:
        code = authority[len(PREFIX):].split("｜", 1)[0]
        result = _report(True, authority + "；未計算、未同步、不變更08歷史",
            details={"outcome": code, "load_messages": msgs[-10:]},
            started=started, warning=(code != "NO_CURRENT_LIST"))
        # Missing evidence is not a transient provider failure. Retry at next slot.
        result["retry_pending"] = False
        return result
    from godpick_h243_tracking_evidence import eligible'''),
('''        df,msgs=_load_recommend_list()
        from godpick_h243_tracking_evidence import missing_ids, repair''','''        df,msgs=_load_recommend_list()
        if df.empty:
            blocked=_h243_identity_guard(df,msgs,t0)
            if blocked is not None:return blocked
        from godpick_h243_tracking_evidence import missing_ids, repair'''),
]

def git_blob_sha(data):
    return hashlib.sha1(f'blob {len(data)}\0'.encode()+data).hexdigest()


def transform(source):
    for old,new in PATCHES:
        if source.count(old) != 1:
            raise ValueError('H245 anchor missing or duplicate; no overlay generated')
        source=source.replace(old,new,1)
    ast.parse(source,filename=TARGET)
    return source


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('project',type=Path)
    a=p.parse_args()
    project=a.project.resolve()
    raw=(project/TARGET).read_bytes()
    if git_blob_sha(raw) != BASE:
        raw=raw.replace(b'\r\n',b'\n')
    if git_blob_sha(raw) != BASE:
        raise SystemExit('[BLOCKED] Unsupported source Git blob: '+git_blob_sha(raw)+'; expected '+BASE)
    if not (project/'godpick_h244_dispatch_truth.py').is_file():
        raise SystemExit('[BLOCKED] H244 prerequisite module missing')
    changed=transform(raw.decode('utf-8-sig')).encode('utf-8')
    payload={TARGET:changed}
    for name in ('godpick_h245_list_authority.py','h245_audit.py','test_h245.py','H245_README.txt'):
        payload[name]=(ROOT/name).read_bytes()
        if name.endswith('.py'):ast.parse(payload[name],filename=name)
    payload['H245_SHA256SUMS.txt']=''.join(f'{hashlib.sha256(v).hexdigest()}  {k}\n' for k,v in payload.items()).encode()
    out=ROOT/'StockApp_V191_H245_Overlay.zip'
    temp=out.with_suffix('.tmp')
    with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED) as z:
        for k,v in payload.items():z.writestr(k,v)
    with zipfile.ZipFile(temp) as z:
        if z.testzip() is not None:raise RuntimeError('ZIP integrity failure')
    temp.replace(out)
    print('[PASS] Git blob, 3 unique anchors, Python AST, ZIP integrity')
    print(out)

if __name__=='__main__':main()
