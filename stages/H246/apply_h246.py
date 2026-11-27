# -*- coding: utf-8 -*-
"""Generate H246 overlay from exact H245-patched source. Never modify target."""
import argparse, ast, hashlib, zipfile
from pathlib import Path
from h245_baseline import PATCHES as H245_PATCHES, BASE as H245_BASE
ROOT=Path(__file__).resolve().parent
TASK='godpick_auto_update_tasks.py'; BRIDGE='godpick_h138_unattended_bridge.py'
BRIDGE_BASE='8b3369a38f9046c5b48caa2a307130dc54b7ace8'
PATCHES={TASK:[('''        # H138: scheduled Page07 already runs headlessly.''','''        # H246: exact same-run remote payload, anchor, list and H241 history.
        # Only the central worker can atomically repair an exact full snapshot.
        try:
            from godpick_h246_seal_consistency import run_check
            result["h246_consistency"] = run_check(result, BASE_DIR)
        except Exception as _h246_exc:
            result["h246_consistency"] = {"status": "H246_EXCEPTION", "confirmed": False,
                                           "error_type": type(_h246_exc).__name__}
        # H138: scheduled Page07 already runs headlessly.'''),('''            msg += "｜H138主管決策遠端封存尚未確認（與Page07掃描成功分開）"''','''            msg += "｜H138主管決策遠端封存尚未確認（與Page07掃描成功分開）" + "｜" + str(result["h138_auto_seal"].get("eod_status") or result["h138_auto_seal"].get("status"))
        if ok:
            msg += "｜H246推薦遠端一致性：" + str(result["h246_consistency"].get("status"))'''),('''warning=bool(result.get("warning") or (ok and not result["h138_auto_seal"].get("confirmed")))''','''warning=bool(result.get("warning") or (ok and (not result["h138_auto_seal"].get("confirmed") or not result["h246_consistency"].get("confirmed"))))''')],BRIDGE:[('''    checker = eod_checker(Path(root or Path(__file__).resolve().parent), day, now=clock)
    if checker.get('status') != 'EOD_THREE_MARKETS_INTERNAL_HASH_VERIFIED':
        return _state('MARKET_DAY_UNVERIFIED', eod_status=checker.get('status', 'UNKNOWN'))''','''    # H246: respect H117 18:15 collection floor and read newly published main
    # evidence from a pinned commit if this central worker's checkout is stale.
    if eod_checker is verified_eod:
        from godpick_h246_seal_consistency import check_eod
        checker = check_eod(Path(root or Path(__file__).resolve().parent), day, now=clock, checker=eod_checker)
    else:
        checker = eod_checker(Path(root or Path(__file__).resolve().parent), day, now=clock)
    if checker.get('status') != 'EOD_THREE_MARKETS_INTERNAL_HASH_VERIFIED':
        return _state('MARKET_DAY_UNVERIFIED', eod_status=checker.get('status', 'UNKNOWN'),
                      eod_missing=checker.get('missing',''), eod_source=checker.get('h246_source',''))''')]}
def blob(data):return hashlib.sha1(f'blob {len(data)}\0'.encode()+data).hexdigest()
def replace_once(source,old,new):
    if source.count(old)!=1:raise ValueError('H246 unique source anchor missing or duplicated')
    return source.replace(old,new,1)
def verified_source(root,name):
    raw=(root/name).read_bytes().replace(b'\r\n',b'\n'); s=raw.decode('utf-8-sig')
    if name==TASK:
        if not (root/'godpick_h245_list_authority.py').is_file():raise ValueError('H245 prerequisite missing')
        original=s
        for old,new in reversed(H245_PATCHES):original=replace_once(original,new,old)
        # UTF-8 BOM may be present in the original blob, but H245 emits UTF-8.
        if H245_BASE not in {blob(original.encode()),blob(b'\xef\xbb\xbf'+original.encode())}:raise ValueError('H245 source differs from verified base')
    elif blob(raw)!=BRIDGE_BASE:raise ValueError('H138 bridge blob differs from verified base')
    return s
def transform(name,s):
    for old,new in PATCHES[name]:s=replace_once(s,old,new)
    ast.parse(s,filename=name);return s

def main():
    a=argparse.ArgumentParser(description=__doc__);a.add_argument('project',type=Path);args=a.parse_args();root=args.project.resolve()
    try:
        output={name:transform(name,verified_source(root,name)).encode() for name in PATCHES}
        for name in ('godpick_h246_seal_consistency.py','test_h246.py','H246_README.txt'):
            output[name]=(ROOT/name).read_bytes()
            if name.endswith('.py'):ast.parse(output[name],filename=name)
        output['H246_SHA256SUMS.txt']=''.join(f'{hashlib.sha256(v).hexdigest()}  {k}\n' for k,v in output.items()).encode()
        out=ROOT/'StockApp_V191_H246_Overlay.zip'; temp=out.with_suffix('.tmp')
        with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED) as z:
            for k,v in output.items():z.writestr(k,v)
        with zipfile.ZipFile(temp) as z:
            if z.testzip():raise ValueError('ZIP integrity failed')
        temp.replace(out);print('[PASS] H245 exact source + H138 blob + 4 unique anchors + AST + ZIP');print(out)
    except Exception as exc:raise SystemExit('[BLOCKED] '+str(exc))
if __name__=='__main__':main()
