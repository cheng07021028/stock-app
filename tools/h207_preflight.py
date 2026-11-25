# -*- coding: utf-8 -*-
"""Check and install only H207 sidebar changes, with hash guards and backup."""
from pathlib import Path
import argparse, hashlib, os, re, tempfile

# Generated from the verified H204 GitHub baseline and H205 delivery.
MANIFEST = {'utils.py': (['ad44a6107507556d2470b562828f26008356929818979db34ff47ee0ac200759'], '23d46020d4a7e2af70690aaade18a19a1ba2fde8c27f2d2d3c32f700ae82ffc1'), 'pages/14_股神權重校正.py': (['2476f9d3dce2ea63ceafc6d1fc6e0251f67bc5c4e836bb8581f910befd537ebf'], 'a9d0857d7153b1e0b71d88737f4b47fecb6f456a2f3c1cfc5b8dd6e31bde6dc7'), 'godpick_h204_sidebar_navigation.py': (['40144b3af608f5fa86f1d7fddd1cb7dea238792a1ecb7b4079b5c07061aa9ffa', '0ca2ede23f376c8383df9c96fea2bc0a0d671d1562b036f6ee36d2b6d7a36d4c'], 'af7b6baf81921134e097f1a71865a59c015dcbe14f840be00373c5bb4e12763f')}
ENTRY_SHA = 'd37de83e5db6d50942cadc89e99c45fa54c7c7fdf74be95deb74340f364f6100'
EXPECTED = set(range(34)) - {13} | {98}

def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()

def check(root, patch, apply=False):
    if digest(root/'streamlit_app.py') != ENTRY_SHA:
        raise ValueError('Unknown entrypoint version; refusing overwrite')
    numbers={int(m.group(1)) for p in (root/'pages').glob('*.py') if (m:=re.match(r'^(\d+)_.*\.py$',p.name))}
    if EXPECTED-numbers:
        raise ValueError(f'Missing expected page numbers: {sorted(EXPECTED-numbers)}')
    changes={}
    for rel,(old,new) in MANIFEST.items():
        src,dst=patch/rel,root/rel
        if digest(src)!=new: raise ValueError(f'Invalid patch: {rel}')
        current=digest(dst)
        if current==new: continue
        if current not in old: raise ValueError(f'Unknown deployed version: {rel}')
        changes[rel]=current
    if not changes: return 'ALREADY_APPLIED_H207'
    if not apply: return 'READY_APPLY_H207'
    backup=root/'.h207_backup'
    staged={}; replaced=[]
    try:
        for rel,old in changes.items():
            dst=root/rel; bak=backup/rel
            bak.parent.mkdir(parents=True,exist_ok=True)
            if not bak.exists():
                with bak.open('xb') as f: f.write(dst.read_bytes())
            if digest(bak)!=old: raise ValueError(f'Backup mismatch: {rel}')
            fd,tmp=tempfile.mkstemp(prefix='.h207_',suffix='.tmp',dir=dst.parent)
            staged[rel]=Path(tmp)
            with os.fdopen(fd,'wb') as f: f.write((patch/rel).read_bytes())
            if digest(staged[rel])!=MANIFEST[rel][1]: raise ValueError(f'Staged hash mismatch: {rel}')
        if any(digest(root/r)!=v for r,v in changes.items()): raise ValueError('Concurrent changes detected')
        for rel,tmp in staged.items():
            os.replace(tmp,root/rel); replaced.append(rel)
        if any(digest(root/r)!=MANIFEST[r][1] for r in changes): raise ValueError('Post-install hash mismatch')
    except BaseException:
        for rel in reversed(replaced):
            if digest(root/rel)==MANIFEST[rel][1]:
                (root/rel).write_bytes((backup/rel).read_bytes())
        raise
    finally:
        for tmp in staged.values(): tmp.unlink(missing_ok=True)
    return 'APPLIED_H207'

def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--root',required=True,type=Path); p.add_argument('--apply',action='store_true'); a=p.parse_args()
    try:
        print(check(a.root.resolve(),Path(__file__).resolve().parents[1],a.apply)); return 0
    except (ValueError,OSError) as e:
        print('BLOCKED_H207:',e); return 1
if __name__=='__main__': raise SystemExit(main())
