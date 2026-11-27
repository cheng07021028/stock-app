# -*- coding: utf-8 -*-
"""H250 exact-source cumulative release builder. No production writes."""
from pathlib import Path
import argparse, hashlib, importlib.util, json, sys, tempfile, zipfile
from unittest.mock import patch
ROOT=Path(__file__).resolve().parent

def load(n,filename):
    p=ROOT/'stages'/f'H{n}';sys.path.insert(0,str(p))
    try:
        spec=importlib.util.spec_from_file_location(f'h250_stage_{n}',p/filename);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
    finally:sys.path.pop(0)

def blob(b):return hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()
def sha(b):return hashlib.sha256(b).hexdigest()
def change(s,pairs):
    for old,new in pairs:
        if s.count(old)!=1:raise ValueError('nonunique anchor: '+repr(old[:100]))
        s=s.replace(old,new,1)
    return s

def stages():
    a,b,c,d,e,f=[load(n,p) for n,p in [(244,'Apply_H244.py'),(245,'apply_h245.py'),(246,'apply_h246.py'),(247,'apply_h247.py'),(248,'apply_h248.py'),(249,'apply_h249.py')]]
    base={};steps={}
    def add(p,h,pairs,n):
        if p in base and base[p]!=h:raise ValueError('inconsistent base '+p)
        base[p]=h;steps.setdefault(p,[]).append((n,list(pairs)))
    for p,h in a.BASE.items():
        pairs=[]
        def collect(s,old,new):pairs.append((old,new));return s
        with patch.object(a,'replace',collect):a.patch(p,'')
        if not pairs:raise ValueError('empty H244 patch '+p)
        add(p,h,pairs,244)
    add(b.TARGET,b.BASE,b.PATCHES,245)
    for p,pairs in c.PATCHES.items():add(p,b.BASE if p==c.TASK else c.BRIDGE_BASE,pairs,246)
    for p,pairs in d.PATCHES.items():
        if p in (d.WF+'h195_daily_command.yml',d.WF+'h220_review.yml'):pairs=[(d.H244_PIP,d.PIP)]
        add(p,d.BASE[p],pairs,247)
    for p,pairs in e.PATCHES.items():add(p,e.BASE[p],pairs,248)
    for p,pairs in f.PATCHES.items():add(p,a.BASE[p] if p==f.WF else e.BASE[p],pairs,249)
    return base,steps

def recover(raw,expected,steps):
    """Accept exact upstream blob or exact output of an earlier stage, never arbitrary anchors."""
    for v in (raw,raw.replace(b'\r\n',b'\n')):
        if blob(v)==expected:return v.decode('utf-8'),0
    text=raw.replace(b'\r\n',b'\n').decode('utf-8')
    for count in range(len(steps),0,-1):
        try:
            original=text
            for _,pairs in reversed(steps[:count]):original=change(original,[(n,o) for o,n in reversed(pairs)])
            for s in (original,original.lstrip('\ufeff'),'\ufeff'+original.lstrip('\ufeff')):
                if blob(s.encode())!=expected:continue
                check=s
                for n,pairs in steps[:count]:check=apply_step(check,n,pairs)
                if check==text:return s,count
        except ValueError:continue
    raise ValueError('source does not match audited blob or exact previous overlay: '+blob(raw)+' expected '+expected)

def apply_step(s,n,pairs):
    if n!=244:s=s.lstrip('\ufeff')
    return change(s,pairs)

def safe_read(root,p):
    q=root/p
    if not q.is_file() or q.is_symlink() or not q.resolve().is_relative_to(root.resolve()):raise ValueError('missing or unsafe file '+p)
    return q.read_bytes()

def generate(project,out):
    import yaml
    base,steps=stages();payload={};observed={};final={}
    added={
      'godpick_h244_dispatch_truth.py':244,'godpick_h245_list_authority.py':245,'h245_audit.py':245,'godpick_h246_seal_consistency.py':246,'godpick_h247_source_health.py':247,'godpick_h248_retry_policy.py':248,'godpick_h249_queue.py':249,
      'tests/test_h247_source_contract.py':247,'tests/test_h248_scheduler_contract.py':248,'tests/test_h249_queue.py':249}
    for p,h in base.items():
        if p in added:
            original=(ROOT/'stages'/f'H{added[p]}'/p).read_bytes()
            if blob(original)!=h:raise ValueError('bundled base mismatch '+p)
            raw=safe_read(project,p) if (project/p).exists() else original
        else:raw=safe_read(project,p)
        try:s,k=recover(raw,h,steps[p])
        except ValueError as exc:raise ValueError(p+': '+str(exc)) from None
        for n,pairs in steps[p]:s=apply_step(s,n,pairs)
        final[p]=s.encode();observed[p]=[n for n,_ in steps[p][:k]]
        if not (project/p).exists() or raw!=final[p]:payload[p]=final[p]
    for p,n in added.items():
        if p in final:continue
        data=(ROOT/'stages'/f'H{n}'/p).read_bytes()
        if (project/p).exists() and safe_read(project,p).replace(b'\r\n',b'\n')!=data.replace(b'\r\n',b'\n'):raise ValueError('unknown existing helper/test '+p)
        final[p]=data
        if not (project/p).exists():payload[p]=data
    # The manifest and H250 read-only audit are release files, not runtime data.
    for p in ('tools/h250_acceptance.py','tests/test_h250_acceptance.py','.github/workflows/h250_acceptance.yml'):
        data=(ROOT/p).read_bytes()
        if (project/p).exists() and safe_read(project,p).replace(b'\r\n',b'\n')!=data.replace(b'\r\n',b'\n'):raise ValueError('unknown H250 file '+p)
        final[p]=data
        if not (project/p).exists():payload[p]=data
    for p,data in final.items():
        if p.endswith('.py'):compile(data,p,'exec')
        elif p.endswith('.yml'):
            doc=yaml.safe_load(data)
            if not isinstance(doc,dict) or not doc.get('jobs') or not ('on' in doc or True in doc):raise ValueError('invalid workflow '+p)
    manifest={'version':'V191-H250','files':{p:{'sha256':sha(v),'git_blob':blob(v)} for p,v in sorted(final.items())},'base_blobs':base}
    m=json.dumps(manifest,ensure_ascii=False,indent=2,sort_keys=True).encode()+b'\n'
    if (project/'h250_release_manifest.json').exists() and safe_read(project,'h250_release_manifest.json').replace(b'\r\n',b'\n')!=m:raise ValueError('existing H250 manifest differs')
    payload['h250_release_manifest.json']=m
    if out.exists():raise ValueError('output already exists')
    out.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=out.parent,suffix='.zip',delete=False) as f:tmp=Path(f.name)
    try:
        with zipfile.ZipFile(tmp,'w',zipfile.ZIP_DEFLATED) as z:
            for p,v in sorted(payload.items()):z.writestr(p,v)
        with zipfile.ZipFile(tmp) as z:
            if z.testzip() or set(z.namelist())!=set(payload):raise ValueError('invalid overlay')
        tmp.replace(out)
    finally:tmp.unlink(missing_ok=True)
    print('[PASS] exact source/previous-stage hashes, compile, YAML, ZIP. Changed files:',len(payload),'manifest files:',len(final));print('[OUTPUT]',out)
    return manifest,observed

def main():
    p=argparse.ArgumentParser();p.add_argument('project',type=Path);p.add_argument('--out',type=Path,default=ROOT/'StockApp_V191_H250_Overlay.zip');a=p.parse_args()
    try:generate(a.project.resolve(),a.out.resolve())
    except Exception as e:print('[BLOCKED]',e);return 2
    return 0
if __name__=='__main__':raise SystemExit(main())
