# -*- coding: utf-8 -*-
"""H236 optional GCS durable H235 evidence. Trusted publisher, read-only restore.
No public upload, no local H234 promotion, no automatic network on research scans.
Cloud and local hashes verify integrity, not official signatures.
"""
import hashlib,json,os,re,shutil,tempfile,threading
from datetime import datetime
from pathlib import Path
import godpick_h190_market_archive as ar
import godpick_h192_financial_growth as fin
import godpick_h235_official_batch as h

VERSION='V191-H236-CLOUD-FINANCIAL'
PREFIX='stockapp/h236/v1'
MANIFEST_LIMIT=2_000_000
_LOCK=threading.RLock()

def keys(p,digest):
    if not re.fullmatch(r'[0-9a-f]{64}',digest):raise ValueError('invalid archive digest')
    prefix=f'{PREFIX}/{fin.qstr(p)}'
    return f'{prefix}/sha256/{digest}.zip',f'{prefix}/manifest.json'

def manifest_key(p):return keys(p,'0'*64)[1]

def _receipt(r,p,now):
    if not isinstance(r,dict) or r.get('version')!=h.VERSION or r.get('period')!=p or r.get('transport')!='DIRECT_HTTPS_NO_REDIRECT':raise ValueError('not an H235 direct receipt')
    h.validate_url(r.get('download_url',''),p)
    t=fin.moment(r.get('observed_at'));d=fin.moment(r.get('download_completed_at'))
    if not t or not d or not fin.period_end('ytd',p)<d.date() or not d<=t<=now:raise ValueError('invalid first-observed time')
    if type(r.get('archive_bytes')) is not int or not 0<r['archive_bytes']<=h.batch.MAX_ARCHIVE:raise ValueError('archive size outside bounds')
    keys(p,r.get('archive_sha256',''))
    if r.get('market_scope_verified') is not False or r.get('trade_authority_changed') is not False:raise ValueError('invalid receipt authority')
    return r

def _manifest(raw,p,now):
    if len(raw)>MANIFEST_LIMIT:raise ValueError('oversized manifest')
    m=ar._verify(json.loads(raw));r=_receipt(m.get('receipt'),p,now)
    t=fin.moment(m.get('published_at'))
    if m.get('version')!=VERSION or m.get('period')!=p or m.get('archive_key')!=keys(p,r['archive_sha256'])[0] or type(m.get('archive_generation')) is not int or m['archive_generation']<=0 or not t or not fin.moment(r['observed_at'])<=t<=now:raise ValueError('invalid cloud manifest identity/time')
    return m

class GCS:
    """Bucket adapter. Credentials only from explicit env JSON or Google ADC."""
    def __init__(self):
        bucket=os.environ.get('H236_GCS_BUCKET','').strip()
        if not re.fullmatch(r'[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]',bucket):raise ValueError('set H236_GCS_BUCKET to a dedicated private bucket')
        from google.cloud import storage
        from google.oauth2 import service_account
        raw=os.environ.get('H236_GCS_CREDENTIALS_JSON','')
        cred=service_account.Credentials.from_service_account_info(json.loads(raw),scopes=['https://www.googleapis.com/auth/devstorage.read_write']) if raw else None
        self.bucket=storage.Client(project=os.environ.get('H236_GCS_PROJECT') or None,credentials=cred).bucket(bucket)
    def head(self,key):
        b=self.bucket.get_blob(key,timeout=20,retry=None)
        return (int(b.generation),int(b.size)) if b is not None else None
    def get(self,key,generation,path,limit):
        # Pin object generation, verify metadata size BEFORE downloading.
        b=self.bucket.blob(key,generation=generation);b.reload(timeout=20,retry=None)
        if int(b.generation)!=generation or not 0<int(b.size)<=limit:raise ValueError('cloud object generation/size invalid')
        b.download_to_filename(str(path),if_generation_match=generation,timeout=60,retry=None,checksum='auto',raw_download=True)
        if path.stat().st_size!=int(b.size):raise ValueError('cloud object truncated')
    def put(self,key,path,content_type):
        from google.api_core.exceptions import PreconditionFailed
        b=self.bucket.blob(key)
        try:b.upload_from_filename(str(path),content_type=content_type,if_generation_match=0,timeout=90,checksum='auto')
        except PreconditionFailed:return None
        return int(b.generation)

def _get_manifest(store,p,now,tmp):
    k=manifest_key(p);head=store.head(k)
    if head is None:return None
    if not 0<head[1]<=MANIFEST_LIMIT:raise ValueError('oversized manifest')
    path=tmp/'cloud-manifest.json';store.get(k,head[0],path,MANIFEST_LIMIT)
    return _manifest(path.read_bytes(),p,now)

def _download(store,m,path):
    r=m['receipt'];key=m['archive_key'];g=m['archive_generation']
    if store.head(key)!=(g,r['archive_bytes']):raise ValueError('cloud archive generation/size mismatch')
    store.get(key,g,path,r['archive_bytes'])
    if path.stat().st_size!=r['archive_bytes'] or h.batch.file_sha(path)!=r['archive_sha256']:raise ValueError('cloud archive SHA256/size mismatch')

def _status(m,status):return {'version':VERSION,'period':fin.qstr(m['period']),'status':status,'archive_sha256':m['receipt']['archive_sha256'],'archive_bytes':m['receipt']['archive_bytes'],'observed_at':m['receipt']['observed_at'],'published_at':m['published_at'],'archive_generation':m['archive_generation'],'trade_authority_changed':False}

def publish(root,p,store=None,now=None):
    """Only trusted collector/operator. H235 re-verification required before upload.
    Publish immutable manifest LAST; orphan raw object can be resumed after full hash check.
    """
    fixed=now is not None;now=now or datetime.now(ar.TZ);root=Path(root)
    if now.tzinfo is None:raise ValueError('timezone required')
    z,rp=h.paths(root,p);_,r,_,_=h.read(root,p);_receipt(r,p,now)
    k,mk=keys(p,r['archive_sha256']);store=store or GCS()
    with tempfile.TemporaryDirectory(prefix='h236-publish-') as t:
        tmp=Path(t);existing=_get_manifest(store,p,now,tmp)
        if existing:
            if existing['receipt']!=r:raise ValueError('cloud period already committed with different evidence')
            if store.head(k)!=(existing['archive_generation'],r['archive_bytes']):raise ValueError('committed cloud archive missing/changed')
            return _status(existing,'ALREADY_PUBLISHED')
        # Snapshot and rehash to avoid reading an archive modified concurrently.
        local=tmp/'source.zip';shutil.copyfile(z,local)
        if local.stat().st_size!=r['archive_bytes'] or h.batch.file_sha(local)!=r['archive_sha256']:raise ValueError('local archive changed during publication')
        generation=store.put(k,local,'application/zip')
        if generation is None:
            head=store.head(k)
            if not head:raise ValueError('concurrent cloud creation unresolved')
            generation=head[0]
            _download(store,{'receipt':r,'archive_key':k,'archive_generation':generation},tmp/'orphan.zip')
        if store.head(k)!=(generation,r['archive_bytes']):raise ValueError('uploaded archive not verified by generation/size')
        completed=now if fixed else datetime.now(ar.TZ)
        m={'version':VERSION,'period':p,'receipt':r,'archive_key':k,'archive_generation':generation,'published_at':completed.isoformat()}
        mp=tmp/'commit.json';mp.write_bytes(ar._bytes(ar._seal(m)))
        if store.put(mk,mp,'application/json') is None:
            other=_get_manifest(store,p,now if fixed else datetime.now(ar.TZ),tmp)
            if not other or other['receipt']!=r or other['archive_generation']!=generation:raise ValueError('concurrent cloud manifest conflict')
            return _status(other,'ALREADY_PUBLISHED')
        return _status(m,'PUBLISHED')

def restore(root,p,store=None,now=None):
    """Read-only GCS. Download and verify raw ZIP before publishing local directory.
    Preserve original receipt time. Never replace an existing local directory.
    """
    root=Path(root);now=now or datetime.now(ar.TZ)
    if now.tzinfo is None:raise ValueError('timezone required')
    store=store or GCS();z,rp=h.paths(root,p)
    with _LOCK:
        # Staging outside data/h235 prevents readers from seeing incomplete archives.
        z.parent.parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='.h236-restore-',dir=z.parent.parent.parent) as t:
            tmp=Path(t);m=_get_manifest(store,p,now,tmp)
            if m is None:raise ValueError('cloud quarter has no committed manifest')
            r=m['receipt']
            if z.parent.exists():
                _,local,_,_=h.read(root,p)
                if local!=r:raise ValueError('local period differs; no overwrite')
                if store.head(m['archive_key'])!=(m['archive_generation'],r['archive_bytes']):raise ValueError('committed cloud archive missing/changed')
                return _status(m,'ALREADY_RESTORED')
            master=h.company.identity_map(root)
            if not master:raise ValueError('current stock master unavailable')
            stage=tmp/'period';stage.mkdir();raw=stage/'source.zip'
            _download(store,m,raw)
            h.scan(raw,p,fin.moment(r['observed_at']),master)
            (stage/'receipt.json').write_bytes(ar._bytes(ar._seal(r)))
            for path in (raw,stage/'receipt.json'):
                with path.open('rb') as f:os.fsync(f.fileno())
            # Exclusive directory claim; readers fail closed on incomplete publication.
            z.parent.mkdir(exist_ok=False)
            os.link(raw,z)
            os.link(stage/'receipt.json',rp)
            h.read(root,p,master)
            return _status(m,'RESTORED')

def cloud_status(root,p,store=None,now=None):
    """Small manifest/metadata query only; no raw ZIP download or market claim."""
    now=now or datetime.now(ar.TZ);store=store or GCS()
    with tempfile.TemporaryDirectory(prefix='h236-status-') as t:
        m=_get_manifest(store,p,now,Path(t))
        if not m:return {'version':VERSION,'period':fin.qstr(p),'status':'NOT_COMMITTED'}
        r=m['receipt'];state='CLOUD_MANIFEST_PRESENT' if store.head(m['archive_key'])==(m['archive_generation'],r['archive_bytes']) else 'CLOUD_ARCHIVE_MISSING_OR_CHANGED'
        return _status(m,state)


def restore_configured(root,store=None,now=None,periods=None,max_bytes=2*1024**3):
    """Opt-in runner hydration, max eight quarters and 2 GiB missing ZIPs per invocation.
    No silent partial success: all configured periods must have committed manifests.
    A failed invocation may leave earlier individually verified restores intact.
    """
    raw=periods if periods is not None else os.environ.get('H236_RESTORE_QUARTERS','')
    if not raw.strip():return []
    labels=[x.strip() for x in raw.split(',')];qs=[fin.quarter(x) for x in labels]
    if not 1<=len(qs)<=8 or None in qs or len(set(qs))!=len(qs):raise ValueError('1..8 unique configured quarters required')
    now=now or datetime.now(ar.TZ);store=store or GCS();root=Path(root)
    _,expected=fin.expected_periods(now)
    if any(not expected-11<=q<=expected for q in qs):raise ValueError('configured quarter outside 12-quarter research window')
    budget=0
    with tempfile.TemporaryDirectory(prefix='h236-plan-') as t:
        for q in qs:
            m=_get_manifest(store,q,now,Path(t))
            if not m:raise ValueError('configured cloud quarter not committed')
            r=m['receipt']
            if store.head(m['archive_key'])!=(m['archive_generation'],r['archive_bytes']):raise ValueError('configured cloud archive missing/changed')
            z,rp=h.paths(root,q)
            if z.parent.exists():
                _,local,_,_=h.read(root,q)
                if local!=r:raise ValueError('configured local period conflicts')
            else:budget+=r['archive_bytes']
        if budget>max_bytes:raise ValueError('configured restoration exceeds 2 GiB missing archive budget')
    return [restore(root,q,store,now) for q in qs]
