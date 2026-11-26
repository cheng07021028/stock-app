# -*- coding: utf-8 -*-
"""H235 direct official ZIP ingestion. Local durable evidence, never GitHub auto-push.
No local user ZIP elevation, no historical publication-time inference.
"""
from collections import Counter
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
import hashlib, os, tempfile, zipfile, time
import godpick_h190_market_archive as ar
import godpick_h192_financial_growth as fin
import godpick_h234_batch_financial as batch
import godpick_h231_company_history as company

VERSION='V191-H235-OFFICIAL-BATCH'
PORTAL=batch.PORTAL
REL='data/h235'
HOSTS={'mopsov.twse.com.tw','mops.twse.com.tw'}
MIN_ROWS={'TWSE':500,'TPEX':350}

def validate_url(url,p):
    u=urlsplit(url); y=p//4; q=p%4+1
    if u.scheme!='https' or u.netloc not in HOSTS or u.path!='/mops/server-java/FileDownLoad' or u.fragment:raise ValueError('only exact HTTPS MOPS FileDownLoad endpoint is allowed')
    args=parse_qs(u.query,strict_parsing=True,keep_blank_values=True)
    if args!={'step':['9'],'fileName':[f'tifrs-{y}Q{q}.zip'],'filePath':[f'/home/html/nas/ifrs/{y}/']}:raise ValueError('official download query must match explicit requested quarter')
    return url

def paths(root,p):
    d=Path(root)/REL/fin.qstr(p)
    return d/'source.zip',d/'receipt.json'

def scan(path,p,observed,master):
    found={};blocked=set();rejected=Counter();stats=Counter()
    with zipfile.ZipFile(path) as z:
        for name,raw in batch.members(z,stats):
            try:
                row=batch.parse(raw,name,observed)
            except (ValueError,TypeError,OverflowError,batch.etree.LxmlError) as e:
                rejected[str(e)[:150]]+=1;continue
            if row['period']!=p:raise ValueError('archive contains a different report quarter')
            c=row['code'];market=master.get(c,{}).get('市場')
            if market not in MIN_ROWS:rejected['outside current listed/OTC master']+=1;continue
            k=(market,c,p)
            if k in blocked:continue
            if k in found and found[k]!=row:
                found.pop(k);blocked.add(k);rejected['conflicting issuer facts']+=1;continue
            found.setdefault(k,row)
    counts=Counter(k[0] for k in found)
    # At least one market must satisfy the existing H222 absolute minimum.
    # Never equate this threshold with complete market coverage.
    accepted={m for m,n in MIN_ROWS.items() if counts[m]>=n}
    if not accepted:raise ValueError(f'no market reaches minimum valid general-industry rows: {dict(counts)}')
    return {k:r for k,r in found.items() if k[0] in accepted},dict(counts),dict(rejected)

def master_key(master):return tuple(sorted((c,v['市場']) for c,v in master.items() if v.get('市場') in MIN_ROWS))
def stamp(path):
    s=path.stat();return (s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_ino)

@lru_cache(maxsize=12)
def _read_cached(zipname,receiptname,zstamp,rstamp,p,identities):
    z=Path(zipname);rp=Path(receiptname);r=ar._read(rp);t=fin.moment(r.get('observed_at'))
    if r.get('version')!=VERSION or r.get('period')!=p or r.get('transport')!='DIRECT_HTTPS_NO_REDIRECT' or not t or fin.period_end('ytd',p)>=t.date():raise ValueError('invalid H235 receipt identity/time')
    validate_url(r.get('download_url',''),p)
    if zstamp[0]>batch.MAX_ARCHIVE or zstamp[0]!=r.get('archive_bytes') or batch.file_sha(z)!=r.get('archive_sha256'):raise ValueError('raw archive hash/size mismatch')
    rows,counts,rejected=scan(z,p,t,{c:{'市場':m} for c,m in identities})
    if stamp(z)!=zstamp or stamp(rp)!=rstamp:raise ValueError('batch evidence changed during verification')
    # Stock master may change between runs; do not silently assert original coverage.
    # Recompute current master eligibility. Receipt records original acceptance only.
    return rows,r,counts,rejected

def read(root,p,master=None):
    z,r=paths(root,p);master=company.identity_map(root) if master is None else master
    if not master:raise ValueError('current stock master unavailable')
    return _read_cached(str(z.resolve()),str(r.resolve()),stamp(z),stamp(r),p,master_key(master))

def collect(root,p,url,*,http=None,now=None):
    """Exactly one direct streamed GET; no redirects/proxies configured by us, TLS verification on.
    The clock injection and HTTP injection are solely for tests. Never use a local ZIP as source.
    """
    validate_url(url,p);fixed=now is not None;now=now or datetime.now(ar.TZ)
    _,q=fin.expected_periods(now)
    if now.tzinfo is None or not q-11<=p<=q:raise ValueError('quarter outside 12-quarter research window')
    root=Path(root);master=company.identity_map(root)
    if not master:raise ValueError('current stock master unavailable')
    z,rp=paths(root,p)
    if rp.exists():
        rows,r,counts,rejected=read(root,p,master)
        return {**r,'status':'ALREADY_VERIFIED','current_counts':counts,'current_rejected':rejected}
    if z.exists():raise ValueError('orphan archive: preserve for manual investigation; no overwrite')
    if http is None:
        import requests
        http=requests.get
    z.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(dir=z.parent,prefix='.download-');tmp=Path(tmp)
    try:
        started=time.monotonic();size=0;h=hashlib.sha256()
        with os.fdopen(fd,'wb') as f:
            fd=-1
            with http(url,stream=True,timeout=(8,30),allow_redirects=False,verify=True,headers={'User-Agent':'StockApp-H235-public-research/1.0','Accept':'application/zip, application/octet-stream'}) as response:
                if response.status_code!=200 or response.url!=url or response.history:raise ValueError('unexpected download HTTP response or redirect')
                declared=response.headers.get('Content-Length')
                if declared is not None and not 0<int(declared)<=batch.MAX_ARCHIVE:raise ValueError('invalid declared archive size')
                for block in response.iter_content(1024*1024):
                    if not block:continue
                    size+=len(block)
                    if size>batch.MAX_ARCHIVE or time.monotonic()-started>600:raise ValueError('download byte/time budget exceeded')
                    h.update(block);f.write(block)
                if declared is not None and int(declared)!=size:raise ValueError('truncated download')
            f.flush();os.fsync(f.fileno())
        observed=now if fixed else datetime.now(ar.TZ)
        rows,counts,rejected=scan(tmp,p,observed,master)
        completed=now if fixed else datetime.now(ar.TZ)
        if batch.file_sha(tmp)!=h.hexdigest():raise ValueError('archive changed during validation')
        receipt={'version':VERSION,'period':p,'download_url':url,'transport':'DIRECT_HTTPS_NO_REDIRECT','observed_at':completed.isoformat(),'download_completed_at':observed.isoformat(),'archive_sha256':h.hexdigest(),'archive_bytes':size,'counts':counts,'accepted_counts':dict(Counter(k[0] for k in rows)),'rejected':rejected,'market_scope_verified':False,'trade_authority_changed':False}
        # Exclusive publication, keep orphan on interruption rather than overwriting.
        os.link(tmp,z)
        blob=ar._bytes(ar._seal(receipt));rf,rt=tempfile.mkstemp(dir=z.parent,prefix='.receipt-')
        try:
            with os.fdopen(rf,'wb') as f:f.write(blob);f.flush();os.fsync(f.fileno())
            os.link(rt,rp)
        finally:os.unlink(rt)
        read(root,p,master)
        return {**receipt,'status':'VERIFIED_SAVED'}
    finally:
        if fd>=0:os.close(fd)
        tmp.unlink(missing_ok=True)

def load(root):
    data={};errors=[];scope=[];master=company.identity_map(root)
    for folder in sorted((Path(root)/REL).glob('*')):
        if not folder.is_dir():continue
        p=fin.quarter(folder.name)
        try:
            if p is None:raise ValueError('invalid period directory')
            rows,r,counts,rejected=read(root,p,master)
            for k,row in rows.items():data[k]=({**row,'h235_archive_sha256':r['archive_sha256'],'h235_download_url':r['download_url']},r['observed_at'],PORTAL)
            scope.append({'季度':fin.qstr(p),'首次核實':r['observed_at'],'上市有效':sum(k[0]=='TWSE' for k in rows),'上櫃有效':sum(k[0]=='TPEX' for k in rows),'原始ZIP SHA256':r['archive_sha256'],'下載網址':r['download_url'],'正式市場達標':False})
        except Exception as e:errors.append(f'H235 {folder.name}: {type(e).__name__}: {str(e)[:160]}')
    return data,errors,scope

def historical_row(kind,p,row):
    if kind not in ('ytd','margin') or row.get('historical_source')!=VERSION or row.get('source_url')!=PORTAL:return False
    import re
    if not re.fullmatch('[0-9a-f]{64}',str(row.get('h235_archive_sha256',''))):return False
    try:validate_url(row.get('h235_download_url',''),p);return True
    except (ValueError,TypeError,AttributeError):return False
