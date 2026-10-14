# -*- coding: utf-8 -*-
"""H119 immutable raw official source captures. Capture != numeric certification.

The TWSE result sheet exposes 權值+息值, which is NEVER automatically interpreted
as cash_dividend_per_share. TPEx schemas can change; unknown shapes are rejected.
Only a separately audited H118 coverage & H119 adjustment file authorizes returns.
"""
from __future__ import annotations
from datetime import datetime,timezone
from pathlib import Path
from typing import Any, Mapping
import gzip
import hashlib
import json
import re
from godpick_h118_performance_authority import _official_url

URLS={
 'TWSE':'https://www.twse.com.tw/exchangeReport/TWT49U?response=json',
 'TPEX':'https://www.tpex.org.tw/openapi/v1/tpex_exright_cal',
}
MAX_BYTES=5_000_000


def validate_official_payload(market:str,payload:Any)->dict[str,Any]:
    """Allow only documented table layouts. A successful HTTP status alone is insufficient."""
    if market=='TWSE':
        if not isinstance(payload,dict):raise ValueError('TWSE除權息資料不是JSON物件')
        if str(payload.get('stat') or '').upper()!='OK':raise ValueError('TWSE尚未完成公告或資料狀態非OK')
        fields=payload.get('fields')
        data=payload.get('data')
        if not isinstance(fields,list) or not isinstance(data,list) or not all(isinstance(x,list) and len(x)==len(fields) for x in data):
            raise ValueError('TWSE除權息欄位／列數格式不符合官方表單')
        if not {'股票代號','權/息'}.issubset({str(f).strip() for f in fields}):
            raise ValueError('TWSE欄位已改版，未能核對事件代號與性質')
        code_i=[str(x).strip() for x in fields].index('股票代號')
        for row in data:
            code=re.sub('<[^>]+>','',str(row[code_i])).strip()
            if not code:raise ValueError('TWSE官方表存在空股票代號')
        return {'rows':len(data),'status':'RAW_OFFICIAL_TABLE_NOT_NUMERIC_REVIEWED'}
    if market=='TPEX':
        if not isinstance(payload,list):raise ValueError('TPEx回傳非官方預期JSON陣列')
        if payload and not all(isinstance(x,dict) for x in payload):raise ValueError('TPEx非逐股列資料')
        if payload:
            keys=set(payload[0])
            if not ({'除權息日期','代號'}<=keys or {'ExDate','SecuritiesCode'}<=keys):
                raise ValueError('TPEx官方欄位未確認，禁止自動認證')
        else:
            # An empty current-day/next-session endpoint cannot prove whether
            # the requested market date was covered. Archive only; no coverage.
            return {'rows':0,'status':'RAW_EMPTY_NOT_COVERAGE_CERTIFIED'}
        return {'rows':len(payload),'status':'RAW_OFFICIAL_TABLE_NOT_NUMERIC_REVIEWED'}
    raise ValueError('未知交易所來源')


def capture_raw(market:str,body:bytes,*,retrieved_at:str,root:Path,url:str|None=None)->dict[str,Any]:
    if not 0 < len(body) < MAX_BYTES:raise ValueError('來源回應空白或超出尺寸上限')
    url=_official_url(url or URLS[market]);stamp=datetime.fromisoformat(retrieved_at.replace('Z','+00:00'))
    if stamp.tzinfo is None:raise ValueError('取得時間需有時區')
    day=stamp.date().isoformat();sha=hashlib.sha256(body).hexdigest()
    try: parsed=json.loads(body)
    except (ValueError,UnicodeDecodeError) as exc:raise ValueError('來源JSON格式無效') from exc
    validation=validate_official_payload(market,parsed)
    envelope={'version':'H119-RAW-1','market':market,'retrieved_at':stamp.isoformat(timespec='seconds'),
              'source_url':url,'source_sha256':sha,'source_body_utf8':body.decode('utf-8'),
              'validation':validation,'promotion':'REQUIRES_OFFICIAL_NUMERIC_REVIEW_AND_COMPLETE_CALENDAR'}
    blob=json.dumps(envelope,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
    target=Path(root)/day/f'{market}_{sha[:16]}.json.gz'
    target.parent.mkdir(parents=True,exist_ok=True)
    if target.exists():
        with gzip.open(target,'rb') as r:
            prior=json.loads(r.read())
        if prior.get('source_sha256')!=sha or hashlib.sha256(prior.get('source_body_utf8','').encode('utf-8')).hexdigest()!=sha:
            raise ValueError('官方原始快照SHA衝突，拒絕覆蓋')
        action='ALREADY_IDENTICAL'
    else:
        # no overwrite; atomic creation and readback
        compressed=gzip.compress(blob,mtime=0)
        try:
            with target.open('xb') as f:f.write(compressed)
        except FileExistsError:raise ValueError('並行來源擷取衝突，需重試')
        with gzip.open(target,'rb') as f:
            if f.read()!=blob:raise RuntimeError('原始公告快照寫後回讀失敗')
        action='CAPTURED_RAW_NOT_CERTIFIED'
    return {'status':action,'market':market,'rows':validation['rows'],
            'source_sha256':sha,'file':str(target),'durability':'LOCAL_UNTIL_GIT_COMMIT'}


def fetch_official_sources(root:Path,session:Any=None,timeout:int=15)->dict[str,Any]:
    import requests
    http=session or requests.Session()
    outcomes={};errors={}
    for market,url in URLS.items():
        try:
            r=http.get(url,headers={'User-Agent':'StockApp-H119/1.0 Official-Only'},timeout=timeout)
            if r.status_code!=200:raise RuntimeError(f'HTTP {r.status_code}')
            now=datetime.now(timezone.utc).isoformat(timespec='seconds')
            outcomes[market]=capture_raw(market,r.content,retrieved_at=now,root=root,url=url)
        except Exception as e:
            errors[market]=f'{type(e).__name__}: {str(e)[:180]}'
    return {'status':'RAW_CAPTURED_REVIEW_REQUIRED' if len(outcomes)==2 else 'PARTIAL_OR_FAILED_SOURCE_REVIEW_REQUIRED',
            'captures':outcomes,'errors':errors,
            'coverage_promoted':0,'return_components_promoted':0,
            'note':'來源JSON已記錄但不代表每日事件清單完整、逐股數值已證實；不得直接宣稱含息績效可驗證'}
