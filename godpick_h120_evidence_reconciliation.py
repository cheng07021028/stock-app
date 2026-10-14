# -*- coding: utf-8 -*-
"""H120 manually attested OFFICIAL RAW->H118 coverage reconciliation.

H119 raw captures by themselves DO NOT prove date-scoped completeness.  This
module NEVER writes / auto-promotes coverage or numeric entitlements. H120
strict mode permits H118 coverage only when the recorded manual attestation,
original compressed bytes, exact ex-date rows and H118 catalog agree.
An audit trail is not cryptographic proof of an official source's completeness.
"""
from __future__ import annotations
from collections import defaultdict
from urllib.parse import urlparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
import gzip, hashlib, json, re
from godpick_h118_performance_authority import _iso_day, _official_url, MARKETS
from godpick_h119_official_source import validate_official_payload

VERSION='V191-H120.1-MANUAL_RAW_RECONCILIATION'
PROOF_COLUMNS=('market','date','raw_sha256','reviewer','reviewed_at','source_scope',
               'expected_stock_events','review_status','source_url')
MAX_RAW_BYTES=10_000_000


def _stamp(text:Any)->datetime:
    val=str(text or '').strip()
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})',val):
        raise ValueError('reviewed_at必須含時區及秒')
    result=datetime.fromisoformat(val.replace('Z','+00:00'))
    if result>datetime.now(timezone.utc).astimezone(result.tzinfo):
        raise ValueError('不能以未來時間簽署')
    return result


def _date_from_feed(value:Any)->str:
    raw=re.sub(r'\s+','',str(value or ''))
    m=re.fullmatch(r'(\d{2,4})(?:年|[-/])(\d{1,2})(?:月|[-/])(\d{1,2})日?',raw)
    if not m:raise ValueError(f'官方除權息日期格式不明：{raw[:25]}')
    year=int(m.group(1));year=year+1911 if year<1911 else year
    return _iso_day(f'{year:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}')


def _event_codes(market:str,payload:Any,day:str)->dict[str,set[str]]:
    """Map ex-date stocks to event classes, rejecting unknown shape/type.

    TWSE 權 does not distinguish free bonus shares from subscription rights;
    the catalog must include an independently reviewed EQUITY event and H119
    numerical evidence remains mandatory for entitlement valuation.
    """
    result=defaultdict(set)
    if market=='TWSE':
        fields=[str(f).strip() for f in payload['fields']]
        dt_key=next((x for x in ('資料日期','除權息日期') if x in fields),None)
        if dt_key is None or '權/息' not in fields:raise ValueError('TWSE raw缺少逐列日期或權息類別')
        di,ci,ti=fields.index(dt_key),fields.index('股票代號'),fields.index('權/息')
        for row in payload['data']:
            if _date_from_feed(row[di])!=day:continue
            code=re.sub(r'<[^>]+>','',str(row[ci])).strip()
            if not re.fullmatch(r'[1-9]\d{3}',code):continue
            kind=str(row[ti]).strip().replace(' ','')
            if kind in ('息','除息'):result[code].add('CASH_DIVIDEND')
            elif kind in ('權','除權'):result[code].add('EQUITY_UNSPECIFIED')
            elif kind in ('權息','除權息'):
                result[code].update(('CASH_DIVIDEND','EQUITY_UNSPECIFIED'))
            else:raise ValueError(f'TWSE未知權息類型：{kind[:24]}')
    elif market=='TPEX':
        for row in payload:
            dt_key=next((x for x in ('除權息日期','ExDate') if x in row),None)
            co_key=next((x for x in ('代號','SecuritiesCode') if x in row),None)
            if dt_key is None or co_key is None:raise ValueError('TPEx缺少逐筆除權息日期與代號')
            if _date_from_feed(row[dt_key])!=day:continue
            code=str(row[co_key]).strip()
            if not re.fullmatch(r'[1-9]\d{3}',code):continue
            # No silent guessing from combined rights-value/price gap fields.
            cash=str(row.get('現金股利') or '').strip().replace(',','')
            shares=str(row.get('每仟股無償配股') or '').strip().replace(',','')
            if not cash or not shares:raise ValueError('TPEx缺少實際現金股利或每仟股無償配股欄位')
            try:c=float(cash);v=float(shares)
            except ValueError:raise ValueError('TPEx實際股利欄位不是數字')
            if not (0<=c<1e6 and 0<=v<1e6):raise ValueError('TPEx股利超出核對範圍')
            if c>0:result[code].add('CASH_DIVIDEND')
            if v>0:result[code].add('STOCK_DIVIDEND')
            if not result[code]:raise ValueError('TPEx該股票其他資本事件未能確認類別')
    return dict(result)


def read_immutable_capture(raw_root:Path,market:str,digest:str)->dict[str,Any]:
    """Verify H119 envelope using raw-body digest AND original official schema."""
    if market not in MARKETS or not re.fullmatch(r'[0-9a-f]{64}',digest):
        raise ValueError('市場或SHA-256格式錯誤')
    candidates=list(Path(raw_root).glob(f'*/{market}_{digest[:16]}.json.gz'))
    if len(candidates)!=1:raise ValueError('找不到唯一的官方原始封存檔')
    if candidates[0].stat().st_size>MAX_RAW_BYTES:raise ValueError('官方封存檔太大')
    with gzip.open(candidates[0],'rb') as f:blob=f.read(MAX_RAW_BYTES+1)
    if len(blob)>MAX_RAW_BYTES:raise ValueError('官方解壓內容過大')
    obj=json.loads(blob)
    body=str(obj.get('source_body_utf8','')).encode('utf-8')
    if obj.get('market')!=market or obj.get('source_sha256')!=digest or hashlib.sha256(body).hexdigest()!=digest:
        raise ValueError('原始官方內容雜湊驗證失敗')
    url=_official_url(obj.get('source_url'))
    # Protect against accidentally treating unvalidated archives as valid official shapes.
    payload=json.loads(body)
    validate_official_payload(market,payload)
    return {'envelope':obj,'payload':payload,'source_url':url,'sha256':digest,
            'retrieved_at':_stamp(obj.get('retrieved_at')),
            'file':str(candidates[0])}


def reconcile_coverage(*,raw_root:Path,proofs:Iterable[Mapping[str,Any]],
                       coverage:Iterable[Mapping[str,Any]],
                       actions:Iterable[Mapping[str,Any]])->dict[str,Any]:
    """Reconcile only; accept coverage subset and explicitly enumerate blockers.

    The proof is human attestation, NOT an automatic declaration of official
    completeness. Absent/empty raw/date scoping is never interpreted as zero.
    """
    proofs=list(proofs)
    cov={(x['market'],x['date']):dict(x) for x in coverage}
    catalog=defaultdict(set)
    for a in actions:
        host=urlparse(a.get('source_url','')).hostname or ''
        source_market='TPEX' if host.endswith('tpex.org.tw') else ('TWSE' if host.endswith('twse.com.tw') else 'UNSCOPED')
        catalog[(source_market,a['ex_date'],a['code'])].add(a['action_type'])
    accepted=[];issues=[];seen=set();rejected_keys=set();captures=[]
    for item in proofs:
        market=str(item.get('market') or '').upper().strip()
        try:
            if market not in MARKETS:raise ValueError('市場不合法')
            day=_iso_day(item.get('date'))
            key=(market,day)
            if key in seen:raise ValueError('同一市場交易日重複人工簽署')
            seen.add(key)
            if key not in cov:raise ValueError('H118未有獨立的OFFICIAL_COMPLETE覆蓋紀錄')
            if str(item.get('review_status'))!='HUMAN_ATTESTED':raise ValueError('非人工簽署')
            if str(item.get('source_scope'))!='FULL_MARKET_EX_DATE':raise ValueError('未確認完整除權息日期範圍')
            reviewer=str(item.get('reviewer') or '').strip()
            if len(reviewer)<3 or len(reviewer)>100:raise ValueError('稽核者識別不足')
            stamp=_stamp(item.get('reviewed_at'))
            n=int(item.get('expected_stock_events','-1'))
            if n<0 or n>10000:raise ValueError('預期當日股票事件數不合法')
            cap=read_immutable_capture(raw_root,market,str(item.get('raw_sha256') or ''))
            if _official_url(item.get('source_url'))!=cap['source_url']:
                raise ValueError('簽署的官方來源網址與原始擷取不同')
            if stamp<cap['retrieved_at'] or stamp.date().isoformat()<day:
                raise ValueError('簽署時間早於來源取得或交易日')
            events=_event_codes(market,cap['payload'],day)
            if not events:
                # Current H119 archives lack a verifiable date-bound empty
                # market response. Do not certify zero-company-action days.
                raise ValueError('無日期明確逐股紀錄，不能憑空列表宣稱當日零事件')
            if len(events)!=n:raise ValueError('簽署事件總數與官方原始股票代碼數不一致')
            if any(date_==day and src=='UNSCOPED' for (src,date_,code) in catalog):
                raise ValueError('有當日事件來源市場不明，須先核對上市／上櫃歸屬')
            relevant={code for (src,date_,code),types in catalog.items() if src==market and date_==day}
            if set(events)!=relevant:
                raise ValueError('官方原始股票清單與H118事件目錄不一致（含漏登／多登）')
            for code,types in events.items():
                actual=catalog[(market,day,code)]
                needs_equity='EQUITY_UNSPECIFIED' in types
                explicit=types-{'EQUITY_UNSPECIFIED'}
                equity={'STOCK_DIVIDEND','RIGHTS_ISSUE'}
                if (actual-equity)!=explicit or (needs_equity and not actual.intersection(equity)):
                    raise ValueError(f'{code} 官方事件性質與H118類別不一致')
                if not needs_equity and types!=actual:
                    raise ValueError(f'{code} H118包含未在官方來源證實的其他事件')
            c=cov[key]
            # coverage's independently supplied source is retained; must match
            if _official_url(c['source_url'])!=cap['source_url']:
                raise ValueError('H118來源與簽署原始來源不一致')
            accepted.append(c)
            captures.append((market,day,cap['sha256']))
        except (ValueError,KeyError,TypeError,OSError,EOFError,json.JSONDecodeError) as exc:
            rejected_keys.add((market,str(item.get('date') or '')))
            issues.append({'market':market,'date':str(item.get('date') or ''),'status':'REJECTED',
                           'reason':str(exc)[:240]})
    for key in sorted(cov):
        if key not in seen:
            issues.append({'market':key[0],'date':key[1], 'status':'NOT_ATTESTED',
                           'reason':'缺少與官方原始快照連結的人工作業核對'})
    accepted=[x for x in accepted if (x['market'],x['date']) not in rejected_keys]
    captures.sort();accepted.sort(key=lambda x:(x['date'],x['market']))
    source={'proofs':[dict(p) for p in proofs],
            'accepted_raw_captures':captures,'coverage_rows':sorted(cov),
            'action_catalog':sorted((market,day,code,sorted(types)) for (market,day,code),types in catalog.items())}
    digest=hashlib.sha256(json.dumps(source,sort_keys=True,ensure_ascii=False,default=str).encode()).hexdigest()
    return {'version':VERSION,'status':('NO_ATTESTED_DAYS' if not accepted else ('PARTIAL_OR_UNVERIFIED' if issues else 'PROOFS_RECONCILED_NO_AUTOMATIC_OFFICIAL_CLAIM')),
            'attested_coverage':accepted,'accepted_days':len(accepted),'rejected_days':len(issues),
            'issues':issues,'evidence_fingerprint_sha256':digest,
            'autocertified':False,'certification_basis':'HUMAN_ATTESTED_RAW_AND_CATALOG_MATCH_ONLY',
            'note':'H120不自動核准原始公告為完整；空事件日、來源日期範圍缺證據皆保持待驗證。'}
