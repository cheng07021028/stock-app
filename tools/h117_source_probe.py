# -*- coding: utf-8 -*-
"""Read-only, bounded diagnostics for existing H117 official collectors.

Logs only response status, schema column names, observed day, counts and classified
errors. Never prints payload rows, URLs with queries, credentials or stock prices.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TZ = ZoneInfo('Asia/Taipei')


def issue_code(exc: Exception) -> str:
    name = type(exc).__name__.upper()
    detail = str(exc)
    match = re.search(r'\bHTTP\s*(\d{3})\b', detail, re.I)
    if match:
        return 'HTTP_' + match.group(1)
    if 'TIMEOUT' in name or 'TIMED OUT' in detail.upper():
        return 'NETWORK_TIMEOUT'
    if 'SSL' in name or 'CERTIFICATE' in detail.upper():
        return 'TLS_ERROR'
    if 'CONNECTION' in name or 'PROXY' in name:
        return 'NETWORK_CONNECTION_ERROR'
    if 'JSON' in name or 'JSON' in detail.upper():
        return 'NON_JSON_RESPONSE'
    if re.search(r'有效行情僅\s*\d+\s*筆', detail):
        return 'NORMALIZED_ROWS_BELOW_MINIMUM'
    if '重複' in detail:
        return 'DUPLICATE_ROWS_OR_COLUMNS'
    if '交易日' in detail or '日期' in detail:
        return 'INVALID_MARKET_DATE'
    if '格式' in detail or '欄位' in detail:
        return 'UNRECOGNIZED_SCHEMA'
    return 'SOURCE_EXCEPTION_' + re.sub(r'[^A-Z0-9_]', '', name)[:36]


def safe_fields(first_row: object) -> list[str]:
    if not isinstance(first_row, dict):
        return []
    result=[]
    for key in list(first_row)[:24]:
        text=str(key)
        if len(text)>55 or any(not (c.isalnum() or c in ' _-/()%') for c in text):
            result.append('<UNSAFE_FIELD_NAME>')
        else:
            result.append(text)
    return result


def inspect_market(get, normalize, source_urls: dict, *, now: datetime):
    """Same three H117 URLs and parser, with a bounded read-only inspection."""
    now=now.astimezone(TZ)
    result={'schema':'H117-DIAG-V1','checked_at_tw':now.isoformat(timespec='seconds'),
            'mode':'READ_ONLY_NO_AUTH_NO_GITHUB_WRITE', 'sources':{},
            'status':'UNKNOWN', 'snapshot_is_not_proof_of_durable_archive':True}
    if now.time()<time(18,15):
        result['status']='WAIT_UNTIL_TW_18_15'
        result['note']='H117本身禁止18:15前宣稱當日官方EOD已完成。'
        return result
    dates=[]
    for name,url in source_urls.items():
        info={'status':'UNKNOWN','http_status':None,'rows_returned':None,'first_row_fields':[],
              'normalized_rows':None,'observed_market_day':None}
        try:
            response=get(url,headers={'Accept':'application/json','User-Agent':'StockApp-H117-research/1.0'},timeout=25)
            info['http_status']=int(response.status_code)
            if response.status_code!=200:
                info['status']='HTTP_FAILED'
                info['issue_code']='HTTP_'+str(response.status_code)
            else:
                if len(response.content)>16_000_000:
                    info['status']='RESPONSE_TOO_LARGE'
                    info['issue_code']='BODY_GT_16MB'
                else:
                    try:
                        rows=response.json()
                    except Exception as exc:
                        info['status']='JSON_PARSE_FAILED'
                        info['issue_code']=issue_code(exc)
                    else:
                        if isinstance(rows,list):
                            info['rows_returned']=len(rows)
                            info['first_row_fields']=safe_fields(rows[0]) if rows else []
                        else:
                            info['status']='UNEXPECTED_TOP_LEVEL_TYPE'
                            info['issue_code']='EXPECTED_JSON_ARRAY'
                        if isinstance(rows,list):
                            try:
                                normalized=normalize(name,rows)
                                info['status']='SCHEMA_AND_COUNTS_OK'
                                info['normalized_rows']=len(normalized['rows'])
                                info['observed_market_day']=normalized['day']
                                dates.append(normalized['day'])
                            except Exception as exc:
                                info['status']='NORMALIZATION_FAILED'
                                info['issue_code']=issue_code(exc)
        except Exception as exc:
            info['status']='REQUEST_FAILED'
            info['issue_code']=issue_code(exc)
        result['sources'][name]=info
    passed=all(v['status']=='SCHEMA_AND_COUNTS_OK' for v in result['sources'].values())
    result['status']='THREE_SOURCES_SCHEMA_OK' if passed else 'SOURCES_NOT_READY'
    if passed:
        result['status']='SAME_MARKET_DAY_OBSERVED' if len(set(dates))==1 else 'MARKET_DAY_MISMATCH'
    result['note']='只讀來源與欄位診斷。即使合格，仍須正式H117封存、GitHub提交與回讀驗證。'
    return result


def main() -> int:
    import requests
    from godpick_h117_official_market_closure import MARKET_URLS, normalize_official
    result=inspect_market(requests.get,normalize_official,MARKET_URLS,now=datetime.now(TZ))
    print(json.dumps(result,ensure_ascii=False,indent=2))
    Path('h117_source_diag.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    summary=Path(__import__('os').environ.get('GITHUB_STEP_SUMMARY',''))
    if str(summary)!='.':
        body=['# H117 官方來源只讀診斷', '',f"**狀態：{result['status']}**", '', '| 來源 | HTTP | 回傳筆數 | 驗證狀態 | 官方交易日 | 錯誤分類 |','|---|---:|---:|---|---|---|']
        for src,x in result['sources'].items():
            body.append(f"| {src} | {x.get('http_status') or '-'} | {x.get('rows_returned') if x.get('rows_returned') is not None else '-'} | {x['status']} | {x.get('observed_market_day') or '-'} | {x.get('issue_code','-')} |")
        body.append('\n只有來源驗證，不代表GitHub已封存。')
        try:
            with summary.open('a',encoding='utf-8') as f:f.write('\n'.join(body)+'\n')
        except OSError:pass
    return 0 if result['status'] in ('SAME_MARKET_DAY_OBSERVED','WAIT_UNTIL_TW_18_15') else 2

if __name__=='__main__':
    raise SystemExit(main())
