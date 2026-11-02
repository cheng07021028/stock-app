# -*- coding: utf-8 -*-
"""H150: reuse existing H83 news cache as conservative *research* evidence.

Never fetches during a stock report and never treats industry-wide headlines as
an individual stock catalyst. H83's scheduled/pre-recommendation refresh owns
network updates. Freshness/source are required; no trading authorization.
"""
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from datetime import timezone, timedelta
from typing import Any, Callable


def match_existing_news(row: dict[str, Any], research_day: date | None,
                        loader: Callable[..., list[dict]] | None = None) -> str:
    if not research_day or not isinstance(row, dict):
        return ''
    try:
        if loader is None:
            from godpick_h83_autofresh import get_news_context
            loader = get_news_context
        items = loader(row, max_items=8)
        if (not isinstance(items,list) or not items) and loader is not None:
            items = _auto_news_for_stock(row)
        if not isinstance(items, list):
            return ''
        for item in items:
            if not isinstance(item, dict) or item.get('match_type') != 'entity':
                continue
            title=str(item.get('title') or '').strip()
            link=str(item.get('link') or '').strip()
            if not title or not link:
                continue
            url=urlparse(link)
            if url.scheme != 'https' or not url.hostname or not url.hostname.endswith(('.com','.tw','.org','.net')):
                continue
            try:
                publication=parsedate_to_datetime(str(item.get('published_at') or ''))
                if publication.tzinfo is None:
                    continue
                day=publication.astimezone(timezone(timedelta(hours=8))).date()
            except (ValueError,TypeError,OverflowError):
                continue
            if research_day-timedelta(days=7) <= day <= research_day:
                return f'關聯新聞待核：{title[:55]}（{day.isoformat()}；H83來源連結可查，非交易因果證據）'
    except (ImportError,ValueError,TypeError,KeyError,AttributeError):
        pass
    return ''


def verified_remote_news_payload(payload: Any, *, now: datetime | None = None) -> list[dict]:
    """Validate H150 cache metadata, SHA, dates and bounds before publication."""
    from hashlib import sha256
    import json
    if not isinstance(payload,dict) or payload.get('schema')!='H150-public-news-1':
        return []
    items=payload.get('items')
    if not isinstance(items,list) or len(items)>80:
        return []
    canonical=json.dumps(items,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
    if sha256(canonical).hexdigest()!=payload.get('items_sha256'):
        return []
    try:
        timepoint=datetime.fromisoformat(str(payload['updated_at']).replace('Z','+00:00'))
        clock=now or datetime.now(timezone.utc)
        if timepoint.tzinfo is None or not timedelta(minutes=-2)<=clock-timepoint<=timedelta(days=3):
            return []
    except (TypeError,ValueError,OverflowError,KeyError):
        return []
    clean=[]
    for x in items:
        if not isinstance(x,dict):continue
        url=urlparse(str(x.get('link') or ''))
        if url.scheme!='https' or not url.hostname:continue
        if not x.get('title') or not x.get('published_at'):continue
        clean.append(x)
    return clean


@__import__('functools').lru_cache(maxsize=2)
def _remote_news(bucket: int) -> list[dict]:
    """Read remote runtime-data public-news cache once per 10 min/process.

    Never downloads headlines for each stock, and does not issue any write.
    """
    import base64
    import json
    try:
        import requests
        import streamlit as st
        token=str(st.secrets.get('GITHUB_TOKEN','') or '')
        owner=str(st.secrets.get('GITHUB_REPO_OWNER','cheng07021028'))
        repo=str(st.secrets.get('GITHUB_REPO_NAME','stock-app'))
        if not token or not all(z.replace('-','').replace('_','').replace('.','').isalnum() for z in (owner,repo)):
            return []
        uri=f'https://api.github.com/repos/{owner}/{repo}/contents/data/h150/H150_news.json'
        res=requests.get(uri,params={'ref':'runtime-data'},
                         headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json'},
                         timeout=(2,4),allow_redirects=False)
        if res.status_code!=200:return []
        obj=res.json()
        if not isinstance(obj,dict) or obj.get('encoding')!='base64':return []
        raw=base64.b64decode(obj.get('content',''),validate=True)
        if len(raw)>100_000:return []
        return verified_remote_news_payload(json.loads(raw))
    except (ValueError,TypeError,KeyError,AttributeError,OSError,requests.RequestException):
        return []


def _auto_news_for_stock(row: dict[str, Any]) -> list[dict]:
    import time
    code=str(row.get('股票代號') or '').strip()
    name=str(row.get('股票名稱') or '').strip()
    if len(code)<4 and len(name)<2:return []
    out=[]
    for x in _remote_news(int(time.time()//600)):
        title=str(x.get('title') or '')
        if (code and len(code)>=4 and code in title) or (name and len(name)>=2 and name in title):
            out.append(dict(x,match_type='entity'))
    return out[:8]
