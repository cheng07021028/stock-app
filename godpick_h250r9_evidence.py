# -*- coding: utf-8 -*-
"""H250-R9: historical evidence identity, read-only presentation, no trade authority.
Publication date and original market remain immutable. A separate evidence mapping
is permitted only with both official market partitions and a unique anchor quote.
"""
from bisect import bisect_left
from datetime import datetime
import json, re, time
import pandas as pd
from godpick_h250r2_nday import TZ, PERIODS, COLS, PROOF, identity, base_price, months, positive, number, text, verified_value

MAP='H250R9_行情身分證據'
MAP_VERSION='H250R9_OFFICIAL_ANCHOR_V1'
EVIDENCE_COLUMNS=[MAP,'官方原價目前報酬%','官方原價行情日期','官方原價收盤價','H250R7_原價即時證據',
                  *['官方原價'+c for c in COLS],'H250R7_N日原價證據','H250R7_觸價證據','還原候選目前報酬%','H250R8_還原候選證據']
VISIBLE_COLUMNS=[c for c in EVIDENCE_COLUMNS if not c.startswith('H250')]

def decode(v):
    try:
        x=json.loads(text(v));return x if isinstance(x,dict) else {}
    except (TypeError,ValueError):return {}

def encode(x):return json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)

def resolve(row,a):
    """Return (effective row, mapping, diagnosis). No mutation or date invention.
    Both markets must have official validated partitions on the anchor session.
    On closed days use preceding session only when explicit recommendation price
    corroborates that close. Future/unfinished sessions are never invented.
    """
    ident=identity(row,a.cal.now)
    if not ident:return row,None,''
    rid,m,c,d=ident;anchor=d
    closed=d not in a.cal.dayset
    if closed:
        if d>a.latest:return row,None,'NOT_DUE' if d[:7] in a.cal.cal else 'CALENDAR_GAP'
        i=bisect_left(a.cal.days,d)
        if not i or d[:7] not in a.cal.cal:return row,None,'CALENDAR_GAP'
        anchor=a.cal.days[i-1]
        if any(x not in a.cal.cal for x in months(anchor,d)):return row,None,'CALENDAR_GAP'
    q=a.quotes.get((m,c),{}).get(anchor)
    other='TPEX' if m=='TWSE' else 'TWSE'
    alt=a.quotes.get((other,c),{}).get(anchor)
    partitions=all(anchor in a.markets.get(k,{}) for k in ('TWSE','TPEX'))
    if not partitions:
        # R8 original policy stays available for ordinary known-market rows.
        return row,None,'CALENDAR_GAP' if closed else ''
    if positive(q) and positive(alt):return row,None,'AMBIGUOUS_OFFICIAL_MARKET'
    actual=m if positive(q) else other if positive(alt) else None
    if not actual:return row,None,'CLOSED_ANCHOR_QUOTE_PENDING' if closed else ''
    close=positive(q if actual==m else alt)
    if not closed and actual==m:return row,None,''
    b=base_price(row)
    if closed and (b is None or abs(b-close)>max(.011,close*1e-5)):
        return row,None,'CLOSED_REFERENCE_PRICE_UNCORROBORATED'
    # A different market is a verified quote routing correction, not a mutation
    # of the historical record. Explicit signal price remains the return basis.
    out=dict(row,市場別=actual,推薦日期=anchor)
    # Never reuse an old unverified Page08 adjusted value on remapped identities.
    out['績效資料來源']=''
    mapping={'version':MAP_VERSION,'original':list(ident),'effective':[rid,actual,c,anchor],
             'anchor_close':close,'reference_price':b,'publication_closed':closed,
             'basis':'H190_BOTH_MARKETS_UNIQUE_ANCHOR_NOT_EXECUTION'}
    return out,mapping,''

def effective_identity(row,now):
    orig=identity(row,now);m=decode(row.get(MAP))
    if not orig:return None
    if not m:return orig
    try:
        e=tuple(m['effective']);a=tuple(m['original']);close=positive(m['anchor_close']);b=base_price(row)
        if (m['version']!=MAP_VERSION or a!=orig or len(e)!=4 or e[0]!=orig[0] or e[2]!=orig[2] or e[1] not in ('TWSE','TPEX')
            or not close or m['basis']!='H190_BOTH_MARKETS_UNIQUE_ANCHOR_NOT_EXECUTION' or m['reference_price']!=b):return None
        dt=datetime.fromisoformat(e[3]).date()
        if dt.isoformat()!=e[3] or e[3]>orig[3]:return None
        if bool(m['publication_closed'])!=(e[3]<orig[3]):return None
        if m['publication_closed'] and (b is None or abs(b-close)>max(.011,close*1e-5)):return None
        return e
    except (KeyError,TypeError,ValueError):return None

def saved_nday(row,n,now=None):
    """Validate stored proof identity + formula; this is not a fresh archive audit."""
    now=now or datetime.now(TZ)
    if text(row.get('資料來源')) in ('本輪推薦清單','最新推薦快照','godpick_recommend_list.json'):return None
    e=effective_identity(row,now)
    if not e:return None
    p=decode(row.get('H250R7_N日原價證據'))
    if p.get('basis')!='H190_RAW_UNADJUSTED_PRICE_RETURN_NOT_NET':return None
    try:
        if p['periods'][str(n)]['target']<=identity(row,now)[3]:return None
    except (KeyError,TypeError):return None
    x=dict(row,市場別=e[1],推薦日期=e[3]);x[PROOF]=row.get('H250R7_N日原價證據');x[f'推薦後{n}日%']=row.get(f'官方原價推薦後{n}日%')
    return verified_value(x,n,now)

def verified_frame(df,now=None):
    """Read-only official sample view; preserve original data/legacy statistics."""
    now=now or datetime.now(TZ);out=pd.DataFrame(index=df.index);rows=df.to_dict('records')
    for n,c in zip(PERIODS,COLS):out[c]=pd.Series([saved_nday(r,n,now) for r in rows],index=df.index,dtype=float)
    return out

def summary_frame(df):
    v=verified_frame(df);rows=[]
    for n,c in zip(PERIODS,COLS):
        s=v[c].dropna();rows.append({'期間':f'T+{n}','已保存核實樣本':len(s),'勝率%':round(float((s>0).mean()*100),2) if len(s) else None,'平均原價報酬%':round(float(s.mean()),2) if len(s) else None})
    return pd.DataFrame(rows)

def render_summary(st,df):
    st.caption('H250-R9｜官方原價 N 日證據統計（非含息／非成交；以已保存身分及公式證據核對，非即時重抓）。舊績效保留供比較，不作正式樣本。')
    st.dataframe(summary_frame(df),hide_index=True,use_container_width=True)

def dispatch_one(st,kind):
    """Only send one job to the existing serialized Worker; no local write."""
    from godpick_h250r6_monitor import API, credentials
    from godpick_scheduler_wakeup_service import dispatch_scheduler_wakeup,WORKFLOW_FILE
    if kind not in ('recommend_list_performance','recommend_list_n_day','recommend_list_hits'):raise ValueError('invalid tracking job')
    key='_r9_page10_dispatch';prev=st.session_state.get(key,{})
    if time.monotonic()-prev.get('at',-1000)<120:
        st.warning('剛已送出或受理不明；請先確認 Actions，勿重複派送。');return
    try:
        token,owner,repo=credentials(st);api=API(token,owner,repo)
        for status in ('queued','pending','in_progress','waiting','requested'):
            d,msg=api.get('/actions/workflows/'+WORKFLOW_FILE+'/runs',params={'status':status,'per_page':1})
            if d is None or not isinstance(d.get('workflow_runs'),list):st.warning('中央工作狀態未確認，暫停派送。');return
            if d['workflow_runs']:st.warning('中央 Worker 已有活動批次，請在17確認結果，避免重複派送。');return
        if str(st.secrets.get('GITHUB_CODE_BRANCH','main') or 'main')!='main':
            st.warning('正式追蹤僅允許 main 分支。');return
        receipt={};st.session_state[key]={'at':time.monotonic(),'job':kind}
        ok,msg=dispatch_scheduler_wakeup(token=token,owner=owner,repo=repo,branch=str(st.secrets.get('GITHUB_CODE_BRANCH','main') or 'main'),wakeup_source='page10_h250r9_tracking',manual_job=kind,receipt=receipt)
        st.session_state[key].update(ok=bool(ok),run_id=receipt.get('run_id',''),message=str(msg))
        if ok:st.info('已受理單項工作；並非已完成。請查閱第17頁中央工作結果。')
        else:st.warning('單項派送未確認；請查閱 GitHub Actions。')
    except Exception as exc:st.warning('中央派送未確認：'+type(exc).__name__)


def publication_dates(series):
    """Strict explicit date with optional timestamp; never parse clock-only as today."""
    parts=series.astype('string').str.extract(r'^\s*(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[ T].*)?\s*$')
    joined=parts[0]+'-'+parts[1].str.zfill(2)+'-'+parts[2].str.zfill(2)
    return pd.to_datetime(joined,format='%Y-%m-%d',errors='coerce')
