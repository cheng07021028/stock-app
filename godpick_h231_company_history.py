# -*- coding: utf-8 -*-
"""Bounded, individually dated official MOPS historical income statements.
Does not count as a full-market partition, does not infer dates from requests.
First observation only; no trading authority. Conservative parser pending live validation.
"""
from datetime import datetime, timedelta
from pathlib import Path
import hashlib, re, time
import godpick_h190_market_archive as ar
import godpick_h192_financial_growth as fin

VERSION='V191-H231-OFFICIAL-COMPANY-HISTORY'
URL='https://mopsov.twse.com.tw/mops/web/ajax_t164sb04'
ROOT=Path(__file__).resolve().parent
REL='data/h222/h231/company'
MAX_RAW=2_000_000
FIELDS={'revenue_ytd':('營業收入','營業收入合計','營業收入淨額'), 'gross_profit_ytd':('營業毛利(毛損)','營業毛利'), 'operating_profit_ytd':('營業利益(損失)','營業利益'), 'net_profit_ytd':('本期淨利(淨損)','本期淨利'), 'eps_ytd':('基本每股盈餘(元)','基本每股盈餘','基本每股盈餘合計')}

def identity_map(root):
    from godpick_h191_opportunity_engine import master_map
    return master_map(fin.read_json(Path(root)/'stock_master_cache.json',[],[]))

def _date_range(s,p):
    # Explicit start/end dates; no request year or positional guessing.
    y=p//4; end=fin.period_end('ytd',p)
    ys=f'(?:{y}|{y-1911})'
    return bool(re.fullmatch(rf'{ys}年0?1月0?1日(?:至|~|－|-){ys}年0?{end.month}月{end.day}日',s))

def _grid(table,norm):
    carry={}; out=[]
    for tr in table.find_all('tr'):
        if tr.find_parent('table') is not table:continue
        a={i:v for i,(v,n) in carry.items()};carry={i:(v,n-1) for i,(v,n) in carry.items() if n>1};i=0
        for c in tr.find_all(['td','th'],recursive=False):
            while i in a:i+=1
            w,h=int(c.get('colspan','1')),int(c.get('rowspan','1'))
            if not 1<=w<=40 or not 1<=h<=8:raise ValueError('invalid company table spans')
            v=norm(c.get_text(' ',strip=True))
            for j in range(i,i+w):
                if j in a:raise ValueError('overlapping company table spans')
                a[j]=v
                if h>1:carry[j]=(v,h-1)
            i+=w
        out.append([a.get(j,'') for j in range(max(a,default=-1)+1)])
    return out

def parse(html,code,p,observed,company_name=None):
    from godpick_h222_financial_history import soup_of,norm
    if not re.fullmatch(r'[1-9]\d{3}',code) or observed.tzinfo is None or fin.period_end('ytd',p)>=observed.date():raise ValueError('invalid company or unclosed period')
    if not isinstance(html,str) or len(html.encode())>MAX_RAW:raise ValueError('company HTML oversized')
    soup=soup_of(html); text=norm(soup.get_text(' ',strip=True))
    headings=' '.join(norm(t.get_text(' ',strip=True)) for t in soup.find_all(['title','h1','h2','h3','h4','caption','center','b','strong','div','p','td','th']) if 0<len(t.get_text(strip=True))<180)
    y=p//4;n=p%4+1;ys=f'(?:{y}|{y-1911})'
    # A report may give the exact issuer short name instead of a stock code.
    # The caller must establish unique current master identity before using it.
    code_match=re.search(rf'(?:公司代號|股票代號|證券代號)\s*[:：]?\s*{code}(?!\d)|(?<!\d){code}(?!\d)[^\d ]{{0,40}}合併綜合損益表',headings)
    fold=lambda x:norm(x).replace('臺','台')
    provider=re.findall(r'本資料由([^ ]{1,80}?)公司提供',headings.replace('臺','台'))
    name_match=bool(company_name and provider and all(x==fold(company_name) for x in provider))
    if not code_match and not name_match:raise ValueError('company identity not confirmed in response')
    if company_name and provider and not name_match:raise ValueError('company provider name conflict')
    if '合併綜合損益表' not in headings or not re.search(rf'{ys}年(?:度)?第?(?:0?{n}|{"一二三四"[n-1]})季',headings):raise ValueError('consolidated year/quarter title not confirmed')
    if not re.search(r'單位[:：]?(?:新台幣|新臺幣)(?:仟|千)元',text) or re.search(r'單位[:：]?(?:新台幣|新臺幣)?(?:美元|人民幣|港幣|百萬元|萬元)',text):raise ValueError('company TWD thousand unit not confirmed')
    # Wrong explicitly labeled company code is not permitted, even if requested code appears elsewhere.
    ids=re.findall(r'(?:公司代號|股票代號|證券代號)\s*[:：]?\s*([1-9]\d{3})(?!\d)',headings)
    if any(c!=code for c in ids):raise ValueError('company identity conflict')
    results=[]
    for table in soup.find_all('table'):
        rows=_grid(table,norm)
        for i,row in enumerate(rows):
            hits=[j for j,v in enumerate(row) if _date_range(v,p)]
            if not hits:continue
            # Typical report date colspan 2: explicitly choose amount, never percentage.
            if len(hits)>1:
                if i+1>=len(rows):continue
                hits=[j for j in hits if j<len(rows[i+1]) and rows[i+1][j] in ('金額','金額(仟元)','金額(千元)')]
            if len(hits)!=1:continue
            col=hits[0]; vals={};bad=False
            for rr in rows[i+1:]:
                if col>=len(rr):continue
                labels=[k for k,aliases in FIELDS.items() if any(v in aliases for v in rr[:col])]
                if len(labels)!=1:continue
                k=labels[0];v=fin.num(rr[col])
                if k in vals or v is None:bad=True;break
                vals[k]=v
            if not bad and set(vals)==set(FIELDS) and vals['revenue_ytd']>0:results.append({'code':code,**vals})
    if len(results)!=1:raise ValueError('unique explicit cumulative amount column / five fields not confirmed')
    return results[0]

def path_for(root,market,code,p):return Path(root)/REL/market/code/(fin.qstr(p)+'.json.gz')

def read(root,market,code,p,path=None):
    path=path or path_for(root,market,code,p);v=ar._read(path);t=fin.moment(v.get('observed_at'));html=v.get('html')
    if path!=path_for(root,market,code,p) or market not in ('TWSE','TPEX') or v.get('version')!=VERSION or v.get('market')!=market or v.get('code')!=code or v.get('period')!=p or v.get('url')!=URL or not t or not isinstance(html,str) or hashlib.sha256(html.encode()).hexdigest()!=v.get('raw_sha256'):raise ValueError('company partition identity/hash mismatch')
    if parse(html,code,p,t,v.get('company_name'))!=v.get('row'):raise ValueError('company partition raw/parsed mismatch')
    return v

def load(root):
    out={};errors=[];master=identity_map(root)
    for path in sorted((Path(root)/REL).glob('*/*/*.json.gz')):
        market,code=path.parent.parent.name,path.parent.name;p=fin.quarter(path.name.split('.')[0])
        try:
            if p is None or master.get(code,{}).get('市場')!=market:raise ValueError('current market identity or period invalid')
            v=read(root,market,code,p,path)
            if v.get('company_name') is not None and v['company_name']!=unique_name(master,code):raise ValueError('current issuer name identity changed or ambiguous')
            from godpick_h232_eps_comparability import extract
            basis=extract(v['html'],p,v['row']['eps_ytd'])
            out[market,code,p]=({**v['row'],'h232_eps_basis':basis},v['observed_at'],URL)
        except Exception as e:errors.append(f'H231 {market}/{code}/{path.name}: {str(e)[:150]}')
    return out,errors

def unique_name(master,code):
    name=master[code].get('股票名稱')
    fold=lambda x:str(x).strip().replace('臺','台')
    return name if name and name!=code and sum(fold(r.get('股票名稱'))==fold(name) for r in master.values())==1 else None

def collect(root=ROOT,*,codes,quarters=8,budget=8,now=None,http=None,target_period=None):
    from godpick_h222_financial_history import _persist
    fixed=now is not None;now=now or datetime.now(ar.TZ)
    if not 8<=quarters<=12 or not 1<=budget<=8 or now.tzinfo is None:raise ValueError('invalid company collection limits')
    codes=list(dict.fromkeys(codes));master=identity_map(root)
    if not codes or len(codes)>20 or any(c not in master for c in codes):raise ValueError('provide 1..20 unambiguous current master codes')
    if http is None:
        import requests
        http=requests.request
    hp=Path(root)/'data/h222/h231/health.json';attempts=ar._read(hp).get('attempts',{}) if hp.exists() else {};result=[];count=0;fails=0
    _,q=fin.expected_periods(now)
    if target_period is not None and (len(codes)!=1 or not isinstance(target_period,int) or not q-quarters<target_period<=q):raise ValueError('invalid explicit company target period')
    periods=(target_period,) if target_period is not None else range(q,q-quarters,-1)
    for code in codes:
        market=master[code]['市場'];name=unique_name(master,code)
        for p in periods:
            key=f'{market}:{code}:{p}';path=path_for(root,market,code,p);r={'市場':market,'股票代號':code,'期間':fin.qstr(p),'狀態':'PENDING'};result.append(r)
            if path.exists():
                try:read(root,market,code,p);r['狀態']='VERIFIED'
                except Exception as e:r.update(狀態='INVALID_EXISTING',原因=str(e)[:150])
                continue
            prev=fin.moment(attempts.get(key,{}).get('at'))
            if count>=budget or fails>=2 or (prev and now-prev<timedelta(hours=12)):continue
            count+=1
            try:
                if not fixed and count>1:time.sleep(2)
                rr=http('POST',URL,timeout=(5,20),allow_redirects=False,headers={'User-Agent':'StockApp-H231-public-research/1.0','Accept':'text/html'},data={'encodeURIComponent':'1','step':'1','firstin':'1','off':'1','isnew':'false','co_id':code,'year':str(p//4-1911),'season':f'{p%4+1:02d}'})
                if rr.status_code!=200 or rr.url.split('?')[0]!=URL or len(rr.content)>MAX_RAW:raise ValueError('unexpected company HTTP response')
                for enc in ('utf-8-sig','cp950','big5'):
                    try:html=rr.content.decode(enc);break
                    except UnicodeDecodeError:continue
                else:raise ValueError('unknown encoding')
                t=now if fixed else datetime.now(ar.TZ);row=parse(html,code,p,t,name)
                _persist(path,{'version':VERSION,'url':URL,'market':market,'code':code,'period':p,'company_name':name,'observed_at':t.isoformat(),'html':html,'raw_sha256':hashlib.sha256(html.encode()).hexdigest(),'row':row});read(root,market,code,p);r['狀態']='VERIFIED_SAVED'
            except Exception as e:fails+=1;r.update(狀態='REJECTED',原因=str(e)[:150])
            attempts[key]={'at':(now if fixed else datetime.now(ar.TZ)).isoformat(),'status':r['狀態'],'error':r.get('原因','')}
    body={'version':VERSION,'asof':(now if fixed else datetime.now(ar.TZ)).isoformat(),'requests':count,'scope':result,'attempts':attempts,'status':('COMPANY_TARGET_VERIFIED' if target_period is not None else 'COMPANY_SCOPE_VERIFIED') if all(r['狀態'] in ('VERIFIED','VERIFIED_SAVED') for r in result) else 'EVIDENCE_PENDING','market_scope_verified':False,'trade_authority_changed':False};ar._write(hp,ar._seal(body));return body
