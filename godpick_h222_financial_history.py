# -*- coding: utf-8 -*-
"""H222 official historical statements. First observation, not historical PIT.
Separate immutable period partitions; never rewrite H192 or trading ledgers.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import gzip, hashlib, json, os, re, tempfile, unicodedata
import godpick_h190_market_archive as ar
import godpick_h192_financial_growth as fin

VERSION='V191-H222-OFFICIAL-FINANCIAL-HISTORY'
ROOT=Path(__file__).resolve().parent
HOST='https://mopsov.twse.com.tw'
QUARTER_URL=HOST+'/mops/web/ajax_t163sb04'
MARKETS={'TWSE':('sii','上市'),'TPEX':('otc','上櫃')}
LIMITS={'TWSE':500,'TPEX':350}
MAX_RAW=8_000_000
SCHEMA_REVISION="H230"
GROSS_PRIMARY=("營業毛利(毛損)","營業毛利")
GROSS_NET=("營業毛利(毛損)淨額","營業毛利淨額")


def source_url(kind,market,p):
    if kind=='revenue':
        y,m=divmod(p,12);return f'{HOST}/nas/t21/{MARKETS[market][0]}/t21sc03_{y-1911}_{m+1}_0.html'
    if kind in ('ytd','margin'):return QUARTER_URL
    raise ValueError('unsupported history kind')


def norm(s):return re.sub(r'\s+','',unicodedata.normalize('NFKC',str(s)))


def soup_of(html):
    from bs4 import BeautifulSoup
    if not isinstance(html,str) or len(html.encode('utf-8'))>MAX_RAW:raise ValueError('invalid or oversized HTML')
    return BeautifulSoup(html,'lxml')


def _columns(kind):
    if kind=='revenue':return {'code':('公司代號',),'revenue_twd':('當月營收',),'previous_month_twd':('上月營收',),'prior_year_month_twd':('去年當月營收',)}
    return {'code':('公司代號',),'revenue_ytd':('營業收入','營業收入合計'),'gross_profit_ytd':('營業毛利(毛損)','營業毛利(毛損)淨額','營業毛利','營業毛利淨額'),'operating_profit_ytd':('營業利益(損失)','營業利益'),'net_profit_ytd':('本期淨利(淨損)','本期淨利'),'eps_ytd':('基本每股盈餘(元)','基本每股盈餘')}


def _rows(soup,kind):
    """Only explicit named columns in rectangular data tables, not positional guesses.
    Skip non-general quarterly schemas (e.g. banks) and reject duplicate securities.
    """
    out=[]; cols=_columns(kind)
    for table in soup.find_all('table'):
        trs=[tr for tr in table.find_all('tr') if tr.find_parent('table') is table]
        ix=None; carry={}
        for tr in trs:
            cells=tr.find_all(['th','td'],recursive=False)
            # Expand multi-row report headers without guessing column positions.
            active={i:v for i,(v,n) in carry.items()};carry={i:(v,n-1) for i,(v,n) in carry.items() if n>1};i=0
            for cell in cells:
                while i in active:i+=1
                v=norm(cell.get_text(' ',strip=True));w=int(cell.get('colspan','1'));h=int(cell.get('rowspan','1'))
                if not 1<=w<=100 or not 1<=h<=10:raise ValueError('invalid table spans')
                for j in range(i,i+w):
                    if j in active:raise ValueError('overlapping table spans')
                    active[j]=v
                    if h>1:carry[j]=(v,h-1)
                i+=w
            labels=[active.get(j,'') for j in range(max(active,default=-1)+1)]
            hits={k:[i for i,x in enumerate(labels) if x in aliases] for k,aliases in cols.items()}
            if kind=='ytd':
                # Gross and gross-net are distinct official fields, not aliases.
                primary=[i for i,x in enumerate(labels) if x in GROSS_PRIMARY]
                net=[i for i,x in enumerate(labels) if x in GROSS_NET]
                hits['gross_profit_ytd']=primary if len(primary)==1 and len(net)<=1 else []
            if all(len(x)==1 for x in hits.values()):ix={k:x[0] for k,x in hits.items()};width=len(labels);continue
            if ix is None or len(labels)!=width:continue
            code=labels[ix['code']]
            if not re.fullmatch(r'[1-9]\d{3}',code):continue
            vals={k:fin.num(labels[i]) for k,i in ix.items() if k!='code'}
            if kind=='revenue':
                if vals['revenue_twd'] is None or vals['revenue_twd']<0:continue
                vals={k:int(round(v*1000)) if v is not None and v>=0 else None for k,v in vals.items()}
            elif any(v is None for v in vals.values()) or vals['revenue_ytd']<=0:continue
            out.append({'code':code,**vals})
    counts=Counter(r['code'] for r in out)
    if any(n>1 for n in counts.values()):raise ValueError('duplicate company rows')
    return out


def structured_rows(market,p,raw,observed):
    """Official general-industry current response, not historical request parameters.
    Only rows explicitly dated to the requested quarter are eligible.
    Exact named gross (not gross-net) and other five-field comparisons.
    """
    if not isinstance(raw,list) or len(raw)>5000:raise ValueError('invalid structured response')
    out=[]
    for r in raw:
        if not isinstance(r,dict):continue
        keys=[norm(k) for k in r]
        if len(keys)!=len(set(keys)):continue
        r=dict(zip(keys,r.values()))
        def one(names):
            vals=[r[k] for k in names if k in r]
            return vals[0] if len(vals)==1 else None
        code=str(one(('公司代號','SecuritiesCompanyCode','CompanyCode')) or '').strip()
        year=one(('年度','Year'));q=one(('季別','Season','Quarter'))
        period=fin.quarter(f'{year}Q{str(q).upper().removeprefix("Q")}')
        report=fin.day(one(('出表日期','Date')))
        if not re.fullmatch(r'[1-9]\d{3}',code) or period!=p or not report or report>observed.date().isoformat() or report<fin.period_end('ytd',p).isoformat():continue
        aliases={**_columns('ytd'),'gross_profit_ytd':GROSS_PRIMARY}
        vals={k:fin.num(one(names)) for k,names in aliases.items() if k!='code'}
        if any(v is None for v in vals.values()) or vals['revenue_ytd']<=0:continue
        out.append({'code':code,**vals})
    counts=Counter(r['code'] for r in out)
    return {r['code']:r for r in out if counts[r['code']]==1}


def anchored_rows(market,p,html,observed,anchor):
    """A title with only market/quarter is insufficient on its own. Bind every
    published row to a same-market, explicitly dated official structured row.
    No extrapolation from a sample of companies, no source-unit conversion guess.
    """
    if not isinstance(anchor,dict) or anchor.get('url')!=fin.URLS['ytd',market]:raise ValueError('official anchor source mismatch')
    at=fin.moment(anchor.get('observed_at'))
    if not at or at>observed or fin.period_end('ytd',p)>=at.date():raise ValueError('anchor observation invalid')
    raw=anchor.get('raw')
    if not isinstance(raw,str) or len(raw.encode('utf-8'))>MAX_RAW or hashlib.sha256(raw.encode('utf-8')).hexdigest()!=anchor.get('sha256'):raise ValueError('anchor hash invalid')
    soup=soup_of(html);text=norm(soup.get_text(' ',strip=True));label=MARKETS[market][1];other='上櫃' if market=='TWSE' else '上市'
    n=p%4+1;cn='一二三四'[n-1]
    headings=' '.join(norm(t.get_text(' ',strip=True)) for t in soup.find_all(['title','h1','h2','h3','h4','caption','center','b','strong','div','p']) if 0<len(t.get_text(strip=True))<220)
    if not re.search(rf'{label}公司(?:第)?(?:0?{n}|{cn})季資料',headings) or re.search(rf'{other}公司(?:第)?(?:[一二三四]|0?[1-4])季資料',headings):raise ValueError('quarter anchor market/title mismatch')
    if re.search(r'單季|當季金額',headings) or re.search(r'單位[:：]?(?:新臺幣|新台幣)?(?:美元|人民幣|港幣|百萬元|萬元|元(?!/股))',text):raise ValueError('quarter explicit basis/unit conflict')
    periods=re.findall(r'(\d{3,4})年(?:度)?第?([一二三四]|0?[1-4])季',headings)
    if any(int(y) not in (p//4,p//4-1911) or ('一二三四'.index(q)+1 if q in '一二三四' else int(q))!=n for y,q in periods):raise ValueError('quarter explicit period conflict')
    refs=structured_rows(market,p,json.loads(raw),at)
    rows=_rows(soup,'ytd')
    # Compare every admitted row. An unmatched row stays outside the verified set.
    matches=[r for r in rows if refs.get(r['code'])==r]
    if len(matches)<LIMITS[market] or len(matches)*100<len(rows)*95 or len(matches)*100<len(refs)*95:raise ValueError(f'quarter anchor coverage insufficient: {len(matches)}/{len(rows)} HTML, {len(refs)} official')
    return matches


def reparse_partition(v,kind,market,p):
    observed=fin.moment(v['observed_at'])
    if v.get('anchor') is not None:
        if kind!='ytd':raise ValueError('unexpected anchor')
        return anchored_rows(market,p,v['html'],observed,v['anchor'])
    return parse(kind,market,p,v['html'],observed)


def parse(kind,market,p,html,observed):
    if market not in MARKETS or kind not in ('revenue','ytd'):raise ValueError('invalid source')
    if observed.tzinfo is None or fin.period_end(kind,p)>=observed.date():raise ValueError('future/unclosed statement')
    soup=soup_of(html); text=norm(soup.get_text(' ',strip=True)); y=p//(12 if kind=='revenue' else 4);n=p%(12 if kind=='revenue' else 4)+1
    label=MARKETS[market][1]; yrs=f'(?:{y}|{y-1911})'
    # Require a visible market/period title, and explicit units. A request echo is not proof.
    if kind=='revenue':
        pattern=rf'{label}公司{yrs}年0?{n}月(?:份)?\(?累計與當月\)?營業收入統計表'
        if not re.search(pattern,text) or not re.search(r'單位[:：]?(?:新臺幣|新台幣)?千元',text):raise ValueError('monthly title/units not confirmed')
    else:
        # Quarter historical POST response format must confirm these independently.
        # An unlabelled single-quarter table must never be treated as YTD.
        headings=' '.join(norm(t.get_text(' ',strip=True)) for t in soup.find_all(['title','h1','h2','h3','h4','caption','center','b','strong','div','p']) if len(t.get_text(strip=True))<220)
        if not re.search(rf'{yrs}年(?:度)?第?(?:0?{n}|{"一二三四"[n-1]})季',headings) or not re.search(rf'{label}公司',headings) or '綜合損益' not in headings or '累計' not in headings or not re.search(r'單位[:：]?(?:新臺幣|新台幣)?千元',text):raise ValueError('quarter market/period/YTD/units not confirmed')
        if re.search(r'單位[:：]?(?:美元|人民幣|港幣|百萬元|萬元)',text) or re.search(r'單季|當季金額',headings):raise ValueError('mixed currency units or statement basis')
    rows=_rows(soup,kind)
    if not rows:raise ValueError('no verified named financial rows')
    return rows


def partition(root,kind,market,p):
    key=fin.monthstr(p) if kind=='revenue' else fin.qstr(p)
    return Path(root)/'data/h222'/kind/market/(key+'.json.gz')


def _persist(path,body):
    # Exclusive atomic publication: never replace a previous observation.
    path.parent.mkdir(parents=True,exist_ok=True);fd,tmp=tempfile.mkstemp(dir=path.parent,prefix='.h222-')
    try:
        blob=ar._bytes(ar._seal(body))
        if len(blob)>16_000_000:raise ValueError('partition expanded limit')
        packed=gzip.compress(blob,mtime=0)
        if len(packed)>8_000_000:raise ValueError('partition compressed limit')
        with os.fdopen(fd,'wb') as f:fd=-1;f.write(packed);f.flush();os.fsync(f.fileno())
        os.link(tmp,path)
    finally:
        if fd>=0:os.close(fd)
        if os.path.exists(tmp):os.unlink(tmp)


def read_partition(path,kind,market,p):
    v=ar._read(path);t=fin.moment(v.get('observed_at'))
    if v.get('version')!=VERSION or v.get('kind')!=kind or v.get('market')!=market or v.get('period')!=p or v.get('url')!=source_url(kind,market,p) or not t or fin.period_end(kind,p)>=t.date():raise ValueError('partition identity mismatch')
    html=v.get('html');rows=v.get('rows')
    if not isinstance(html,str) or hashlib.sha256(html.encode('utf-8')).hexdigest()!=v.get('raw_sha256') or not isinstance(rows,list) or len(rows)<LIMITS[market] or len({r['code'] for r in rows})!=len(rows):raise ValueError('partition evidence invalid')
    if kind=='ytd' and reparse_partition(v,kind,market,p)!=rows:raise ValueError('quarter raw/parsed rows mismatch')
    return v


def targets(now,months=13,quarters=8):
    if not 13<=months<=36 or not 8<=quarters<=12:raise ValueError('history limits: months 13..36; quarters 8..12')
    m,q=fin.expected_periods(now)
    # Round-robin newest periods and markets, avoid starving quarters.
    for i in range(max(months,quarters)):
        for kind,end,count in [('revenue',m,months),('ytd',q,quarters)]:
            if i<count:
                for market in MARKETS:yield kind,market,end-i


def response_diagnostic(html):
    """Bounded header-only evidence for official schema failures; never publish rejected rows."""
    soup=soup_of(html)
    titles=[norm(t.get_text(' ',strip=True))[:200] for t in soup.find_all(['title','h1','h2','h3','h4','caption','center','b','strong']) if 0<len(t.get_text(strip=True))<220][:12]
    headers=[]
    for table in soup.find_all('table')[:12]:
        for tr in table.find_all('tr')[:3]:
            cells=tr.find_all('th',recursive=False)
            if cells:headers.append([norm(c.get_text(' ',strip=True))[:80] for c in cells[:30]])
    return {'titles':titles,'headers':headers[:16],'table_count':len(soup.find_all('table')),'raw_sha256':hashlib.sha256(html.encode('utf-8')).hexdigest()}


def collect(root=ROOT,*,now=None,months=13,quarters=8,budget=8,http=None):
    import time
    fixed=now is not None;now=now or datetime.now(ar.TZ)
    if now.tzinfo is None or not 1<=budget<=12:raise ValueError('invalid time/budget')
    if http is None:
        import requests
        http=requests.request
    root=Path(root);healthpath=root/'data/h222/health.json'; old={}
    if healthpath.exists():old=ar._read(healthpath).get('attempts',{})
    attempts=dict(old); report=[];requests_count=0;failures=Counter();scope=list(targets(now,months,quarters))
    for kind,market,p in scope:
        path=partition(root,kind,market,p);key=f'{kind}:{market}:{p}';row={'kind':kind,'market':market,'period':fin.monthstr(p) if kind=='revenue' else fin.qstr(p)};report.append(row)
        if path.exists():
            try:v=read_partition(path,kind,market,p);row.update(status='VERIFIED',rows=len(v['rows']),verification='DUAL_OFFICIAL_ROW_MATCH' if v.get('anchor') is not None else 'EXPLICIT_REPORT_METADATA')
            except Exception as e:row.update(status='INVALID_EXISTING',error=str(e)[:200])
            continue
        prev=fin.moment(attempts.get(key,{}).get('at'))
        schema_retry=(kind=='ytd' and attempts.get(key,{}).get('error')=='quarter market/period/YTD/units not confirmed' and attempts.get(key,{}).get('schema_revision')!=SCHEMA_REVISION)
        if requests_count>=budget or failures[kind,market]>=2 or (prev and now-prev<timedelta(hours=12) and not schema_retry):
            row['status']='PENDING';continue
        url=source_url(kind,market,p);requests_count+=1;html=None
        try:
            if requests_count>1 and not fixed:time.sleep(1)
            kwargs={'timeout':(5,20),'allow_redirects':False,'headers':{'User-Agent':'StockApp-H222-public-research/1.0','Accept':'text/html'}}
            if kind=='ytd':kwargs['data']={'encodeURIComponent':'1','step':'1','firstin':'1','off':'1','TYPEK':MARKETS[market][0],'year':str(p//4-1911),'season':f'{p%4+1:02d}'}
            response=http('GET' if kind=='revenue' else 'POST',url,**kwargs)
            if response.status_code!=200 or response.url.split('?')[0]!=url:raise ValueError('unexpected HTTP or redirect')
            raw=response.content
            if len(raw)>MAX_RAW:raise ValueError('oversized source')
            for enc in ('utf-8-sig','cp950','big5'):
                try:html=raw.decode(enc);break
                except UnicodeDecodeError:continue
            else:raise ValueError('source encoding unknown')
            observed=now if fixed else datetime.now(ar.TZ);anchor=None
            try:rows=parse(kind,market,p,html,observed)
            except ValueError as strict_error:
                # Current structured API does not provide arbitrary historical periods.
                # Only attempt the latest expected quarter and never exceed request budget.
                if kind!='ytd' or p!=fin.expected_periods(now)[1] or str(strict_error)!='quarter market/period/YTD/units not confirmed' or requests_count>=budget:raise
                anchor_url=fin.URLS['ytd',market];requests_count+=1
                if not fixed:time.sleep(1)
                rr=http('GET',anchor_url,timeout=(5,20),allow_redirects=False,headers={'User-Agent':'StockApp-H230-public-research/1.0','Accept':'application/json'})
                if rr.status_code!=200 or rr.url.split('?')[0]!=anchor_url or len(rr.content)>MAX_RAW:raise ValueError('official structured anchor response invalid')
                anchor_raw=rr.content.decode('utf-8-sig');observed=now if fixed else datetime.now(ar.TZ)
                anchor={'url':anchor_url,'observed_at':observed.isoformat(),'raw':anchor_raw,'sha256':hashlib.sha256(anchor_raw.encode('utf-8')).hexdigest()}
                rows=anchored_rows(market,p,html,observed,anchor)
            if len(rows)<LIMITS[market]:raise ValueError(f'LOW_COVERAGE_{len(rows)}')
            body={'version':VERSION,'kind':kind,'market':market,'period':p,'url':url,'observed_at':observed.isoformat(),'raw_sha256':hashlib.sha256(html.encode('utf-8')).hexdigest(),'html':html,'rows':rows}
            if anchor is not None:body['anchor']=anchor
            _persist(path,body);read_partition(path,kind,market,p);row.update(status='VERIFIED_SAVED',rows=len(rows),verification='DUAL_OFFICIAL_ROW_MATCH' if anchor is not None else 'EXPLICIT_REPORT_METADATA')
        except Exception as e:
            failures[kind,market]+=1;row.update(status='REJECTED',error=str(e)[:200])
            if html is not None:
                try:row['diagnostic']=response_diagnostic(html)
                except Exception:pass
        attempts[key]={'at':(now if fixed else datetime.now(ar.TZ)).isoformat(),'status':row['status'],'error':row.get('error',''),'schema_revision':SCHEMA_REVISION}
    result={'version':VERSION,'asof':(now if fixed else datetime.now(ar.TZ)).isoformat(),'requests':requests_count,'scope':report,'attempts':attempts,'status':'VERIFIED_SCOPE' if all(x['status'] in ('VERIFIED','VERIFIED_SAVED') for x in report) else 'EVIDENCE_PENDING','trade_authority_changed':False}
    ar._write(healthpath,ar._seal(result));return result


def _base(kind,market,p,code,t):
    return {'code':code,'source':market,'source_url':source_url(kind,market,p),'availability_basis':'FIRST_OBSERVED','available_at':t,'historical_source':VERSION,('year_month' if kind=='revenue' else 'year_quarter'):(fin.monthstr(p) if kind=='revenue' else fin.qstr(p))}


def historical_row(kind,market,p,row):
    """Explicit allowlist; source URL must match the exact official partition."""
    if row.get('availability_basis')!='FIRST_OBSERVED':return False
    if kind in ('margin','ytd'):
        from godpick_h235_official_batch import historical_row as batch_row
        if batch_row(kind,p,row):return True
        import godpick_h231_company_history as company
        if row.get('historical_source')==company.VERSION and row.get('source_url')==company.URL:return True
    return kind in ('revenue','margin','ytd') and row.get('historical_source')==VERSION and row.get('source_url')==source_url(kind,market,p)


def load_rows(root):
    root=Path(root);out={k:[] for k in ('revenue','margin','ytd')};errors=[];quarters={}
    for kind in ('revenue','ytd'):
        for market in MARKETS:
            for path in sorted((root/'data/h222'/kind/market).glob('*.json.gz')):
                p=fin.month(path.stem.split('.')[0]) if kind=='revenue' else fin.quarter(path.stem.split('.')[0])
                try:
                    if p is None:raise ValueError('invalid period filename')
                    v=read_partition(path,kind,market,p);t=v['observed_at']
                    for r in v['rows']:
                        code=r['code'];b=_base(kind,market,p,code,t)
                        if kind=='revenue':out[kind].append({**b,**r})
                        else:
                            quarters[market,code,p]=(r,t,QUARTER_URL)
                except Exception as e:errors.append(f'H222 {kind}/{market}/{path.name}: {type(e).__name__}: {str(e)[:120]}')
    # Individual official reports are supplementary, not full-market completion.
    import godpick_h231_company_history as company
    individual,individual_errors=company.load(root);errors.extend(individual_errors)
    import godpick_h235_official_batch as batch
    batch_rows,batch_errors,_=batch.load(root);errors.extend(batch_errors)
    # Preserve both source histories until H192 applies the research as-of cutoff.
    # Derive margins inside each family only; do not subtract mixed sources.
    def base(kind,market,p,code,t,source,r):
        b=_base(kind,market,p,code,t)
        if source==company.URL:b.update(source_url=source,historical_source=company.VERSION)
        if source==batch.PORTAL:b.update(source_url=source,historical_source=batch.VERSION,h235_archive_sha256=r['h235_archive_sha256'],h235_download_url=r['h235_download_url'])
        return b
    for family in (quarters,individual,batch_rows):
        for (market,code,p),(r,t,source) in family.items():
            out['ytd'].append({**base('ytd',market,p,code,t,source,r),**{k:r[k] for k in fin.METRICS['ytd']},'h232_eps_basis':r.get('h232_eps_basis'),'history_financial_values':{k:r[k] for k in ('revenue_ytd','gross_profit_ytd',*fin.METRICS['ytd'])},'industry_type':'GENERAL','basis':'CUMULATIVE_YTD_NOT_SINGLE_QUARTER','unit':'元／股','profit_basis':'SAME_SOURCE_DISCLOSED_UNIT','operating_profit_ytd_label':'營業利益（損失）','net_profit_ytd_label':'本期淨利（淨損）','report_date':None})
            if p%4 and (market,code,p-1) not in family:continue
            prev,pt,ps=family.get((market,code,p-1),({},t,source)) if p%4 else ({},t,source)
            if ps!=source:continue
            d={k:r[k]-prev.get(k,0) for k in ('revenue_ytd','gross_profit_ytd','operating_profit_ytd','net_profit_ytd')}
            if d['revenue_ytd']<=0:continue
            vals={k:round(d[v]/d['revenue_ytd']*100,4) for k,v in zip(fin.METRICS['margin'],('gross_profit_ytd','operating_profit_ytd','net_profit_ytd'))}
            if any(not -100<=v<=1000 for v in vals.values()):continue
            visible=max((t,pt),key=fin.moment);out['margin'].append({**base('margin',market,p,code,visible,source,r),**vals,'industry_type':'GENERAL','statement_basis':'SINGLE_QUARTER','derived_from':'SAME_H222_CUMULATIVE_INCOME_STATEMENTS' if source==QUARTER_URL else 'SAME_H235_CUMULATIVE_INCOME_STATEMENTS' if source==batch.PORTAL else 'SAME_H231_CUMULATIVE_INCOME_STATEMENTS'})
    return out,errors


def inspect(root=ROOT,now=None):
    now=now or datetime.now(ar.TZ);root=Path(root);data,errors=fin.load_inputs(root);ledger,rejected,conflicts=fin.build_ledgers(data,now);m,q=fin.expected_periods(now)
    master=fin.read_json(root/'stock_master_cache.json',[],errors)
    from godpick_h191_opportunity_engine import master_map
    identities=master_map(master);summary=[];stocks=[]
    for c,identity in identities.items():
        market=identity['市場'];rev,mar,ytd,pe=(ledger[k,market,c] for k in ('revenue','margin','ytd','pe'))
        p=fin.financial_profile(rev,mar,ytd,pe,m,q,'')
        from godpick_h232_eps_comparability import high_growth
        stocks.append({**identity,'13月缺期':'、'.join(fin.monthstr(i) for i in range(m-12,m+1) if i not in rev),'三率缺期':'、'.join(fin.qstr(i) for i in (q,q-1,q-4,q-5) if i not in mar),'累計財報缺期':'、'.join(fin.qstr(i) for i in range(q-5,q+1) if i not in ytd),**{k:p[k] for k in ('營收年月','營收新鮮','營收年增%','連3月成長','近13月新高','連兩季三率年增','獲利突破','TTM EPS重建','TTM狀態','EPS可比性','EPS分母核實期數','EPS分母變動%','EPS同分母TTM','EPS權益事件','EPS證據期間')},'H232同分母成長複核':high_growth(p),'高成長財報複核':bool((p['連3月成長'] is True or p['近13月新高'] is True) and (p['連兩季三率年增'] is True or p['獲利突破'] is True) and p['TTM EPS重建'] is not None and p['TTM EPS重建']>=5 and p['獲利率為正'])})
    for market in MARKETS:
        rs=[r for r in stocks if r['市場']==market]
        summary.append({'市場':market,'母體':len(rs),'13月完整':sum(not r['13月缺期'] for r in rs),'三率指定四期完整':sum(not r['三率缺期'] for r in rs),'累計六季完整':sum(not r['累計財報缺期'] for r in rs),'三率三升成立':sum(r['連兩季三率年增'] is True for r in rs),'TTM可重建':sum(r['TTM EPS重建'] is not None for r in rs),'TTM>=5':sum(r['TTM EPS重建'] is not None and r['TTM EPS重建']>=5 for r in rs),'高成長財報複核':sum(r['高成長財報複核'] for r in rs),'H232同分母成長複核':sum(r['H232同分母成長複核'] for r in rs)})
    try:health=ar._read(root/'data/h222/health.json')
    except (OSError,ValueError):health={}
    try:company_health=ar._read(root/'data/h222/h231/health.json')
    except (OSError,ValueError):company_health={}
    try:auto_health=ar._read(root/'data/h222/h233/health.json')
    except (OSError,ValueError):auto_health={}
    from godpick_h235_official_batch import load as batch_load
    _,_,batch_scope=batch_load(root)
    batch_scope=[r for r in batch_scope if fin.moment(r['首次核實'])<=now]
    return {'version':VERSION,'asof':now.isoformat(),'batch_scope':batch_scope,'auto_health':auto_health,'company_health':company_health,'h232_candidates':[r for r in stocks if r['H232同分母成長複核']],'summary':summary,'stocks':stocks,'candidates':[r for r in stocks if r['高成長財報複核']],'health':health,'errors':errors,'rejected':rejected,'conflicts':conflicts,'trade_authority_changed':False}


def excel(r):
    import io
    from openpyxl import Workbook
    from godpick_h219_first_review import table
    w=Workbook();w.remove(w.active)
    for name,rows in [('01_母體驗收',r['summary']),('02_高成長財報複核',r['candidates']),('03_逐股缺期',r['stocks']),('04_回補日誌',r['health'].get('scope',[])),('05_異常',[{'來源異常':x} for x in r['errors']]+[{'證據衝突種類':k,'衝突數':v} for k,v in r['conflicts'].items()])]:
        table(w,name,rows)
    if r.get('company_health'):table(w,'06_個股歷史補證',r['company_health'].get('scope',[]))
    if 'h232_candidates' in r:table(w,'07_H232同分母成長',r['h232_candidates'])
    if r.get('auto_health'):
        table(w,'08_H233自動補證',r['auto_health'].get('queue',[]))
        table(w,'09_H233佇列摘要',[{**r['auto_health'].get('counts',{}),'資料時間':r['auto_health'].get('asof'),'狀態':r['auto_health'].get('status'),'彙總請求':r['auto_health'].get('bulk_requests'),'個股請求':r['auto_health'].get('company_requests')}])
    if r.get('batch_scope'):table(w,'10_H235官方批次',r['batch_scope'])
    out=io.BytesIO();w.save(out);return out.getvalue()
