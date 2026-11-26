# -*- coding: utf-8 -*-
"""H234 offline batch iXBRL HTML review, separate from official runtime ledgers.
Operator-downloaded MOPS archive is not authenticated transport. Never changes
H222 partitions, H194 snapshots, or trade authorization. No historical PIT claim.
"""
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path, PurePosixPath
from decimal import Decimal, InvalidOperation
import hashlib, io, re, stat, zipfile
from lxml import html as lh, etree
import godpick_h192_financial_growth as fin
import godpick_h190_market_archive as ar
import godpick_h231_company_history as company
from godpick_h222_financial_history import norm

VERSION='V191-H234-BATCH-REVIEW'
PORTAL='https://mopsov.twse.com.tw/mops/web/t203sb02'
NAME=re.compile(r'tifrs-fr1-m1-ci-cr-([1-9]\d{3})-(20\d{2})Q([1-4])\.html?',re.I)
CODES={'4000':('revenue_ytd',('營業收入合計','營業收入')),'5900':('gross_profit_ytd',('營業毛利(毛損)','營業毛利')),'6900':('operating_profit_ytd',('營業利益(損失)','營業利益')),'8200':('net_profit_ytd',('本期淨利(淨損)','本期淨利')),'9750':('eps_ytd',('基本每股盈餘合計','基本每股盈餘'))}
MAX_MEMBER=20_000_000; MAX_NESTED=200_000_000; MAX_TOTAL=8_000_000_000; MAX_ENTRIES=40000; MAX_ARCHIVE=2_000_000_000


def digest(raw):return hashlib.sha256(raw).hexdigest()
def file_sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def txt(node):return norm(' '.join(node.itertext()))
def date_label(s,p):
    y=p//4;e=fin.period_end('ytd',p)
    # Bilingual official layout: year on start only or both ends; optional exact English repeat.
    yy=f'(?:{y}|{y-1911})'; mm=f'0?{e.month}'; dd=str(e.day)
    return bool(re.fullmatch(rf'{yy}年0?1月0?1日(?:至|~|-|－)(?:{yy}年)?{mm}月{dd}日(?:{y}/0?1/0?1(?:To|to|~|-)(?:{y}/)?{mm}/{dd})?',s))


def grid(table):
    carry={};out=[]
    for tr in table.xpath('./tr|./thead/tr|./tbody/tr|./tfoot/tr'):
        active={i:v for i,(v,n) in carry.items()};carry={i:(v,n-1) for i,(v,n) in carry.items() if n>1};i=0
        for cell in tr.xpath('./td|./th'):
            while i in active:i+=1
            w,h=int(cell.get('colspan','1')),int(cell.get('rowspan','1'))
            if not 1<=w<=40 or not 1<=h<=10:raise ValueError('invalid table span')
            for j in range(i,i+w):
                if j in active:raise ValueError('overlapping table cells')
                active[j]=txt(cell)
                if h>1:carry[j]=(active[j],h-1)
            i+=w
        out.append([active.get(j,'') for j in range(max(active,default=-1)+1)])
    return out


def number(s):
    s=s.replace(',','').replace('−','-')
    if re.fullmatch(r'\(\d+(?:\.\d+)?\)',s):s='-'+s[1:-1]
    if not re.fullmatch(r'-?\d+(?:\.\d+)?',s):raise ValueError('non-numeric financial fact')
    try:v=Decimal(s)
    except InvalidOperation:raise ValueError('invalid financial fact')
    if not v.is_finite() or abs(v)>=Decimal('1e15'):raise ValueError('financial fact out of range')
    return float(v)


def parse(raw,filename,now):
    m=NAME.fullmatch(PurePosixPath(filename.rsplit('!',1)[-1]).name)
    if not m or not isinstance(raw,bytes) or len(raw)>MAX_MEMBER or now.tzinfo is None:raise ValueError('invalid filename, size or time')
    code,y,q=m.groups();y=int(y);q=int(q);p=y*4+q-1
    if fin.period_end('ytd',p)>=now.date():raise ValueError('unclosed quarter')
    for enc in ('utf-8-sig','cp950','big5'):
        try:s=raw.decode(enc);break
        except UnicodeDecodeError:continue
    else:raise ValueError('unknown document encoding')
    doc=lh.fromstring(s,parser=lh.HTMLParser(no_network=True,huge_tree=False))
    # Never trust a filename alone: title + visible consolidated header + issuer code.
    title=' '.join(doc.xpath('//title/text()'))
    body=doc.xpath('//body');lead=norm(' '.join(__import__('itertools').islice((body[0] if body else doc).itertext(),400)))[:5000]
    if not re.search(rf'(?<!\d){code}(?!\d)',title) or not re.search(rf'{y}Q{q}(?!\d)',title,re.I):raise ValueError('document title identity/period mismatch')
    if not re.search(rf'(?<!\d){code}(?!\d)',lead) or not re.search(rf'(?:{y}|{y-1911})年(?:度)?第?0?{q}季合併財務報告',lead):raise ValueError('consolidated issuer header missing')
    found=[]
    for table in doc.xpath('//table'):
        # Only a table with explicit accounting code and current cumulative column.
        first=table.xpath('./tr|./thead/tr|./tbody/tr')[:4]
        if not any('會計項目' in txt(r) and '代號' in txt(r) for r in first):continue
        rows=grid(table)
        if not any(r and r[0] in CODES for r in rows):continue
        # Require monetary and EPS units in local visible report metadata, not global notes.
        near=table.xpath('preceding::*[not(self::script or self::style) and not(*)][position()<=30]')
        # preceding axis may be returned in document order; last 30 terminal nodes.
        siblings=[]
        for sibling in list(table.itersiblings(preceding=True))[:5]:
            if sibling.tag=='table' or sibling.xpath('.//table'):break
            if len(txt(sibling))<500:siblings.append(txt(sibling))
        units=''.join(txt(x) for x in near)+''.join(siblings)+norm(table.get('summary',''))+''.join(txt(x) for x in table.xpath('./caption'))
        if not re.search(r'單位[:：]?(?:新臺幣|新台幣)(?:千|仟)元',units) or not re.search(r'每股盈餘單位[:：]?(?:新臺幣|新台幣)元',units):raise ValueError('income-statement TWD thousand/EPS unit not explicit')
        if re.search(r'單位[:：]?(?:美元|人民幣|港幣|百萬元|萬元)',units):raise ValueError('mixed statement units')
        for i,r in enumerate(rows[:4]):
            hits=[j for j,v in enumerate(r) if date_label(v,p)]
            if len(hits)>1 and i+1<len(rows):hits=[j for j in hits if j<len(rows[i+1]) and rows[i+1][j] in ('金額','金額Amount','Amount')]
            if len(hits)!=1:continue
            j=hits[0];vals={}
            for rr in rows[i+1:]:
                if not rr or rr[0] not in CODES:continue
                key,labels=CODES[rr[0]]
                if j<2 or len(rr)<=j or len(rr)<2 or not any(rr[1].startswith(x) for x in labels) or (rr[0]=='5900' and '淨額' in rr[1]) or key in vals:raise ValueError('account code/label/column conflict')
                vals[key]=number(rr[j])
            if set(vals)=={v[0] for v in CODES.values()} and vals['revenue_ytd']>0:found.append(vals)
    if len(found)!=1:raise ValueError('unique cumulative income statement not found')
    return {'code':code,'period':p,**found[0]}


def members(z,stats,depth=0,prefix=''):
    if depth>2:raise ValueError('nested archive depth exceeded')
    names=set()
    for entry in z.infolist():
        stats['entries']+=1
        if stats['entries']>MAX_ENTRIES:raise ValueError('archive entry budget exceeded')
        n=entry.filename;parts=PurePosixPath(n).parts
        if n in names or not parts or n.startswith(('/','\\')) or '\\' in n or ':' in n or '..' in parts or stat.S_ISLNK(entry.external_attr>>16) or entry.flag_bits&1:raise ValueError('unsafe/duplicate/encrypted ZIP member')
        names.add(n)
        if entry.is_dir():continue
        lower=n.lower();nested=lower.endswith('.zip');wanted=NAME.fullmatch(PurePosixPath(n).name)
        if not nested and not wanted:continue
        limit=MAX_NESTED if nested else MAX_MEMBER
        stats['bytes']+=entry.file_size
        if entry.file_size>limit or stats['bytes']>MAX_TOTAL or entry.file_size>max(1,entry.compress_size)*500:raise ValueError('archive expansion budget exceeded')
        raw=z.read(entry)
        if len(raw)!=entry.file_size:raise ValueError('archive member size mismatch')
        if nested:
            with zipfile.ZipFile(io.BytesIO(raw)) as inner:yield from members(inner,stats,depth+1,prefix+n+'!')
        else:yield prefix+n,raw


def review(root,archives,now=None):
    now=now or datetime.now(ar.TZ);root=Path(root);archives=[Path(x) for x in archives]
    if now.tzinfo is None or len(archives)>12:raise ValueError('maximum 12 archives; timezone required')
    master=company.identity_map(root)
    if not master:raise ValueError('current stock master unavailable')
    data,errors=fin.load_inputs(root);ledger,_,_=fin.build_ledgers(data,now);month,q=fin.expected_periods(now)
    entries={};blocked=set();log=[];sources=[];stats=Counter()
    for path in archives:
        # Each archive is atomic: an unsafe/truncated archive contributes nothing.
        batch=[];details=[]
        try:
            if path.stat().st_size>MAX_ARCHIVE:raise ValueError('archive file size exceeds 2GB')
            a_sha=file_sha(path)
            with zipfile.ZipFile(path) as z:
                for name,raw in members(z,stats):
                    try:
                        r=parse(raw,name,now);c,p=r['code'],r['period'];identity=master.get(c)
                        if not identity or identity['市場'] not in ('TWSE','TPEX'):raise ValueError('issuer outside current stock master')
                        if not q-11<=p<=q:raise ValueError('outside current 12-quarter research scope')
                        k=(identity['市場'],c,p);batch.append((k,r,{'archive_sha256':a_sha,'member_sha256':digest(raw),'member':name,'archive':path.name}))
                    except (ValueError,TypeError,OverflowError,etree.LxmlError) as e:details.append({'檔案':path.name,'成員':name,'狀態':'REJECTED','原因':str(e)[:180]})
            if file_sha(path)!=a_sha:raise ValueError('archive changed during read')
        except (OSError,ValueError,zipfile.BadZipFile,RuntimeError) as e:
            log.append({'檔案':path.name,'狀態':'ARCHIVE_REJECTED','原因':str(e)[:180]});continue
        sources.append({'檔案':path.name,'SHA256':a_sha,'來源':'OPERATOR_DOWNLOADED_NOT_AUTHENTICATED','取得／分析時間':now.isoformat(),'解析筆數':len(batch)})
        log.extend(details)
        for k,r,proof in batch:
            if k in blocked:continue
            if k in entries and entries[k][0]!=r:
                entries.pop(k);blocked.add(k);log.append({'檔案':path.name,'成員':proof['member'],'狀態':'CONFLICT','原因':'同股同期數值不同'});continue
            entries.setdefault(k,(r,proof))
    # Reconcile with already verified official history. Do not overwrite or merge conflicts.
    from godpick_h222_financial_history import load_rows
    official,_=load_rows(root);official_index=defaultdict(list)
    for x in official['ytd']:
        if fin.moment(x.get('available_at')) and fin.moment(x['available_at'])<=now and x.get('history_financial_values'):
            official_index[x['source'],x['code'],fin.quarter(x['year_quarter'])].append(x['history_financial_values'])
    by_code=defaultdict(dict);rows=[]
    for (market,c,p),(r,proof) in sorted(entries.items()):
        vals={k:r[k] for k,_ in CODES.values()};refs=official_index[market,c,p]
        status='LOCAL_DOCUMENT_ONLY'
        if refs:status='OFFICIAL_OVERLAP_MATCH' if all(x==vals for x in refs) else 'OFFICIAL_CONFLICT'
        live=ledger['ytd',market,c].get(p,{})
        if any(live.get(k) is not None and live[k]!=vals[k] for k in fin.METRICS['ytd']):status='OFFICIAL_CONFLICT'
        if status!='OFFICIAL_CONFLICT':by_code[market,c][p]=r
        rows.append({'市場':market,'股票代號':c,'股票名稱':master[c]['股票名稱'],'期間':fin.qstr(p),'狀態':status,**vals,**proof})
    # Only calculate using the same local document family, with original verified revenue
    # and PE from the existing as-of ledger. No publication into H192 or H222.
    profiles=[]
    for (market,c),series in sorted(by_code.items()):
        yy={};mm={}
        for p,r in series.items():
            yy[p]={**r,'_period':p,'_time':now,'source_url':PORTAL,'profit_basis':'SAME_SOURCE_DISCLOSED_UNIT','operating_profit_ytd_label':'營業利益（損失）','net_profit_ytd_label':'本期淨利（淨損）'}
            if p%4 and p-1 not in series:continue
            prev=series.get(p-1,{}) if p%4 else {}
            d={k:r[k]-prev.get(k,0) for k,_ in CODES.values() if k!='eps_ytd'}
            if d['revenue_ytd']<=0:continue
            v={k:d[a]/d['revenue_ytd']*100 for k,a in zip(fin.METRICS['margin'],('gross_profit_ytd','operating_profit_ytd','net_profit_ytd'))}
            if all(-100<=x<=1000 for x in v.values()):mm[p]={**v,'_period':p,'_time':now,'source_url':PORTAL,'statement_basis':'SINGLE_QUARTER'}
        profile=fin.financial_profile(ledger['revenue',market,c],mm,yy,ledger['pe',market,c],month,q,'')
        candidate=bool((profile['連3月成長'] is True or profile['近13月新高'] is True) and (profile['連兩季三率年增'] is True or profile['獲利突破'] is True) and profile['TTM EPS重建'] is not None and profile['TTM EPS重建']>=5 and profile['獲利率為正'])
        profiles.append({'市場':market,'股票代號':c,'股票名稱':master[c]['股票名稱'],'資料性質':'本機文件研究／來源未獨立驗證','缺季':'、'.join(fin.qstr(i) for i in range(q-7,q+1) if i not in series),'本機高成長複核':candidate,**{k:profile[k] for k in ('營收年增%','連3月成長','近13月新高','連兩季三率年增','獲利突破','TTM EPS重建','TTM狀態','EPS可比性')}})
    scope=[]
    for market in ('TWSE','TPEX'):
        total=sum(x['市場']==market for x in master.values())
        for p in range(q-7,q+1):
            rr=[x for x in rows if x['市場']==market and x['期間']==fin.qstr(p)]
            scope.append({'市場':market,'季度':fin.qstr(p),'當前母體':total,'本機有效筆數':sum(x['狀態']!='OFFICIAL_CONFLICT' for x in rr),'官方重疊一致':sum(x['狀態']=='OFFICIAL_OVERLAP_MATCH' for x in rr),'衝突':sum(x['狀態']=='OFFICIAL_CONFLICT' for x in rr),'正式市場達標':False})
    return {'version':VERSION,'asof':now.isoformat(),'status':'LOCAL_REVIEW_ONLY','scope':scope,'sources':sources,'rows':rows,'profiles':profiles,'candidates':[r for r in profiles if r['本機高成長複核']],'log':log,'official_input_errors':errors,'trade_authority_changed':False}


def excel(r):
    from openpyxl import Workbook
    from godpick_h219_first_review import table
    w=Workbook();w.remove(w.active)
    for n,rs in [('01_季度批次覆蓋',r['scope']),('02_本機成長複核',r['candidates']),('03_逐股財報',r['rows']),('04_逐股成長證據',r['profiles']),('05_原始封存索引',r['sources']),('06_拒絕與衝突',r['log']+[{'原因':x} for x in r['official_input_errors']])]:table(w,n,rs)
    b=io.BytesIO();w.save(b);return b.getvalue()
