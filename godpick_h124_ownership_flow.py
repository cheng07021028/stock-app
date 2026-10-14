# -*- coding: utf-8 -*-
"""H124 fail-closed official TPEx institutions / TDCC weekly ownership.

Exact provenance is mandatory. A weekly TDCC increase is NOT proof of a
particular large investor accumulating; it is only holder-bracket concentration.
No network request is executed while scanning stocks.
"""
from __future__ import annotations
import csv, gzip, hashlib, io, json, re
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Mapping, Sequence
from godpick_h123_flow_provenance import (digest, iso_date, aware_stamp, shares, code_id, _streak)

VERSION='V191-H124.1-TPEx-TDCC-EVIDENCE'
TDCC_URL='https://openapi.tdcc.com.tw/v1/opendata/1-5'
# A report with only a requested date is NOT a dated exchange report.
TPEX_FIELDS={
 'code':('證券代號','股票代號','SecuritiesCode'),
 'foreign':('外資及陸資買賣超股數(不含外資自營商)','外資及陸資淨買賣超股數(不含自營商)','外資及陸資淨買股數(不含自營商)','外資及陸資買賣超股數'),
 'trust':('投信買賣超股數','投信淨買賣超股數','投信淨買股數'),
 'total':('三大法人買賣超股數合計','三大法人買賣超股數'),
 'day':('資料日期','交易日期','報表日期','Date'),
}
TDCC_FIELDS={
 'day':('資料日期',), 'code':('證券代號',), 'level':('持股分級',),
 'shares':('股數',), 'ratio':('占集保庫存數比例%','佔集保庫存數比例%'),
}


def _field(headers:Sequence[str],names:Sequence[str])->str:
    available=[x for x in names if x in headers]
    if len(available)!=1:raise ValueError('官方欄位缺失或重複，禁止猜測：'+ '/'.join(names))
    return available[0]


def _official_https(url:str,host:str)->None:
    p=urlparse(url)
    if p.scheme!='https' or p.hostname!=host or p.username or p.password:
        raise ValueError('拒絕非指定官方HTTPS來源')


def _int_no_unit(v:Any)->int:
    return shares(v)


def _ratio(v:Any)->float:
    s=str(v).strip().replace(',','').rstrip('%')
    if not re.fullmatch(r'\d+(?:\.\d+)?',s):raise ValueError('TDCC比例非合法百分數')
    x=float(s)
    if x>100:raise ValueError('比例超出100%')
    return x


def _stock(v:Any)->str:
    return code_id(v)


def _day(v:Any)->str:
    s=str(v or '').strip()
    # TDCC official open data is YYYYMMDD; refuse silently guessed ROC dates.
    return iso_date(s)


def _seal(payload:dict)->dict:
    payload['evidence_sha256']=digest(payload)
    return payload


def valid_capture(obj:Mapping[str,Any])->bool:
    if not isinstance(obj,dict) or obj.get('version')!=VERSION:return False
    a=dict(obj);h=a.pop('evidence_sha256',None)
    if not isinstance(h,str) or digest(a)!=h:return False
    if obj.get('raw_sha256')!=hashlib.sha256(obj.get('raw_bytes_base64','').encode()).hexdigest():
        return False
    return True


def build_tpex(raw:Sequence[Mapping[str,Any]], *, trade_day:str, collected_at:str, source_url:str,
               official_market_date_attested:bool=False)->dict:
    """Accept only official row-schema with exact *share* columns and row date.

    If official report does not date its rows, it remains pending: an operator's
    command-line --day alone is not evidence of market publication date.
    """
    _official_https(source_url,'www.tpex.org.tw')
    day=_day(trade_day);stamp=aware_stamp(collected_at)
    if stamp.date()<date.fromisoformat(day):raise ValueError('觀察時間早於交易日')
    if not isinstance(raw,list) or not raw or not all(isinstance(r,dict) for r in raw):
        raise ValueError('TPEx報表列缺失')
    cols=list(raw[0]);mapping={k:_field(cols,names) for k,names in TPEX_FIELDS.items() if k!='day'}
    try:day_col=_field(cols,TPEX_FIELDS['day'])
    except ValueError:
        # Explicitly require row date; no silent trust in a command-line flag.
        if official_market_date_attested:raise ValueError('未核對到官方日期欄，單純標記attested無效')
        raise ValueError('缺少官方報表交易日期欄位，暫不認證')
    records={}
    for row in raw:
        if set(row)!=set(cols):raise ValueError('TPEx報表不同列欄位不一致')
        if _day(row[day_col])!=day:raise ValueError('官方行日期與交易日不符')
        try:code=_stock(row[mapping['code']])
        except ValueError:continue
        if code in records:raise ValueError('TPEx證券代號重複')
        records[code]={'foreign_shares':_int_no_unit(row[mapping['foreign']]),
                       'trust_shares':_int_no_unit(row[mapping['trust']]),
                       'total_shares':_int_no_unit(row[mapping['total']])}
    if not records:raise ValueError('上櫃正式四碼個股為空')
    raw_txt=json.dumps(raw,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    body={'version':VERSION,'source':'TPEX','market':'上櫃','trade_date':day,
          'first_observed_at':stamp.isoformat(),'source_url':source_url,
          'schema_map':mapping,'raw_sha256':hashlib.sha256(raw_txt.encode()).hexdigest(),
          'raw_bytes_base64':__import__('base64').b64encode(raw_txt.encode()).decode(),
          'stocks':records,'row_count':len(raw),'date_proof':'EACH_OFFICIAL_ROW',
          'status':'SCHEMA_VERIFIED_OBSERVATION_NOT_PUBLISH_TIME'}
    body['raw_sha256']=hashlib.sha256(body['raw_bytes_base64'].encode()).hexdigest()
    return _seal(body)


def _parse_tdcc_bytes(raw:bytes)->list[dict]:
    if len(raw)>15_000_000:raise ValueError('TDCC資料過大')
    text=raw.decode('utf-8-sig')
    if text.lstrip().startswith('['):
        rows=json.loads(text)
        if not isinstance(rows,list) or not all(isinstance(x,dict) for x in rows):raise ValueError('TDCC JSON非物件清單')
        return rows
    if text.lstrip().startswith('<'):raise ValueError('TDCC服務回傳HTML，不是官方資料')
    parsed=list(csv.DictReader(io.StringIO(text)))
    if not parsed:raise ValueError('TDCC CSV沒有資料')
    return parsed


def build_tdcc(raw_bytes:bytes, *,collected_at:str, source_url:str=TDCC_URL)->dict:
    """Aggregates ONLY bracket 15 (>1,000,001 shares), 16=adjustment 17=total.

    Bracket 14 also contains exactly 1,000,000 shares; the public grouped feed
    cannot recover that exact boundary, so report bracket-15 rather than an
    unjustified exact >=1,000,000-share assertion.
    """
    _official_https(source_url,'openapi.tdcc.com.tw')
    if not urlparse(source_url).path.endswith('/v1/opendata/1-5'):raise ValueError('非TDCC官方1-5資料集')
    stamp=aware_stamp(collected_at);rows=_parse_tdcc_bytes(raw_bytes)
    fields=list(rows[0]);col={k:_field(fields,v) for k,v in TDCC_FIELDS.items()}
    weeks=set();group={};duplicates=set()
    for r in rows:
        if set(r)!=set(fields):raise ValueError('TDCC欄位不一致')
        day=_day(r[col['day']]);weeks.add(day)
        if day>stamp.date().isoformat():raise ValueError('TDCC資料日期晚於首次觀察日')
        try:code=_stock(r[col['code']])
        except ValueError:continue
        level=_int_no_unit(r[col['level']]);n=_int_no_unit(r[col['shares']]);ratio=_ratio(r[col['ratio']])
        if level<1 or level>17 or n<0:raise ValueError('TDCC級距/股數不合理')
        key=(code,level)
        if key in duplicates:raise ValueError('TDCC相同股票持股級距重複')
        duplicates.add(key)
        group.setdefault(code,{})[level]=(n,ratio)
    if len(weeks)!=1:raise ValueError('TDCC當期快照混雜不同週')
    week=next(iter(weeks));stocks={}
    for code,levels in group.items():
        if 15 not in levels or 17 not in levels:continue
        # 17 is official total, do NOT use adjustment group16 as holder group.
        top_n,top_pct=levels[15];total_n,total_pct=levels[17]
        if total_n<=0 or top_n>total_n or total_pct<95 or total_pct>100.01:raise ValueError('TDCC總計/級距不合邏輯')
        if abs(100*top_n/total_n-top_pct)>0.18:raise ValueError('TDCC比例/股數不一致')
        stocks[code]={'over_1000_lots_bracket_shares':top_n,'bracket15_pct':top_pct,
                      'total_custody_shares':total_n}
    if not stocks:raise ValueError('TDCC級距15/17資料不完整')
    raw_b64=__import__('base64').b64encode(raw_bytes).decode()
    body={'version':VERSION,'source':'TDCC','market':'上市櫃股權分散','trade_date':week,
          'first_observed_at':stamp.isoformat(),'source_url':source_url,
          'raw_bytes_base64':raw_b64,'raw_sha256':hashlib.sha256(raw_b64.encode()).hexdigest(),
          'raw_byte_digest':hashlib.sha256(raw_bytes).hexdigest(),'raw_row_count':len(rows),
          'stocks':stocks,'status':'BRACKET15_WEEKLY_OBSERVED_NOT_DAY_TRADE'}
    return _seal(body)


def verify_capture(obj:Mapping[str,Any])->bool:
    if not valid_capture(obj):return False
    try:
        raw=__import__('base64').b64decode(obj['raw_bytes_base64'],validate=True)
        if obj['source']=='TDCC':
            if hashlib.sha256(raw).hexdigest()!=obj['raw_byte_digest']:return False
            return build_tdcc(raw,collected_at=obj['first_observed_at'],source_url=obj['source_url'])==obj
        if obj['source']=='TPEX':
            return build_tpex(json.loads(raw),trade_day=obj['trade_date'],
                    collected_at=obj['first_observed_at'],source_url=obj['source_url'])==obj
    except (TypeError,ValueError,KeyError,UnicodeError,OverflowError):return False
    return False


def save_capture(root:Path, obj:Mapping[str,Any])->Path:
    if not verify_capture(obj):raise ValueError('H124原始證據核對失敗')
    src=obj['source'];d=_day(obj['trade_date'])
    folder=Path(root)/'data'/'h124'/src/d;folder.mkdir(parents=True,exist_ok=True)
    for p in folder.glob('*.json.gz'):
        with gzip.open(p,'rt',encoding='utf-8') as fh:old=json.load(fh)
        if not verify_capture(old):raise ValueError('已有證據毀損，拒絕寫入')
        if old['raw_sha256']==obj['raw_sha256']:return p
    path=folder/(obj['evidence_sha256']+'.json.gz')
    if path.exists():raise ValueError('不可變證據路徑衝突')
    with path.open('xb') as fp:
        with gzip.GzipFile(filename='',mode='wb',fileobj=fp,mtime=0) as g:
            g.write((json.dumps(obj,ensure_ascii=False,sort_keys=True)+'\n').encode())
    return path


def load_captures(root:Path,src:str, max_periods:int=8)->dict[str,dict]:
    if src not in ('TPEX','TDCC'):raise ValueError('未知證據市場')
    base=Path(root)/'data'/'h124'/src;out={}
    dirs=sorted(x for x in base.glob('*') if x.is_dir())[-max_periods:]
    for folder in dirs:
        for path in folder.glob('*.json.gz'):
            with gzip.open(path,'rt',encoding='utf-8') as f:obj=json.load(f)
            if not verify_capture(obj):raise ValueError('H124檔案來源驗證失敗：'+str(path))
            d=_day(obj['trade_date'])
            if d in out and out[d]['raw_sha256']!=obj['raw_sha256']:
                raise ValueError('同日期官方檔案不同版本需人工確認：'+d)
            out[d]=obj
    return out


def assess_tpex(*,code:str,evidence:Mapping[str,dict],sessions:Sequence[str],cutoff:str,preverified:bool=False)->dict:
    out={'H123法人實證狀態':'PENDING_EVIDENCE','H123法人連買實證':'否',
         'H123法人實證原因':'上櫃官方逐日報表不足','H123來源雜湊':None,'H123法人逐日有效天數':0}
    try:
        stock=_stock(code);t=aware_stamp(cutoff);days=sorted({_day(x) for x in sessions});days=[x for x in days if x<=t.date().isoformat()][-5:]
        if len(days)!=5:raise ValueError('完整官方五交易日不足')
        # No synthetic weekend / weekday skips; holidays need certified calendar.
        from datetime import timedelta
        for a,b in zip(days,days[1:]):
            x=date.fromisoformat(a)+timedelta(days=1)
            while x.weekday()>=5:x+=timedelta(days=1)
            if x.isoformat()!=b:raise ValueError('無官方假期核對，存在日期缺口')
        values=[];hashes=[]
        for day in days:
            ev=evidence.get(day)
            if ev is None or (not preverified and not verify_capture(ev)) or ev['source']!='TPEX':raise ValueError('缺上櫃日證據：'+day)
            if aware_stamp(ev['first_observed_at'])>t:raise ValueError('事後取得的法人證據不得回填：'+day)
            x=ev['stocks'].get(stock)
            if x is None:raise ValueError('官方當日缺該股票：'+day)
            values.append({'date':day,'foreign':x['foreign_shares'],'trust':x['trust_shares'],'total':x['total_shares']});hashes.append(ev['evidence_sha256'])
        f=[x['foreign'] for x in values];i=[x['trust'] for x in values];combo=sum(x['total'] for x in values)
        ok=combo>0 and ((_streak(f)>=2 and sum(f[-3:])>0 and sum(f)>0) or (_streak(i)>=2 and sum(i[-3:])>0 and sum(i)>0))
        out.update({'H123法人實證狀態':'PROVEN_FLOW' if ok else 'VERIFIED_BUT_NOT_BUYING',
          'H123法人連買實證':'是' if ok else '否',
          'H123法人實證原因':'上櫃5日已驗證' if ok else '完整上櫃5日，但連買不成立',
          'H123法人逐日有效天數':5,'H123外資連買天數':_streak(f),'H123投信連買天數':_streak(i),
          'H123近5日法人合計股數':combo,'H123來源雜湊':digest(hashes),
          'H123法人來源':'TPEX_OFFICIAL_DAILY_ROWS_5_SESSIONS',
          'H123外資逐日股數':[{'date':x['date'],'net':x['foreign']} for x in values],
          'H123投信逐日股數':[{'date':x['date'],'net':x['trust']} for x in values],
          'H123外資近3日買賣超_股':sum(f[-3:]),'H123外資近5日買賣超_股':sum(f),
          'H123投信近3日買賣超_股':sum(i[-3:]),'H123投信近5日買賣超_股':sum(i)})
    except (ValueError,KeyError,TypeError) as exc:out['H123法人實證原因']=str(exc)
    return out


def assess_tdcc(*,code:str,evidence:Mapping[str,dict],cutoff:str,min_delta_pp:float=0.05,preverified:bool=False)->dict:
    out={'H124大戶實證狀態':'PENDING_EVIDENCE','H124大戶集中合格':'否',
         'H124大戶變化pp':None,'H124大戶來源雜湊':None,'H124大戶原因':'需要至少連續兩次已觀察官方週報'}
    try:
        stock=_stock(code);t=aware_stamp(cutoff)
        known=sorted((d,obj) for d,obj in evidence.items()
                     if d<=t.date().isoformat() and aware_stamp(obj['first_observed_at'])<=t)
        if len(known)<2:raise ValueError('尚未累積兩週事前TDCC官方快照')
        (d0,a),(d1,b)=known[-2:]
        if not (6 <= (date.fromisoformat(d1)-date.fromisoformat(d0)).days <= 9):
            raise ValueError('TDCC兩期日期非相鄰週，無法判定週變化')
        if (t.date()-date.fromisoformat(d1)).days>14:raise ValueError('TDCC週資料已過期')
        if not preverified and not (verify_capture(a) and verify_capture(b)):raise ValueError('TDCC來源雜湊驗證失敗')
        x=a['stocks'].get(stock);y=b['stocks'].get(stock)
        if not x or not y:raise ValueError('TDCC該股票未同時具備兩週官方級距15與合計17')
        delta=round(y['bracket15_pct']-x['bracket15_pct'],5)
        status='VERIFIED_RISING' if delta>=min_delta_pp else 'VERIFIED_NOT_RISING'
        out.update({'H124大戶實證狀態':status,'H124大戶集中合格':'是' if status=='VERIFIED_RISING' else '否',
                    'H124大戶變化pp':delta,'H124大戶來源雜湊':digest([a['evidence_sha256'],b['evidence_sha256']]),
                    'H124大戶證據週':d1,'H124大戶原因':'TDCC第15級持股比週變；非逐日主力交易訊號'})
    except (ValueError,KeyError,TypeError) as exc:out['H124大戶原因']=str(exc)
    return out
