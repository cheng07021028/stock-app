# -*- coding: utf-8 -*-
"""H152 | Same-session relative resilience + evidence-gated next-session scenarios.

Read-only research.  Does not alter H109/H113/H123/H124/H126 or create a trade.
No future prices, implicit brokerage holdings, fabricated probabilities or stale
market data.  Cash/OTC benchmarks must be from the exact stock research date.
"""
from __future__ import annotations
from collections import Counter
from datetime import date
from math import isfinite
from typing import Any, Mapping
import pandas as pd
from godpick_h147_five_gate_review import _code, _day, _s

VERSION = 'V191-H152-RESILIENCE-FORECAST-EVIDENCE'
OBSERVATION_ONLY = '逆勢／次日情境研究；H126未授權不得買進'
DISPLAY_COLUMNS = (
    '股票代號','股票名稱','產業','研究層級','研究交易日','行情來源','對照指數',
    '大盤同日漲跌%','個股同日漲跌%','相對強度pp','當日成交額百萬','20日均成交額百萬',
    '成交量張','量比','抗跌判定','隔日方向情境','歷史相似樣本數','歷史樣本次日上漲率%',
    '證據與反證','H109風控','交易資格',
)


def _frame(x: Any) -> pd.DataFrame:
    return x if isinstance(x, pd.DataFrame) else pd.DataFrame()


def _num(x: Any) -> float | None:
    if x is None or isinstance(x,bool): return None
    try:
        raw = str(x).strip().replace(',','').replace('%','')
        if not raw: return None
        n = float(raw)
        return n if isfinite(n) else None
    except (TypeError,ValueError,OverflowError):
        return None


def _first_number(row: Mapping[str,Any], *keys: str) -> float | None:
    for k in keys:
        if k in row:
            val = _num(row.get(k))
            if val is not None:return val
    return None


def _market(row: Mapping[str,Any]) -> tuple[str,str]:
    text=' '.join(_s(row.get(k)) for k in ('市場別','市場','market_type','市場類型','上市櫃'))
    if '上市櫃' in text:return '未確認','市場別待核'
    if any(k in text for k in ('上櫃','OTC','TPEX')):return '上櫃','櫃買指數'
    if any(k in text for k in ('上市','TWSE','TSE')):return '上市','加權指數'
    return '未確認','市場別待核'


def _snapshot(snapshot: Any, research_day: date | None) -> dict:
    s=snapshot if isinstance(snapshot,Mapping) else {}
    day = _day(s.get('data_date') or s.get('market_date') or s.get('twse_data_date'))
    result={'date':str(day) if day else '未驗證','aligned':bool(day and research_day and day==research_day),
            'twse':_first_number(s,'twse_change_pct','TWSE_change_pct'),
            'otc':_first_number(s,'otc_change_pct','TPEX_change_pct'),
            'source':_s(s.get('source')) or '來源待查'}
    if not result['aligned']:
        result['twse']=None;result['otc']=None
    # Guard against unit confusion (percent-point rather than ratio) and API error.
    for k in ('twse','otc'):
        v=result[k]
        if v is not None and not (-15<=v<=15):result[k]=None
    return result


def _strict_stock_return(row: Mapping[str,Any], day: date | None) -> tuple[float | None,str]:
    evidence=_day(row.get('H114K線證據日'))
    bars=_first_number(row,'H114K線有效根數')
    state=_s(row.get('H114K線資料狀態')).upper()
    prior=_first_number(row,'H114前日收盤價')
    close=_first_number(row,'H114當日收盤價')
    prior_day=_day(row.get('H114前次交易日'))
    if not (day and evidence==day and bars is not None and bars>=60 and state=='VALID_OHLCV'):
        return None,'K線交易日、有效根數或成交量單位尚未通過'
    if not (prior_day and prior_day<day and (day-prior_day).days<=10):
        return None,'缺前一有效交易日或停牌／長假間隔待核'
    if prior is None or close is None or not (prior>0 and close>0):
        return None,'同股前日／本日收盤價缺失'
    calc=(close/prior-1)*100
    reported=_first_number(row,'H114當日漲跌幅%')
    if reported is not None and abs(reported-calc)>0.11:
        return None,'同股當日漲跌幅與收盤價矛盾'
    if not (-15<=calc<=15):return None,'單日報酬超出現行研究合理區間，須查公司行動／資料'
    return round(calc,3),'H114有效同股前後收盤'


def build_resilience_research(*, diagnosis_frame: Any, research_frame: Any,
                               research_day: Any, market_snapshot: Any,
                               authority_frame: Any=None,
                               limit: int=12) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame,dict]:
    """Return all validated resilience leads, amended EXISTING research and summary.

    Research frame keeps its original rows/columns and appends only independent
    verified anti-fall candidates. Next-session scenarios never contain a model-
    trained probability or an unvalidated target price.
    """
    src=_frame(diagnosis_frame); existing=_frame(research_frame).copy(deep=True)
    day=_day(research_day);bench=_snapshot(market_snapshot,day)
    counter=Counter();record={};duplicates=set()
    for row in src.to_dict('records'):
        code=_code(row.get('股票代號'))
        if not code:counter['無效股票代號']+=1;continue
        if code in record:duplicates.add(code)
        else:record[code]=row
    authority={_code(x.get('股票代號')) for x in _frame(authority_frame).to_dict('records')}
    authority.discard('')
    research_by_code={}
    for rec in existing.to_dict('records'):
        code=_code(rec.get('股票代號'))
        if code:research_by_code.setdefault(code,rec)
    leads=[];observations={}
    for code,row in record.items():
        if code in duplicates:
            counter['重複代號隔離']+=1;continue
        market,idx=_market(row)
        market_ret=bench['otc'] if market=='上櫃' else bench['twse'] if market=='上市' else None
        ret,return_state=_strict_stock_return(row,day)
        if ret is None:counter['個股行情不完整']+=1;continue
        if market_ret is None:
            counter['同日對應大盤缺失']+=1;continue
        excess=ret-market_ret
        amount=_first_number(row,'H113當日成交額百萬','成交額百萬')
        avg=_first_number(row,'H113二十日均成交額百萬','20日均成交額百萬')
        lots=_first_number(row,'H114當日成交量張')
        ratio=_first_number(row,'H114K線量比')
        if market_ret > -1.5:
            counter['大盤未達重挫門檻']+=1;continue
        if not (ret>=-0.5 and excess>=1.5):
            counter['並未實際抗跌']+=1;continue
        if amount is None or amount<150 or avg is None or avg<150 or lots is None or lots<500 or ratio is None or ratio<0.8:
            counter['抗跌但成交證據不足']+=1;continue
        risk=_s(row.get('H109決策權威')) or 'UNKNOWN'
        status='逆勢收紅／相對抗跌' if ret>0 else '平盤抗跌' if ret>=-0.05 else '跌幅顯著小於大盤'
        # Historical within-stock analogues are descriptive, not calibrated
        # odds; only expose a frequency if >=30 complete historical examples.
        n=_first_number(row,'H114同型歷史樣本數')
        rate=_first_number(row,'H114同型樣本隔日上漲率%')
        n=int(n) if n is not None and n>=0 else 0
        rate=round(rate,2) if n>=30 and rate is not None and 0<=rate<=100 else None
        if ret>=0 and ratio>=1.2:
            outlook='偏多觀察情境（次日仍需量價確認）'
        elif ret>=-0.05:
            outlook='中性偏多情境（等待續強）'
        else:
            outlook='中性待確認（抗跌不保證續漲）'
        veto=risk not in ('ALLOW','PASS','NONE','RESEARCH_ELIGIBLE')
        if veto:outlook+='／H109不可買進'
        proof=(f'{idx}{market_ret:+.2f}%／個股{ret:+.2f}%／超額{excess:+.2f}pp；'
               f'同日{bench["date"]}、成交額{amount:g}百萬、量比{ratio:g}；'
               f'來源{bench["source"]}、H114內部K線仍須官方核對；'
               '若次日失去量價、跌破前日低點或資料推翻，撤銷情境；'
               '未驗證法人／TDCC與正式買進資格')
        row_out={
            '股票代號':code,'股票名稱':_s(row.get('股票名稱')),'產業':_s(row.get('類別')),
            '研究層級':'R｜逆勢抗跌研究／非交易','研究交易日':str(day),'行情來源':return_state,
            '對照指數':idx,'大盤同日漲跌%':round(market_ret,3),
            '個股同日漲跌%':ret,'相對強度pp':round(excess,3),
            '當日成交額百萬':amount,'20日均成交額百萬':avg,
            '成交量張':lots,'量比':ratio,'抗跌判定':status,'隔日方向情境':outlook,
            '歷史相似樣本數':n,'歷史樣本次日上漲率%':rate,'證據與反證':proof,
            'H109風控':risk,'交易資格':OBSERVATION_ONLY,
        }
        leads.append(row_out); observations[code]=row_out
    leads.sort(key=lambda r:(r['H109風控'] not in ('ALLOW','PASS','NONE','RESEARCH_ELIGIBLE'),
                              -r['相對強度pp'],-r['當日成交額百萬'],r['股票代號']))
    selected=leads[:max(1,min(int(limit),30))]
    def _scenario_for_stock(raw: Mapping[str,Any] | None) -> str:
        if not raw:
            return '同輪個股資料未取得，無法產生隔日情境'
        v,reason=_strict_stock_return(raw,day)
        if v is None:return '個股日K未驗：'+reason
        ratio=_first_number(raw,'H114K線量比')
        amount=_first_number(raw,'H113當日成交額百萬','成交額百萬')
        risk=_s(raw.get('H109決策權威')) or 'UNKNOWN'
        if ratio is None or amount is None:
            return '量能來源未證，暫不提供方向判斷'
        if v>=2 and ratio>=1.2 and amount>=150:
            label='偏多續攻情境（隔日開盤與量價須重驗）'
        elif v<=-2 and ratio>=1.2:
            label='偏空風險情境（放量下跌，禁止據此反向猜底）'
        else:
            label='中性震盪情境（缺足夠方向優勢）'
        if risk not in ('ALLOW','PASS','NONE','RESEARCH_ELIGIBLE'):
            label+='／H109 '+risk+' 未解除'
        return label+'；非校準預測、非H126交易授權'

    # Add clearly differentiated resilience candidates without overwriting
    # any previously selected H148.3 observation. Preserve old column order.
    basecols=list(existing.columns)
    if not basecols:
        from godpick_h1483_research_recall import RESEARCH_COLUMNS
        basecols=list(RESEARCH_COLUMNS)
    enrich=('抗跌判定','對照指數','大盤同日漲跌%','個股同日漲跌%','相對強度pp',
            '隔日方向情境','歷史相似樣本數','歷史樣本次日上漲率%','抗跌反證')
    rows=[]
    for r in existing.to_dict('records'):
        code=_code(r.get('股票代號'));ev=observations.get(code)
        same_run=record.get(code) if code not in duplicates else None
        extra={'抗跌判定':ev['抗跌判定'] if ev else '未證成抗跌',
               '對照指數':ev['對照指數'] if ev else '同日基準未證或不適用',
               '大盤同日漲跌%':ev['大盤同日漲跌%'] if ev else None,
               '個股同日漲跌%':ev['個股同日漲跌%'] if ev else None,
               '相對強度pp':ev['相對強度pp'] if ev else None,
               '隔日方向情境':ev['隔日方向情境'] if ev else _scenario_for_stock(same_run),
               '歷史相似樣本數':ev['歷史相似樣本數'] if ev else None,
               '歷史樣本次日上漲率%':ev['歷史樣本次日上漲率%'] if ev else None,
               '抗跌反證':ev['證據與反證'] if ev else '非經同日對應市場驗證的抗跌訊號；以上僅條件情境，仍須H126確認'}
        rows.append({**r,**extra})
    for ev in selected:
        if ev['股票代號'] in research_by_code:continue
        rows.append({**{col:None for col in basecols},
                     '股票代號':ev['股票代號'],'股票名稱':ev['股票名稱'],
                     '產業':ev['產業'],'研究層級':ev['研究層級'],
                     'H114行情日期':ev['研究交易日'],
                     '來源證據':ev['行情來源']+'；'+bench['source'],
                     '當日成交額百萬':ev['當日成交額百萬'],
                     '20日均成交額百萬':ev['20日均成交額百萬'],
                     '成交量張':ev['成交量張'],'量比':ev['量比'],
                     '突破／續攻判定':'逆勢抗跌／非起漲確認',
                     'H109風控':ev['H109風控'],
                     '尚需驗證與不得買進原因':ev['證據與反證'],
                     '抗跌判定':ev['抗跌判定'],'對照指數':ev['對照指數'],
                     '大盤同日漲跌%':ev['大盤同日漲跌%'],
                     '個股同日漲跌%':ev['個股同日漲跌%'],
                     '相對強度pp':ev['相對強度pp'],
                     '隔日方向情境':ev['隔日方向情境'],
                     '歷史相似樣本數':ev['歷史相似樣本數'],
                     '歷史樣本次日上漲率%':ev['歷史樣本次日上漲率%'],
                     '抗跌反證':ev['證據與反證']})
    revised=pd.DataFrame(rows,columns=basecols+list(enrich))
    summary=pd.DataFrame([
        {'檢查項目':'H152研究交易日','結果':str(day) if day else '未確認','說明':'只讀本輪研究日，不以匯出時間當行情日'},
        {'檢查項目':'H152大盤資料日','結果':bench['date'],'說明':'必須與H114個股交易日一致；不使用舊快取'},
        {'檢查項目':'H152同日指數可驗證','結果':'是' if bench['aligned'] else '否','說明':bench['source']},
        {'檢查項目':'H152抗跌候選總數','結果':len(leads),'說明':'只讀同日跌市、量價與相對強度；不是買進資格'},
        {'檢查項目':'H152抗跌新加入研究股','結果':sum(x['股票代號'] not in research_by_code for x in selected),'說明':'不覆蓋H148.3原研究名單'},
        {'檢查項目':'H152沒有上漲機率數值的原因','結果':'未驗證樣本外校準','說明':'歷史相似日勝率僅描述樣本，不冒充隔日上漲機率'},
    ] + [{'檢查項目':'H152排除-'+key,'結果':count,'說明':'缺證／未達條件的追蹤，不等於基本面不佳'} for key,count in counter.most_common()])
    meta={'version':VERSION,'day':str(day) if day else '未核',
          'market_date':bench['date'],'market_aligned':bench['aligned'],
          'source':bench['source'],'strong_resilience':len(leads),
          'visible':len(selected),'added':sum(x['股票代號'] not in research_by_code for x in selected),
          'reasons':dict(counter),'forecast_calibrated':False,
          'formal_h126_modified':False,
          'warning':('本輪大盤快照與研究日未對齊；不得宣稱強勢抗跌或隔日上漲機率。'
                     if not bench['aligned'] else
                     '情境分析不是個股隔日保證；正式交易必須另經H126。')}
    return pd.DataFrame(selected,columns=DISPLAY_COLUMNS),revised,summary,meta


def supplement_with_verified_h117(snapshot: Any, *, research_day: Any, root: Any) -> dict:
    """Use locally available H117 *immutable official* index only if two
    authenticated manifests supply consecutive observed TAIEX sessions.
    No network, no date guessing, no OTC inference and no persistence.
    """
    from pathlib import Path
    import gzip,json
    from godpick_h117_official_market_closure import verify_manifest
    result=dict(snapshot) if isinstance(snapshot,Mapping) else {}
    day=_day(research_day)
    if day is None:return result
    folder=Path(root)/'TAIEX'
    try:
        files=sorted(x for x in folder.glob('*.json.gz') if x.name[:10] <= str(day))[-2:]
        if len(files)!=2 or files[-1].name != str(day)+'.json.gz':return result
        bodies=[]
        for path in files:
            with gzip.open(path,'rt',encoding='utf-8') as handle:body=json.load(handle)
            if (not verify_manifest(body) or body.get('source')!='TAIEX' or
                body.get('date')+'.json.gz'!=path.name or body.get('row_count')!=len(body.get('rows') or [])):
                return result
            if len(body.get('rows') or [])!=1:return result
            row=body['rows'][0]
            if row.get('date')!=body['date'] or row.get('code')!='BENCHMARK':return result
            bodies.append(body)
        previous,current=bodies
        if previous['date']>=current['date']:return result
        prev=_num(previous['rows'][0].get('close'))
        close=_num(current['rows'][0].get('close'))
        if prev is None or close is None or prev<=0 or close<=0:return result
        # Never combine index percentages across different trading dates.
        if _day(result.get('data_date')) != day:
            result={'data_date':str(day)}
        result['twse_change_pct']=round((close/prev-1)*100,6)
        result['source']='H117已驗SHA的交易所日資料（TAIEX）；上櫃仍需另行查證'
        return result
    except (OSError,ValueError,KeyError,TypeError,json.JSONDecodeError):
        return result


__all__=['VERSION','DISPLAY_COLUMNS','build_resilience_research','supplement_with_verified_h117']
