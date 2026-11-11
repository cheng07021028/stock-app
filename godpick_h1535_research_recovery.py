# -*- coding: utf-8 -*-
"""H153.5 | current-run research recovery and ranked non-tradable counterevidence.

This module NEVER changes official H126, H109, formal recommendation outputs,
prices, user settings, or historical ledgers. A vetoed stock may receive a
*verification-priority* slot, NOT a recommendation, approval, or BUY rank.
"""
from __future__ import annotations
from collections import Counter
from collections.abc import Mapping
from math import isfinite
from typing import Any
import pandas as pd
from godpick_h147_five_gate_review import _code, _day, _s
from godpick_h1531_quality_repair import classify_h109

VERSION = 'V191-H153.5-RESEARCH-RECOVERY'
MATCH_FIELDS = (
    'H114K線證據日','H114K線資料狀態','H114K線有效根數',
    'H114當日收盤價','H114K線量比','H114當日成交量張',
    'H114當前5日漲跌幅%','H114當日漲跌幅%',
)
DISCLAIMER = '風控否決／同輪待核，僅供人工補證與機會成本研究；H126未授權禁止買進'


def _df(value: Any) -> pd.DataFrame:
    return value if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):return None
    try:
        n=float(str(v).strip().replace(',','').replace('%',''))
        return n if isfinite(n) else None
    except (ValueError,TypeError,OverflowError):return None


def _val(row:Mapping[str,Any], *keys:str) -> float | None:
    for key in keys:
        n=_num(row.get(key))
        if n is not None:return n
    return None


def _only_stocks(frame:pd.DataFrame) -> tuple[pd.DataFrame,dict]:
    if frame.empty or '股票代號' not in frame:
        return pd.DataFrame(),{'excluded_nonstocks':0,'duplicate_codes':0}
    out=frame.copy(deep=True)
    out['__h1535_code']=out['股票代號'].map(_code)
    invalid=int((out['__h1535_code']=='').sum())
    out=out[out['__h1535_code']!=''].copy()
    duplicated=out['__h1535_code'].duplicated(keep=False)
    dup_codes=int(out.loc[duplicated,'__h1535_code'].nunique())
    out=out.loc[~duplicated].copy()
    out['股票代號']=out.pop('__h1535_code')
    return out.reset_index(drop=True),{'excluded_nonstocks':invalid,'duplicate_codes':dup_codes}


def restore_verified_research_pool(limited:Any, full:Any, *,
                                   current_signature:Any, full_signature:Any):
    """Permit only a SAME-RUN superset of named stocks; never validate future K.

    A single ETF (e.g. 0056) must not invalidate all 2,000 valid stocks. On an
    overlapping code, any conflicting nonblank H114 fact blocks restoration.
    A blank full-scan H114 field may be filled from a SAME-RUN limited H114 row;
    extra stocks without historical evidence remain unverified/blank.
    """
    base=_df(limited)
    source=_df(full)
    meta={'status':'ORIGINAL_LIMITED_RESEARCH','limited_rows':len(base),
          'full_rows':len(source),'selected_rows':len(base),
          'research_only':True,'h126_modified':False}
    if source.empty or len(source)<=len(base):return base,meta
    if not current_signature or not full_signature or str(current_signature)!=str(full_signature):
        meta['status']='BLOCKED_RUN_SIGNATURE_MISSING_OR_DIFFERENT';return base,meta
    base_valid,base_info=_only_stocks(base)
    full_valid,full_info=_only_stocks(source)
    meta.update({'limited_excluded_nonstocks':base_info['excluded_nonstocks'],
                 'full_excluded_nonstocks':full_info['excluded_nonstocks'],
                 'limited_duplicate_codes':base_info['duplicate_codes'],
                 'full_duplicate_codes':full_info['duplicate_codes']})
    if base_valid.empty or full_valid.empty:
        meta['status']='BLOCKED_EMPTY_STOCK_POOL';return base,meta
    # Reject ambiguous duplicates of stocks common to the two pools.
    # Quarantine *all* rows of an unrelated duplicated symbol, never pick the
    # highest-scoring duplicate. Only overlap/limited duplicates block the run.
    if base_info['duplicate_codes']:
        meta['status']='BLOCKED_DUPLICATE_STOCK_ID';return base,meta
    if full_info['duplicate_codes']:
        full_codes=source.get('股票代號',pd.Series(dtype=str)).map(_code)
        dup_set=set(full_codes[full_codes.duplicated(keep=False)])-{'',}
        if dup_set.intersection(set(base_valid['股票代號'])):
            meta['status']='BLOCKED_OVERLAP_DUPLICATE_STOCK_ID';return base,meta
    base_map=base_valid.set_index('股票代號',drop=False)
    full_map=full_valid.set_index('股票代號',drop=False)
    if not set(base_map.index).issubset(set(full_map.index)):
        meta['status']='BLOCKED_NOT_SUPERSET';return base,meta
    updated=full_map.copy(deep=True)
    filled=0
    same_run_research_fields=set(MATCH_FIELDS) | {
        c for c in base_map.columns if str(c).startswith(('H114','H113','H1532'))
    }
    for code in base_map.index:
        for field in sorted(same_run_research_fields):
            if field not in base_map:continue
            left=base_map.at[code,field]
            # Opaque historical objects are NOT copied into a different frame.
            if isinstance(left,(list,dict,tuple,set)) or pd.isna(left) or str(left).strip()=='':continue
            right=updated.at[code,field] if field in updated.columns else None
            if pd.notna(right) and str(right).strip():
                if str(left).strip()!=str(right).strip():
                    meta['status']='BLOCKED_OVERLAP_H114_CONFLICT'
                    meta['conflict_code']=code;meta['conflict_field']=field
                    return base,meta
            else:
                if field not in updated.columns:updated[field]=None
                updated.at[code,field]=left
                filled+=1
    meta.update(status='VERIFIED_SAME_RUN_RESEARCH_SUPERSET',
                selected_rows=len(updated),matched_h114_fields=filled,
                warning='新增個股若缺H114同日證據只能列資料待補，不能獲取研究排名')
    return updated.reset_index(drop=True),meta


WATCH_COLUMNS=(
    '核查優先序','排名等級','研究排名','研究證據分','股票代號','股票名稱','產業',
    '研究分類','研究交易日','行情證據日','當日成交額百萬','20日均成交額百萬',
    '成交量張','量比','近5日漲幅%','行情有效根數','技術觀察證據',
    'H109風控','H109證據分類','交易資格','反證與待補','資料層級',
)


def build_verification_watchlist(diagnosis:Any, *,research_day:Any,limit:int=12,
                                 existing_codes:Any=None):
    """Rank *verification work*, never nominal winners or forecast probabilities.

    Every listed stock has valid same-session H114 and explicit ≥150M daily
    + 20-session turnover, ≥500 lots. One genuine technical/price observation
    is required; volume alone cannot pass. Hard veto is ALWAYS retained.
    """
    pool=_df(diagnosis);day=_day(research_day)
    good,quality=_only_stocks(pool)
    counts=Counter();items=[]
    for r in good.to_dict('records'):
        code=_code(r.get('股票代號'))
        if not day or _day(r.get('H114K線證據日'))!=day or _s(r.get('H114K線資料狀態'))!='VALID_OHLCV' or (_val(r,'H114K線有效根數') or 0)<60:
            counts['交易日／K線缺證']+=1;continue
        amount=_val(r,'H113當日成交額百萬','成交額百萬')
        avg=_val(r,'H113二十日均成交額百萬','20日均成交額百萬')
        lots=_val(r,'H114當日成交量張')
        ratio=_val(r,'H114K線量比')
        if amount is None or avg is None or lots is None or ratio is None:
            counts['量能缺證']+=1;continue
        if amount<150 or avg<150 or lots<500 or ratio<0.55:
            counts['流動性未達底線']+=1;continue
        close=_val(r,'H114當日收盤價');ma20=_val(r,'H114MA20');ma60=_val(r,'H114MA60')
        if close is None or close<=0:
            counts['同日收盤缺證']+=1;continue
        r5=_val(r,'H114當前5日漲跌幅%')
        dayret=_val(r,'H114當日漲跌幅%')
        if r5 is None or dayret is None:
            counts['漲跌幅未核']+=1;continue
        if r5>12 or dayret>8 or r5< -20 or dayret< -10:
            counts['過熱或急跌不屬預發動補證']+=1;continue
        facts=[];quality_score=0
        if ma20 and close>=ma20:
            facts.append('收盤守MA20');quality_score+=12
        if ma60 and close>=ma60:
            facts.append('收盤守MA60');quality_score+=8
        obv=_s(r.get('H1532OBV量價背離'))
        if obv.startswith('觀察｜'):
            facts.append('同日OBV方向改善（非大戶持股）');quality_score+=8
        bb=_s(r.get('H1532布林收斂'))
        if bb.startswith('是｜'):
            facts.append('布林帶寬收斂（方向未定）');quality_score+=6
        tscore=_val(r,'H114突破前蓄勢分')
        if tscore is not None and tscore>=55:
            facts.append('H114蓄勢分達55（非獨立回測勝率）');quality_score+=8
        if not facts:
            counts['沒有獨立技術觀察證據']+=1;continue
        # Close vs both averages is a single trend family: cap correlation.
        quality_score=min(35,quality_score)
        if -4<=r5<=7:quality_score+=8
        if -2.5<=dayret<=3:quality_score+=5
        if 0.8<=ratio<=1.6:quality_score+=5
        risk=_s(r.get('H109決策權威')) or 'UNKNOWN'
        classification=classify_h109(risk)
        if classification in ('EXPLICIT_VETO','EVIDENCE_CONFLICT'):
            category='隔離反證／禁止買進'
        else:
            category='資料待補／非交易研究'
        contra=['研究並非買進授權；H126必須獨立放行',
                'H114當輪內部行情仍需交易所來源核實',
                '尚未驗證三率、EPS／PE、籌碼與法人資金流',
                '研究母體可能不完整，不代表全市場名次']
        if classification in ('EXPLICIT_VETO','EVIDENCE_CONFLICT'):
            contra.insert(0,'H109明確否決 '+risk+' 不得解除；研究僅追查錯殺／機會成本')
        if ratio>2.5:contra.append('放量偏高，需防追價與事件波動')
        items.append({
            '核查優先序':None,'研究排名':None,'排名等級':'W｜非交易核查',
            '研究證據分':None,'股票代號':code,'股票名稱':_s(r.get('股票名稱')),
            '產業':_s(r.get('類別')),'研究分類':category,'研究交易日':str(day),
            '行情證據日':str(day),'當日成交額百萬':amount,'20日均成交額百萬':avg,
            '成交量張':lots,'量比':ratio,'近5日漲幅%':r5,
            '行情有效根數':_val(r,'H114K線有效根數'),
            '技術觀察證據':'；'.join(facts),
            'H109風控':risk,'H109證據分類':classification,
            '交易資格':DISCLAIMER,'反證與待補':'；'.join(contra),
            '資料層級':'H114當輪內部完整K線｜非官方獨立股價認證',
            '__priority':quality_score,
        })
    # Across all-risk-veto pools, still present research *audit* priorities, not buys.
    items.sort(key=lambda x:(-x['__priority'],-x['當日成交額百萬'],x['股票代號']))
    cap=max(1,min(int(limit),30))
    # H153.6: the original implementation truncated BEFORE eliminating names
    # already shown in H153 research. That loses distinct, still-qualified
    # audit candidates and makes "shown 12" different from the Excel reality.
    # Retain all pre-dedup audit counts; only the display slice is disjoint.
    existing_values=list(existing_codes) if existing_codes is not None else []
    excluded={_code(v) for v in existing_values if _code(v)}
    existing_hits=sum(item['股票代號'] in excluded for item in items)
    chosen=[item for item in items if item['股票代號'] not in excluded][:cap]
    # W ranks are independent *verification priorities*. After deduplication
    # they must start from 1, not inherit a hidden stock's old ordinal.
    previous=None;ordinal=0;rank=0
    for item in chosen:
        ordinal+=1
        if item['__priority']!=previous:rank=ordinal;previous=item['__priority']
        item['核查優先序']=rank
        item.pop('__priority',None)
    summary=[
        {'檢查項目':'H153.5補證優先觀察候選總數','結果':len(items),
         '說明':'研究核查優先序，不能稱正式推薦或當日全市場排名'},
        {'檢查項目':'H153.5補證優先榜顯示列','結果':len(chosen),
         '說明':'排除01表已顯示個股後的實際不同股票數；不保證收益'},
        {'檢查項目':'H153.6核查榜與既有研究重疊','結果':existing_hits,
         '說明':'先去重再取前12；已出現的股票不佔核查榜名額'},
        {'檢查項目':'H153.5其中H109明確否決','結果':sum(x['H109證據分類'] in ('EXPLICIT_VETO','EVIDENCE_CONFLICT') for x in chosen),
         '說明':'只做風控反證，禁止放入H126買進或正式推薦清單'},
        {'檢查項目':'H153.5研究母體有效代號','結果':len(good),
         '說明':'ETF及重複股票不參與，不把120檔稱為全市場'},
    ]
    summary.extend({'檢查項目':'H153.5未入核查榜-'+k,'結果':v,'說明':'不可當作個股必跌證據'} for k,v in counts.most_common())
    meta={'version':VERSION,'limited_rows':len(pool),'valid_stocks':len(good),
          'total_watch':len(items),'shown_watch':len(chosen),
          'existing_research_overlap':existing_hits,
          'removed_nonstocks':quality['excluded_nonstocks'],
          'duplicate_codes':quality['duplicate_codes'],
          'formal_trade_modified':False,'h109_modified':False,
          'ranking_rule':'核查順位依當輪日期/量能/可解釋技術證據；同分並列，不是上漲機率或正式排名'}
    return pd.DataFrame(chosen,columns=WATCH_COLUMNS),pd.DataFrame(summary),meta


def merge_nontrading_watchlist(research:Any,watch:Any)->pd.DataFrame:
    """Show verified watch leads only if not already visible; never fill H126 or 5 plans."""
    original=_df(research).copy(deep=True)
    leads=_df(watch)
    if leads.empty:return original
    existing={_code(c) for c in original.get('股票代號',pd.Series(dtype=str)).tolist()}
    additions=leads.loc[~leads['股票代號'].isin(existing)].copy()
    if additions.empty:return original
    additions['H153成長與技術主題']='非交易：同日技術核查優先（非買進）'
    additions['H153來源與反證']=additions['反證與待補']
    additions['研究層級']='W｜風控否決／待補證核查，不得進場'
    additions['來源證據']='H153.5同輪只讀補證，非H126交易權威'
    if original.empty:return additions.reset_index(drop=True)
    return pd.concat([original,additions],ignore_index=True,sort=False)

__all__=['VERSION','WATCH_COLUMNS','restore_verified_research_pool',
         'build_verification_watchlist','merge_nontrading_watchlist']
