# -*- coding: utf-8 -*-
"""H153 | as-of growth/value + pre-breakout technical evidence + explicit tied ranks.

Research layer only: never grants or rewrites H126, H109, H113 or user data.
Every fundamental statement requires independently timestamped TWSE/TPEx/MOPS
observations. A missing observation is not a negative growth signal and is never
awarded ranking points. Ranks are *research ordering*, not return probabilities.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
from statistics import median
from math import isfinite
from typing import Any, Mapping
import pandas as pd
from godpick_h147_five_gate_review import _code, _day, _s
from godpick_h114_parallel_alpha import evaluate as evaluate_h114
from godpick_h1531_quality_repair import classify_h109, liquidity_reasons, source_coverage

VERSION = 'V191-H153.1-GROWTH-TECH-PIT-QUALITY'
RANK_COLUMNS = (
    '研究排名', '排名等級', '研究證據分', '研究主題', '股票代號', '股票名稱', '產業',
    '行情證據日', '技術型態', '黃金交叉', '箱型型態', '本益比', '近四季EPS',
    '營收年增率%', '三月營收年增率%', '營收創高', '相對同業PE折價%',
    '基本面來源狀態', '下月營收模型情境(百萬)', '營收預估創高判定', '預估資料限制',
    '技術來源狀態', '量能驗證', '法人／大戶', 'H109風控',
    '研究成立理由', '反證與待補', '研究聲明',
    'H109證據分類','原始PE_EPS狀態','已公告營收月數',
    '當日成交額百萬','20日均成交額百萬','成交量張','量比','近5日漲幅%',
    '3日相對抗跌pp','5日相對抗跌pp','多日抗跌來源狀態',
)
WATCH_ONLY = '研究名次僅為已驗證資訊的相對優先序；非明日上漲機率、非H126買進授權'


def _frame(value: Any) -> pd.DataFrame:
    return value if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        x = float(str(value).strip().replace(',', '').replace('%',''))
        return x if isfinite(x) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _n(rec: Mapping[str, Any], *names: str) -> float | None:
    for name in names:
        v = _num(rec.get(name))
        if v is not None: return v
    return None


def _index(frame: pd.DataFrame) -> tuple[dict[str,dict],set[str],int]:
    out={};bad=set(); invalid=0
    for rec in frame.to_dict('records'):
        code = _code(rec.get('股票代號'))
        if not code:invalid+=1;continue
        if code in out:bad.add(code)
        else:out[code]=rec
    for code in bad:out.pop(code,None)
    return out,bad,invalid


def _good_day(rec: Mapping[str, Any], day: date | None) -> bool:
    return bool(day and _day(rec.get('H114K線證據日')) == day and
                _s(rec.get('H114K線資料狀態')) == 'VALID_OHLCV' and
                (_n(rec,'H114K線有效根數') or 0)>=60 and
                (_n(rec,'H114當日收盤價') or 0)>0)


def forecast_revenue_scenario(history: Any, asof: Any) -> dict[str, Any]:
    """Illustrative next-month scenario from 24 previously published months.

    NOT official revenue, forecast consensus, probability, or a trained model.
    Need complete PIT monthly data and same-month prior-year baseline.
    """
    empty={'estimate_m':None,'record_state':'資料不足／未估算',
           'note':'需要已公告連續24個月營收、來源與公告日；未經樣本外校準'}
    if not isinstance(history,(list,tuple)):
        return empty
    from godpick_h114_parallel_alpha import _ym, _idx, _d, _f
    study=_day(asof)
    if not study:return empty
    month_index=study.year*12+study.month-1
    records={}
    for r in history:
        if not isinstance(r,Mapping):continue
        ym=_ym(r.get('year_month') or r.get('年月'))
        release=_d(r.get('released_at') or r.get('公告日'))
        source=_s(r.get('source') or r.get('來源')).upper()
        val=_f(r.get('revenue_twd') if 'revenue_twd' in r else r.get('營收元'))
        if (not ym or not release or release>study or source not in {'TWSE','TPEX','MOPS'}
            or val is None or val<=0 or _idx(ym)>=month_index):
            continue
        index=_idx(ym)
        records[index]=val
    if not records:return empty
    newest=max(records)
    if month_index-newest>2 or not all(newest-i in records for i in range(24)):
        return empty
    yoy=[]
    for i in range(3):
        baseline=records.get(newest-i-12)
        actual=records.get(newest-i)
        if not baseline or not actual:return empty
        yoy.append(actual/baseline-1)
    last_year_next=records.get(newest-11)
    if not last_year_next:return empty
    growth=max(-0.5,min(1.0,median(yoy)))
    prediction=last_year_next*(1+growth)
    high=max(records[newest-i] for i in range(24))
    possible=prediction>high
    return {'estimate_m':round(prediction/1_000_000,2),
            'record_state':('模型情境可能創高｜未驗證' if possible else '模型情境未高於既有24月高點'),
            'note':'僅以已公告24月資料及近三月年增中位數推估下月，非官方實績、未校準預測、不得加研究分'}


def build_growth_setup_ranking(*, diagnosis_frame: Any, research_frame: Any,
                               research_day: Any, verified_fundamentals: Any=None,
                               limit: int=30) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Rank same-session liquid research candidates using observable signals.

    Evidence gate excludes duplicate/ETF/stale/no-volume rows from ranked list.
    A technical research stock may rank without published fundamentals, but is
    explicitly labelled as *unverified growth*, never certified growth/value.
    Equal scores share a rank; ticker only stabilizes display within a tie.
    """
    diagnosis=_frame(diagnosis_frame)
    research=_frame(research_frame)
    day=_day(research_day)
    raw,dupes,invalid=_index(diagnosis)
    existing,_,_=_index(research)
    facts=verified_fundamentals if isinstance(verified_fundamentals,Mapping) else {}
    counts=Counter(); rows=[]
    for code, r in raw.items():
        if code.startswith(('00','01')) and code.isdigit():
            counts['ETF或其他非一般個股（保留來源分類）']+=1;continue
        if not _good_day(r,day):
            counts['同日有效K線／60根證據不足']+=1;continue
        amount=_n(r,'H113當日成交額百萬','成交額百萬')
        avg=_n(r,'H113二十日均成交額百萬','20日均成交額百萬')
        lots=_n(r,'H114當日成交量張')
        ratio=_n(r,'H114K線量比')
        # H153.1: an independently auditable reason per missing or failing
        # field. Do not call a missing official observation "low liquidity".
        failed=liquidity_reasons(amount=amount,average=avg,lots=lots,ratio=ratio)
        if failed:
            counts['流動性／量能未完成研究門檻（不重複股票數）']+=1
            for why in failed:counts[why]+=1  # reasons can overlap
            continue
        ret5=_n(r,'H114當前5日漲跌幅%')
        today=_n(r,'H114當日漲跌幅%')
        if ret5 is None or today is None:
            counts['近期漲跌幅未驗證']+=1;continue
        # H114 technical signals are backed by the SAME dated OHLCV input.
        cross=_s(r.get('H114黃金交叉'))
        box=_s(r.get('H114箱型狀態'))
        pre=_s(r.get('H114量縮墊高'))=='是'
        compress=_s(r.get('H114波動收斂'))=='是'
        gap=_n(r,'H114距20日高點%')
        golden=cross.startswith('確認')
        breakout=box.startswith('確認突破')
        box_ready=box.startswith('接近箱頂')
        near=gap is not None and 0<=gap<=6
        existing_r=existing.get(code,{})
        old_tier=_s(existing_r.get('研究層級'))
        resilience=old_tier.startswith('R')
        setup=pre or compress or box_ready or near or golden or breakout or resilience
        if not setup:
            counts['無可核對蓄勢／黃金交叉／箱型／抗跌證據']+=1;continue
        if ret5>12 or today>8:
            counts['已有大幅上漲／非優先預發動']+=1;continue
        inst=_s(r.get('H123法人實證狀態')) or 'PENDING_EVIDENCE'
        tdcc=_s(r.get('H124大戶實證狀態')) or 'PENDING_EVIDENCE'
        risk=_s(r.get('H109決策權威')) or 'UNKNOWN'
        # Evaluation does not fetch network data; only PIT-verified official
        # evidence, filtered by release date, can score fundamentals.
        independent=facts.get(code,{})
        if not isinstance(independent,Mapping):independent={}
        enriched=dict(r)
        enriched.update({k:v for k,v in independent.items() if str(k).startswith(('H114','H115'))})
        f=evaluate_h114(enriched,info_asof=day)
        scenario=forecast_revenue_scenario(enriched.get('H114月營收歷史'),day)
        revenue_verified=f.get('H114營收資料狀態')=='VERIFIED_ASOF'
        value_verified=f.get('H114估值資料狀態')=='VERIFIED_ASOF'
        yoy=_num(f.get('H114營收年增率%'))
        yoy3=_num(f.get('H114三月累計年增率%'))
        discount=_num(f.get('H114相對本益比折價%'))
        # Official single-name PE/EPS do not require enough comparable firms.
        # But both must come from the separate H115 PIT ledger, not arbitrary
        # stock-pool fields or a post-decision price quote.
        value_day=_day(independent.get('H114估值資料日期'))
        value_source=_s(independent.get('H114估值資料來源')).upper()
        pe_raw=_n(independent,'H114本益比實值')
        eps_raw=_n(independent,'H114近四季EPS實值')
        raw_valuation_ok=bool(day and value_day and 0 <= (day-value_day).days<=14
            and value_source in {'TWSE','TPEX','MOPS','OFFICIAL_VERIFIED'}
            and pe_raw is not None and pe_raw>0 and eps_raw is not None and eps_raw>0)
        pe=pe_raw if raw_valuation_ok else None
        eps=eps_raw if raw_valuation_ok else None
        raw_valuation_state=('官方同日可得PE/EPS，缺同業比較不等於缺原始資料'
            if raw_valuation_ok and not value_verified else
            '官方PE/EPS及同業比較均已核' if raw_valuation_ok else '原始PE/EPS待來源或時點驗證')
        history=independent.get('H114月營收歷史')
        history_count=len({str(x.get('year_month')) for x in history if isinstance(x,Mapping) and x.get('year_month')}) if isinstance(history,list) else 0
        growth=bool(revenue_verified and yoy is not None and yoy>=15 and yoy3 is not None and yoy3>=12)
        cheap=bool(value_verified and discount is not None and discount>=15)
        # Score is an explicit evidence-weighted research index, NOT odds.
        score=24  # same-day valid OHLCV AND explicit turnover/volume checks
        evidence=[]
        if pre and compress:score+=20;evidence.append('量縮墊高＋波動收斂')
        elif pre or compress:score+=10;evidence.append('蓄勢型態初現')
        if box_ready:score+=13;evidence.append('接近箱頂，尚未突破')
        if breakout:score+=17;evidence.append('箱型確認突破，需防假突破')
        if golden:score+=13;evidence.append(cross)
        if near and not(box_ready or breakout):score+=7;evidence.append('距20日高點6%內')
        if resilience:score+=7;evidence.append('逆勢抗跌研究，待核市場同日證據')
        if growth:score+=14;evidence.append('公告營收年增與三月動能同向')
        if revenue_verified and f.get('H114營收創高')=='近12月新高':score+=7;evidence.append('已公布營收近12月新高')
        if cheap:score+=8;evidence.append('官方同日同業PE折價達15%')
        if inst=='PROVEN_FLOW':score+=4;evidence.append('H123法人來源已證')
        if tdcc=='VERIFIED_RISING':score+=4;evidence.append('H124週資料已證，非鎖單證明')
        # Do not mislabel high EPS: positive EPS alone is not peer-high EPS.
        # Growth/value signals must both be PIT-vetted before the label.
        if growth and cheap and (pre or box_ready or golden):theme='高成長低估／技術蓄勢'
        elif growth and (pre or box_ready or golden):theme='營收成長／預發動'
        elif pre and compress:theme='量縮蓄勢／尚未發動'
        elif box_ready:theme='箱型準突破'
        elif golden:theme='均線黃金交叉'
        elif breakout:theme='箱型突破／防追高'
        elif resilience:theme='相對抗跌／等待資金確認'
        else:theme='技術蓄勢研究'
        # H153.1: UNKNOWN is NOT a veto, but is also NOT a pass.
        h109_class=classify_h109(risk)
        veto=h109_class in {'EXPLICIT_VETO','EVIDENCE_CONFLICT'}
        if veto:score=max(0,score-12)
        score=min(100,score)
        if veto:
            tier='C｜明確否決／來源衝突隔離'
        elif growth and (pre or box_ready or golden) and h109_class=='RESEARCH_ALLOWED':
            tier='A｜成長與技術交叉研究'
        elif h109_class=='PENDING_EVIDENCE':
            tier='B｜研究證據待補（不得交易）'
        else:
            tier='B｜量價技術研究'
        missing=['H126未授權，研究名次不得下單']
        if veto:missing.append('H109:'+risk+' 已明確否決／來源衝突，禁止混入可用研究')
        elif h109_class=='PENDING_EVIDENCE':missing.append('H109:'+risk+' 狀態待核，不是已否決亦非已授權')
        if not revenue_verified:missing.append('官方逐月營收公告證據不足')
        if not raw_valuation_ok:missing.append('官方原始PE／EPS證據不足')
        elif not value_verified:missing.append('同日同業PE樣本不足；原始PE／EPS仍可研究')
        if not growth and revenue_verified:missing.append('營收成長尚未滿足研究假設')
        if inst!='PROVEN_FLOW':missing.append('H123法人待證')
        if tdcc!='VERIFIED_RISING':missing.append('H124集保週增待證')
        missing.append('隔日方向／大漲機率未校準，跌破箱底或均線需重審')
        rows.append({
            '研究排名':None,'排名等級':tier,'研究證據分':int(score),'研究主題':theme,
            '股票代號':code,'股票名稱':_s(r.get('股票名稱')) or _s(existing_r.get('股票名稱')),
            '產業':_s(r.get('類別')) or _s(existing_r.get('產業')),
            '行情證據日':str(day),'技術型態':'／'.join(evidence) or '證據不足',
            '黃金交叉':cross or '未驗證','箱型型態':box or '未驗證',
            '本益比':pe,'近四季EPS':eps,'營收年增率%':yoy if revenue_verified else None,
            '三月營收年增率%':yoy3 if revenue_verified else None,
            '營收創高':f.get('H114營收創高') if revenue_verified else '未驗證',
            '相對同業PE折價%':discount if value_verified else None,
            '基本面來源狀態':f"營收:{f.get('H114營收資料狀態')}；原始估值:{raw_valuation_state}；同業:{f.get('H114估值資料狀態')}",
            '下月營收模型情境(百萬)':scenario['estimate_m'],
            '營收預估創高判定':scenario['record_state'],
            '預估資料限制':scenario['note'],
            '技術來源狀態':'H114同日VALID_OHLCV ≥60根','量能驗證':f'成交額{amount:g}/均額{avg:g}百萬；量{lots:g}張；比{ratio:g}',
            '法人／大戶':inst+'／'+tdcc,'H109風控':risk,
            '研究成立理由':'；'.join(evidence) or '已核對量價',
            '反證與待補':'；'.join(missing),'研究聲明':WATCH_ONLY,
            'H109證據分類':h109_class,'原始PE_EPS狀態':raw_valuation_state,
            '已公告營收月數':history_count,
            '當日成交額百萬':amount,'20日均成交額百萬':avg,
            '成交量張':lots,'量比':ratio,'近5日漲幅%':ret5,
        })
    # Verified research clearance ranks ahead of unknown; formal H126 stays untouched.
    tier_num=lambda x:int({'A':0,'B':1,'C':2}.get(str(x['排名等級'])[:1],3))
    risk_num=lambda x:0 if x.get('H109證據分類')=='RESEARCH_ALLOWED' else (1 if x.get('H109證據分類')=='PENDING_EVIDENCE' else 2)
    rows.sort(key=lambda x:(tier_num(x),risk_num(x),-x['研究證據分'],x['股票代號']))
    previous=None; ordinal=0;rank=0
    for row in rows:
        ordinal+=1
        key=(tier_num(row),risk_num(row),row['研究證據分'])
        if key!=previous:rank=ordinal;previous=key
        row['研究排名']=rank # 1,1,3 competition ranks; stable ticker NOT a tie breaker
    cap=max(1,min(int(limit),50))
    top=rows[:cap]
    coverage=source_coverage(facts)
    summary=[
        {'檢查項目':'H153有效診斷池','結果':len(raw),'說明':'不是證交所全市場母體保證'},
        {'檢查項目':'H153.1 H109研究待補證','結果':sum(r['H109證據分類']=='PENDING_EVIDENCE' for r in rows),'說明':'不再誤判為風控否決；H126仍禁止未授權買進'},
        {'檢查項目':'H153.1 H109明確否決／來源衝突','結果':sum(r['H109證據分類'] in {'EXPLICIT_VETO','EVIDENCE_CONFLICT'} for r in rows),'說明':'保留隔離，不讓否決股擠入正常研究榜'},
        {'檢查項目':'H153.1有官方原始PE/EPS排名股','結果':sum(r['本益比'] is not None and r['近四季EPS'] is not None for r in rows),'說明':'不依賴同業樣本；同業折價須另有官方可比證據'},

        {'檢查項目':'H153研究證據排名候選','結果':len(rows),'說明':'排名按等級與有來源證據分，同分並列；不是預測漲幅'},
        {'檢查項目':'H153已揭露名單','結果':len(top),'說明':'完整候選可稽核；不以資料缺失股票湊榜'},
        {'檢查項目':'H153營收與技術交叉A級','結果':sum(r['排名等級'].startswith('A') for r in rows),'說明':'已公告營收／技術型態共同驗證，仍非買進資格'},
        {'檢查項目':'H153剔除重複代號','結果':len(dupes),'說明':'同代號多列不挑對排名有利的一列'},
        {'檢查項目':'H153禁止誤稱預測機率','結果':'T+1/T+5/T+10/T+20 待樣本外驗證','說明':'研究證據分並非上漲機率或獲利機率'},
    ]+[{'檢查項目':'H153.1基本面覆蓋-'+k,'結果':v,'說明':'只反映H115已可讀PIT帳本，不代表全市場覆蓋'} for k,v in sorted(coverage.items())] + [{'檢查項目':'H153排除-'+k,'結果':v,'說明':'原因可能重疊，非股票不佳或必跌'} for k,v in counts.most_common()]
    meta={'version':VERSION,'research_day':str(day) if day else '未驗證',
          'total':len(rows),'shown':len(top),'duplicate':len(dupes),
          'reasons':dict(counts),'h115_coverage':coverage,'formal_h126_modified':False,
          'ranking_rule':'先A/B/C級、H109已驗證／待核、再研究證據分；同級同狀態同分並列；代碼只供穩定顯示',
          'warning':WATCH_ONLY}
    return pd.DataFrame(top,columns=RANK_COLUMNS),pd.DataFrame(summary),meta


def attach_rank_to_research(research_frame: Any, ranked: Any) -> pd.DataFrame:
    """Preserve ALL original research rows in a ranked research workbook sheet.

    Verified ranked evidence is displayed first. Unverified old research rows
    remain after ranked rows with blank rank, instead of losing candidates.
    """
    frame=_frame(research_frame).copy(deep=True)
    rank=_frame(ranked)
    if rank.empty:
        for col in ('研究排名','排名等級','研究證據分','H153成長與技術主題','H153來源與反證'):
            if col not in frame.columns:frame[col]=None
        return frame
    ordered={_code(x['股票代號']):x for x in rank.to_dict('records')}
    seen=set();rows=[]
    for x in frame.to_dict('records'):
        code=_code(x.get('股票代號'))
        if code in seen:continue
        seen.add(code)
        info=ordered.get(code)
        if info:
            old_fields=dict(x)
            x.update({key:info.get(key) for key in RANK_COLUMNS if key not in ('股票代號','股票名稱','產業')})
            # H153.1: old H148.3 columns must not be empty while SAME dated
            # H153 evidence exists. Detect conflicts rather than hiding them.
            aliases={'H114行情日期':'行情證據日',
                     '當日成交額百萬':'當日成交額百萬',
                     '20日均成交額百萬':'20日均成交額百萬',
                     '成交量張':'成交量張','量比':'量比',
                     '近5日漲幅%':'近5日漲幅%'}
            conflicts=[]
            for legacy,verified in aliases.items():
                trusted=info.get(verified)
                if trusted is None:continue
                prior=old_fields.get(legacy)
                if prior is not None and not pd.isna(prior) and str(prior).strip() not in {'','未核實','未驗證'}:
                    a=_num(prior); b=_num(trusted)
                    difference=abs(a-b)>0.001 if a is not None and b is not None else str(prior)!=str(trusted)
                    if difference:conflicts.append(f'{legacy}原值={prior}')
                x[legacy]=trusted
            x['H153.1欄位對齊']='已按同股同日H114核實資料填入' if not conflicts else '同股資料衝突，採本輪同日證據'
            x['H153.1原值差異']='；'.join(conflicts)
            x.update({'H153成長與技術主題':info['研究主題'],'H153來源與反證':info['反證與待補']})
        else:
            x.update({'研究排名':None,'排名等級':'待核｜原研究線索',
                      '研究證據分':None,'H153成長與技術主題':'原始觀察，未達H153名次證據門檻',
                      'H153來源與反證':'資料不足不得取得研究名次；不改原風控'})
        rows.append(x)
    # Include newly found technical+growth candidates outside old H113/H148 research.
    for code,info in ordered.items():
        if code not in seen:
            rows.append({**info,'H114行情日期':info.get('行情證據日'),
                         'H153.1欄位對齊':'已按同股同日H114核實資料填入',
                         'H153.1原值差異':'',
                         '研究層級':info['排名等級'],
                         'H153成長與技術主題':info['研究主題'],
                         'H153來源與反證':info['反證與待補'],
                         '來源證據':'H153當輪技術／官方基本面研究；非H126交易',
                         '尚需驗證與不得買進原因':info['反證與待補']})
    # ensure the chief's four rank columns precede old wide pool detail
    priority=['研究排名','排名等級','研究證據分','股票代號','股票名稱',
              '研究主題','本益比','近四季EPS','營收年增率%','營收創高',
              '黃金交叉','箱型型態','H109證據分類','反證與待補',
              'H153.1欄位對齊','H153.1原值差異','H153成長與技術主題','H153來源與反證']
    result=pd.DataFrame(rows)
    result['__rank_tier']=result['排名等級'].astype(str).str[:1].map({'A':0,'B':1,'C':2}).fillna(3)
    result['__rank_sort']=pd.to_numeric(result['研究排名'],errors='coerce').fillna(999999)
    result=result.sort_values(['__rank_tier','__rank_sort','股票代號'],kind='stable').drop(columns=['__rank_tier','__rank_sort'])
    for c in priority:
        if c not in result.columns:result[c]=None
    return result[priority+[c for c in result.columns if c not in priority]]


def attach_rank_to_plans(plans_frame: Any, ranked: Any, *, limit: int=5) -> pd.DataFrame:
    """Rank real H149 plans; do not manufacture entry/stop/target data."""
    plans=_frame(plans_frame).copy(deep=True)
    rank=_frame(ranked)
    if plans.empty:return plans
    bycode={_code(x['股票代號']):x for x in rank.to_dict('records')}
    plans['研究排名']=plans['股票代號'].map(lambda c:bycode.get(_code(c),{}).get('研究排名'))
    plans['H153研究證據分']=plans['股票代號'].map(lambda c:bycode.get(_code(c),{}).get('研究證據分'))
    plans['H153高成長／技術證據']=plans['股票代號'].map(lambda c:bycode.get(_code(c),{}).get('研究主題','未完成H153核實'))
    plans['H153研究非買進']=WATCH_ONLY
    plans['_rank']=pd.to_numeric(plans['研究排名'],errors='coerce').fillna(999999)
    plans=plans.sort_values(['_rank','股票代號'],kind='stable').drop(columns=['_rank'])
    return plans.head(max(1,min(int(limit),20)))



def build_ranked_five_plans(*, ranked: Any, diagnosis_frame: Any,
                            previous_plans: Any, research_day: Any,
                            limit: int=5) -> pd.DataFrame:
    """Translate top research ranks into auditable plans, never fabricated prices.

    Preserve existing H149 columns and populate them only from original H149
    plans or same-day verified raw H114 support/resistance, otherwise blank.
    """
    rank=_frame(ranked)
    plans=_frame(previous_plans)
    # C = explicit H109 veto or source conflict, and must never be used to
    # fill an otherwise-empty manager five-stock research opportunity list.
    # This changes only the research *presentation*, not H126 permissions.
    if rank.empty:
        return pd.DataFrame(columns=['研究排名','研究優先序','H153研究證據分',
                                      '股票代號','股票名稱','研究機會','反證／失效條件','交易資格'])
    rank=rank[rank['排名等級'].astype(str).str.startswith(('A','B'))].copy()
    if rank.empty:
        return pd.DataFrame(columns=['研究排名','研究優先序','H153研究證據分',
                                      '股票代號','股票名稱','研究機會','反證／失效條件','交易資格'])
    raw,_,_=_index(_frame(diagnosis_frame))
    old,_dup,_invalid=_index(plans)
    day=_day(research_day)
    # Reuse H149 validated quote/level contract and never infer a target.
    from godpick_h149_trade_intelligence import _valid_close_levels
    lines=[]
    for ref in rank.head(max(1,min(limit,20))).to_dict('records'):
        code=ref['股票代號']
        o=dict(old.get(code,{}))
        price,support,resistance,rr,note=_valid_close_levels(raw.get(code,{}),day)
        o.update({'3日相對抗跌pp':ref.get('3日相對抗跌pp'),
                  '5日相對抗跌pp':ref.get('5日相對抗跌pp'),
                  '多日抗跌來源狀態':ref.get('多日抗跌來源狀態'),
                  '研究優先序':f"同分並列第{ref['研究排名']}名（H153研究非買進）",
                  '研究排名':ref['研究排名'], 'H153研究證據分':ref['研究證據分'],
                  'H153高成長／技術證據':ref['研究主題'],
                  '股票代號':code,'股票名稱':ref['股票名稱'],
                  '研究層級':ref['排名等級'],'研究機會':ref['研究主題'],
                  '成立證據':ref['研究成立理由'], '反證／失效條件':ref['反證與待補'],
                  '研究現價':price,'支撐價':support,'壓力價':resistance,
                  '參考風險報酬比':rr,'價位計算狀態':note,
                  '交易資格':'研究觀察｜非H126買進授權','H153研究非買進':WATCH_ONLY})
        lines.append(o)
    out=pd.DataFrame(lines)
    p=['研究排名','研究優先序','H153研究證據分','股票代號','股票名稱','研究機會','反證／失效條件']
    for k in p:
        if k not in out.columns:out[k]=None
    return out[p+[c for c in out.columns if c not in p]]

__all__=['VERSION','RANK_COLUMNS','build_growth_setup_ranking','attach_rank_to_research','attach_rank_to_plans','build_ranked_five_plans','forecast_revenue_scenario']
