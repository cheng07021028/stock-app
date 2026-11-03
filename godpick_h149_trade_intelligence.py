# -*- coding: utf-8 -*-
"""V191-H149: evidence-first six-pillar manager research, NOT a trade engine.

No network, persistence, streamlit, orders, modified H109/H126 gates or AI
fabrication. Reuses ONLY the exact current-run H148.3 observations and their
same-stock diagnostic records. An absent source is NOT a bearish fact.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
from math import isfinite
from typing import Any, Mapping

import pandas as pd
from godpick_h147_five_gate_review import _code, _day, _s

VERSION = 'V191-H149-SIX-PILLAR-EVIDENCE-FIRST'
NO_TRADE = '研究觀察／H126未授權不得買進'
OPPORTUNITY_COLUMNS = (
    '研究優先序','股票代號','股票名稱','研究層級','研究機會','成立證據','反證／失效條件',
    '研究現價','支撐價','壓力價','參考風險報酬比','價位計算狀態',
    '主流族群','法人／大戶','消息證據','交易資格',
)
SUMMARY_COLUMNS = ('研究面向','本輪事實','主要盲點／反證','下一步')


def _frame(value: Any) -> pd.DataFrame:
    return value if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _num(value: Any) -> float | None:
    text = _s(value).replace(',', '').replace('%', '')
    if not text:
        return None
    try:
        f = float(text)
        return f if isfinite(f) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _pick(row: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in row:
            value = _num(row.get(key))
            if value is not None:
                return value
    return None


def _safe_excel(value: Any) -> Any:
    # User-provided ticker/name/news must never become a workbook formula.
    if isinstance(value, str) and value.lstrip().startswith(('=', '+', '-', '@')):
        return "'" + value
    return value


def _indexed(frame: pd.DataFrame) -> tuple[dict[str, dict], set[str]]:
    first: dict[str, dict] = {}
    duplicates: set[str] = set()
    for row in frame.to_dict('records'):
        code = _code(row.get('股票代號'))
        if not code:
            continue
        if code in first:
            duplicates.add(code)
        else:
            first[code] = row
    for code in duplicates:
        first.pop(code, None)
    return first, duplicates


def _valid_close_levels(raw: Mapping[str, Any], day: date | None) -> tuple[Any,Any,Any,Any,str]:
    """Never synthesize a target, stop or market quote without same-day evidence."""
    evidence_day = _day(raw.get('H114K線證據日'))
    bars = _pick(raw,'H114K線有效根數')
    state = _s(raw.get('H114K線資料狀態')).upper()
    same = bool(day and evidence_day == day and bars is not None and bars >= 60
                and 'VALID_OHLCV' in state)
    if not same:
        return None,None,None,None,'行情日／60根有效K線未核，不提供價位'
    close = _pick(raw,'H114當日收盤價','當日收盤價','收盤價','最新收盤價')
    support = _pick(raw,'H114有效支撐價','有效支撐價','支撐價')
    resistance = _pick(raw,'H114有效壓力價','有效壓力價','壓力價')
    if close is None or close <= 0:
        return None,None,None,None,'缺本交易日可核對收盤價，不捏造進場價'
    if support is None or resistance is None or not (0 < support < close < resistance):
        return round(close,3), None, None, None,'僅提供同日收盤參考；有效支撐／壓力未證，不產生目標／停損'
    rr = (resistance - close) / (close - support)
    if not (0 < rr <= 100):
        return round(close,3), None, None, None,'支撐／壓力數據不合理，禁止推算風險報酬'
    return (round(close,3), round(support,3), round(resistance,3),
            round(rr,2), '同股同日已有支撐／壓力；僅為假設性研究，不是H126授權')


def _journal_diagnostics(closed_trades: Any) -> tuple[str,str]:
    """Only explicit *executed and closed* trades may inform hit-rate/PF.

    No mixing recommendation snapshots, hypothetical signals or live positions.
    """
    trades = _frame(closed_trades)
    required = {'成交狀態','進場日期','出場日期','已實現報酬率%'}
    if trades.empty or not required.issubset(trades.columns):
        return '未提供已成交且已平倉的完整交易紀錄', '不可回測成真實勝率；先提供成交日期、進出場與成本'
    results=[]
    for row in trades.tail(20).to_dict('records'):
        if _s(row.get('成交狀態')).upper() not in {'已平倉','CLOSED_FILLED'}:
            continue
        start,end=_day(row.get('進場日期')),_day(row.get('出場日期'))
        rate=_num(row.get('已實現報酬率%'))
        if start is None or end is None or end<start or rate is None or rate < -100:
            continue
        results.append(rate)
    if len(results)<5:
        return f'已驗證平倉樣本{len(results)}筆，樣本不足', '至少五筆才能提供描述統計；未驗證進出場成本及滑價時禁止聲稱策略優勢'
    winners=sum(v>0 for v in results)
    gains=sum(v for v in results if v>0)
    losses=-sum(v for v in results if v<0)
    pf=f'{gains/losses:.2f}' if losses>0 else '未定義（樣本無虧損）'
    # These are user-reported closed trades, not a strategy backtest.
    return (f'最近{len(results)}筆已平倉紀錄：正報酬{winners/len(results):.1%}，正負報酬總和比{pf}',
            '僅是已成交樣本描述，非策略回測；須排除挑樣本及計入交易費用')


def build_six_pillar_review(*, diagnosis_frame: Any, research_frame: Any,
                            authority_frame: Any, research_day: Any,
                            h145_bridge: Mapping[str,Any] | None = None,
                            closed_trades: Any = None, holdings: Any = None,
                            feedback_health: Any = None,
                            limit: int = 5) -> tuple[pd.DataFrame,pd.DataFrame,dict[str,Any]]:
    """Read-only, transparent five-lead brief plus six-dimensional blind spots.

    No request for historical backtest data, external news or real holdings is
    made. Missing details remain UNKNOWN. Never joins data by fuzzy ticker or
    crosses scan dates. H126 is not recalculated or rewritten.
    """
    diagnosis, research = _frame(diagnosis_frame), _frame(research_frame)
    day = _day(research_day)
    source, duplicates = _indexed(diagnosis)
    authority = _frame(authority_frame)
    authority_codes = {_code(r.get('股票代號')) for r in authority.to_dict('records')}
    authority_codes.discard('')
    bridge = h145_bridge if isinstance(h145_bridge, Mapping) else {}
    scope_proven = bool(bridge.get('same_run_proven') is True and bridge.get('candidate_complete') is True)
    seen: set[str] = set()
    opportunities=[]
    exclusions=Counter()
    for row in research.to_dict('records'):
        code = _code(row.get('股票代號'))
        if not code or code in duplicates or code in seen or code not in source:
            exclusions['缺失／重複代號或診斷來源']+=1
            continue
        seen.add(code)
        raw=source[code]
        tier=_s(row.get('研究層級'))
        if not tier.startswith(('P1｜','P2｜','A｜','B｜','C｜')):
            exclusions['研究層級不明']+=1
            continue
        amount=_pick(raw,'H113當日成交額百萬','成交額百萬')
        lots=_pick(raw,'H114當日成交量張','H113當日成交量張','最新成交量_張')
        ratio=_pick(raw,'H114K線量比')
        if not (amount is not None and amount>=150 and lots is not None and lots>=500 and ratio is not None and
                ratio >= (0.55 if tier.startswith(('P1｜','P2｜')) else 1.2)):
            exclusions['量價證據不足']+=1
            continue
        quote, support, resistance, rr, price_note=_valid_close_levels(raw,day)
        risk=_s(raw.get('H109決策權威')) or _s(row.get('H109風控')) or 'UNKNOWN'
        inst=_s(raw.get('H123法人實證狀態')) or 'PENDING_EVIDENCE'
        tdcc=_s(raw.get('H124大戶實證狀態')) or 'PENDING_EVIDENCE'
        sector=_s(raw.get('類別')) or _s(row.get('產業')) or '產業未證'
        phase=_s(raw.get('H108族群生命週期')) or '缺來源'
        signal=_s(row.get('突破／續攻判定')) or '動能未辨識'
        note=_s(raw.get('消息標題'))
        news_date=_day(raw.get('消息日期'))
        news_source=_s(raw.get('消息來源網址'))
        # Headlines are NOT causal or price forecasts. News is dated and
        # referenced only if a source link AND event date are both supplied.
        news=(f'事件待核：{note[:48]}（{news_date.isoformat()}；有來源連結）'
              if note and news_date and news_source and day and news_date<=day
              else '未有可查證同輪新聞證據；不以題材代替量價')
        if news.startswith('未有可查證'):
            # H150 only reads prior H83 entity-matched cache. It never fetches
            # news during Excel generation or asserts a news/return causal link.
            try:
                from godpick_h150_news_evidence import match_existing_news
                news = match_existing_news(raw, day) or news
            except (ImportError, ValueError, TypeError, KeyError):
                pass
        reasons=[]
        if not scope_proven:reasons.append('完整診斷母體／同輪簽章未確認')
        if tier.startswith(('B｜','C｜')):reasons.append('同日技術或長期量能尚有缺口')
        if tier.startswith(('P1｜','P2｜')):reasons.append('蓄勢尚未確認突破與放量；非已發動買點')
        if risk not in {'ALLOW','PASS','NONE','RESEARCH_ELIGIBLE'}:
            reasons.append('H109否決或待核：'+risk)
        if inst!='PROVEN_FLOW':reasons.append('法人連買未認證')
        if tdcc!='VERIFIED_RISING':reasons.append('TDCC週增未認證，不能稱鎖碼')
        if rr is None:reasons.append('缺有效價位與風險報酬')
        reasons.append('若放量失效、突破跌回或資料更新推翻證據，停止觀察／重審')
        opportunities.append({
           '_research_sector':sector,
           '_evidence_order':(0 if risk in {'ALLOW','PASS','NONE','RESEARCH_ELIGIBLE'} else 1,
               0 if tier.startswith(('P1｜','P2｜')) else 1 if tier.startswith('A｜') else 2 if tier.startswith('B｜') else 3,
               0 if inst=='PROVEN_FLOW' else 1, 0 if tdcc=='VERIFIED_RISING' else 1,
               0 if rr is not None else 1, -(ratio or 0), -(amount or 0), code),
           '研究優先序':'研究候選｜非買進排名', '股票代號':code,
           '股票名稱':_s(row.get('股票名稱')) or _s(raw.get('股票名稱')),
           '研究層級':tier,'研究機會':signal,
           '成立證據':f'成交額{amount:g}百萬、成交量{lots:g}張、量比{ratio:g}；'+_s(row.get('來源證據')),
           '反證／失效條件':'；'.join(reasons),
           '研究現價':quote,'支撐價':support,'壓力價':resistance,
           '參考風險報酬比':rr,'價位計算狀態':price_note,
           '主流族群':sector+'／H108 '+phase,
           '法人／大戶':inst+'／'+tdcc,'消息證據':news,'交易資格':NO_TRADE,
        })
    # Order by independently observable evidence, not a fabricated hit rate or
    # H110 tie-break by ticker. Prefer multiple research sectors when credible
    # evidence is comparable, but do not hide remaining single-sector leads.
    opportunities.sort(key=lambda x:x['_evidence_order'])
    ordered=[]
    deferred=[]
    pick_counts:Counter[str]=Counter()
    for item in opportunities:
        sector_name=item['_research_sector']
        if pick_counts[sector_name]<2:
            ordered.append(item);pick_counts[sector_name]+=1
        else:
            deferred.append(item)
    ordered.extend(deferred)
    take=max(1,min(int(limit),10))
    for i, item in enumerate(ordered, 1):
        item['研究優先序']=f'{i}｜研究證據排序／非買進順位'
    opportunity=pd.DataFrame(ordered[:take],columns=OPPORTUNITY_COLUMNS)
    sectors=Counter(_s(x['主流族群']).split('／H108 ')[0] for x in opportunities)
    exposure='、'.join(f'{k}{v}/{len(opportunities)}' for k,v in sectors.most_common(3)) if opportunities else '無可核的候選研究族群'
    concentration=max(sectors.values())/len(opportunities) if opportunities else None
    journal_text,journal_risk=_journal_diagnostics(closed_trades)
    # Existing H129 validated replay evidence is still NOT executed trading.
    # Quote its actual denominator without promoting it into a live win rate.
    performance_text='本輪未連結可核對的歷史策略樣本'
    perf=_frame(feedback_health)
    if {'項目','數值'}.issubset(perf.columns):
        facts={_s(r['項目']):r['數值'] for r in perf.to_dict('records')}
        total=_num(facts.get('H129原T+1檢討筆數'))
        valid=_num(facts.get('H129經H103驗證可用筆數'))
        alpha=_num(facts.get('H129有效Alpha平均pp'))
        if total is not None and valid is not None and total>=valid>=0:
            performance_text=f'H129研究T+1回放有效{int(valid)}/{int(total)}筆'
            if valid>=5 and alpha is not None:
                performance_text+=f'、有效樣本平均Alpha {alpha:.2f}pp'
            performance_text+='（非真實交易績效）'
    holdings_df=_frame(holdings)
    holdings_state = ('未提供真實持股權重；不可計算投組跌20%的實際損失或個股相關係數'
                      if holdings_df.empty else '有持股輸入，但尚未以有效報酬序列驗證相關性；禁止冒稱避險效果')
    formal_count=len(authority_codes)
    summary = pd.DataFrame([
      ('1｜交易機會',f'{len(opportunities)}檔具名量價研究候選；主管顯示{len(opportunity)}檔',
       '沒有驗證的勝率不叫高機率；研究榜也非H126可買', '優先看同日成交量、起漲或續攻與實際失效點'),
      ('2｜日週線技術',f'{sum(x["研究現價"] is not None for x in opportunities)}檔有同日參考收盤；{sum(x["參考風險報酬比"] is not None for x in opportunities)}檔有有效上下界',
       '單日K線不等於週線確認；沒有有效支撐壓力不可捏造目標價', '補同股同日支撐壓力、週線及風險報酬'),
      ('3｜消息事件',f'{sum(x["消息證據"].startswith("事件待核") for x in opportunities)}檔有日期與來源的事件線索',
       '新聞≠必然上漲；沒有發布時點和來源不能認定市場尚未反映','查核原始新聞、公告日期和事件前後成交'),
      ('4｜策略回測',performance_text,'H129研究回放不能當成交勝率；'+journal_risk,
       '以嚴格歷史交易日、成本、滑價及樣本外資料另行回測'),
      ('5｜組合風險',f'候選族群（非實際持倉）：{exposure}',
       ('候選同族集中度偏高；'+holdings_state if concentration is not None and concentration>=0.5 else holdings_state),
       '提供實際持股比例、各標的報酬序列後才能做市場-20%壓力測試'),
      ('6｜交易日誌',journal_text, '僅靠系統推薦快照不能評估人性偏誤；'+journal_risk,
       '檢查真實成交前20筆，分析追高、提前出場及無紀律停損'),
      ('流程規則1｜H126權威','正式交易必須獨立通過H126；研究A/B/C均非買進信號',
       '不以榜單名次、單日漲幅或消息代替正式准入', '每筆計畫均保留H109/H126判定與不可交易原因'),
      ('流程規則2｜失效點','欠缺可核對支撐／壓力／風險報酬就不給數值化交易計畫',
       '已漲停、量比大或同族強勢，不代表隔日風險可控', '必須記下有效日期、失效點與追高風險；未證則觀望'),
      ('流程規則3｜閉環反證','當日診斷、當輪簽章及隔日結果分開永久追蹤',
       '用事後已知資訊回填當日分數會造成前視偏差', '用成熟樣本、成本及樣本外日期挑戰每一次推薦'),
    ],columns=SUMMARY_COLUMNS)
    meta={
       'version':VERSION,'research_day':day.isoformat() if day else '未核實',
       'diagnosis_rows':len(diagnosis),'research_input':len(research),
       'reviewable':len(opportunities),'displayed':len(opportunity),'different_sectors':len({x['_research_sector'] for x in ordered[:take]}),
       'same_day_quote':sum(r['研究現價'] is not None for r in opportunities),
       'calculated_rr':sum(r['參考風險報酬比'] is not None for r in opportunities),
       'scope_proven':scope_proven,'formal_h126_count':formal_count,
       'conflict_codes':sorted(duplicates),'excluded':dict(exclusions),
       'journal_verified':not _frame(closed_trades).empty,
       'formal_h126_modified':False,
       'warning':('H145母體未證，僅能說本輪取得的股票研究線索；不得宣稱全市場排名。'
                   if not scope_proven else '同輪資料已標記確認，但仍需查核市場資料是否充分。')
    }
    return opportunity,summary,meta


def append_six_pillar_to_sheet(ws: Any, candidates: Any, summary: Any, *, limit: int = 5) -> dict[str,int]:
    """Append on worksheet one, leaving header/filter and H126 worksheet intact."""
    from openpyxl.styles import Alignment, Font, PatternFill
    table=_frame(candidates)
    audit=_frame(summary)
    at=max(ws.max_row+3,11)
    ws.cell(at,1,'H149｜六維交易研究與自我反證（研究線索，非正式交易授權）')
    ws.merge_cells(start_row=at,start_column=1,end_row=at,end_column=16)
    ws.cell(at,1).font=Font(bold=True,color='FFFFFF',size=13)
    ws.cell(at,1).fill=PatternFill('solid',fgColor='24446A')
    ws.row_dimensions[at].height=31
    at+=1
    for ci,name in enumerate(SUMMARY_COLUMNS,1):
        c=ws.cell(at,ci,name);c.font=Font(bold=True,color='FFFFFF');c.fill=PatternFill('solid',fgColor='36516D')
    for row in audit.to_dict('records'):
        at+=1
        for ci,k in enumerate(SUMMARY_COLUMNS,1):
            c=ws.cell(at,ci,_safe_excel(row.get(k,'')))
            c.alignment=Alignment(wrap_text=True,vertical='center')
        ws.row_dimensions[at].height=57
    at+=2
    for ci,name in enumerate(OPPORTUNITY_COLUMNS,1):
        c=ws.cell(at,ci,name);c.font=Font(bold=True,color='FFFFFF')
        c.fill=PatternFill('solid',fgColor='196C7E')
        c.alignment=Alignment(wrap_text=True,vertical='center')
    ws.row_dimensions[at].height=32
    n=0
    for row in table.head(max(0,int(limit))).to_dict('records'):
        at+=1;n+=1
        for ci,k in enumerate(OPPORTUNITY_COLUMNS,1):
            c=ws.cell(at,ci,_safe_excel(row.get(k)))
            c.alignment=Alignment(wrap_text=True,vertical='center')
        ws.row_dimensions[at].height=73
    if not n:
        at+=1; ws.cell(at,1,'本輪沒有足夠可核對的強勢量價研究資料，禁止用舊分數或冷門股湊足五檔。')
    return {'rows':n,'last_row':at}


__all__=['VERSION','OPPORTUNITY_COLUMNS','SUMMARY_COLUMNS','build_six_pillar_review','append_six_pillar_to_sheet']
