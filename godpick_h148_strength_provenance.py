# -*- coding: utf-8 -*-
"""V191-H148 read-only opportunity leakage and source-provenance challenge.

ONLY audits the exact current export inputs. This is NOT a second market scan,
a new formal buy gate, a price prediction, a verified sector rotation signal,
or evidence of complete TWSE/TPEX coverage. H109/H113/H123/H124/H126 untouched.
"""
from __future__ import annotations
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any, Mapping
import re
import pandas as pd

from godpick_h147_five_gate_review import evaluate_five_gates, _day, _n, _s, _code
from godpick_h142_evidence_review import assess_sector_fade

VERSION = 'V191-H148.0-SAME-RUN-STRENGTH-TRACE'
COLUMNS = [
    '股票代號','股票名稱','產業類別','H148研究分類','H148漏網位置',
    'H148量價狀態','H148突破階段','H148族群證據','H148法人證據',
    'H148大戶週證','H148風控','H148補證優先','H148交易日','H148樣本來源',
    'H148非交易聲明',
]
PROOF_SHA = re.compile(r'^[0-9a-fA-F]{64}$')


def _frame(value:Any) -> pd.DataFrame:
    return value if isinstance(value,pd.DataFrame) else pd.DataFrame()


def _evidence_time(row:Mapping[str,Any], cutoff:date|None) -> str:
    # Quotes validated as of the research cutoff, NEVER the time of export.
    quote = _day(row.get('H114K線證據日'))
    status = _s(row.get('H114K線資料狀態'))
    if cutoff is None: return '研究交易日未核實'
    if quote is None: return '缺H114交易日'
    if quote > cutoff: return '未來資料衝突'
    if quote != cutoff: return 'K線不同日'
    if status != 'VALID_OHLCV':
        return '同日K線來源待驗' if not status else f'K線無效:{status[:35]}'
    bars = _n(row,'H114K線有效根數')
    if bars is None or bars < 60: return 'K線不足60根／僅研究'
    return '同日完整OHLCV（內部標記）'


def _same_run_code_index(frame:pd.DataFrame) -> tuple[dict[str,dict],set[str]]:
    """Reject conflicting duplicates, rather than choosing a convenient winner."""
    index:dict[str,dict]={}; conflicts:set[str]=set()
    for raw in frame.to_dict('records'):
        code=_code(raw.get('股票代號'))
        if not code:continue
        if code in index:
            # Any differing meaningful fields means a non-authoritative row.
            left=index[code]
            keys=set(left)|set(raw)
            if any(_s(left.get(k)) != _s(raw.get(k)) for k in keys):conflicts.add(code)
        else:index[code]=raw
    return index,conflicts


def _proof(v:Any) -> bool:
    return bool(PROOF_SHA.fullmatch(_s(v)))


def _institution(row:Mapping[str,Any], cutoff:date|None) -> str:
    state=_s(row.get('H123法人實證狀態'))
    if state=='VERIFIED_BUT_NOT_BUYING':return '已核實／非連買'
    if state!='PROVEN_FLOW':return '待補官方逐日證據' if state in ('','PENDING_EVIDENCE') else f'未證連買:{state[:30]}'
    count=_n(row,'H123法人逐日有效天數')
    observed=_day(row.get('H123研究實際時間'))
    if not _proof(row.get('H123來源雜湊')) or count!=5 or not cutoff or not observed or observed>cutoff:
        return '法人狀態宣稱／缺來源時點或五日雜湊'
    return '五日來源雜湊齊／連買標記（原H123）'


def _holder(row:Mapping[str,Any]) -> str:
    state=_s(row.get('H124大戶實證狀態'))
    if state in ('VERIFIED_NOT_RISING','VERIFIED_FALLING','VERIFIED_FLAT'):
        return '週增未成立'
    if state!='VERIFIED_RISING':
        return '待補TDCC週報' if state in ('','PENDING_EVIDENCE') else f'未證週增:{state[:30]}'
    delta=_n(row,'H124大戶變化pp')
    if not _proof(row.get('H124大戶來源雜湊')) or delta is None or delta<.05:
        return '週增狀態宣稱／缺兩週雜湊或幅度'
    return '兩週集保級距週增已核（非特定大戶鎖單）'


def _trade_allowed(row:Mapping[str,Any]) -> bool:
    return _s(row.get('H126能否買進')) in {'是','允許買進','可買進'}


def build_strength_trace(*, candidate_frame:Any, audit_frame:Any,
                         result_frame:Any, authority_frame:Any,
                         scan_report:Mapping[str,Any]|None,
                         h145_bridge:Mapping[str,Any]|None,
                         research_day:Any=None) -> tuple[pd.DataFrame,pd.DataFrame,dict[str,Any]]:
    """Trace existing same-run selection stages; no network, data writes or model reruns.

    A claimed full pool is usable for research only with H145 same-run evidence,
    unique codes and the authoritative diagnostic count; otherwise partial.
    Analysis is NEVER a new H126 formal entitlement. Candidate-only signals
    are explicitly called unverified if they lack H113 audit provenance.
    """
    cdf,adf,rdf,hdf=(_frame(v) for v in (candidate_frame,audit_frame,result_frame,authority_frame))
    cand,conf_c=_same_run_code_index(cdf)
    audited,conf_a=_same_run_code_index(adf)
    selected,conf_r=_same_run_code_index(rdf)
    auth,conf_h=_same_run_code_index(hdf)
    duplicates=conf_c|conf_a|conf_r|conf_h
    bridge=h145_bridge if isinstance(h145_bridge,Mapping) else {}
    report=scan_report if isinstance(scan_report,Mapping) else {}
    expected=_n(report,'candidate_diagnosis_count')
    scanned=_n(report,'total_count')
    final_expected=_n(report,'final_candidate_count')
    funnel_valid=bool(scanned is not None and expected is not None and scanned>=expected>0
                      and (final_expected is None or final_expected<=expected))
    cutoff=_day(research_day)
    proven=bool(bridge.get('candidate_complete') is True and bridge.get('same_run_proven') is True and
                funnel_valid and expected==len(cdf)==len(cand) and not conf_c)
    # Full candidate_frame here is a full *diagnosis input*, never a full market.
    status='已證明本輪診斷候選池' if proven else '局部快照／不可宣稱全市場覆蓋'
    universe = sorted(set(cand)|set(audited)|set(selected)|set(auth))
    if len(universe)>10000:
        raise ValueError('H148拒絕異常大小資料池；需驗證來源後再進行')
    rows=[]
    summary=Counter()
    for code in universe:
        original=audited.get(code) or cand.get(code) or selected.get(code) or auth.get(code) or {}
        # Never merge different sources' unknown dates into a synthetic complete stock.
        c_day=_day(cand.get(code,{}).get('H110研究資訊截止日'))
        a_day=_day(audited.get(code,{}).get('H110研究資訊截止日'))
        source_mismatch=bool(c_day and a_day and c_day!=a_day)
        has_audit=code in audited
        if code in duplicates or source_mismatch:
            date_state='跨池資料衝突'
        else:date_state=_evidence_time(original,cutoff)
        gate=evaluate_five_gates(original,research_day=cutoff)
        amount_ok=gate['成交量能'] in ('量能支持','流動性過／量比缺','流動性過／未放量')
        ratio=_n(original,'H114K線量比')
        distance=_n(original,'H114距20日高點%')
        gap20=_n(original,'收盤距MA20%','MA20乖離%')
        gap60=_n(original,'收盤距MA60%','MA60乖離%')
        ignited=('ALREADY-IGNITED' in _s(original.get('H105發動階段')).upper()
                 or _s(original.get('H105今日強勢排除'))=='是')
        safe_date=(date_state=='同日完整OHLCV（內部標記）')
        # H114 high distance = (previous20dayHigh-close)/high*100.
        # Negative = breakout above previous 20-day high (not yet a buy signal).
        breakout=bool(safe_date and distance is not None and distance<=0 and
                      ratio is not None and ratio>=1.2 and amount_ok)
        healthy=bool(safe_date and ratio is not None and ratio>=1.2 and amount_ok and
                     gap60 is not None and gap60>=0 and gap20 is not None and -6<=gap20<=12)
        h109=_s(original.get('H109決策權威')) or 'UNKNOWN'
        h142=assess_sector_fade(original)
        phase=_s(original.get('H108族群生命週期')).upper()
        prior=_day(original.get('H108族群前次資料日'))
        flow_change=_n(original,'H108族群資金流變化')
        breadth_change=_n(original,'H108族群廣度變化')
        sample_confidence=_n(original,'H108內部樣本可信度')
        timely_sector=bool(cutoff and prior and prior<cutoff and safe_date)
        two_positive=bool(timely_sector and flow_change is not None and flow_change>0
                          and breadth_change is not None and breadth_change>0
                          and sample_confidence is not None and sample_confidence>=50)
        if h109=='FADE_VETO':
            sector=('原FADE否決保留／'+h142['H142退潮證據級別']+
                    ('／同輪內部廣度資金流矛盾待查' if two_positive else ''))
        elif ('IGNITION' in phase or 'EXPANSION' in phase) and two_positive:
            sector='H108內部雙指標支持／尚未獨立驗證'
        elif ('IGNITION' in phase or 'EXPANSION' in phase) and safe_date:
            sector='H108點火/擴張僅標籤／缺跨期雙指標'
        else:
            sector='族群時序待補證' if not phase else '族群未證同步強勢'
        institutional=_institution(original,cutoff)
        holder=_holder(original)
        if date_state=='跨池資料衝突':
            kind='來源矛盾／禁止推論'
        elif not safe_date:
            kind='行情或交易日待核'
        elif breakout and healthy and not has_audit:
            kind='候選內部強勢訊號／待H113稽核'
        elif breakout and healthy and not ignited:
            kind='突破確認研究／非買點'
        elif (ignited or breakout) and healthy and has_audit:
            kind='已發動健康續攻研究／防追高'
        elif healthy:
            kind='量價趨勢研究／尚缺突破'
        elif ignited:
            kind='已發動／續攻證據不足'
        elif amount_ok:
            kind='流動性已核／強勢未證'
        else:
            kind='資料或量價不足'
        if code in duplicates:
            leakage='同代號資料矛盾／隔離'
        elif code not in cand:
            leakage='不在H145診斷輸入／來源待查'
        elif not proven:
            leakage='本輪母體未證／不可確定漏網'
        elif code not in audited:
            leakage='診斷候選未進H113稽核／待查原因'
        elif code not in selected:
            leakage='H113已稽核／未入最終作戰'
        else:
            leakage='已進最終作戰名單'
        if code in auth and code not in duplicates and not source_mismatch and _trade_allowed(auth[code]):
            authority='H126原表允許／請直接核對原表'
        elif code in auth:
            authority='H126不允許或未認證'
        else:
            authority='H126未見此代號／無買進授權'
        blockers=[]
        if date_state!='同日完整OHLCV（內部標記）':blockers.append('行情日期/歷史K')
        if not amount_ok:blockers.append('流動性/量比')
        if ratio is None or ratio<1.2:blockers.append('放量未證')
        if h109 not in ('RESEARCH_ELIGIBLE','ALLOW','PASS','NONE'):blockers.append('H109:'+h109[:24])
        if institutional!='五日來源雜湊齊／連買標記（原H123）':blockers.append('H123法人')
        if holder!='兩週集保級距週增已核（非特定大戶鎖單）':blockers.append('H124週資料')
        if not proven:blockers.append('完整母體未核')
        if not blockers:blockers=['仍以H126正式權威為準']
        src=('H113稽核' if code in audited else 'H145診斷' if code in cand else '正式列表' if code in selected else 'H126權威')
        rows.append(dict(zip(COLUMNS,[
            code,_s(original.get('股票名稱')),_s(original.get('類別')) or 'UNKNOWN',
            kind,leakage,date_state,('已發動' if ignited else '突破' if breakout else '預發動未證'),
            sector,institutional,holder,h109,'；'.join(blockers[:4]),
            str(cutoff) if cutoff else '未核實',src,
            authority+'｜研究盤不等於買進建議'
        ])))
        summary['已核實研究突破']+=int(kind=='突破確認研究／非買點')
        summary['健康續攻研究']+=int(kind=='已發動健康續攻研究／防追高')
        summary['行情待核']+=int(not safe_date)
        summary['法人待補']+=int(institutional!='五日來源雜湊齊／連買標記（原H123）')
        summary['大戶待補']+=int(holder!='兩週集保級距週增已核（非特定大戶鎖單）')
        summary['FADE否決']+=int(h109=='FADE_VETO')
        summary['族群雙指標支持']+=int(sector=='H108內部雙指標支持／尚未獨立驗證')
        summary['H113稽核漏接']+=int(code in cand and code not in audited)
        summary['H126原表可買']+=int(code in auth and code not in duplicates and not source_mismatch and _trade_allowed(auth[code]))
    output=pd.DataFrame(rows,columns=COLUMNS)
    # Sorting only controls audit review order; it NEVER changes H110 priority rank.
    group_rank={'突破確認研究／非買點':0,'已發動健康續攻研究／防追高':1,'候選內部強勢訊號／待H113稽核':2,'量價趨勢研究／尚缺突破':3,
                '已發動／續攻證據不足':4,'流動性已核／強勢未證':5,
                '行情或交易日待核':6,'資料或量價不足':7,'來源矛盾／禁止推論':8}
    if not output.empty:
        output['__order']=output['H148研究分類'].map(lambda s:group_rank.get(s,8))
        output=output.sort_values(['__order','股票代號'],kind='stable').drop(columns='__order').reset_index(drop=True)
    group_data=defaultdict(lambda:Counter())
    for rec in rows:
        industry=rec['產業類別']
        group_data[industry]['已取樣個股']+=1
        if rec['H148研究分類']=='突破確認研究／非買點':group_data[industry]['突破研究']+=1
        if rec['H148研究分類']=='已發動健康續攻研究／防追高':group_data[industry]['續攻研究']+=1
        if rec['H148風控']=='FADE_VETO':group_data[industry]['FADE否決']+=1
        if rec['H148族群證據']=='H108內部雙指標支持／尚未獨立驗證':group_data[industry]['雙指標']+=1
    sectors=pd.DataFrame([{'產業類別':k,'候選樣本數':v['已取樣個股'],
                           '量價突破研究數':v['突破研究'],'健康續攻研究數':v['續攻研究'],
                           'H109退潮否決數':v['FADE否決'],'H108雙指標支持數':v['雙指標'],'資料性質':'同輪輸入池內統計；非全市場族群輪動'}
                         for k,v in group_data.items()]).sort_values('候選樣本數',ascending=False) if group_data else pd.DataFrame()
    formal=summary['H126原表可買']
    if formal:
        conclusion='H126原表存在授權檔，須逐筆核對權威，H148不另行核准'
    elif not proven:
        conclusion='零正式推薦不可歸因全市場｜診斷候選母體不完整或同輪證據不足'
    elif summary['行情待核']:
        conclusion='零正式推薦主因尚不可歸因市場｜部分行情/K線證據待核'
    elif summary['FADE否決']:
        conclusion='零正式推薦存在H109風控否決｜需逐檔確認退潮跨期來源'
    elif summary['法人待補'] or summary['大戶待補']:
        conclusion='零正式推薦存在官方法人/TDCC證據缺口｜不是無強勢股'
    else:
        conclusion='本輪無H126認證｜僅限已核對的診斷候選池，非市場結論'
    meta={'version':VERSION,'scope_status':status,'proven_diagnosis_pool':proven,
          'expected_diagnosis':expected,'scanned_reported':scanned,'funnel_geometry_valid':funnel_valid,'candidate_rows':len(cdf),'candidate_unique':len(cand),
          'audit_unique':len(audited),'result_unique':len(selected),'authority_unique':len(auth),
          'review_unique':len(output),'duplicate_codes':sorted(duplicates),
          'counts':dict(summary),'zero_formal_diagnosis':conclusion,
          'sector_sample_count':len(sectors),'h126_modified':False,
          'warning':'H148只查本輪可取得的候選股票，未重掃全市場；資料來源不完整時不得宣稱無強勢股；同日K線標記仍需官方來源稽核。'}
    return output,sectors,meta


__all__=['VERSION','COLUMNS','build_strength_trace']
