# -*- coding: utf-8 -*-
"""H144 independent export-scope audit and shadow observation (research only).

NEVER upgrades formal entitlement, removes H109 veto or uses old H102 prices.
Existing same-run frames only; NO IO, no internet, no persistence.
"""
from __future__ import annotations
from datetime import date
from typing import Any, Mapping
import math
import re
import pandas as pd

VERSION = 'V191-H144.0-SCOPE-INTEGRITY'

def _df(x: Any) -> pd.DataFrame:
    return x.copy(deep=False) if isinstance(x, pd.DataFrame) else pd.DataFrame()

def _n(x: Any, default=0) -> int:
    try:
        n=float(x)
        return int(n) if math.isfinite(n) and n>=0 else default
    except (ValueError, TypeError, OverflowError): return default

def _str(x: Any) -> str:
    try:
        if x is None or pd.isna(x): return ''
    except (TypeError, ValueError): pass
    return str(x).strip()

def _m(df: pd.DataFrame, key: str, default=None):
    if df.empty or not {'項目','數值'}.issubset(df.columns): return default
    a=df.loc[df['項目'].astype(str)==key,'數值']
    return a.iloc[-1] if not a.empty else default

def _number(x: Any):
    try:
        n=float(str(x).strip().replace(',','').replace('%',''))
        return n if math.isfinite(n) else None
    except (ValueError, TypeError): return None

def _col(df: pd.DataFrame, names: tuple[str,...]):
    return next((c for c in names if c in df.columns), None)

def audit_h144(*, scan_report: Mapping[str, Any]|None, candidate_frame: Any,
               result_frame: Any, sector_frame: Any, tables: Mapping[str,Any]|None,
               core_source: str='', research_day: str=''):
    """Four DataFrames and small safe status; no reranking, no implicit market claims.

    Expected diagnostic count is independently recorded by scanner; the exporter
    cannot silently downgrade that count to its actual one-row input.  A small
    market-scan sample is NOT an opportunity-performance denominator.
    """
    report=scan_report if isinstance(scan_report, Mapping) else {}
    tab=tables if isinstance(tables,Mapping) else {}
    cand=_df(candidate_frame); result=_df(result_frame);sector=_df(sector_frame)
    health=_df(tab.get('h126_health'))
    scanned=_n(report.get('total_count'))
    full=_n(report.get('candidate_diagnosis_count'), _n(report.get('analyzed_ok')))
    final=_n(report.get('final_candidate_count'), _n(report.get('final_count'),
          _n(report.get('final_battle_candidate_count'), _n(_m(health,'最終作戰候選數',len(result))))))
    export=len(cand)
    h126=_n(_m(health,'原候選去重數',export))
    excluded=_n(_m(health,'禁止新倉／隔離觀察',0))
    formal=_n(_m(health,'H126授權進場檔數',0))
    day=_str(research_day)
    valid_day=False
    try:
        valid_day=(date.fromisoformat(day).isoformat()==day)
    except ValueError: pass
    mismatch=(full>=20 and export<max(5,math.ceil(full*0.20))) or (
        full>=20 and export<full and ('bounded-export-recovery' in core_source))
    if scanned and full>scanned: mismatch=True
    status='EXPORT_SCOPE_INCOMPLETE' if mismatch else ('SCOPE_NOT_PROVEN' if not full or not valid_day else 'SCOPE_RECONCILED_FOR_DISPLAY')
    funnel=pd.DataFrame([
        ('01｜預計掃描股票',scanned,'全市場/指定池之掃描母體','不等於已取得日K'),
        ('02｜完整候選診斷',full,'掃描報表記錄的有效候選','應與同輪候選保存核對'),
        ('03｜最終作戰候選',final,'原推薦策略選出的較小候選集','不等於H126買進授權'),
        ('04｜Excel核心實際輸入',export,'H97/H91決策核心收到的列數','不可與全市場母體混用'),
        ('05｜H126原候選去重',h126,'H126實際判斷的母體','母體不完整時不能宣稱全市場無機會'),
        ('06｜H126禁止新倉',excluded,'正式H109/H126權限維持','不得當成全部強勢股退潮證據'),
        ('07｜正式可買進',formal,'只看H126，且需要全部正式門檻','0不代表全市場沒有強勢股'),
        ('08｜研究日',day or 'UNKNOWN','報表宣稱的日期','仍需獨立事前時點/官方來源'),
        ('09｜H97資料來源',core_source or 'UNKNOWN','回退模式須另查來源','無法補救時須重新封存當輪核心'),
        ('10｜本次匯出母體一致性',status,'供主管判讀與封存前核查','非交易授權、不可自動擴增候選'),
    ],columns=['檢查環節','數量／狀態','實際母體','使用限制'])
    # Shadow strengths: sector averages are NOT individual stock breakouts.
    watch=[]
    if not sector.empty and {'類別','類股平均漲幅'}.issubset(sector.columns):
        for _,r in sector.iterrows():
            rise=_number(r.get('類股平均漲幅'))
            n=_n(r.get('股票數'))
            conf=_number(r.get('族群樣本可信度'))
            if rise is None or rise<1.5 or n<5 or conf is None or conf<50: continue
            watch.append({'來源類型':'族群強度觀察','代號':'','名稱或產業':_str(r.get('類別')),
                          '當輪觀測漲幅%':round(rise,2),'樣本數':n,'樣本可信度%':round(conf,1),
                          '研究資料日':day,'判斷':'族群平均強勢，不能推論個股可買',
                          '下一步':'核對個股真實OHLCV／成交量／法人與H109否決','正式買進':'否'})
    # Do not turn already-ignited stocks into future blackhorse candidates.
    observed=pd.concat([result,_df(tab.get('ignited_watch'))],ignore_index=True,sort=False)
    if not observed.empty:
        cc=_col(observed,('股票代號','代號','證券代號'))
        nc=_col(observed,('股票名稱','名稱'))
        pct=_col(observed,('當日漲跌幅%','漲跌幅%','今日漲幅%','當日漲幅%','漲幅%','漲跌幅'))
        date_col=_col(observed,('資料交易日','資料日期','市場資料日','行情日期'))
        if cc and pct:
            seen=set()
            for _,r in observed.iterrows():
                code=_str(r.get(cc));change=_number(r.get(pct))
                if not re.fullmatch(r'[1-9]\d{3}',code) or code in seen or change is None or change<=0:continue
                seen.add(code)
                row_day=_str(r.get(date_col))[:10] if date_col else ''
                # Never present an out-of-date row as current strength.
                current=bool(valid_day and row_day==day)
                watch.append({'來源類型':'個股已發動觀察' if current else '個股待核日期',
                              '代號':code,'名稱或產業':_str(r.get(nc)) if nc else '',
                              '當輪觀測漲幅%':round(change,2),'樣本數':'','樣本可信度%':'',
                              '研究資料日':row_day or '未載明',
                              '判斷':'同日上漲（非預發動黑馬）' if current else '無法證明屬本交易日',
                              '下一步':'補量價、流動性、法人、風控及當期交易日認證','正式買進':'否'})
    shadow=pd.DataFrame(watch,columns=['來源類型','代號','名稱或產業','當輪觀測漲幅%','樣本數','樣本可信度%','研究資料日','判斷','下一步','正式買進'])
    if not shadow.empty:shadow=shadow.head(50).reset_index(drop=True)
    blind=pd.DataFrame([
        ('P0','主管候選池可能縮水',f'當輪掃描有效{full}檔，Excel僅{export}檔','小母體H126=0不得外推全市場','核對H97 session/local-core/run_id；缺當輪核心就標示匯出不完整'),
        ('P0','有效掃描不等於8檔正式推薦',f'原策略作戰候選{final}檔','H105會排除已發動股；H109保留正式硬否決','分開顯示已發動、預發動、資料不足三種觀察'),
        ('P0','族群轉弱與個股強勢可同時成立',f'04原始類股資料{len(sector)}列','產業平均上漲與資金退潮分類並不互斥','將市場強勢觀察移至影子表，不做買入推薦'),
        ('P1','官方證據與上游名單相互獨立','H123/H124及真實行情各自有時間窗口','本輪有代號不代表已官方認證','逐項補證，不准用代理欄位取代官方證據'),
        ('P1','高分與績效都需反證','舊H102排行非交易授權','沒有完整T+／成本／獨立PIT不能宣稱策略成功','持續用樣本外比較，保留所有失敗/缺價樣本'),
    ],columns=['優先級','盲點','本輪證據','挑戰原結論','最小改善動作'])
    counter=pd.DataFrame([
        ('零正式推薦代表全市場沒有強勢股','不成立','H105預發動與當日強勢是不同策略；核心輸入也可能縮水','觀測相同交易日全市場收盤與完整H97候選母體'),
        ('1檔全被否決證明H109準確','不成立','母體不完整；FADE標籤仍可能缺跨期證據','逐股官方/內部時序與先前預封存決策'),
        ('族群平均漲幅高即可挑進買進榜','不成立','族群平均非個股OHLCV；可能流動性不足/已追高','個股可稽核日K、量能、流動性、風控與成本後RR'),
        ('把門檻降低就會改善績效','沒有證據','只會放寬暴露，尚未解決來源/樣本問題','影子研究與原策略並行樣本外驗證，不動正式交易權限'),
        ('今日匯出就等於今日行情可信','未驗證','工作簿日期與事前PIT/官方封存不同','當輪來源URL/發布交易日/雜湊/完整性確認'),
    ],columns=['自我挑戰的主張','判斷','反面理由','什麼證據能推翻判斷'])
    return {'status':status,'scope_incomplete':mismatch,'scan_count':scanned,'diagnosis_count':full,
            'export_count':export,'reported_final':final,'h126_count':h126,'shadow_count':len(shadow)},funnel,shadow,blind,counter
