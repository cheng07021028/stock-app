# -*- coding: utf-8 -*-
"""V191-H143: research pool clarity, blind-spot audit and adversarial review.

Pure reporting. Never creates trade permission, substitutes a prior H102 rank for
H126 authority, fetches market data, or writes persistent recommendations.
"""
from __future__ import annotations
from datetime import date, datetime
from collections import Counter
import re
from typing import Any, Mapping
import pandas as pd

VERSION = 'V191-H143.0-DECISION-CLARITY'
UNKNOWN = '未取得可核對證據'


def _as_frame(value: Any) -> pd.DataFrame:
    return value.copy(deep=True) if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _str(v: Any) -> str:
    if v is None: return ''
    try:
        if bool(pd.isna(v)): return ''
    except (ValueError, TypeError): pass
    return str(v).strip()


def _metric(df: pd.DataFrame, key: str, default: Any=None) -> Any:
    if df.empty or not {'項目','數值'}.issubset(df.columns): return default
    x = df.loc[df['項目'].astype(str) == key, '數值']
    return x.iloc[-1] if not x.empty else default


def _num(value: Any, default: int=0) -> int:
    try: return max(0, int(float(value)))
    except (ValueError, TypeError): return default


def _records(df: pd.DataFrame, code_col: str='股票代號') -> pd.DataFrame:
    if df.empty or code_col not in df.columns: return df.iloc[0:0].copy()
    codes = df[code_col].map(_str)
    # E.g. 2330.0 can appear in old CSV; never silently coerce a different code.
    return df.loc[codes.map(lambda s: bool(re.fullmatch(r'[1-9]\d{3}',s)))].copy()


def _safe_research_day(health: pd.DataFrame) -> str:
    for key in ('H128研究資訊截止日（非匯出時間）','H128市場資料交易日（非匯出時間）'):
        s = _str(_metric(health,key))
        try: return date.fromisoformat(s).isoformat()
        except (TypeError,ValueError): pass
    return ''


def audit_h143(tables: Mapping[str, Any], *, generated_at: str|None=None):
    """Return four DataFrames: summary, blind spots, pool triage and counterclaims.

    Input mirrors the data already held by H126/H141/H142/H129 in the *same*
    manager Excel export; no extra spreadsheets or network calls are required.
    """
    tables = tables if isinstance(tables, Mapping) else {}
    health = _as_frame(tables.get('h126_health'))
    audit = _records(_as_frame(tables.get('audit')))
    fade = _records(_as_frame(tables.get('h142_evidence')))
    quality = _as_frame(tables.get('h129_t1_health'))
    authority = _records(_as_frame(tables.get('h126_authority')))
    # Research cutoff is authoritative only as a reported label, not PIT proof.
    cutoff = _safe_research_day(health)
    export_time = _str(generated_at) or datetime.now().isoformat(timespec='seconds')
    try: export_day = date.fromisoformat(export_time[:10])
    except ValueError: export_day = None
    cutoff_age = (export_day-date.fromisoformat(cutoff)).days if export_day and cutoff else None
    bad_count = _num(_metric(health,'禁止新倉／隔離觀察',0))
    h126_count = _num(_metric(health,'原候選去重數', len(authority)+bad_count))
    audit_count = len(audit)
    fade_count = int(audit['H109決策權威'].map(_str).eq('FADE_VETO').sum()) if 'H109決策權威' in audit else 0
    if fade_count==0 and 'H109原風控' in fade:
        fade_count = int(fade['H109原風控'].map(_str).eq('FADE_VETO').sum())
    unverified = int(fade['H142退潮證據級別'].map(_str).eq('UNVERIFIED_FADE_LABEL').sum()) if 'H142退潮證據級別' in fade else 0
    sectors = len({_str(v) for v in fade.get('產業類別',pd.Series(dtype=str)) if _str(v)})
    liq_no = int(audit['H113流動性合格'].map(_str).eq('否').sum()) if 'H113流動性合格' in audit else 0
    lock_no = int(audit['H113大戶鎖碼合格'].map(_str).eq('否').sum()) if 'H113大戶鎖碼合格' in audit else 0
    org_missing = sum(int(audit[col].map(_str).ne('PROVEN_FLOW').sum()) for col in ['H123法人實證狀態'] if col in audit)
    tdcc_missing = sum(int(audit[col].map(_str).ne('VERIFIED_RISING').sum()) for col in ['H124大戶實證狀態'] if col in audit)
    t1_total = _num(_metric(quality,'H129原T+1檢討筆數'))
    t1_valid = _num(_metric(quality,'H129經H103驗證可用筆數'))
    t1_alpha = _metric(quality,'H129有效Alpha平均pp')
    t1_alpha_str = _str(t1_alpha) or UNKNOWN
    if h126_count >= audit_count:
        unaccounted = h126_count-audit_count
        denominator_note = ('H126較H113/H142多%d檔；不同階段母體，尚無逐檔映射可證明原因' % unaccounted)
    else:
        unaccounted = 0
        denominator_note = 'H113/H142筆數高於H126；須查去重或欄位映射，禁止直接做共同母體勝率'
    stale = cutoff_age is None or cutoff_age>0
    summary = pd.DataFrame([
        ('H143版本',VERSION,'僅資訊整理，沒有交易授權'),
        ('匯出時間（非研究時點）',export_time,'不得當成行情發布或正式決策日期'),
        ('原研究資料截止日',cutoff or UNKNOWN,'來源：H126缺口表/H128研究截止；尚非獨立PIT'),
        ('與匯出日相差天數',cutoff_age if cutoff_age is not None else UNKNOWN,'日曆天數，不推測台股休市日'),
        ('主管指示','停止新倉／先修資料證據' if bad_count or stale else '依H126正式權威判斷','H143不生成買進信號'),
        ('H126原候選去重',h126_count,'母體：H126完整候選'),
        ('H113/H142可稽核股票',audit_count,'母體：H113/H142稽核清單'),
        ('H126－H113/H142母體差',unaccounted,denominator_note),
        ('H126隔離與禁止新倉',bad_count,'不等於實際成交／停損紀錄'),
        ('H126可辨識權威研究列',len(authority),'不能用H102或H113補足'),
        ('H113稽核範圍FADE否決',fade_count,'分母是H113 audit，不是H126完整候選'),
        ('H142退潮僅標籤未證明',unverified,'不得直接改為研究合格，亦不得宣稱官方退潮認證'),
        ('FADE覆蓋族群',sectors,'類別不等於獨立一致的退潮證據'),
        ('H113流動性不合格',liq_no,'純H113代理，並非完整交易可行性'),
        ('H113大戶鎖碼不合格',lock_no,'與H124官方認證不同概念'),
        ('H123法人尚未PROVEN',org_missing,'獨立官方逐日來源／發布時點需要驗證'),
        ('H124大戶尚未VERIFIED',tdcc_missing,'TDCC週資料，不能冒充日資料'),
        ('T+1原始研究觀察筆數',t1_total,'含未成交、非交易日與模擬交易'),
        ('T+1 H103驗證可用筆數',t1_valid,'不是券商成交或真實勝率'),
        ('T+1有效Alpha平均pp',t1_alpha_str,'小樣本與回放資料；非預測證據'),
        ('研究決策優先序','H126 > H109硬風控 > H113/H123/H124 > H142稽核 > H102舊榜','任何舊版順位不得恢復買進'),
    ],columns=['重點','結果','使用與限制'])
    blind = []
    def add(code, issue, observation, alternative, action, level='高'):
        blind.append({'編號':code,'優先級':level,'忽略的盲點':issue,'本輪觀察':observation,
                      '挑戰原判斷／其他解釋':alternative,'改進動作':action,'是否可買進':'否；仍依H126/H109'})
    if stale:
        add('B01','匯出時間冒充研究新鮮度',f'匯出{export_time[:10]}；截止{cutoff or UNKNOWN}；差{cutoff_age if cutoff_age is not None else "未知"}日',
            '可能是歷史回放／快取，而非當日官方新數據','顯示研究時點與採用快照的hash；未刷新不得顯示今日推薦')
    if h126_count!=audit_count:
        add('B02','混用候選母體',f'H126={h126_count}、H113/H142={audit_count}，相差{abs(h126_count-audit_count)}檔',
            '兩表可能過濾/去重時機不同；不能直接算單一否決率','建立候選stock_code映射與差異清單；禁止跨母體推論勝率')
    if fade_count:
        add('B03','過度一體化退潮否決',f'{fade_count}/{max(audit_count,1)}稽核股FADE；其中{unverified}檔僅標籤',
            '有可能全市場退潮，也可能是上游沿用舊類股狀態；缺跨日雙指標無法判別',
            '分離時間有效的退潮證據與缺資料；原硬否決不解除')
    if org_missing or tdcc_missing:
        add('B04','代理合格被誤認官方認證',f'H123未通過{org_missing}、H124未通過{tdcc_missing}項',
            'H113大戶合格或外資買超字串不等於H124週證據與H123逐日時間戳',
            '新增證據來源／日期／週次及空窗欄；PENDING不得分配買進權限')
    if t1_total:
        add('B05','T+1回放績效樣本偏差',f'有效{t1_valid}/{t1_total}，隔離{max(t1_total-t1_valid,0)}',
            '不利樣本缺失、成交模擬或非交易日會影響績效；不能宣稱真實策略勝率',
            '分母保留全部歷史訊號；僅完整成熟同口徑交易日可納入樣本外')
    add('B06','產業全市場母體與推薦池誤混','04主流資金為更大市場／分類觀測；05僅H113稽核股票',
        '相同類別的市場廣度不一定代表候選股廣度','在每張表標明母體、截止日與樣本數，分母不明時禁止強度結論','中')
    blind = pd.DataFrame(blind)
    # The triage order is for *collecting proof*, not ranking expected returns.
    audit_records = { _str(row.get('股票代號')):row for row in audit.to_dict('records') }
    fade_records = { _str(row.get('股票代號')):row for row in fade.to_dict('records') }
    triage=[]
    for code, row in audit_records.items():
        if not code: continue
        fr=fade_records.get(code,{})
        liq=_str(row.get('H113流動性合格'))
        holder=_str(row.get('H113大戶鎖碼合格'))
        org=_str(row.get('H123法人實證狀態'))
        tdcc=_str(row.get('H124大戶實證狀態'))
        stage=_str(row.get('H109決策權威'))
        fades=_str(fr.get('H142退潮證據級別'))
        if liq=='是' and holder=='是': group='01｜優先核對官方證據（非買進）'
        elif liq!='是':group='02｜先確認流動性與資料量'
        else:group='03｜先查大戶時間證據'
        tags = []
        if stage=='FADE_VETO' and fades=='UNVERIFIED_FADE_LABEL':tags.append('退潮只有標籤')
        if org!='PROVEN_FLOW':tags.append('法人待認證')
        if tdcc!='VERIFIED_RISING':tags.append('TDCC待認證')
        if liq!='是':tags.append('流動性未過')
        if holder!='是':tags.append('H113大戶代理未過')
        triage.append({'資料補證順序':group,'股票代號':code,'股票名稱':_str(row.get('股票名稱')),
                       '類別':_str(row.get('類別')),'H109硬風控':stage,'H142退潮來源':fades or UNKNOWN,
                       'H113流動性':liq or UNKNOWN,'H113大戶代理':holder or UNKNOWN,
                       'H123官方法人':org or UNKNOWN,'H124官方大戶':tdcc or UNKNOWN,
                       '盲點標籤':'；'.join(tags) if tags else UNKNOWN,
                       '反向解釋':'目前僅證據缺失，未證明利多／利空方向',
                       '研究處置':'核資料來源／日期；不得復權為買進'})
    pool=pd.DataFrame(triage)
    if not pool.empty:
        pool=pool.sort_values(['資料補證順序','股票代號'],kind='stable').reset_index(drop=True)
    challenge=pd.DataFrame([
        ('C01','118/120或120/120被否決，所以全部產業真退潮','不成立（證據不足）','H142證據只有標籤、H110 PARTIAL','要求每個產業至少一組歷史可核對的資金/廣度時序，再討論判斷準確性'),
        ('C02','流動性及大戶代理通過的股票就可買','不成立','H113代理不能替代H123/H124/H109/H126；還可能有其他風險','官方證據＋研究截止時點＋風控完整一致才進下一階段'),
        ('C03','H102高分股票就是新的黑馬推薦','不成立','H102舊榜順位與H126權威分層不同','高分只能產生研究問題，不能產生進場/停損/目標價'),
        ('C04','Excel產生於今天所以股價是今天最新','不成立' if stale else '未獨立證明','匯出日和H128截止日不同或缺PIT證據','用交易日+原始檔來源+SHA明列，不可引用本日新行情'),
        ('C05','7筆有效T+1代表策略有效或無效','不足以下定論','80筆原樣本只有小部分可用，也未獨立驗證交易紀錄','保持所有樣本分母，做跨日期樣本外比較、成交成本和不確定區間'),
        ('C06','只要放寬H109門檻就能改善選股','拒絕這種捷徑','上游時序與來源尚未釐清；放寬會錯把PENDING當PASS','先補來源和分層，並平行影子研究，不改正式推薦閘門'),
    ],columns=['編號','需要挑戰的結論','判斷','反面證據／限制','可推翻我的結論所需的證據'])
    return summary,blind,pool,challenge
