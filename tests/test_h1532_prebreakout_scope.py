# -*- coding: utf-8 -*-
"""H153.2 no-lookahead, no-fake-authorization, no-cold-pool-ranking checks."""
from datetime import date, timedelta
import pandas as pd
from godpick_h114_parallel_alpha import analyze_history
from godpick_h1532_technical_evidence import evaluate_prebreakout
from godpick_h1532_scope_repair import restore_research_scope
from godpick_h153_growth_setup_rank import build_growth_setup_ranking, build_ranked_five_plans

DAY=date(2026,11,6)

def bars(volume=True):
    # 110 historical daily observations. Late close path tightens while OBV
    # rises; do not require a bullish indicator to occur in every scenario.
    dates=pd.bdate_range(end=DAY, periods=110)
    rows=[]
    for i,d in enumerate(dates):
        if i<90:
            high=100+(i%7)*.7;low=98-(i%6)*.7
            close=99+(i%5)*.25
        else:
            close=99.5+(i-90)*.06
            high=close+.16;low=close-.13
        row={'日期':d.date(),'開盤價':close-.04,'最高價':high,'最低價':low,'收盤價':close}
        if volume:row['成交量(張)']=1500+(i%3)*100
        rows.append(row)
    return pd.DataFrame(rows)

def normalized(df):
    return pd.DataFrame({'day':df['日期'],'open':df['開盤價'],'high':df['最高價'],
                          'low':df['最低價'],'close':df['收盤價'],
                          'lots':df['成交量(張)']})

def test_true_dated_technical_evidence_and_asof_consistency():
    df=bars()
    out=analyze_history(df,asof=DAY)
    assert out['H114K線資料狀態']=='VALID_OHLCV'
    assert out['H1532技術來源'].startswith('同股同日')
    assert out['H1532布林帶寬%'] is not None
    assert out['H1532布林收斂'].startswith(('是｜','否｜'))
    assert out['H1532OBV近10日方向']=='向上'
    assert out['H1532ADX'] is not None
    assert out['H1532DMI方向'] in ('多向(+DI>-DI)','空向(+DI<-DI)','方向不明')
    # Future bars cannot change an as-of research result.
    extra=df.iloc[-1:].copy()
    extra['日期']=[DAY+timedelta(days=4)]
    extra['收盤價']=[9999]
    extra['最高價']=[10000]
    extra['最低價']=[9998]
    extra['開盤價']=[9999]
    extended=analyze_history(pd.concat([df,extra],ignore_index=True),asof=DAY)
    for k in ('H1532布林收斂','H1532OBV量價背離','H1532ADX'):
        assert out[k]==extended[k]


def test_no_known_units_or_stale_bars_do_not_create_indicators():
    df=bars(volume=False)
    out=analyze_history(df,asof=DAY)
    assert out['H1532布林收斂']=='未驗證'
    assert out['H1532ADX'] is None
    bad=bars();bad['成交量(張)']=0
    out=analyze_history(bad,asof=DAY)
    assert out['H1532技術來源']=='OHLCV 價格或成交量無效'
    assert evaluate_prebreakout(normalized(bars()).iloc[:-40])['H1532ADX'] is None


def test_no_bollinger_only_promotion_and_h126_remains_independent():
    r=analyze_history(bars(),asof=DAY)
    r.update({'股票代號':'2330','股票名稱':'樣本','類別':'半導體',
              'H113當日成交額百萬':360,'H113二十日均成交額百萬':300,
              'H109決策權威':'FADE_VETO',
              'H114黃金交叉':'未驗證','H114箱型狀態':'未形成可信窄幅箱型',
              'H114量縮墊高':'否','H114波動收斂':'否','H114距20日高點%':12})
    ranked,_,m=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([r]),
        research_frame=pd.DataFrame(),research_day=DAY)
    if not ranked.empty:
        assert ranked.iloc[0]['排名等級'].startswith('C')
        plan=build_ranked_five_plans(ranked=ranked,diagnosis_frame=pd.DataFrame([r]),
                                    previous_plans=pd.DataFrame(),research_day=DAY)
        assert plan.empty
    assert m['formal_h126_modified'] is False


def test_full_scope_needs_same_run_superset_and_no_H114_conflict():
    tiny=pd.DataFrame([{'股票代號':'2330','H114K線證據日':'2026-11-06',
                        'H114K線資料狀態':'VALID_OHLCV'}])
    full=pd.concat([tiny,pd.DataFrame([{'股票代號':'2317',
            'H114K線證據日':'2026-11-06','H114K線資料狀態':'VALID_OHLCV'}])],ignore_index=True)
    ok,meta=restore_research_scope(tiny,full,current_signature='scan1',full_signature='scan1')
    assert len(ok)==2 and meta['status']=='RESTORED_VERIFIED_SUPERSET_RESEARCH_ONLY'
    assert meta['h126_modified'] is False
    no,meta=restore_research_scope(tiny,full,current_signature='scan1',full_signature='other')
    assert len(no)==1 and meta['status']=='BLOCKED_DIFFERENT_SCAN_SIGNATURE'
    changed=full.copy();changed.at[0,'H114K線證據日']='2026-11-05'
    no,meta=restore_research_scope(tiny,changed,current_signature='scan1',full_signature='scan1')
    assert len(no)==1 and meta['status']=='BLOCKED_OVERLAP_EVIDENCE_CONFLICT'
    changed=full.drop(columns=['H114K線證據日'])
    no,meta=restore_research_scope(tiny,changed,current_signature='scan1',full_signature='scan1')
    assert len(no)==1 and meta['status']=='BLOCKED_FULL_SOURCE_LOST_H114_EVIDENCE'


def test_study_scope_no_duplicate_promotion():
    d=pd.DataFrame([{'股票代號':'2330'},{'股票代號':'2330'}])
    full=pd.concat([d,pd.DataFrame([{'股票代號':'2317'}])],ignore_index=True)
    result,meta=restore_research_scope(d,full,current_signature='A',full_signature='A')
    assert len(result)==2 and meta['status']=='BLOCKED_DUPLICATE_OR_NOT_SUPERSET'
