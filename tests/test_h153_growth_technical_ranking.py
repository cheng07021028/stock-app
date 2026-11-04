# -*- coding: utf-8 -*-
from datetime import date, timedelta
import pandas as pd
from godpick_h114_parallel_alpha import analyze_history
from godpick_h153_growth_setup_rank import (
    build_growth_setup_ranking, attach_rank_to_research, build_ranked_five_plans)

DAY='2026-11-03'

def kbars(*,near=True,volume=True):
    days=pd.bdate_range(end=DAY,periods=65)
    # last 20 sessions are tight: 100->102 near box ceiling
    prices=[100+i*.01 for i in range(65)]
    prices[-1]=101.00 if near else 116.
    rows=[]
    for i,(day,c) in enumerate(zip(days,prices)):
        rows.append({'日期':day.date().isoformat(),'開盤價':c-.3,'最高價':c+(.1 if i==64 and near else .65),
                     '最低價':c-.7,'收盤價':c,'成交量(張)':800 if i<64 else 900})
    f=pd.DataFrame(rows)
    if not volume:f=f.drop(columns=['成交量(張)'])
    return f

def diagnostic(code='2330',**updates):
    a=analyze_history(kbars(),asof=DAY)
    assert a['H114K線資料狀態']=='VALID_OHLCV',a
    assert a['H114箱型狀態'].startswith('接近箱頂'),a['H114箱型狀態']
    a.update({'股票代號':code,'股票名稱':'研究樣本','類別':'電子','H113當日成交額百萬':360,
              'H113二十日均成交額百萬':310,'H109決策權威':'RESEARCH_ELIGIBLE'})
    a.update(updates)
    return a

def test_h114_box_from_previous_20_sessions():
    data=analyze_history(kbars(),asof=DAY)
    assert data['H114箱頂']>data['H114箱底']
    assert data['H114黃金交叉'] in ['確認｜MA5上穿MA20','確認｜MA20上穿MA60','均線交叉未確認']
    assert data['H114MA60'] is not None
    assert '非自動停損價' in data['H114技術失效條件']

def test_h114_no_volume_no_certification():
    d=analyze_history(kbars(volume=False),asof=DAY)
    assert d['H114K線資料狀態'] == 'VALID_OHLC_ONLY_VOLUME_UNIT_UNKNOWN'
    assert d['H114黃金交叉']=='未驗證'

def test_empty_fundamentals_dont_create_growth_score():
    f,s,m=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([diagnostic()]),research_frame=pd.DataFrame(),research_day=DAY)
    assert len(f)==1 and f.loc[0,'本益比'] is None
    assert f.loc[0,'營收創高']=='未驗證'
    assert f.loc[0,'研究主題']!='高成長低估／技術蓄勢'
    assert f.loc[0,'研究排名']==1
    assert m['formal_h126_modified'] is False

def test_no_stale_or_unknown_volume_in_rank():
    d=diagnostic()
    d['H114K線證據日']='2026-11-02'
    f,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([d]),research_frame=pd.DataFrame(),research_day=DAY)
    assert f.empty
    d=diagnostic(); d['H114當日成交量張']=None
    f,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([d]),research_frame=pd.DataFrame(),research_day=DAY)
    assert f.empty

def test_ties_share_ranks_and_stable_order_not_false_ranking():
    rows=[diagnostic('2330'),diagnostic('2317'),diagnostic('2454',H114量縮墊高='是')]
    # code 2454 highest by +10; 2317 and 2330 exactly tied
    f,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame(rows),research_frame=pd.DataFrame(),research_day=DAY)
    assert f['股票代號'].tolist()==['2454','2317','2330']
    assert f['研究排名'].tolist()==[1,2,2]

def test_veto_cannot_beat_safe_technicals_and_is_not_formal():
    rows=[diagnostic('2317',H109決策權威='FADE_VETO',H114量縮墊高='是'),diagnostic('2330')]
    f,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame(rows),research_frame=pd.DataFrame(),research_day=DAY)
    assert f.loc[0,'股票代號']=='2330'
    assert f.loc[1,'排名等級'].startswith('C')
    assert all('非H126' in x for x in f['研究聲明'])

def test_duplicate_stock_rows_cannot_select_best():
    rows=[diagnostic('2330'),diagnostic('2330',H114量縮墊高='是'),diagnostic('2317')]
    f,s,m=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame(rows),research_frame=pd.DataFrame(),research_day=DAY)
    assert f['股票代號'].tolist()==['2317']
    assert m['duplicate']==1

def test_research_keep_all_unranked_and_prioritize_ranked():
    r=pd.DataFrame([{'股票代號':'9999','研究層級':'C｜待補','股票名稱':'舊研究'}, {'股票代號':'2330','研究層級':'P1｜蓄勢','股票名稱':'研究樣本'}])
    f,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([diagnostic()]),research_frame=r,research_day=DAY)
    result=attach_rank_to_research(r,f)
    assert result.iloc[0]['股票代號']=='2330'
    assert result.iloc[0]['研究排名']==1
    assert pd.isna(result.iloc[1]['研究排名'])
    assert len(result)==len(r)

def test_ranked_five_plans_do_not_fake_price_targets():
    f,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([diagnostic()]),research_frame=pd.DataFrame(),research_day=DAY)
    x=build_ranked_five_plans(ranked=f,diagnosis_frame=pd.DataFrame([diagnostic()]),previous_plans=pd.DataFrame(),research_day=DAY)
    assert len(x)==1
    assert pd.isna(x.loc[0,'參考風險報酬比'])
    assert x.loc[0,'研究排名']==1
    assert '非H126' in x.loc[0,'交易資格']

def test_no_leads_no_fake_rank():
    f,s,m=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([{'股票代號':'2330'}]),research_frame=pd.DataFrame(),research_day=DAY)
    assert f.empty and m['total']==0

def monthly_pit(start='2024-10',periods=24,*,future_last=False):
    pds=pd.period_range(start,periods=periods,freq='M')
    rows=[]
    for i,p in enumerate(pds):
        # Actual reported 12-month improvement, not unknown future data.
        revenue=100_000_000 if i<12 else 130_000_000+i*100_000
        published=(p+1).to_timestamp()+pd.Timedelta(days=4)
        if future_last and i==periods-1:published=pd.Timestamp('2026-11-12')
        rows.append({'year_month':str(p).replace('-',''),
                     'released_at':published.date().isoformat(),
                     'revenue_twd':revenue,'source':'TWSE'})
    return rows

def test_growth_low_pe_cross_ranks_a_only_with_verified_publication_dates():
    facts={'2330':{'H114月營收歷史':monthly_pit(),
                  'H114本益比實值':10,'H114同業本益比中位數_官方':20,
                  'H114近四季EPS實值':8.5,'H114估值資料來源':'OFFICIAL_VERIFIED',
                  'H114估值資料日期':'2026-11-02'}}
    d=diagnostic()
    d['H114量縮墊高']='是'
    f,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([d]),
         research_frame=pd.DataFrame(),research_day=DAY,verified_fundamentals=facts)
    assert f.iloc[0]['排名等級'].startswith('A')
    assert f.iloc[0]['本益比']==10 and f.iloc[0]['近四季EPS']==8.5
    assert f.iloc[0]['營收年增率%']>15
    assert f.iloc[0]['營收創高']=='近12月新高'
    assert f.iloc[0]['下月營收模型情境(百萬)'] is not None
    assert '模型情境' in f.iloc[0]['營收預估創高判定']
    assert '未校準' in f.iloc[0]['預估資料限制']

def test_asof_future_publication_never_awarded_growth_points():
    facts={'2330':{'H114月營收歷史':monthly_pit(future_last=True),
                  'H114本益比實值':10,'H114同業本益比中位數_官方':20,
                  'H114近四季EPS實值':8.5,'H114估值資料來源':'OFFICIAL_VERIFIED',
                  'H114估值資料日期':'2026-11-12'}}
    f,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([diagnostic()]),
         research_frame=pd.DataFrame(),research_day=DAY,verified_fundamentals=facts)
    assert f.iloc[0]['本益比'] is None
    assert f.iloc[0]['營收年增率%'] is None
    assert f.iloc[0]['下月營收模型情境(百萬)'] is None
    assert f.iloc[0]['排名等級'].startswith('B')

def test_forecast_missing_month_no_synthetic_peak():
    from godpick_h153_growth_setup_rank import forecast_revenue_scenario
    records=monthly_pit()
    assert forecast_revenue_scenario(records,DAY)['estimate_m'] is not None
    missing=records[1:]
    assert forecast_revenue_scenario(missing,DAY)['estimate_m'] is None
