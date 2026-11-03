# -*- coding: utf-8 -*-
"""H152 anti-fall / next session verification: no price/date invention."""
from __future__ import annotations
from pathlib import Path
import pandas as pd
from godpick_h152_resilience_forecast import build_resilience_research

DAY='2026-11-02'
BENCH={'data_date':DAY,'twse_change_pct':-2.75,'otc_change_pct':-3.10,'source':'verified TWSE/TPEX snapshot'}

def stock(code='2352', market='上市', ret=0.3, vol=1.5, **extra):
    p=100
    row={'股票代號':code,'股票名稱':'示範股','類別':'電子','市場別':market,
         'H114K線資料狀態':'VALID_OHLCV','H114K線有效根數':80,
         'H114K線證據日':DAY,'H114前次交易日':'2026-10-30',
         'H114前日收盤價':p,'H114當日收盤價':p*(1+ret/100),
         'H114當日漲跌幅%':ret,'H113當日成交額百萬':500,
         'H113二十日均成交額百萬':220,'H114當日成交量張':4200,
         'H114K線量比':vol,'H109決策權威':'UNKNOWN',
         'H114同型歷史樣本數':13,'H114同型樣本隔日上漲率%':66}
    row.update(extra)
    return row

def run(rows, snapshot=None, leads=None):
    return build_resilience_research(diagnosis_frame=pd.DataFrame(rows),
        research_frame=pd.DataFrame(leads or []),research_day=DAY,
        market_snapshot=BENCH if snapshot is None else snapshot,
        authority_frame=pd.DataFrame([{'股票代號':'2330','H126買進資格':'禁止買進'}]))

def test_resilient_flat_stock_gets_research_even_if_not_in_old_h113_pool():
    obs,research,stat,meta=run([stock(ret=0.0)])
    assert len(obs)==1 and obs.iloc[0]['股票代號']=='2352'
    assert obs.iloc[0]['相對強度pp']==2.75
    assert '抗跌' in obs.iloc[0]['抗跌判定']
    assert research.iloc[0]['研究層級'].startswith('R｜')
    assert '未授權' in obs.iloc[0]['交易資格']
    assert meta['added']==1 and meta['formal_h126_modified'] is False

def test_otc_must_use_otc_benchmark_and_never_twse():
    obs,_,_,_=run([stock(code='3105',market='上櫃',ret=-0.4)])
    assert len(obs)==1 and obs.iloc[0]['對照指數']=='櫃買指數'
    assert obs.iloc[0]['大盤同日漲跌%']==-3.10

def test_stale_market_snapshot_blocks_resilience_even_if_stock_strong():
    obs,board,stat,meta=run([stock()],snapshot={**BENCH,'data_date':'2026-10-30'})
    assert obs.empty and len(board)==0 and meta['strong_resilience']==0
    assert not meta['market_aligned'] and '未對齊' in meta['warning']

def test_missing_market_classification_cannot_switch_to_twse_by_default():
    obs,*_=run([stock(market='')]);assert obs.empty

def test_fake_stock_date_or_halt_gaps_block_resilience():
    for extra in [{'H114K線證據日':'2026-10-30'},
                  {'H114前次交易日':'2026-10-01'},
                  {'H114K線有效根數':30},
                  {'H114當日收盤價':110}]:
        obs,*_=run([stock(**extra)])
        assert obs.empty

def test_thin_volume_halts_fake_antifall_and_fade_preserves_veto():
    obs,*_=run([stock(H113當日成交額百萬=5)]);assert obs.empty
    obs,*_=run([stock(vol=0.3)]);assert obs.empty
    obs,*_=run([stock(H109決策權威='FADE_VETO')]);assert 'FADE_VETO' in obs.iloc[0]['隔日方向情境'] or 'H109不可買進' in obs.iloc[0]['隔日方向情境']
    assert obs.iloc[0]['交易資格'].startswith('逆勢／')

def test_no_unearned_probability_or_false_sample_confidence():
    obs,*_=run([stock()]);assert pd.isna(obs.iloc[0]['歷史樣本次日上漲率%'])
    obs,*_=run([stock(**{'H114同型歷史樣本數':35,'H114同型樣本隔日上漲率%':49})])
    assert obs.iloc[0]['歷史樣本次日上漲率%']==49
    assert '情境' in obs.iloc[0]['隔日方向情境']
    assert '機率' not in obs.iloc[0]['隔日方向情境']

def test_original_leads_preserved_without_invented_h126_or_rank():
    old={'研究層級':'A｜同日量價研究已核','股票代號':'2330','股票名稱':'示範龍頭',
         '來源證據':'現有證據','量比':2}
    obs,merged,summary,meta=run([stock(),stock(code='2330',ret=-4)],leads=[old])
    assert len(merged)==2 and merged.iloc[0]['股票代號']=='2330'
    assert merged.iloc[0]['來源證據']=='現有證據'
    assert merged.iloc[1]['股票代號']=='2352'
    assert 'H152抗跌候選總數' in summary['檢查項目'].tolist()
    assert meta['added']==1

def test_duplicate_code_does_not_select_preferred_stock_record():
    obs,_,summary,meta=run([stock(),stock(H114當日收盤價=98)])
    assert obs.empty and meta['reasons']['重複代號隔離']==1

def test_old_main_sheet_and_h126_are_untouched_by_h152():
    p=Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py'
    src=p.read_text('utf-8')
    assert '_h152_research(' in src and '_h151_section_sheets(' in src
    assert "research=locals().get('h152_named_research'" in src
    assert "('00_H126主管決策'," in src or '"00_H126主管決策", tables' in src


def test_h114_derived_day_return_has_real_prior_session_and_no_future_data():
    from datetime import date, timedelta
    from godpick_h114_parallel_alpha import analyze_history
    start=date(2026,7,1);d=[]
    current=start
    for k in range(90):
        while current.weekday()>=5:current+=timedelta(days=1)
        close=90+k*.2
        d.append({'日期':str(current),'開盤價':close-.1,'最高價':close+.3,
                  '最低價':close-.3,'收盤價':close,'成交量(張)':1000+k*2})
        current+=timedelta(days=1)
    report=analyze_history(pd.DataFrame(d),asof=d[-1]['日期'])
    assert report['H114K線資料狀態']=='VALID_OHLCV'
    assert report['H114前日收盤價']==round(d[-2]['收盤價'],3)
    assert report['H114前次交易日']==d[-2]['日期']
    assert report['H114當日收盤價']==round(d[-1]['收盤價'],3)
    assert abs(report['H114當日漲跌幅%']-(d[-1]['收盤價']/d[-2]['收盤價']-1)*100)<.01
    earlier=analyze_history(pd.DataFrame(d),asof=d[-5]['日期'])
    assert earlier['H114K線證據日']==d[-5]['日期']
    assert earlier['H114當日收盤價']==d[-5]['收盤價']


def test_h117_official_index_fallback_requires_sha_and_two_dates(tmp_path):
    import gzip,json
    from godpick_h117_official_market_closure import make_official_day_manifest,MARKET_URLS
    from godpick_h152_resilience_forecast import supplement_with_verified_h117
    folder=tmp_path/'TAIEX';folder.mkdir()
    for ds,close in [('2026-10-30',100),('2026-11-02',98)]:
        info={'source':'TAIEX','day':ds,
              'source_url':MARKET_URLS['TAIEX'],
              'rows':[{'code':'BENCHMARK','date':ds,'close':close}],
              'skipped':0}
        manifest=make_official_day_manifest(info,collected_at=ds+'T20:00:00+08:00')
        with gzip.open(folder/(ds+'.json.gz'),'wt',encoding='utf-8') as f:json.dump(manifest,f)
    new=supplement_with_verified_h117({'data_date':'2026-07-11','twse_change_pct':-0.8},
        research_day=DAY,root=tmp_path)
    assert new['data_date']==DAY and new['twse_change_pct']==-2.0
    assert 'otc_change_pct' not in new
    # Once content was tampered, the fallback must fail safely.
    path=folder/'2026-11-02.json.gz'
    with gzip.open(path,'rt',encoding='utf-8') as f:bad=json.load(f)
    bad['rows'][0]['close']=109
    with gzip.open(path,'wt',encoding='utf-8') as f:json.dump(bad,f)
    refused=supplement_with_verified_h117({'data_date':'2026-07-11','twse_change_pct':-0.8},
        research_day=DAY,root=tmp_path)
    assert refused['data_date']=='2026-07-11'
