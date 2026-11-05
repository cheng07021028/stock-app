# -*- coding: utf-8 -*-
"""H153.1 no inferred resistance without same-date official SHA source."""
import gzip,json
from pathlib import Path
import pandas as pd
from godpick_h117_official_market_closure import make_official_day_manifest
from godpick_h1531_multiday_resilience import enrich_multiday_resilience
from test_h153_growth_technical_ranking import diagnostic,DAY


def store_bench(root:Path,*,tamper=False):
    days=['2026-10-27','2026-10-28','2026-10-29','2026-10-30','2026-11-02','2026-11-03']
    closes=[10000,9900,9700,9600,9550,9500]
    p=root/'TAIEX';p.mkdir(parents=True,exist_ok=True)
    for d,v in zip(days,closes):
        normalized={'source':'TAIEX','day':d,'source_url':'https://openapi.twse.com.tw/v1/indicesReport/MI_5MINS_HIST',
           'rows':[{'code':'BENCHMARK','date':d,'open':v,'high':v+20,'low':v-20,'close':v,'exchange':'TAIEX'}]}
        manifest=make_official_day_manifest(normalized,collected_at=d+'T16:00:00+08:00')
        if tamper and d=='2026-10-29':manifest['rows'][0]['close']=1
        with gzip.open(p/(d+'.json.gz'),'wt',encoding='utf8') as f:json.dump(manifest,f)
    return days


def sample():
    diag=diagnostic('2330')
    diag.update({'市場別':'上市','H114近3日基準交易日':'2026-10-29',
      'H114近5日基準交易日':'2026-10-27',
      'H114近3日漲跌幅%':0.1,'H114當前5日漲跌幅%':0.4})
    return pd.DataFrame([diag]),pd.DataFrame([{'股票代號':'2330','排名等級':'B｜研究','研究排名':1}])


def test_missing_market_file_refuses_multiday(tmp_path):
    src,rank=sample()
    result,summary,meta=enrich_multiday_resilience(rank,src,research_day=DAY,root=tmp_path)
    assert pd.isna(result.loc[0,'3日相對抗跌pp'])
    assert meta['matched_3d']==0


def test_valid_sha_same_date_supports_three_five_day(tmp_path):
    store_bench(tmp_path)
    src,rank=sample()
    result,_,meta=enrich_multiday_resilience(rank,src,research_day=DAY,root=tmp_path)
    assert result.loc[0,'3日相對抗跌pp']>0
    assert result.loc[0,'5日相對抗跌pp']>0
    assert meta['matched_3d']==1 and meta['matched_5d']==1


def test_otc_never_uses_taiex(tmp_path):
    store_bench(tmp_path)
    src,rank=sample();src.loc[0,'市場別']='上櫃'
    result,_,meta=enrich_multiday_resilience(rank,src,research_day=DAY,root=tmp_path)
    assert meta['matched_3d']==0
    assert '不得使用加權指數' in result.loc[0,'多日抗跌來源狀態']


def test_tampered_official_market_sha_is_rejected(tmp_path):
    store_bench(tmp_path,tamper=True)
    src,rank=sample()
    result,_,meta=enrich_multiday_resilience(rank,src,research_day=DAY,root=tmp_path)
    assert meta['matched_3d']==0 and pd.isna(result.loc[0,'3日相對抗跌pp'])


def test_market_reference_day_must_match_stock(tmp_path):
    store_bench(tmp_path)
    src,rank=sample();src.loc[0,'H114近3日基準交易日']='2026-10-28'
    result,_,meta=enrich_multiday_resilience(rank,src,research_day=DAY,root=tmp_path)
    assert meta['matched_3d']==0 and meta['matched_5d']==1
