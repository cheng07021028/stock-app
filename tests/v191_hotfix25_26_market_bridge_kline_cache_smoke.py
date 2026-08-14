# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import sys
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))

import godpick_formal_recommendation_engine as formal
from godpick_market_candidate_bridge import apply_authoritative_market_context
import utils


def a_minus_row(kline='2026-08-14'):
    lag=0 if kline=='2026-08-14' else 1
    return {
        '股票代號':'9999','股票名稱':'H25測試','市場別':'上市','類別':'半導體',
        '最新價':100,'推薦買點_拉回':99,'實戰觸發價':102,'觸發後守價':100,
        '實戰停損距離%':5.0,'實戰壓力空間%':6.0,'實戰風險報酬比':1.20,'風險報酬比':1.20,
        '買進分數':68,'Entry進場買點分':75,'Risk風控安全分':68,'股神實戰總分':75,
        '技術結構分數':75,'起漲前兆分數':72,'交易可行分數':70,'推薦總分':78,'候選強度分':78,
        '主流資金分':65,'族群攻擊強度':60,'族群輪動分':60,'資金攻擊有效分':65,
        '成交額百萬':400,'20日均成交額百萬':350,'最新成交量_張':1500,'20日均量_張':1400,
        '追價風險分':45,'近5日漲幅%':1.0,'近20日漲幅%':8.0,'今日漲幅%':1.0,'當日量比':1.1,'當日收盤位置%':70,
        '爆發雷達分':60,'隔日爆發分':60,'強勢動能分':60,'強勢前兆分':60,'推薦角色':'B｜等突破確認','族群廣度分':60,
        'K線最後交易日':kline,'K線落後交易日':lag,'K線資料新鮮度':'最新' if lag==0 else '落後1個交易日',
        '本輪市場最新交易日':'2026-08-14',
        '官方資料完整度':100,'官方因子資料狀態':'完整','每日因子來源可信度':100,'因子來源可信度':100,
        '來源可信度狀態':'官方高可信','官方資料日期':'2026-08-14','官方因子資料日期':'2026-08-14',
        '法人資料日期':'2026-08-14','估值資料日期':'2026-08-14',
    }

SNAP={'twse_data_date':'2026-08-14','market_score':73.1,'market_trend':'中性偏多','market_risk_level':'中低','risk_gate':'normal','twse_source':'TWSE 收盤紀錄'}


def test_h25_market_date_stamp_restores_existing_a_minus_route():
    raw=pd.DataFrame([a_minus_row()])
    before=formal.apply_formal_recommendation_engine(raw.copy())
    assert before.iloc[0]['正式推薦分區'] not in {'正式下週主推薦','A-｜準主推薦小量試單'}
    bridged,report=apply_authoritative_market_context(raw,snapshot=SNAP)
    assert report['ok'] is True and report['stamped_rows']==1
    assert bridged.iloc[0]['大盤資料日期']=='2026-08-14'
    assert formal._market_risk_info(bridged.iloc[0])['formal_ready'] is True
    after=formal.apply_formal_recommendation_engine(bridged)
    assert after.iloc[0]['正式推薦分區']=='A-｜準主推薦小量試單', after.iloc[0].to_dict()


def test_h25_does_not_promote_t_minus_1_kline():
    bridged,_=apply_authoritative_market_context(pd.DataFrame([a_minus_row('2026-08-13')]),snapshot=SNAP)
    assert formal._market_risk_info(bridged.iloc[0])['formal_ready'] is True
    out=formal.apply_formal_recommendation_engine(bridged)
    assert out.iloc[0]['正式推薦分區'] not in {'正式下週主推薦','A-｜準主推薦小量試單'}, out.iloc[0].to_dict()


def test_h26_completed_friday_strict_after_midnight_weekend():
    tz=ZoneInfo('Asia/Taipei')
    assert utils._history_cache_allowed_business_lag(date(2026,8,14),datetime(2026,8,14,15,0,tzinfo=tz))==0
    assert utils._history_cache_allowed_business_lag(date(2026,8,14),datetime(2026,8,15,0,45,tzinfo=tz))==0
    assert utils._history_cache_allowed_business_lag(date(2026,8,14),datetime(2026,8,16,12,0,tzinfo=tz))==0
    assert utils._history_cache_allowed_business_lag(date(2026,8,14),datetime(2026,8,14,10,0,tzinfo=tz))==1


def test_page7_order_official_market_bridge_formal():
    src=(ROOT/'pages'/'7_股神推薦.py').read_text(encoding='utf-8')
    start=src.index('def _h20_rebuild_formal_partition_after_official_factors')
    end=src.index('def _recalc_night_strategy_after_macro_v100',start)
    block=src[start:end]
    assert block.index('_apply_official_factor_cache_v109(work)') < block.index('apply_authoritative_market_context(work, base_dir=BASE_DIR)') < block.index('apply_formal_recommendation_engine(work)')
