# -*- coding: utf-8 -*-
import pandas as pd
from godpick_h74_fresh_mainstream_capital_engine import (
    VERSION, apply_h74_fresh_mainstream_capital, build_h74_decision_overview,
    build_h74_excel_guide, build_h74_governance_summary,
)

H73V="v191_h73_leadership_breadth_distribution_truth_20260916"

def row(code,name,*,quality=70,strong=65,main=55,inst=60,cap3=20,cap5=15,vol3=15,vol5=10,fresh=60,repeat=0,near5=0,tdcc=60,delta=None,h73=70):
    return {
        "股票代號":code,"股票名稱":name,"類別":"半導體業","市場別":"上市",
        "H73版本":H73V,"H73研究排序分":h73,"H73全市場順位":1,"H73領先加速分":strong,
        "H73族群廣度分":main,"H72風險調整分":72,"H72品質獲利模型分":quality,
        "H72動能相對強度模型分":strong,"H72突破時機模型分":strong,"H72法人需求模型分":inst,
        "H72大戶鎖碼模型分":tdcc,"H64真強勢分":strong,"H64主流真相分":main,
        "H66短線動能分":strong,"H66法人加速度分":inst,"H66主流點火分":main,
        "H57主流形成前兆分":main,"H57提前視窗分":fresh,"今日訊號新鮮分":fresh,"H50族群新鮮度分":main,
        "H47個股相對強度分":strong,"法人連買代理分數":inst,
        "成交額3日加速度%":cap3,"成交額5日加速度%":cap5,"成交量3日加速度%":vol3,"成交量5日加速度%":vol5,
        "外資近1日買賣超":2000 if inst>=60 else -1000,"外資近5日買賣超":6000 if inst>=60 else -3000,
        "投信近1日買賣超":800 if inst>=60 else -300,"投信近5日買賣超":2000 if inst>=60 else -700,
        "H61重複慣性扣分":repeat,"近5次入榜次數":near5,"H60千張大戶持股比%":tdcc,
        "H60千張大戶週變化pp":delta,"H64有效權威":"A-MINUS","H68次日執行狀態":"REVALIDATE",
    }

def main():
    df=pd.DataFrame([
        row("A","FreshChip",quality=74,strong=84,main=72,inst=86,cap3=120,cap5=80,vol3=100,vol5=70,fresh=86,tdcc=68,delta=0.55,h73=76),
        row("B","StaleQuality",quality=96,strong=63,main=40,inst=43,cap3=-45,cap5=-35,vol3=-30,vol5=-25,fresh=34,repeat=15,near5=4,tdcc=92,delta=None,h73=88),
        row("C","RenewedLeader",quality=80,strong=88,main=68,inst=90,cap3=95,cap5=75,vol3=80,vol5=70,fresh=82,repeat=18,near5=4,tdcc=72,delta=0.22,h73=79),
        row("D","StaticHolder",quality=78,strong=62,main=53,inst=55,cap3=5,cap5=2,vol3=3,vol5=1,fresh=55,tdcc=95,delta=None,h73=74),
        row("E","Distributor",quality=76,strong=78,main=60,inst=72,cap3=50,cap5=45,vol3=40,vol5=35,fresh=70,tdcc=80,delta=-0.60,h73=78),
    ])
    out=apply_h74_fresh_mainstream_capital(df)
    assert VERSION=="v191_h74_fresh_mainstream_capital_rotation_truth_20260917"
    ranked=out.sort_values("H74全市場順位")
    assert ranked.iloc[0]["股票名稱"] in {"FreshChip","RenewedLeader"}
    stale=out.loc[out["股票名稱"].eq("StaleQuality")].iloc[0]
    fresh=out.loc[out["股票名稱"].eq("FreshChip")].iloc[0]
    renewed=out.loc[out["股票名稱"].eq("RenewedLeader")].iloc[0]
    static=out.loc[out["股票名稱"].eq("StaticHolder")].iloc[0]
    dist=out.loc[out["股票名稱"].eq("Distributor")].iloc[0]
    assert fresh["H74決策總分"] > stale["H74決策總分"]
    assert stale["H74陳舊品質扣分"] > 0
    assert renewed["H74熟面孔慣性扣分"] < 18  # fresh evidence can renew a familiar leader
    assert str(static["H74大戶鎖碼狀態"]).startswith("UNCONFIRMED_NO_PRIOR")
    assert static["H74大戶鎖碼真相分"] <= 52
    assert str(dist["H74大戶鎖碼狀態"]).startswith("DISTRIBUTING")
    assert out["H64有效權威"].eq("A-MINUS").all()  # H74 never creates Formal
    overview=build_h74_decision_overview(df,max_rows=5)
    assert overview.columns[0]=="AI決策順位" and "是否正式推薦" in overview.columns
    guide=build_h74_excel_guide(); assert guide.iloc[0]["活頁"]=="AI決策總覽"
    gov=build_h74_governance_summary(df); assert "TDCC缺前期比較" in set(gov["治理項目"])
    print("H74 FRESH MAINSTREAM CAPITAL SMOKE: PASS")

if __name__=="__main__": main()
