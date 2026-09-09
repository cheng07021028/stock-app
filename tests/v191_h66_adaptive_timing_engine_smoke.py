# -*- coding: utf-8 -*-
import pandas as pd
from godpick_h66_adaptive_timing_engine import (
    VERSION, apply_h66_adaptive_timing, build_h66_t1_timing_table,
    build_h66_learning_governance_table,
)

H65V = "v191_h65_multifactor_observation_radar_20260909"

def base(code, name):
    return {
        "股票代號": code, "股票名稱": name, "類別": "測試族群",
        "H65版本": H65V, "H65觀察層級": "W2｜提前卡位觀察", "H65多因子觀察分": 68,
        "H65主流分": 65, "H65趨勢分": 66, "H65大戶分": 62, "H65營收分": 70,
        "H65獲利EPS分": 68, "H65估值分": 55, "H65技術分": 65, "H65量能流動性分": 70,
        "Risk風控安全分": 68, "H54耗竭風險分": 45, "追價風險分": 45,
        "Entry進場買點分": 66, "H51Pivot起漲分": 66, "H51量價確認分": 68,
        "H57相對強度轉折分": 65, "H57資金加速度分": 65, "H57飆股發動前兆分": 66,
        "H57主流形成前兆分": 65, "H57壓縮轉擴張分": 65, "H57提前視窗分": 65,
        "H60主升段分": 65, "H60三因子共振分": 65, "H60大戶鎖碼真相分": 62,
        "H62新領漲分": 65, "H62增量機會分": 65,
        "法人籌碼官方分數": 65, "法人連買天數": 2,
        "外資近1日買賣超": 100, "外資近3日買賣超": 250, "外資近5日買賣超": 350,
        "投信近1日買賣超": 50, "投信近3日買賣超": 100, "投信近5日買賣超": 120,
        "三大法人近1日合計": 150, "三大法人近3日合計": 350, "三大法人近5日合計": 500,
        "法人買超占量比%": 3.0, "成交額百萬": 300,
        "月營收YoY%": 12, "EPS成長分數": 65,
        "H60千張大戶週變化pp": 0.2, "H64有效權威": "RESEARCH-ONLY",
    }

rows=[]
# Good next-session timing: strong close + flow + ignition.
g=base("GOOD","好時機")
g.update({"今日漲幅%":3.2,"當日收盤位置%":91,"上影線比例%":6,
          "H60主升階段":"MR1｜主升","H60三因子層級":"T3｜共振","H57前兆階段":"PI3｜高品質前兆",
          "外資近1日買賣超":5000,"投信近1日買賣超":1800,"三大法人近1日合計":7000,
          "H62增量機會分":58,"H64有效權威":"EFFECTIVE-FORMAL"})
rows.append(g)
# Bad contradiction: looks like a high opportunity but is being sold into the close.
b=base("BAD","高分倒貨")
b.update({"今日漲幅%":-4.6,"當日收盤位置%":4,"上影線比例%":38,
          "H60主升階段":"MR0｜非主升","H60三因子層級":"T0｜未共振","H57前兆階段":"PI0｜NORMAL",
          "外資近1日買賣超":-5000,"投信近1日買賣超":-800,"三大法人近1日合計":-6500,
          "外資近3日買賣超":-6000,"三大法人近3日合計":-7000,"H62增量機會分":82,
          "月營收YoY%":35,"H60千張大戶週變化pp":-0.4})
rows.append(b)
# Fundamental strength but price rejects good news.
n=base("NEWS","利多不漲")
n.update({"今日漲幅%":-2.4,"當日收盤位置%":22,"上影線比例%":44,"月營收YoY%":45,
          "EPS成長分數":88,"三大法人近1日合計":-1000,"外資近1日買賣超":-900,
          "H60主升階段":"MR2｜延續","H57前兆階段":"PI2｜形成中"})
rows.append(n)
# Swing quality good, T1 timing mediocre.
s=base("SWING","波段品質")
s.update({"今日漲幅%":-0.5,"當日收盤位置%":48,"上影線比例%":18,"H65營收分":90,"H65獲利EPS分":88,
          "H65大戶分":85,"H65主流分":80,"H65趨勢分":78,"三大法人近1日合計":20,
          "H60主升階段":"MR3｜主升研究","H57前兆階段":"PI1｜EARLY-SIGNAL"})
rows.append(s)

df=pd.DataFrame(rows)
out=apply_h66_adaptive_timing(df, truth_rows=[])
tab=build_h66_t1_timing_table(out,max_rows=10,max_per_sector=10,truth_rows=[])
assert VERSION == "v191_h66_adaptive_alpha_t1_timing_truth_20260909"
assert out.loc[out["股票代號"]=="GOOD","H64有效權威"].iloc[0] == "EFFECTIVE-FORMAL"  # H66 cannot rewrite Formal.
assert float(out.loc[out["股票代號"]=="BAD","H66矛盾訊號扣分"].iloc[0]) >= 20
assert float(out.loc[out["股票代號"]=="BAD","H66利多不漲扣分"].iloc[0]) >= 4
assert float(out.loc[out["股票代號"]=="NEWS","H66利多不漲扣分"].iloc[0]) >= 7
assert int(tab.loc[tab["股票代號"]=="GOOD","H66T1觀察順位"].iloc[0]) < int(tab.loc[tab["股票代號"]=="BAD","H66T1觀察順位"].iloc[0])
assert str(out.loc[out["股票代號"]=="BAD","H66T1層級"].iloc[0]).startswith("R0")
assert float(out.loc[out["股票代號"]=="SWING","H66T5波段品質分"].iloc[0]) > float(out.loc[out["股票代號"]=="SWING","H66T1自適應排序分"].iloc[0])

# Adaptive learning must be bounded and sample-aware.
truth=[]
for i in range(40):
    truth.append({
        "T1成熟": True,
        "Selection Alpha%": (i-20)/10.0,
        "H66收盤品質分": i*2.0,
        "H66法人加速度分": 100-i*2.0,
        "H66主流點火分": 50+i*0.2,
        "H66技術買點分": 50+i*0.1,
        "H66量能流動性分": 60,
        "H66結構品質分": 55+i*0.1,
        "H66短線動能分": 50+i*0.2,
    })
gov=build_h66_learning_governance_table(truth)
close_mult=float(gov.loc[gov["H66學習項目"]=="收盤品質","自適應倍率"].iloc[0])
inst_mult=float(gov.loc[gov["H66學習項目"]=="法人加速度","自適應倍率"].iloc[0])
assert 1.0 < close_mult <= 1.15
assert 0.85 <= inst_mult < 1.0
assert gov["成熟樣本數"].eq(40).all()
print("PASS H66 adaptive timing engine", tab[["H66T1觀察順位","股票代號","H66T1自適應排序分","H66T1層級"]].to_dict("records"))
