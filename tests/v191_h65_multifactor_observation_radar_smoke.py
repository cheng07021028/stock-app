# -*- coding: utf-8 -*-
from pathlib import Path
import random
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from godpick_h65_multifactor_observation_engine import (
    VERSION, PILLAR_WEIGHTS, apply_h65_multifactor_observation,
    build_h65_observation_radar_table, build_h65_indicator_coverage_table,
)

EXPECTED = "v191_h65_multifactor_observation_radar_20260909"


def rich_row(code: str, sector: str = "AI伺服器", level: float = 85.0, formal: bool = False):
    return {
        "股票代號": code, "股票名稱": f"測試{code}", "類別": sector,
        "H64版本": "v191_h64_strong_mainstream_holder_core_truth_20260908",
        "H64有效權威": "EFFECTIVE-FORMAL" if formal else "RADAR",
        "H64研究層級": "C1｜核心強勢主流Formal" if formal else "C2｜核心強勢主流研究",
        "H64主流真相分": level, "H64真強勢分": level,
        "H64鎖碼確認分": level, "H64鎖碼趨勢狀態": "LC｜TDCC千張大戶增持鎖碼確認",
        "H64核心共振分": level, "H64品質閘門": "PASS｜測試",
        "H51族群主線分": level, "H53族群共振分": level, "H53族群廣度分": level,
        "H57主流形成前兆分": level, "H60主升段分": level,
        "H47個股相對強度分": level, "H51個股領漲品質分": level, "強勢動能分": level,
        "技術結構分數": level, "近5日漲幅%": 2 + (level-50)/20, "近20日漲幅%": 7 + (level-50)/10,
        "法人籌碼官方分數": level, "外資近5日買賣超": (level-50)*1000,
        "投信近5日買賣超": (level-50)*500, "三大法人近5日合計": (level-50)*1600,
        "法人連買天數": max(0, int((level-50)/7)),
        "H60大戶鎖碼真相分": level, "H60千張大戶持股比%": 66,
        "H60千張大戶週變化pp": 0.65,
        "營收成長官方分數": level, "月營收YoY%": level-45, "月營收MoM%": (level-60)/2,
        "累計營收YoY%": level-50,
        "官方基本面成長分數": level, "EPS成長分數": level, "EPS成長率%": level-50,
        "估算EPS": max(0.5, (level-45)/5), "ROE%": max(3, (level-35)/2),
        "官方估值風險分數": max(15, 100-level), "PER本益比": 18 + max(0, 70-level)/3,
        "PBR股價淨值比": 2.2, "股利殖利率%": 3.0,
        "Entry進場買點分": level, "H51Pivot起漲分": level, "H51量價確認分": level,
        "RSI14": 60, "強勢前兆分": level,
        "H51流動性分": level, "成交額百萬": level*20, "量比": 1.8,
        "H57資金加速度分": level, "H57飆股發動前兆分": level, "H62新領漲分": level,
        "H62增量上漲空間分": level, "H55催化代理分": level, "今日訊號新鮮分": level,
        "H54耗竭風險分": 35, "追價風險分": 35, "Risk風控安全分": 82,
        "K線資料新鮮度": "最新", "股神資料總新鮮度": "最新", "V188交易許可": "研究",
    }


def main():
    assert VERSION == EXPECTED
    assert round(sum(PILLAR_WEIGHTS.values()), 8) == 100.0

    # A. Strong multi-factor research name becomes W1/W2; formal identity stays formal.
    df = pd.DataFrame([
        rich_row("1001", level=94),
        rich_row("1002", level=86),
        rich_row("1003", sector="半導體設備", level=76),
        rich_row("1004", sector="散熱", level=63),
        rich_row("1005", sector="航運", level=44),
        rich_row("1006", sector="AI伺服器", level=91, formal=True),
    ])
    out = apply_h65_multifactor_observation(df)
    assert set(["H65多因子觀察分", "H65觀察層級", "H65升級缺口", "H65資料覆蓋%"]).issubset(out.columns)
    assert out["H65多因子觀察分"].between(0, 100).all()
    assert out["H65資料覆蓋%"].between(0, 100).all()
    by = out.set_index("股票代號")
    assert str(by.at["1001", "H65觀察層級"]).startswith(("W1", "W2")), by.loc["1001"].to_dict()
    assert str(by.at["1006", "H65觀察層級"]).startswith("F1"), by.loc["1006"].to_dict()
    assert by.at["1006", "H64有效權威"] == "EFFECTIVE-FORMAL"  # H65 never rewrites authority.

    # B. Formal=0 still yields a useful diversified radar rather than an empty screen.
    no_formal = df.loc[df["H64有效權威"].ne("EFFECTIVE-FORMAL")].copy()
    radar = build_h65_observation_radar_table(no_formal, max_rows=5, max_per_sector=2)
    assert len(radar) == 5, radar
    assert radar["H65觀察順位"].tolist() == list(range(1, 6))
    assert radar["股票代號"].astype(str).nunique() == len(radar)
    assert radar["H65觀察層級"].astype(str).str.startswith(("W1", "W2", "W3", "R0")).all()

    # C. Missing/stale data is not silently promoted.
    sparse = pd.DataFrame([{
        "股票代號": "2001", "股票名稱": "缺資料", "類別": "測試",
        "H64版本": "v191_h64_strong_mainstream_holder_core_truth_20260908",
        "H64有效權威": "RADAR", "H64研究層級": "D0｜非核心候選降權",
        "H64主流真相分": 92, "K線資料新鮮度": "待更新",
    }])
    sparse_out = apply_h65_multifactor_observation(sparse).iloc[0]
    assert float(sparse_out["H65資料覆蓋%"]) < 40
    assert str(sparse_out["H65觀察層級"]).startswith("R0")

    # D. Indicator map explicitly audits broader future coverage instead of hiding gaps.
    coverage = build_h65_indicator_coverage_table(df)
    assert len(coverage) >= 15
    assert {"市場環境", "法人籌碼", "大戶TDCC", "營收成長", "獲利EPS", "估值", "AI績效學習"}.issubset(set(coverage["指標群"]))

    # E. Fuzz/missing-column simulation: 1,000 rows must remain bounded and sortable.
    random.seed(6509)
    rows = []
    for i in range(1000):
        r = rich_row(f"{3000+i:04d}", sector=f"族群{i%17}", level=random.uniform(35, 95))
        # Randomly drop many non-identity fields to simulate incomplete official feeds.
        for k in list(r.keys()):
            if k not in {"股票代號", "股票名稱", "類別", "H64版本", "H64有效權威", "H64研究層級", "H64核心共振分"} and random.random() < 0.35:
                r.pop(k, None)
        rows.append(r)
    fuzz = apply_h65_multifactor_observation(pd.DataFrame(rows))
    assert len(fuzz) == 1000
    assert fuzz["H65多因子觀察分"].between(0, 100).all()
    assert fuzz["H65風險扣分"].between(0, 22).all()
    fuzz_radar = build_h65_observation_radar_table(fuzz, max_rows=30, max_per_sector=3)
    assert len(fuzz_radar) == 30
    assert fuzz_radar["股票代號"].nunique() == 30

    page = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h66_adaptive_alpha_t1_timing_truth_20260909"' in page
    assert 'EXCEL_COLUMN_LAYOUT_VERSION = "V191-H66-ADAPTIVE-ALPHA-T1-TIMING-TRUTH-20260909"' in page
    assert "H65 全市場多因子觀察雷達" in page
    assert '"多因子觀察雷達"' in page and '"多因子指標覆蓋"' in page
    assert "W1/W2/W3" in page
    print("PASS H65 multi-factor observation radar smoke + 1000-row fuzz simulation")


if __name__ == "__main__":
    main()
