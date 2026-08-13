# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import date, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_perf_fast_update_v77 import update_record_perf
from godpick_t1_trade_truth import refresh_t1_trade_truth, build_probability_calibration


def quote(rec_date: str, rec_price: float, next_row: dict):
    return {
        "ok": True,
        "history": [
            {"日期": rec_date, "開盤價": rec_price, "最高價": rec_price, "最低價": rec_price,
             "收盤價": rec_price, "還原收盤價": rec_price, "成交量": 1000},
            dict(next_row),
        ],
        "latest": next_row["收盤價"],
        "source": "H9 synthetic OHLC",
        "fetched_at": f"{next_row['日期']} 15:00:00",
    }


# 1) 2026-08-12 京元電子事故：當日最低 253，理論回測價 250.9667 沒有成交。
# H9 must never fabricate the lower theoretical fill or count it as a WIN.
ky = {
    "推薦日期": "2026-08-12", "股票代號": "2449", "股票名稱": "京元電子", "推薦價格": 259.0,
    "主要進場路徑": "觸發守價回測", "守價回測參考價": 264.0,
    "實戰觸發價": 268.5, "觸發後守價": 264.0, "回測承接參考價": 250.9667,
}
ky_day = {"日期": "2026-08-13", "開盤價": 261.0, "最高價": 262.0, "最低價": 253.0,
          "收盤價": 253.5, "還原收盤價": 253.5, "成交量": 10000}
ky_out = update_record_perf(ky, quote("2026-08-12", 259.0, ky_day), track_days=[1])
assert ky_out["是否納入可執行績效"] is True, ky_out
assert 253.0 <= float(ky_out["執行基準價"]) <= 262.0, ky_out
assert abs(float(ky_out["執行基準價"]) - 250.9667) > 1.0, ky_out
assert abs(float(ky_out["理論進場參考價"]) - 250.9667) < 1e-6, ky_out
assert ky_out["執行價可成交驗證"] is True, ky_out
assert float(ky_out["觸發當日收盤績效%"]) < 0, ky_out
print("PASS H9 京元：理論250.9667與實際可成交價分離；採OHLC內254.7312，舊+1.01% WIN翻為實際負報酬")


# 2) 2026-08-12 大毅事故：High 141 crossed 134.5 but Close 130 broke Hold 132.
# It is a touched-then-failed signal, NOT an untriggered candidate.
yi = {
    "推薦日期": "2026-08-12", "股票代號": "2478", "股票名稱": "大毅", "推薦價格": 128.5,
    "主要進場路徑": "回測承接", "實戰觸發價": 134.5, "觸發後守價": 132.0,
    "回測承接參考價": 125.57,
}
yi_day = {"日期": "2026-08-13", "開盤價": 131.0, "最高價": 141.0, "最低價": 129.5,
          "收盤價": 130.0, "還原收盤價": 130.0, "成交量": 10000}
yi_out = update_record_perf(yi, quote("2026-08-12", 128.5, yi_day), track_days=[1])
assert "觸發後失守" in yi_out["進場觸發狀態"], yi_out
assert "假突破" in yi_out["進場觸發狀態"], yi_out
assert yi_out["進場觸發日期"] == "2026-08-13", yi_out
assert yi_out["是否納入可執行績效"] is False, yi_out
assert abs(float(yi_out["執行基準價"]) - 134.5) < 1e-8, yi_out
assert yi_out["執行價可成交驗證"] is True, yi_out
print("PASS H9 大毅：High已觸發但Close跌破守價 => 觸發後失守，不再誤標未觸發")


# 3) Gap-up breakout cannot pretend the old trigger was the fill.
gap = {
    "推薦日期": "2026-08-12", "股票代號": "9999", "推薦價格": 100.0,
    "主要進場路徑": "突破確認", "實戰觸發價": 105.0, "觸發後守價": 104.0,
}
gap_day = {"日期": "2026-08-13", "開盤價": 108.0, "最高價": 112.0, "最低價": 107.0,
           "收盤價": 111.0, "還原收盤價": 111.0, "成交量": 5000}
gap_out = update_record_perf(gap, quote("2026-08-12", 100.0, gap_day), track_days=[1])
assert gap_out["是否納入可執行績效"] is True, gap_out
assert abs(float(gap_out["執行基準價"]) - 108.0) < 1e-8, gap_out
assert "開盤" in gap_out["執行價來源"], gap_out
print("PASS H9 跳空突破：採實際Open 108，不用不存在的Trigger 105美化績效")


# 4) Daily cohort identity: first-seen date may be old, but each new batch date remains its own truth sample.
today = date.today()
cohort1 = (today - timedelta(days=4)).isoformat()
cohort2 = (today - timedelta(days=2)).isoformat()
d1 = (today - timedelta(days=3)).isoformat()
d2 = (today - timedelta(days=1)).isoformat()
base = {
    "股票代號": "5347", "股票名稱": "世界", "市場別": "上櫃", "類別": "半導體",
    "推薦價格": 100.0, "正式推薦分區": "盤中雷達追蹤", "主要進場路徑": "突破確認",
    "實戰觸發價": 101.0, "觸發後守價": 100.0, "SuperAI模型版本": "super_ai_h9",
    "SuperAI隔日上漲機率%": 60.0, "SuperAI校準後隔日上漲機率%": 60.0,
    "原始推薦日期": cohort1,
}
records = [
    {**base, "推薦日期": cohort1, "推薦批次日期": cohort1, "推薦執行ID": "run-old"},
    {**base, "推薦日期": cohort1, "推薦批次日期": cohort2, "推薦執行ID": "run-new"},
    # Same-day rerun must not inflate sample count.
    {**base, "推薦日期": cohort1, "推薦批次日期": cohort2, "推薦執行ID": "run-new-rerun"},
]

def provider(row):
    cohort = row.get("推薦批次日期")
    if cohort == cohort1:
        hist = [
            {"日期": cohort1, "開盤價": 100, "最高價": 100, "最低價": 100, "收盤價": 100, "還原收盤價": 100},
            {"日期": d1, "開盤價": 101, "最高價": 103, "最低價": 100, "收盤價": 102, "還原收盤價": 102},
        ]
    else:
        hist = [
            {"日期": cohort2, "開盤價": 100, "最高價": 100, "最低價": 100, "收盤價": 100, "還原收盤價": 100},
            {"日期": d2, "開盤價": 101, "最高價": 104, "最低價": 100, "收盤價": 103, "還原收盤價": 103},
        ]
    return {"ok": True, "history": hist, "latest": hist[-1]["收盤價"], "source": "cohort synthetic", "fetched_at": d2}

res = refresh_t1_trade_truth(
    records, history_provider=provider, benchmark_provider=lambda: {"twse": [], "otc": []},
    max_records=10, max_workers=1, persist=False,
)
world_rows = [r for r in res["records"] if r.get("股票代號") == "5347" and r.get("推薦日期") in {cohort1, cohort2}]
assert len(world_rows) == 2, world_rows
assert {r["推薦日期"] for r in world_rows} == {cohort1, cohort2}, world_rows
new_row = next(r for r in world_rows if r["推薦日期"] == cohort2)
assert new_row["原始推薦日期"] == cohort1, new_row
assert new_row["推薦執行ID"] in {"run-new", "run-new-rerun"}, new_row
print("PASS H9 每日cohort：世界跨日重現保留兩個日期；同日rerun不重複灌樣本")

# 4b) Legacy Page07 repair: old builder could keep first-seen 推薦日期 while 建立時間
# was the real batch date.  H9 must recover the 8/12 cohort without mutating authority.
legacy_world = {
    **base,
    "推薦日期": cohort1,
    "建立時間": f"{cohort2} 14:42:10",
    "紀錄來源": "07_股神推薦",
}
legacy_res = refresh_t1_trade_truth(
    [legacy_world], history_provider=provider, benchmark_provider=lambda: {"twse": [], "otc": []},
    max_records=10, max_workers=1, persist=False,
)
legacy_truth = next(r for r in legacy_res["records"] if r.get("股票代號") == "5347")
assert legacy_truth["推薦日期"] == cohort2, legacy_truth
assert legacy_truth["推薦批次日期"] == cohort2, legacy_truth
assert legacy_truth["原始推薦日期"] == cohort1, legacy_truth
print("PASS H9 歷史批次修復：舊Page07 first-seen日期 + 建立時間可還原真正每日cohort")


# 5) Calibration only counts OHLC-verifiable executable fills.
truth_like = [
    {"T1成熟": True, "SuperAI校準後上漲機率%": 64.6, "隔日候選漲跌%": -2.12,
     "是否納入可執行績效": True, "觸發當日收盤績效%": -0.48},
    {"T1成熟": True, "SuperAI校準後上漲機率%": 64.4, "隔日候選漲跌%": 1.17,
     "是否納入可執行績效": False, "觸發當日收盤績效%": -3.35},
    {"T1成熟": True, "SuperAI校準後上漲機率%": 59.8, "隔日候選漲跌%": -0.15,
     "是否納入可執行績效": False},
]
cal = build_probability_calibration(truth_like)
assert cal["eligible_samples"] == 3, cal
assert cal["executable_samples"] == 1, cal
assert cal["executable_win_rate_pct"] == 0.0, cal
assert float(cal["avg_executable_ret_pct"]) < 0, cal
print("PASS H9 校準：京元改以可成交負報酬計入；大毅失守不灌交易勝率")
