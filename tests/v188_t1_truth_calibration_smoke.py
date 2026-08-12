# -*- coding: utf-8 -*-
from __future__ import annotations
import sys
from pathlib import Path
from datetime import date, timedelta

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))

from godpick_t1_trade_truth import refresh_t1_trade_truth, build_probability_calibration

# Use recent relative dates so maturity logic remains valid on any test day.
today=date.today(); rec=(today-timedelta(days=4)); d1=rec+timedelta(days=1); d2=rec+timedelta(days=2)
# If dates land weekend it does not matter: synthetic replay is business-date ordered by strings.
rec_s=rec.isoformat(); d1_s=d1.isoformat(); d2_s=d2.isoformat()

base={
    "推薦日期":rec_s,"市場別":"上市","類別":"半導體","推薦價格":100,
    "SuperAI模型版本":"super_ai_godpick_v188_20260812","SuperAI隔日上漲機率%":60,"SuperAI校準後隔日上漲機率%":60,
    "正式推薦分區":"盤中核心雷達","主要進場路徑":"突破確認","實戰觸發價":102,"觸發後守價":101,"回測承接參考價":99,"停損參考":96,
}
records=[
    {**base,"股票代號":"1111","股票名稱":"觸發成功"},
    {**base,"股票代號":"2222","股票名稱":"未觸發但上漲","SuperAI隔日上漲機率%":55,"SuperAI校準後隔日上漲機率%":55,"回測承接參考價":95},
    {**base,"股票代號":"3333","股票名稱":"假突破","SuperAI隔日上漲機率%":65,"SuperAI校準後隔日上漲機率%":65},
]

def hist(code):
    if code=="1111":
        return [
            {"日期":rec_s,"開盤價":99,"最高價":101,"最低價":98,"收盤價":100,"還原收盤價":100},
            {"日期":d1_s,"開盤價":101,"最高價":106,"最低價":101,"收盤價":104,"還原收盤價":104},
            {"日期":d2_s,"開盤價":104,"最高價":108,"最低價":103,"收盤價":107,"還原收盤價":107},
        ]
    if code=="2222":
        return [
            {"日期":rec_s,"開盤價":99,"最高價":101,"最低價":98,"收盤價":100,"還原收盤價":100},
            {"日期":d1_s,"開盤價":100,"最高價":101.5,"最低價":99.5,"收盤價":101,"還原收盤價":101},
            {"日期":d2_s,"開盤價":101,"最高價":101.8,"最低價":100,"收盤價":101.5,"還原收盤價":101.5},
        ]
    return [
        {"日期":rec_s,"開盤價":99,"最高價":101,"最低價":98,"收盤價":100,"還原收盤價":100},
        {"日期":d1_s,"開盤價":101,"最高價":103,"最低價":97,"收盤價":99,"還原收盤價":99},
        {"日期":d2_s,"開盤價":98,"最高價":99,"最低價":95,"收盤價":96,"還原收盤價":96},
    ]

def history_provider(row):
    h=hist(row["股票代號"])
    return {"ok":True,"history":h,"latest":h[-1]["收盤價"],"source":"synthetic","fetched_at":d2_s}

bench=[{"date":rec_s,"close":1000},{"date":d1_s,"close":1005},{"date":d2_s,"close":1010}]
def benchmark_provider(): return {"twse":bench,"otc":bench}

res=refresh_t1_trade_truth(records,history_provider=history_provider,benchmark_provider=benchmark_provider,max_records=10,max_workers=3,persist=False)
assert res["ok"] and res["processed_this_run"]==3
by={r["股票代號"]:r for r in res["records"] if r["股票代號"] in {"1111","2222","3333"}}
assert by["1111"]["是否納入可執行績效"] is True
assert by["1111"]["Entry結果"] in {"WIN","OPEN｜已觸發，後續績效待成熟"}
assert by["1111"]["MFE%"] is not None and by["1111"]["MAE%"] is not None
assert by["2222"]["是否納入可執行績效"] is False
assert by["2222"]["Entry結果"].startswith("NO-TRADE")  # crucial: candidate gain is not trade win
assert by["2222"]["隔日候選漲跌%"]>0
assert by["3333"]["是否假突破"] is True
assert by["3333"]["Entry結果"].startswith("FAIL-SIGNAL")
# Benchmark +0.5%; first stock candidate +4% => +3.5% Selection Alpha.
assert abs(float(by["1111"]["Selection Alpha%"])-3.5)<0.05
cal=build_probability_calibration(list(by.values()))
assert cal["eligible_samples"]==3
assert cal["executable_samples"]==1
assert cal["brier_score"] is not None
print("PASS V188 T+1 truth/calibration | matured=3 | executable=1 | untriggered_not_trade_win=PASS")
