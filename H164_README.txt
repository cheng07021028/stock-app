V191-H164｜交易員決策重點 × 抗跌雙層分級 × 證據分池 × 次日情境
===========================================================
原始程式基準：GitHub main 2026-11-16 核對
  pages/7_股神推薦.py:  a3d8efaa0e820eb41683e9b05819e97750df1efe（未修改）
  godpick_h157_trader_audit.py: 290d8b370e1ef1b68c26cdcf5da556bb8d540da7（僅追加H164呼叫）
原始檔案：StockPick_H1521_Potential_20261115_020049.xlsx（本次不修改、未覆蓋）

一、確實完成的修改
1) godpick_h157_trader_audit.py
   - 在既有 11_交易員重點導航末端追加 H164 精簡交易員研究分池；保留所有既有欄位、14張工作表與原順序。
   - 保留原H157分析、H145完整性阻擋、H126正式授權，絕不把研究股當成買進股。
2) godpick_h164_trader_focus.py（新）
   - 以 H157 同輪來源為唯一資料；分 R近持平/收紅、R相對抗跌但仍下跌、G官方成長雙證、T技術突破、P預發動、W非交易、U待核、C風控隔離。
   - 相對抗跌R2：市場<=-2%、個股仍跌但跌幅不超過市場一半、同日相對強度>=3個百分點；只能在H157已核實同日大盤/個股/相對值時使用。
   - 保留 H153 原研究順位，不重新計算或擅自提升買進權；欠缺PE/EPS/營收/三率/RSI/MACD/ATR欄明確顯示待補證，不填假數值。
   - 次日只列雙情境與失效條件，沒有樣本外校準時不顯示不實上漲機率；研究日不等於今天時會警告過期。
3) tests/test_h164_trader_focus.py
   - 10項H164新情境含日期、分池、缺證、假抗跌、禁止重複、原頁不覆蓋、Excel可再次開啟。
4) H164_README.txt

二、本次 Excel 資料檢查（來源限11/13當輪）
- Excel 14張工作表，01研究53檔 = B級30 + W級12 + 其他待核11；00與01 A/B同順序30檔；03五檔與01前五一致；12含全部53檔。
- H145: BLOCKED_FINAL_RESULT_COUNT_GAP；H126買進授權0檔。
- 01前30檔官方PE/EPS/營收缺認證，H1534三率突破無可驗雙證；H157 RSI/MACD/ATR未填有效數值。
- 研究日 2026-11-13，不得將匯出時間11/15或11/16大盤大跌當同一研究輪。今天市場必須重新抓11/16同日OHLCV才能做11/17逐股推估。
- 11/16大盤下跌6.03%，證交所官方參考 https://www.twse.com.tw/en/exchangeReport/FMTQIK?date=202611&response=html
- 外部示例1477跌0.44%、2891跌2.41%，僅作當日抗跌概念稽核；非當輪選股演算輸入，若來源不一致以交易所正式價為準。

三、部署與注意
- ZIP只覆蓋上述相對路徑。請勿上傳舊版Page07/8、Secrets、runtime-data、推薦歷史或設定。
- 保留H126/H109/H141/H161/H163既有程式，不影響自動排程。
- 執行新版主管研究Excel後，先開『11_交易員重點導航』，最上面C1會指出H164精簡研究所在列；舊導航A1:B30完全保留。
- 只是離線模擬測試，尚未實際部署雲端驗證；未修復H145與官方基本面來源缺口。
- 股市有風險，技術與相對抗跌是研究線索，不是保證及買進授權。

四、測試
- python -m pytest tests/test_h157_trader_audit.py tests/test_h164_trader_focus.py tests/test_h153_growth_technical_ranking.py tests/test_h1534_three_margin.py tests/test_h1539_mainstream_freshness.py tests/test_h154_export_parity.py -q
- 49 passed；python -m py_compile passed。
- 若使用者提到『如何從40檔中選哪檔買』，必須先完成最新OHLCV、官方營收/季報、H145及H126，不可依舊Excel硬給交易建議。
