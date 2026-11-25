V191-H208｜漏選飆股原因分析與潛力股召回改善
基準：實際 GitHub main H206 與 H207 交付程式。只更新四個正式檔案：
  godpick_h208_recall_review.py
  godpick_h206_market_gap.py
  tools/h206_daily_review.py
  .github/workflows/h206_market_gap.yml

使用：解壓於獨立目錄，python tools/h208_preflight.py --root "D:\stock-app"
預檢 READY_APPLY_H208 後再執行同一指令加 --apply；若顯示版本不符，勿強行覆蓋。
四個正式檔案依原目錄一併提交 GitHub main。執行既有 Actions H206 public market strength reconciliation。
每日台灣時間 00:25／09:15（實際排程可能延遲）。H208 同步寫入 data/h206/latest.json.gz 的 h208 欄位。
第33頁既有 H206 區塊自動顯示 H208 召回池；第7頁建立主管 Excel 時新增 H208_潛力召回、H208_漏選複核。H206 原表保留。
H208 以官方60個連續交易日OHLCV、實際成交額、H198反證為必要證據，另設較寬接近突破研究池（非正式買進）。
兩池獨立排序，不使用未來行情或今日漲幅排名作為候選輸入。正式推薦對帳須同日、同輪身分與H126數量一致；母體不足標待核。
當日強勢與前日可預見的起漲是不同問題。未還原價格不能視為投資報酬。所有新門檻僅研究假設；不更改 H109/H126、H191/H198、既有正式排名、歷史推薦紀錄、H207 側欄或 Firestore。
公開排程不讀取／發佈本輪私有正式名單；本輪實際 H126 比對仍於第7頁匯出時執行。未新增逐日正式推薦遠端封存。
資料不足仍可呈現部分合格市場，並保留驗收失敗；禁止為了有名單虛構價格、成交量、資金流或勝率。
本機測試採合成官方封存、既有回歸案例，非實際雲端推薦績效。正式 Actions、Streamlit、真實60日逐股覆蓋率及召回品質待部署驗收。
