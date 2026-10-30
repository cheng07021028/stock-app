# StockApp V191-H148.3.1 | Page07 supervisor Excel missing-module repair

日期：2026-10-30

## 故障原因

正式頁面顯示 `ModuleNotFoundError: No module named 'godpick_h1482_source_audit'`。先前 H148.3 增量 ZIP 只含第七頁及 H148.3 新模組；第七頁本身同時需要 H147、H148、H148.1、H148.2 的模組，不能單獨覆蓋到未安裝全部前置修改的程式庫。

## 本次修正（完整依賴累積包，不須重裝 H147/H148/H148.1/H148.2）

直接在 GitHub 專案根目錄合併/覆蓋這 8 個正式檔案，保持下列原始相對路徑：

- `pages/7_股神推薦.py`（包含至 H148.3 的最新累積版；不降回舊版）
- `godpick_h144_focus_board.py`
- `godpick_h147_five_gate_review.py`
- `godpick_h147_scan_bridge.py`
- `godpick_h148_strength_provenance.py`
- `godpick_h1481_focus_gate.py`
- `godpick_h1482_source_audit.py`（這次畫面報錯的關鍵缺檔）
- `godpick_h1483_research_recall.py`

另外提供：`tools/h14831_verify_deploy.py` 及 `tests/test_h14831_deployment_closure.py`（只做檢查，不連線、無任何正式資料寫入）。

**重要：** 本次修復依照使用者先前上傳的 H146 原始碼＋截至 H148.3 的逐版累積檔組成。若 GitHub `main` 在此之後有其他新修改，請先比較差異、合併，切勿粗暴覆蓋新改動。請一次提交上述 8 檔，以免部署暫態讀到缺檔。

## 安裝驗收

1. ZIP 內沒有多包一層目錄。把內部檔案放回 GitHub repo 根目錄；中文正式程式名 `pages/7_股神推薦.py` 需要原樣保留。
2. 在 repo 根目錄執行：`python tools/h14831_verify_deploy.py --root .`，應顯示 `PASS: all seven H144/H147-H148.3 dependencies present and readable`。
3. 有完整測試環境時執行：`python -m pytest -q tests`。
4. 在 Streamlit Cloud 確認部署完成，重新啟動頁面、重新開啟第 7 頁，點擊「建立主管研究 Excel（含排名可信度）」並下載讀取，驗證 2 張工作表 `00_主管重點觀察榜`、`00_H126主管決策`。
5. 本修復只修正缺模組與部署依賴；不會重抓新行情、不會直接增加合格推薦數、不改 H126 正式交易門檻。其他未被本次錯誤證實的問題，仍需真實盤後完整掃描驗證。

## 測試與資料安全

- 本機累積 H148.3 程式 `python -m pytest -q tests`：550 passed（含新加 3 項依賴與錯誤重現測試）。
- H147～H148.3 7 個必要模組已實際 `import` 成功，H148.2 的 `build_source_audit`、`append_source_audit_to_sheet` 皆可正常匯入；Excel 實際程式片段測試通過。
- 本包只含必要程式、測試與驗證說明；**無** JSON 推薦紀錄、永久設定、Secrets、憑證、runtime-data、歷史快取、股票報表。絕不可改寫這些正式資料。
- 尚未直接替使用者部署至 Streamlit Cloud；雲端執行結果需在正式環境回讀確認。
