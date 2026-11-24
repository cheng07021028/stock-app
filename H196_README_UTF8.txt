V191-H196｜正式資料驗收與 TPEx 歷史行情修正

基準：H195 已安裝；2026-11-24 GitHub main 確認 H190 核心 blob 444e68cc2e74ed70d3f93bb763088d1d1dee948f、H195 核心 f272cb91a58070659f531f5b0dad224bddfb41ff。首次正式查詢 H190 health：TWSE 45 日，TPEX 0 日，paired 0，最近三個 TPEX 請求 valid=0 eligible=0。H192 workflow 未找到；H195 workflow 於後續查詢確認已存在。

正式修改：
1. godpick_h190_market_archive.py：TPEx 歷史 API 加入 type=EW（排除權證牛熊證），保留原始股數/元欄位、日期、完整性門檻與既有不可覆寫歷史。TPEx 日行情是該來源報表口徑，可能不含零股；不可等同所有交易時段總量。最近60交易日優先、失敗次數排序保留。前台掃描不增加網路請求。
2. tools/h196_production_audit.py：唯讀檢查 H190-H195 部署、日曆、雙市場近60日完整及有效股數、逐股缺日/異常、財報八來源、H194 信號/結果、H195 快照。以主檔四位數普通股為覆蓋分母，停牌/新上市仍列為缺口；財報有效 PE 與一般業三率不適用全部股票。95% 為驗收目標，非交易授權條件。沒有預測樣本不能宣稱模型有效。
3. .github/workflows/h196_production_audit.yml：唯讀驗收每日台灣 00:25，手動可執行；輸出 acceptance.json artifact，保留90日。缺資料時 exit 2，Actions 紅色代表驗收未通過，不代表所有抓取都失敗。
4. .github/workflows/h192_financial_archive.yml：原 H192 交付檔，內容不變，補齊漏部署排程。

安裝：在解壓資料夾執行 python tools/h196_preflight.py --root "D:\stock-app"；READY_APPLY_H196 後加 --apply。若 UNKNOWN_VERSION 停止，不直接覆蓋。同步四個正式檔至 main，勿漏 .github。先手動執行 H190 official full-market historical archive、H192 official financial growth archive；確認來源健康與遠端提交；再執行 H194、H195，最後執行 H196 production data acceptance。既有日排程仍保留。H190 每輪最多48次市場日請求，雙市場補齊可能需多輪；不清空已有資料。

驗證：H190-H196 指定回歸 138 passed（本機原始 H115 資料 + 模擬官方 API）；本機 DNS 無法連線官方 API，沒有聲稱真實 TPEx 已恢復。H196 不修改 H126、正式推薦、交易紀錄、Firestore 或現有 UI。未部署/未驗證正式抓取成功。除權息還原、歷史完整財報、模型成熟及正式掃描速度另須驗收。

來源端點參考（第三方實測文件，非本次正式 API 成功證據）：https://github.com/TaiwanCCyoyo/stock-data-downloader/blob/main/docs/tpex-history-sources.md
