V191-H215 累積修正包（H209.1～H215）
原始 ZIP：stock-app-main(4).zip＋H214 差異包。
安裝：將本包解壓至獨立資料夾。python tools/h215_preflight.py --root /path/to/stock-app 先核對；確認 READY_APPLY_H215 後加 --apply。未知檔案版本拒絕覆寫。備份資料夾不要提交 GitHub。只上傳正式程式與工作流；測試可一併提交。不可覆蓋資料目錄及憑證。
H215：第32頁唯讀；新增官方停復牌正向事件封存與缺日對帳，不能當成完整停止交易日證據。不得從母體移除或補K線。H209 增加緊湊民國日期區間解析及拒絕回應的欄位、日期、筆數診斷，不放寬來源核對。H213 已確認休市日不誤要求當日研究。H215 工作流台灣23:35執行，23:30品質截止，日終不合格回傳非零並保留報表；成熟績效獨立狀態。自動工作流延遲至盤前仍會失敗警示。
正式驗收：安裝後在 GitHub Actions 手動執行 H215 daily data proof and status audit。下載 H215-proof artifact。檢查 status_collection.json、health.json、H215_Proof.xlsx。若 H209 TPEX 區間錯誤繼續，查看 data/h209/collector.json diagnostics 中的回應日期與欄位。當晚交易日23:35排程須 PASS_DAILY 才認定每日資料閘門達標；NON_SESSION 為有證據的休市，不是選股勝率。H194及H209真實樣本須經時間成熟。
不修改 H126、私人推薦紀錄或權重；不保證績效。減資與分割不新增還原公式；完整停牌公告格式需正式驗證。
實測限制：424 項選定回歸通過；完整本機報表超時；官方狀態API實際回應及雲端排程尚未驗證。GitHub 正式健康紀錄已讀取，未部署或觸發。
