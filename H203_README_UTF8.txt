V191-H203 | 主管Excel股票名稱獨立欄位修正
正式修改檔：godpick_h175_evidence_rescue.py（僅報表寫入函式）
基準 Git blob SHA: eb85d8d31fc1725c56d8715530e29478d19b1f97，已核對 GitHub。
00主管首頁 H175 區 A=來源序／代號，B=股票名稱，C=股票池，D=H114資料問題，E:G=優先查證／說明。B 不再被強制加寬至68；摘要僅在空白且不涉及原表格／篩選範圍時合併 B:G。既有名次及資料值、H126 權威與第11表完整逐股資料不變。
安裝：python tools/h203_preflight.py --root "D:\stock-app"；顯示 READY_APPLY_H203 後加 --apply。將唯一正式檔同步 GitHub main，重新啟動 Streamlit 並重新建立主管Excel，不清除行情快取、斷點或重掃。舊Excel不會自行變更。
測試：H203四項及 H189 六項通過（共10項）；另跑舊 H175 測試曾出現四項舊同輪測試資料缺漏，未調整正式資格規則。全量回歸未完成；正式 Streamlit / Excel 桌面顯示待驗收。
