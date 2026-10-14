AI股神 V191-H121.1｜夜間排程健康 × 三市場資料完整性 × 遠端持久化確認
2026-10-14 ｜ H120相容安全覆蓋升級

目標：修補H120之後仍無法確認「排程有執行、行情來源均齊、不可變推薦快照可核對、GitHub真正保存」的營運驗收缺口。
本版不修改任何選股、H113證據准入、H114平行策略、H111永久設定、H118/H119/H120績效品質門檻、Formal買進權限；不推算休市、股利或缺資料績效。

[正式檔案]
NEW godpick_h121_operations.py
    讀取 H117 的TWSE/TPEx/TAIEX已封存官方來源，驗證SHA-256、來源行數、同日完整性；查核H116不可變推薦快照、績效輸入檔與來源雜湊；H120有人工作業認證的情況，再核對最新企業行動原始證據及核對指紋。缺漏FAIL CLOSED。
NEW tools/h121_operations_cli.py
    inspect：輸出H121穩定健康狀態檔與本次執行收據。相同底層資料與狀態不改寫穩定檔。
    verify-remote：git ls-remote實際比對GitHub遠端分支HEAD與本機提交SHA。未吻合回傳碼2，不宣稱跨Reboot已保存。
    summary：產生GitHub Actions Job Summary與疑難排解說明。
UPDATED .github/workflows/h117_official_ohlcv_feedback.yml
    保留原H117官方行情、H119原始除權息來源、H116嚴格績效與H120稽核流程。新增加權健康收據、14天執行產物、GitHub實際遠端HEAD確認。只有H116合法輸出與H121穩定健康檔被加入版本控制；執行紀錄與錯誤log不提交進App。
NEW tests/h121_operations_regression.py
    7組離線測試：缺三市場、缺來源、資料SHA被修改、快照資料來源與績效報告不一致、報告不重寫、GitHub SHA不同等。

[安裝]
1. 先備份最新版的完整stock-app，確認已安裝H115～H120，且已按H116定點修補第7頁（這次不含第7/8頁）。
2. 將ZIP按原路徑解壓到stock-app專案根目錄。此次只新增模組、工具、測試、說明，替換H117既有排程 YAML。
3. 執行：py -3 -m unittest discover -s tests -p "h121*.py" -v
4. 執行：py -3 tools\h121_operations_cli.py inspect
   本地資料不足時，PENDING_VALIDATION是正確結果，不能人工改成READY。
5. 把正式修改檔提交至GitHub main（或當前部署分支）；啟用GitHub Actions、授予 workflow contents:write；手動在Actions執行一次H121排程。
6. 檢查該次 workflow 的Job Summary、artifact `h121-run-*`、`reports/h121/H121_operational_state.json`及遠端verify-remote=REMOTE_HEAD_MATCH。
   當API缺資料/限流，或H120缺人工認證，會標示待驗證，絕不偽造已完成的資料。

[注意]
- GitHub Actions預設平日台灣時間18:45及20:45嘗試執行。休市日僅能標示官方沒發布新資料，不會因此宣稱官方行事曆已完成查驗。
- 遠端Commit SHA相同，代表當次工作樹已推送至指定分支；不代表雲端App已正確重新部署、備份已跨供應商保存，也不代表交易策略已獲利。
- 如公開GitHub Repo儲存含個人推薦快照，可能對外公開；請確保Repo權限與資料政策符合要求。
- 故障產物保留14天供稽核，不保證平台通知已投遞；需另確認GitHub Actions失敗通知設定。
- H120企業行動核對仍需真實人工來源憑據；本次無法證明過往任何一天已有合法認證。H114/H113的60/120交易日成效仍待實際樣本。
- GitHub本機工作目錄若未設定origin或其分支非部署分支，`verify-remote`應回傳失敗，不應強制改成成功。

[快速命令]
  py -3 tools\h121_operations_cli.py inspect
  py -3 tools\h121_operations_cli.py verify-remote --branch main
  py -3 tools\h121_operations_cli.py summary

[資料安全]
不包含使用者設定JSON、掃描歷史、推薦紀錄、金鑰、舊版Page7/8或生成器安裝檔。所有GitHub Secrets仍由使用者在平台設定。
