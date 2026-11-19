V191-H170｜治理前後正式候選對帳 × 官方累計EPS證據鏈
====================================================
建立基準：GitHub main 88ff5118c3bf3495075713a7aaa977566ee1dc85。
重要：該 main 尚未部署H169，因此H170 ZIP採「H169+H170累積必要修改檔」
一次覆蓋，請勿將H170當成只有新增檔案的增量包。

A. H145問題真相
1. Page07 的 scan_quality_report 是在 V183 SuperAI／H34 等後續治理前建立；
2. 後來 governed_candidate_df 重新經過operational_recommendation_rows，
   因此原報表『正式候選2檔』不一定與治理後『匯出1檔』是同一階段。
3. H170現在保存同輪 scanner-stage 與 post-governance-stage 股票代號，
   並追蹤被淘汰的代號在完整治理母體中是否仍存在。
4. 僅同run_id、同scan_signature、所有母體與代號一致，且H145既有
   全量母體檢查通過，才改標 RECONCILED_POST_GOVERNANCE_RESELECTION。
   若缺任何證據仍維持 BLOCKED_FINAL_RESULT_COUNT_GAP。
5. H109、H126、買進風控及交易紀錄完全不變；報表不會自己補股票。

B. H115官方EPS資料的實際性質
1. 證交所 https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ci
   櫃買   https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_ci
   官方一般業綜合損益表有基本每股盈餘（元），但第2/3季數據是
   年初至今累計EPS，不能直接當單季或近四季TTM。
2. H170新增TWSE/TPEx官方累計EPS日終快照（首次實際觀察時間），
   狀態只分 EPS_YTD_BOTH_MARKETS / EPS_YTD_PARTIAL；
   仍不認定H115 PE+TTM EPS雙證，不會給三率/高成長/交易分數。
3. 可在11_交易員重點導航的H169區查看是否有H170官方累計EPS，
   及其取用時間是否早於研究日。只有研究日的Excel但不含執行時間者，
   當日較晚首次觀察的資料會明確標不能回填。
4. 四季EPS真正認證還需要：上一年度整年及上一年同期累計EPS、
   與本年度最新累計EPS的正確同公司季報與公告時間交集；
   股本/會計政策調整仍需覆核，不能只用本期YTD。
5. 台灣金融業/保險業的報表與一般業不同，H170不冒稱全市場覆蓋。

C. 本次相對H169新增正式修改：
  pages/7_股神推薦.py                    [新修改：同輪兩階段記錄]
  godpick_h145_core_bridge.py             [H170累積修改]
  godpick_h1536_manager_clarity.py        [H170累積修改]
  godpick_h169_pe_watch.py                [H170累積修改]
  godpick_h170_stage_reconcile.py         [新增]
  godpick_h170_eps_evidence.py            [新增]
  tools/h170_refresh_eps_ytd.py           [新增]
  .github/workflows/h170_official_eps_ytd.yml [新增]
  tests/test_h170_stage_eps_pit.py        [新增]
  H170_README.txt                         [新增]
保留H169其餘必要檔（本次單包仍可從主分支直接覆蓋）：
  godpick_h114_parallel_alpha.py
  godpick_h157_trader_audit.py
  tools/h169_refresh_pe_watch.py
  .github/workflows/h169_official_pe_only.yml
  tests/test_h169_evidence_contract.py
  H169_README.txt

部署：
1. ZIP同路徑覆蓋 GitHub main（不觸碰 runtime-data/Secrets/推薦紀錄）。
2. 先在 Actions 觀察兩個來源任務 H169 PE / H170 EPS YTD，
   官方來源欄位變動或連線失敗須查工作日誌、不得擅自改為SUCCESS。
3. 新一輪完整掃描後再匯出Excel（舊Excel不會自動更新）。
4. 看02_研究召回統計H145/H170前後候選代號是否對帳，
   11_交易員重點導航有沒有PE/EPS時點與非TTM警示。
5. H126仍只能使用00_H126正式決策；沒有授權不得買進。

本機測試、GitHub Actions回歸可證明程式契約一致，
不能證明實際推薦當天盈利。真實雲端API與當輪選股仍須部署驗收。
