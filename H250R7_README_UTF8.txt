V191-H250-R7｜10 三項中央追蹤工作：08 歷史權威／H190 原始行情／精確 ID 保存

【適用基準】
已部署 H250-R6。正式 repository 查核時：
  tools/run_godpick_auto_scheduler_v191.py  a5b0c39abcd7231eb60b4b31eae3ddd76d499608
  godpick_h250r2_nday.py                   c1f60b619095247a4cef3fc5dfbdf0cfa6a724dc
  godpick_auto_update_tasks.py            1cfe096d955a82ab7e8aff0cf3715eb82cfd8610
  godpick_h241_github_records.py           bd7276b9507ddb94557fbc536a76303f7fba7d2d
  godpick_h190_market_archive.py           ed7602580c8a6dffb172d169d38489a67f2d68d1
  godpick_auto_scheduler.py               1c3b887f70294e6c8478c72f55d65c669d34ac97
  pages/10_推薦清單.py                    2e19a1b2786d650676f60c56c2bfd5bc049c360e
本包不覆蓋上述其他既有檔案，原 H250-R2 原價計算 helper 需已存在（已核對正式 main 的 Git blob）。
如果 Worker 已經有其他更新，請勿直接覆蓋不同基準，先做版本比對。

【部署：只有兩個正式程式檔】
1. 等待正在執行的中央批次完成，不取消、不重送。
2. 備份原 tools/run_godpick_auto_scheduler_v191.py。
3. 同一 commit 依原始相對路徑上傳：
   godpick_h250r7_tracking.py（新增）
   tools/run_godpick_auto_scheduler_v191.py（替換 R6 入口，保留 R6 戰情 callback）
   其餘 README、tests、測試 log 和 SHA256 清單是本機驗收資料，不需上傳。
4. 無需 BAT、prepare workflow、重新配置 GitHub secrets 或更動 runtime-data。
5. 下一輪中央 Worker 才會使用 R7。Page17 查看新 run 的三項工作；訊息需顯示 R7 及 08 權威筆數。不得把上次 R6 歷史回執當本次結果。

【設計／不回退】
- 只替換中央 Worker 內 recommend_list_performance / recommend_list_n_day / recommend_list_hits 三個 handler，其餘工作不變。R7 import/contract 失敗時僅三項明確 FAIL，不回退空清單更新，其他工作仍走既有 scheduler。
- H241.ensure + H240 VERIFIED + H241._base(signature) + 完整原始列比對 + _commit CAS。只保存既有 record_id 的白名單績效欄位，不新增、刪除歷史或復活已刪除 ID。遠端與本機永久確認成功才計入本批成功。
- 目前 07 正式推薦清單為 0 時仍可追蹤 08 歷史權威；不重跑選股，不把歷史當新推薦，不改 07 當輪快照、不寫 godpick_recommend_list.json、不改 H126 買進限制。原 H159 批次驗收及 R6 戰情中心保留。
- 三工作共享經 H190 校驗的本機封存索引，每工作掃描全體歷史；只限制實際有變更的最多 300 筆/次。未成熟正常略過；缺身份/行情/日曆/成本基準等保留待補證，不虛構價格、日期或成交。日曆或封存整體校驗失敗明確 FAIL。可計算部分且仍有缺口/待續回報 WARNING，不宣稱全數成功。
- 更新推薦後績效：重新計算官方原價目前報酬、行情日和證據；同時核對 Page08 既有 Yahoo 還原候選績效的日期、來源標籤、狀態與官方最新收盤價。這是沿用檢查，不是獨立驗證還原價或淨報酬。還原績效不符合條件標示 ADJUSTED_PENDING，不使用原價代替還原價，不另外重新抓 Yahoo。
- 正式 N 日績效：按官方交易日 T+1/3/5/10/20 計算，基準為合法原推薦價或推薦日官方收盤，不可用目前價倒填成本。官方原價獨立存於「官方原價推薦後N日%」和 H250R7_N日原價證據；既有「推薦後N日%」及原還原績效不覆蓋，避免 AI 回測口徑混用。
- 隔日命中：必須有推薦日及後續完整官方 OHLC 交易日序列；從下一個實際交易日算進場/突破/停損/壓力首觸日及 1/3/5/10 日最大漲跌。未設定價位不當作未命中；同日進場/停損順序不明，標為觸價證據，不推定實際成交或實際報酬。缺中間行情保留待補證。
- R7 新增官方原價績效欄位儲存於歷史權威；本包不改 Page10 既有畫面、手動按鈕及勝率欄位投影。舊 UI 尚可能看不到新增欄位，不能宣稱 Page10 原有勝率已改用官方證據。若需要整合，應另做精確 Page10 投影修正，不把不同績效口徑混用。本包覆蓋的是中央 Worker 路徑，不是全部舊 UI 更新入口。

【判讀】
R7 訊息「08權威 X 筆／官方至 YYYY-MM-DD／永久確認 Y 筆」：X 是此次核實可讀的歷史總筆數，Y 是有修改且永久確認成功的筆數，不等於所有歷史皆可計算。READY＝證據條件達成；NOT_DUE＝尚未成熟；IDENTITY_PENDING＝原身分/日期缺失或重複；CALENDAR_GAP＝日曆缺口；QUOTE_PENDING＝行情缺口；BASE_PENDING＝基準價缺口；PLAN_PENDING＝缺策略價位；ADJUSTED_PENDING＝Page08 還原績效尚未核對；DEFERRED＝本批上限後待續。不同診斷維度可重疊。NO_NEW_DUE 是已核實無需新寫入，不冒充更新成功筆數。PARTIAL_SAVED 代表已有真實保存、仍有警示。REMOTE_PENDING/FAIL 不計本次永久確認筆數。

【驗收限制】
離線 30 項 R7 測試全通過，R6+R5 回歸 63 項其中 57 通過、6 因缺完整正式專案跳過。R2 回歸結果見 R2_REGRESSION.log。正式完整資料、GitHub CAS 實際寫入、H190 目前覆蓋與耗時需要下一輪正式 Worker 實測。未直接部署或修改 GitHub runtime-data。不可保證三項立即綠燈，缺證仍應顯示實際警示。回復只需還原備份 Worker 入口；新增 module 可保留未引用，不刪除已核實歷史結果。
