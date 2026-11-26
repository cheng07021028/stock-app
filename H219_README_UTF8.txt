V191-H219｜首批前瞻績效檢討、強勢命中與精簡研究

累積 H209.1～H219。只含必要程式、測試、安裝說明；不包含私人紀錄、憑證或任何行情檔。
安裝：完整解壓至獨立資料夾，先執行 python tools/h219_preflight.py --root "實際專案目錄"（唯讀），確認 READY_APPLY_H219，再加 --apply。未知雜湊拒絕覆蓋，會備份已存在的修改檔。GitHub 更新保留原路徑（包括 .github）。
第32頁新增 H219；第7頁既有 Excel 附 H219_精簡研究，保留原H126、H195及其他表。
H209 健康紀錄新增 capture_diagnostics，H208 以實際股數判斷95%而非四捨五入百分比。H219 不會改變正式交易授權、研究排名或自動調權。
抗跌廣度以研究日起始官方四碼有價證券為母體（非指數、非含息、非歷史普通股名冊），雙日有價覆蓋至少95%、至少100筆；中位跌幅至少1%、下跌比例至少60%才列為弱勢廣度。原價與淨績效嚴格分開，缺資料時不產生全批次勝率。
正式資料：2026-11-26 07:50 H194 signals=623, mature=0, pending=3115；10:27 H190 雙市場250日；07:43 H209 collector pending=0, invalid_existing_count=0, unverified_total=0。這些是正式健康檔回報，不是本機資料或H219部署成果。
本機原始包（11/25 15:34觀測）無前瞻樣本；強勢對帳40檔，召回率不可計算；H208 calendar not finalized as of research day。正式首批T+1仍待盤後結算。H219尚未部署，未實測Streamlit瀏覽器及雲端工作流，無已證實勝率提升。
手動執行 GitHub Actions: H219 first prospective performance review；下載 H219_Review.xlsx。
