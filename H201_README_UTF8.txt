V191-H201 | H126 manager Excel status compatibility hotfix
正式修改檔僅兩個：godpick_h1537_manager_visible.py、godpick_h154_export_parity.py。
基準：已核對 GitHub 正式兩檔及 H126 來源程式，H200 第7頁雜湊亦相符。
根因：H126 具名研究列原文為「禁止以研究榜買進｜僅01正式交易計畫具備核准權」，舊驗證器未認得該狀態，H154 又固定讀 G2。
新驗證器依表頭檢查全部權威資料列，只接受明確既有禁止狀態；缺欄、未知、空白、矛盾仍阻擋。說明列不能代替權威資料列。不改寫 H126 授權、不改排名、快取或推薦紀錄。
本機驗證：88 項相關測試通過，包括13項新增測試，使用 H126 實際產生器、第7頁寫入函式、Excel 存檔後重新讀取。H160 舊測試的無表頭模擬已補齊正式欄位（只改測試）；另一輪 H175 舊測試兩項失敗，該模組未修改，不列入通過數。正式 Streamlit 未部署驗收。
安裝：從解壓後目錄執行 python tools/h201_preflight.py --root "D:\stock-app"，READY_APPLY_H201 才追加 --apply。僅將兩個正式檔同步 GitHub main。tests 可選，不必上傳。不要覆蓋其他檔案或刪除快取、斷點。
