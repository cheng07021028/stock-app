V191-H207｜側邊欄雙重編號與版面修復（緊急修正版）

已比對 GitHub main 的 utils.py、pages/14_股神權重校正.py、streamlit_app.py 及 H204 導航檔案。
根因：utils.py 舊 v67 以 nth-of-type(1..21) 產生 00.～20.，並預留 3.25rem；第14頁另有舊備援。H204 st.Page 已提供固定編號。舊的 v25.2 顏色也以列表位置判斷，會隨分群錯位。
修正：僅從 st.Page 標題取得模組編號；移除兩處舊編號 CSS、舊位置配色；保留原主題、hover、選中狀態，並用 H205 原生四大分群與共用側欄文字換行樣式。首頁不編號；00～33（無13）及98的34個既有模組保留，原路徑不變。
本版包含 H205 導航分群，即使正式環境仍是 H204 亦可升級；不回退 H206 功能。

正式檔案（3個，須一起更新）:
utils.py
godpick_h204_sidebar_navigation.py
pages/14_股神權重校正.py

安裝：解壓至獨立資料夾，先執行 python tools/h207_preflight.py --root "D:\stock-app"，READY_APPLY_H207 後可加 --apply。工具支援已知 H204 / H205 導航版，拒絕未知檔案，保留 .h207_backup。僅將三個正式檔案提交 GitHub main，Reboot App 後驗收。

驗證：20項 pytest（H204 3項、H205 13項、H207 4項；其中2項用 Chromium 模擬側欄 DOM，檢查35個連結、單一編號、換行）。Python編譯成功；H204/H205安裝及重複安裝、未知版本阻擋驗證成功。未安裝實際 Streamlit 1.59.2，無法宣稱正式網站逐頁驗收或真實 DOM 已測。取消位置配色是為避免分群後誤標，不是取消頁面功能。未更改交易邏輯、資料、權限或報表。
