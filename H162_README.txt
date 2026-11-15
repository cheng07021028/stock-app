V191-H162｜第 17 頁安全 GitHub 授權診斷／憑證來源一致化
==================================================
基準：GitHub main commit 17402e547d45f4f369c2b9eee46a52d8b47c5ed3
目標：排查 Streamlit Secrets 已有 GITHUB_TOKEN、Actions/Contents 已讀寫，
      但股神推薦紀錄保存仍報 GitHub HTTP 403。
本次只修改／新增：
1. pages/17_系統健康檢查.py：管理員專用授權診斷 UI。
2. godpick_persistence_service.py：Streamlit 的 GITHUB_TOKEN 優先採用 st.secrets；
   在非 Streamlit 工作者中仍支援環境變數；原 V186 runtime-data 分支保護維持。
3. godpick_h162_github_auth_diagnostic.py：安全唯讀檢查及小型 Contents 探針。
4. tests/test_h162_github_auth_diagnostic.py：離線 12 項單元測試。
不變動歷史紀錄、H126、排程、Firestore、GitHub Secrets 或 runtime-data。

操作：
登入管理員 → 17_系統健康檢查 → 「H162｜GitHub 安全授權診斷」。
先按「檢查目前憑證與儲存庫（唯讀）」，檢查有效憑證來源、衝突、
存取儲存庫及 runtime-data 分支狀態；唯讀成功不代表 Contents 可寫。
如需真正驗證 Contents 寫入權限，勾選臨時檔確認後按
「測試 Contents 寫入／回讀／清除」。
小型探針路徑 data/.h162_auth_probe_<隨機值>.json，成功後自動清除。
如果清除未被遠端驗證，介面會顯示唯一專用檔案路徑以供人工處理。
診斷結果絕不顯示、記錄或存檔 Token 本身或 Token 雜湊。

驗收：
A. 實際生效憑證來源為 Streamlit Secrets（若與環境變數不同會警告）。
B. 唯讀 GitHub repo / runtime-data 皆 HTTP 200。
C. 自動建立、回讀比對、刪除探針三步完整成功後才能宣稱 Contents Write 成功。
D. 只在 C 達成且完整權威恢復後，才依 H161 重新載入 → 勾選 → 刪除。
E. Firestore 429 屬另一個配額問題，不因 GitHub 授權成功而消失。

權限：診斷操作只開放登入管理員。一般使用者不可自行進行寫入探針。
測試不碰 2,632 筆歷史，且不可能自行解除 GitHub 403。
注意：本 ZIP 為相對路徑必要檔覆蓋包，不含任何 Secret 或資料權威。
