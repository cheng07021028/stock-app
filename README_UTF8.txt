H250-R11-Diag | 只覆蓋根目錄 godpick_h162_github_auth_diagnostic.py，備份後提交 main 並 Reboot App。
基準 Git blob 9711bc82ce9d162516f364fa6981c6e60338c64f。
只新增 H162 授權診斷分類，未修改歷史、保存機制、權限或排程。
請於第17頁管理員 H162 在無其他人工批次寫入時先唯讀，再執行一次小型測試。限流則遵守 Retry-After 或配額重置時間；未提供時間至少等待1分鐘，仍失敗則延長間隔，不反覆測試。
23項離線測試通過；未執行真實 GitHub 寫入。
