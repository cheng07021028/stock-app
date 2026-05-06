# -*- coding: utf-8 -*-
"""
v90 帳號管理頁清理工具
用途：保留正確檔名 pages/15_帳號管理.py，刪除上一版誤新增的 pages/第15_帳號管理.py。
執行方式：python cleanup_account_page_v90.py
"""
from pathlib import Path

root = Path(__file__).resolve().parent
wrong_files = [
    root / "pages" / "第15_帳號管理.py",
]

for p in wrong_files:
    if p.exists():
        p.unlink()
        print(f"已刪除：{p}")
    else:
        print(f"不存在，略過：{p}")

print("完成：請保留 pages/15_帳號管理.py。")
