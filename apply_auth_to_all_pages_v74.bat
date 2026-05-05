@echo off
chcp 65001 >nul
cd /d %~dp0
python apply_auth_to_all_pages_v74.py
pause
