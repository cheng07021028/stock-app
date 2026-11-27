@echo off
chcp 65001 >nul
cd /d "%~dp0"
set /p "PROJECT=Enter the actual H246 stock-app project folder: "
python "%~dp0apply_h247.py" "%PROJECT%"
if errorlevel 1 echo [FAIL] No compatible overlay generated. Do not deploy an old overlay.
pause
