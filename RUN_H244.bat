@echo off
setlocal
cd /d "%~dp0"
echo H244 exact-source overlay generator
 echo Original project will NOT be changed.
set /p "PROJECT=Enter extracted stock-app-main folder (without quotes): "
if not defined PROJECT goto end
where py >nul 2>nul
if errorlevel 1 (
  python "%~dp0Apply_H244.py" "%PROJECT%" --out "%~dp0StockApp_V191_H244_Overlay.zip"
) else (
  py -3 "%~dp0Apply_H244.py" "%PROJECT%" --out "%~dp0StockApp_V191_H244_Overlay.zip"
)
if errorlevel 1 echo [BLOCKED] No overlay produced. See message above.
:end
pause
