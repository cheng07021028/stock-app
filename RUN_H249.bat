@echo off
setlocal
cd /d "%~dp0"
echo V191-H249 - Generate verified overlay only.
set /p "PROJECT=Enter stock-app project folder: "
if not defined PROJECT exit /b 2
set "PROJECT=%PROJECT:"=%"
python apply_h249.py "%PROJECT%"
if errorlevel 1 (
  echo [BLOCKED] No project files changed.
) else (
  echo [PASS] Review generated overlay before copying.
)
pause
