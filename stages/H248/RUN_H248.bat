@echo off
setlocal
cd /d "%~dp0"
echo V191-H248 - Generate verified overlay only.
set /p "PROJECT=Enter stock-app project folder: "
if not defined PROJECT exit /b 2
set "PROJECT=%PROJECT:"=%"
python apply_h248.py "%PROJECT%"
if errorlevel 1 (
  echo [BLOCKED] No project files changed.
) else (
  echo [PASS] Review generated overlay before copying.
)
pause
