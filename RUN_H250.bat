@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
echo H250 - exact source cumulative overlay generator
python -c "import yaml" 2>nul
if errorlevel 1 (
  echo [BLOCKED] PyYAML missing. Run: python -m pip install PyYAML
  pause
  exit /b 2
)
set /p "PROJECT=Enter latest stock-app project folder: "
if not defined PROJECT exit /b 2
set "PROJECT=%PROJECT:"=%"
python "%~dp0BUILD_H250.py" "%PROJECT%"
if errorlevel 1 (
  echo [BLOCKED] No overlay generated. Do not force overwrite.
  pause
  exit /b 2
)
echo [PASS] Upload ONLY files extracted from StockApp_V191_H250_Overlay.zip to main.
pause
