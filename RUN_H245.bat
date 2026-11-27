@echo off
setlocal
cd /d "%~dp0"
set /p "PROJECT=Enter actual stock-app source folder: "
if "%PROJECT%"=="" exit /b 2
python "%~dp0apply_h245.py" "%PROJECT%"
if errorlevel 1 (echo [BLOCKED] No compatible overlay generated.& pause & exit /b 1)
pause
