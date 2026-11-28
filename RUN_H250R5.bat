@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
echo H250-R5 - Direct local source upgrade. Backup and tests required.
set /p "PROJ=Enter latest stock-app project folder: "
if not defined PROJ goto failed
set "PROJ=%PROJ:"=%"
where py >nul 2>nul
if errorlevel 1 (
    python "%~dp0APPLY_H250R5.py" "%PROJ%" --apply
) else (
    py -3 "%~dp0APPLY_H250R5.py" "%PROJ%" --apply
)
if errorlevel 1 goto failed
echo [PASS] Local source updated. Upload actual modified files or commit the project changes to GitHub.
pause
exit /b 0
:failed
echo [BLOCKED] No successful source upgrade. Read error above; do not force overwrite.
pause
exit /b 1
