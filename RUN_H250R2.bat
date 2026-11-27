@echo off
chcp 65001 >nul
cd /d "%~dp0"
set /p "PROJECT=Enter actual stock-app project folder: "
if not defined PROJECT exit /b 1
py -3 BUILD_H250R2.py "%PROJECT%"
if errorlevel 1 echo [BLOCKED] Do not overwrite the project.
pause
