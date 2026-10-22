@echo off
setlocal
cd /d "%~dp0"
color 0B
title Hospital Readmission - Airflow and Spark Demo
echo Starting the local Airflow and Spark presentation setup...
echo.
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0present-demo.ps1"
if errorlevel 1 (
    color 0C
    echo.
    echo The presentation setup reported an error. Read the message above.
)
echo.
pause
