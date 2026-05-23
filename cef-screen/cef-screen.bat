@echo off
title CEF Screener
cd /d "%~dp0"
python -m cef_screener.web
if %errorlevel% neq 0 (
    echo.
    echo CEF Screener failed to start. Press any key to close.
    pause ^>nul
)
