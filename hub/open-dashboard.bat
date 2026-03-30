@echo off
title My Automations
cd /d "%~dp0"
python hub_server.py
if %errorlevel% neq 0 (
    echo.
    echo Hub failed to start. Press any key to close.
    pause >nul
)
