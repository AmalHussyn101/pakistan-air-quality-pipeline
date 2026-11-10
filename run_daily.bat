@echo off
REM ============================================================
REM  Daily automatic run of the Smog Early-Warning pipeline.
REM  Windows Task Scheduler starts this file every morning.
REM ============================================================
cd /d "%~dp0"
call venv\Scripts\activate.bat
python run_pipeline.py --daily
