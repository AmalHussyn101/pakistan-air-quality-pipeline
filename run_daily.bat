@echo off
REM ============================================================
REM  Daily automatic run of the Smog Early-Warning pipeline.
REM  Windows Task Scheduler starts this file every morning.
REM  When it finishes, the 7-day forecast and the bulletin open in Notepad.
REM ============================================================
cd /d "%~dp0"
call venv\Scripts\activate.bat
python run_pipeline.py --daily
if exist reports\results\latest_forecast_table.txt start "" notepad reports\results\latest_forecast_table.txt
if exist reports\alerts\latest_bulletin.txt start "" notepad reports\alerts\latest_bulletin.txt
echo.
echo Pipeline finished. Forecast and bulletin opened in Notepad.
pause
