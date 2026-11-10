@echo off
REM Opens the Smart Environmental Monitoring System in the browser.
cd /d "%~dp0"
call venv\Scripts\activate.bat
streamlit run app\app.py
