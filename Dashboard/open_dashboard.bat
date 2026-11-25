@echo off
cd /d "%~dp0.."
title HCMC Real Estate Dashboard

set "PYTHON_EXE=python"
if exist ".venv\Scripts\python.exe" set "PYTHON_EXE=.venv\Scripts\python.exe"
if exist "venv\Scripts\python.exe" set "PYTHON_EXE=venv\Scripts\python.exe"

echo Dang khoi dong HCMC Real Estate Market Intelligence Dashboard...
echo Trinh duyet se tu dong mo tai http://localhost:8501
echo (Giu cua so nay mo khi dang xem dashboard; nhan Ctrl+C hoac dong cua so de tat)
echo.

"%PYTHON_EXE%" -m streamlit run Dashboard\app.py
if errorlevel 1 (
    echo.
    echo [LOI] Khong the khoi dong dashboard. Kiem tra da cai streamlit chua: pip install -r requirements.txt
    pause
)
