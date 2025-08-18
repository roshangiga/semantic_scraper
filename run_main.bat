@echo off
setlocal
REM Ensure working directory is the script's directory
cd /d "%~dp0"

chcp 65001 >nul
set PYTHONIOENCODING=utf-8
set PYTHONPATH=%CD%;%PYTHONPATH%

REM Require local virtual environment Python
set "VENV_PY=.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo ERROR: Virtual environment not found: %VENV_PY%
    echo.
    echo To create it, run:
    echo     python -m venv .venv
    echo     .venv\Scripts\python -m pip install -r requirements.txt
    echo.
    pause
    exit /b 1
)

REM Check if the script exists
if not exist "main.py" (
    echo ERROR: main.py not found in current directory
    pause
    exit /b 1
)

REM Run main.py using the venv interpreter
"%VENV_PY%" main.py
set EXITCODE=%ERRORLEVEL%

echo.
echo Press Enter to exit...
pause >nul

endlocal & exit /b %EXITCODE%
