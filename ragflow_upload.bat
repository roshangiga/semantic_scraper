@echo off
setlocal
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
echo ======================================
echo RAGFlow Upload Console
echo ======================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Check if the script exists
if not exist "ragflow_uploader.py" (
    echo ERROR: ragflow_uploader.py not found in current directory
    pause
    exit /b 1
)

REM Run the RAGFlow uploader console
python ragflow_uploader.py
set EXITCODE=%ERRORLEVEL%

echo.
echo Press Enter to exit...
pause >nul

endlocal & exit /b %EXITCODE%