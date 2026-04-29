@echo off
REM Double-click launcher for Windows source-tree users.
REM cd's to this script's folder, locates a Python 3 interpreter, then
REM runs the bootstrap launcher. Keeps the console open if anything fails
REM so the user can read the error.

setlocal
cd /d "%~dp0"

set "PYTHON_BIN="

REM Prefer the Windows launcher with a pinned version when available.
where py >nul 2>nul
if not errorlevel 1 (
    py -3.13 --version >nul 2>nul && set "PYTHON_BIN=py -3.13"
    if not defined PYTHON_BIN py -3.12 --version >nul 2>nul && set "PYTHON_BIN=py -3.12"
    if not defined PYTHON_BIN py -3 --version >nul 2>nul && set "PYTHON_BIN=py -3"
)

if not defined PYTHON_BIN (
    where python >nul 2>nul
    if not errorlevel 1 set "PYTHON_BIN=python"
)

if not defined PYTHON_BIN (
    echo.
    echo ERROR: No Python 3 interpreter found.
    echo Install Python 3.12 or 3.13 from https://www.python.org/downloads/
    echo and re-run this launcher.
    echo.
    pause
    exit /b 1
)

%PYTHON_BIN% "scripts\bootstrap_launcher.py"
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo Launcher exited with status %EXIT_CODE%.
    echo See .launcher\launcher.log for details.
    echo.
    pause
)

endlocal & exit /b %EXIT_CODE%
