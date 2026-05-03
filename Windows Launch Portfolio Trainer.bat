@echo off
REM Double-click launcher for Windows source-tree users.
REM cd's to this script's folder, locates a Python 3 interpreter, then
REM runs the bootstrap launcher. Keeps the console open if anything fails
REM so the user can read the error.

setlocal
cd /d "%~dp0"

REM Pre-flight: catch the case where bootstrap_launcher.py isn't readable.
REM Windows doesn't have the macOS TCC framework that protects Desktop /
REM Documents / Downloads, so the EPERM that the .command guards against
REM doesn't reproduce here. But these analogous failures cause the same
REM cryptic Python traceback that the user can't act on:
REM   * OneDrive Files On-Demand has the file as a cloud-only placeholder
REM     (Documents and Desktop are OneDrive-synced by default on Win10/11)
REM   * The user only partially extracted the GitHub zip
REM   * Antivirus / Defender quarantined the script
REM Bail with a clear instructive message before invoking Python.
if not exist "scripts\bootstrap_launcher.py" (
    echo.
    echo =================================================================
    echo ERROR: bootstrap_launcher.py is missing or not accessible.
    echo =================================================================
    echo.
    echo Folder: %CD%
    echo.
    echo Likely causes:
    echo   1. This folder is inside OneDrive and files are still cloud-only
    echo      placeholders. Open the folder in File Explorer once so
    echo      OneDrive downloads every file, then re-run this launcher.
    echo   2. The GitHub zip wasn't fully extracted. Re-extract it (use
    echo      Windows Explorer's built-in "Extract All", not a partial
    echo      tool that may skip files).
    echo   3. Windows Defender or another antivirus quarantined a file.
    echo      Check the AV's quarantine list and restore it.
    echo.
    echo Quickest fix: move the folder somewhere local and not synced to
    echo the cloud, such as C:\Users\^<you^>\Projects\multi-asset-simulator.
    echo.
    pause
    exit /b 1
)

REM Informational: warn (don't block) when running from inside a OneDrive
REM directory. Files On-Demand can still surface partway through pip install
REM or during venv creation if the network drops, so users who hit weird
REM later failures should know this is a risk factor.
echo %CD% | findstr /I /C:"OneDrive" >nul
if not errorlevel 1 (
    echo.
    echo NOTE: This folder is inside OneDrive. If you encounter slow startup,
    echo pip install failures, or missing-file errors, the cause is usually
    echo OneDrive's Files On-Demand making files cloud-only. Move the folder
    echo OUT of OneDrive and re-run if that happens.
    echo.
)

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
