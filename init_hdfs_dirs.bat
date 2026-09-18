@echo off
setlocal enabledelayedexpansion
title SpinWatch - HDFS Directory Initializer

set HADOOP_HOME=C:\hadoop
set HDFS_CMD=%HADOOP_HOME%\bin\hdfs.cmd
set HDFS_BASE=hdfs://localhost:9000

REM ─── Compute today's date in YYYY-MM-DD format via PowerShell ─
for /f %%D in ('powershell -NoProfile -Command "Get-Date -Format 'yyyy-MM-dd'"') do set TODAY=%%D

echo.
echo =====================================================
echo     SpinWatch  ^|  HDFS Directory Initializer
echo =====================================================
echo.

REM ─── CHECK HDFS IS REACHABLE ──────────────────────────
echo [*] Checking HDFS connectivity at %HDFS_BASE%...
"%HDFS_CMD%" dfs -test -d / >nul 2>&1
if errorlevel 1 (
    echo.
    echo [ERROR] Cannot reach HDFS NameNode at localhost:9000.
    echo         Please start HDFS first by running:
    echo           start_spinwatch_infra.bat
    echo         Then retry this script.
    echo.
    pause & exit /b 1
)
echo [OK]  HDFS NameNode is reachable.
echo.

REM ─── CREATE BASE DIRECTORIES ─────────────────────────
echo [*] Creating base SpinWatch HDFS directories...

"%HDFS_CMD%" dfs -mkdir -p "%HDFS_BASE%/data/machines/raw"
echo [OK]  %HDFS_BASE%/data/machines/raw

"%HDFS_CMD%" dfs -mkdir -p "%HDFS_BASE%/data/machines/predictions"
echo [OK]  %HDFS_BASE%/data/machines/predictions

"%HDFS_CMD%" dfs -mkdir -p "%HDFS_BASE%/data/machines/insights"
echo [OK]  %HDFS_BASE%/data/machines/insights

REM ─── CREATE TODAY'S PARTITION DIRECTORY ──────────────
echo.
echo [*] Creating today's raw partition directory (dt=%TODAY%)...
"%HDFS_CMD%" dfs -mkdir -p "%HDFS_BASE%/data/machines/raw/dt=%TODAY%"
echo [OK]  %HDFS_BASE%/data/machines/raw/dt=%TODAY%

REM ─── ALSO CREATE YESTERDAY'S PARTITION ───────────────
for /f %%D in ('powershell -NoProfile -Command "(Get-Date).AddDays(-1).ToString('yyyy-MM-dd')"') do set YESTERDAY=%%D
echo.
echo [*] Creating yesterday's raw partition directory (dt=%YESTERDAY%)...
"%HDFS_CMD%" dfs -mkdir -p "%HDFS_BASE%/data/machines/raw/dt=%YESTERDAY%"
echo [OK]  %HDFS_BASE%/data/machines/raw/dt=%YESTERDAY%

REM ─── VERIFY ───────────────────────────────────────────
echo.
echo =====================================================
echo   HDFS Directory Verification
echo =====================================================
echo.
echo -- data/machines/ --
"%HDFS_CMD%" dfs -ls "%HDFS_BASE%/data/machines/"
echo.
echo -- data/machines/raw/ --
"%HDFS_CMD%" dfs -ls "%HDFS_BASE%/data/machines/raw/"
echo.

echo =====================================================
echo  [+] All HDFS directories initialised successfully!
echo      You can now run:  python run_project.py
echo =====================================================
echo.
pause
endlocal
