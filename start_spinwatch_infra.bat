@echo off
setlocal enabledelayedexpansion
title Hadoop + Kafka Infrastructure Launcher

echo.
echo =====================================================
echo     SpinWatch  ^|  Full Infrastructure Launcher
echo =====================================================
echo.

REM ─── CONFIGURE YOUR PATHS HERE ───────────────────────
set HADOOP_HOME=C:\hadoop
set KAFKA_HOME=C:\kafka
set JAVA_HOME=C:\jdk-17.0.20+8
set KAFKA_TOPIC=machine-readings
set KAFKA_PARTITIONS=3
set KAFKA_REPLICATION=1
set PATH=%HADOOP_HOME%\bin;%HADOOP_HOME%\sbin;%JAVA_HOME%\bin;%PATH%
REM ──────────────────────────────────────────────────────

REM ─── CHECK FOR ADMIN RIGHTS ────────────────────────────
REM Hadoop-on-Windows frequently hits spurious AccessDeniedException
REM during NameNode format/start when not run elevated. Warn early
REM rather than let it fail deep inside HDFS internals.
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [WARN] This script is not running as Administrator.
    echo        If NameNode formatting fails with AccessDeniedException,
    echo        re-run this script from an elevated ^(Run as Administrator^) prompt.
    echo.
)

REM ─── VALIDATE INSTALLS ────────────────────────────────
if not exist "%HADOOP_HOME%\sbin\start-dfs.cmd" (
    echo [ERROR] Hadoop not found at %HADOOP_HOME%
    pause & exit /b 1
)
if not exist "%KAFKA_HOME%\bin\windows\kafka-server-start.bat" (
    echo [ERROR] Kafka not found at %KAFKA_HOME%
    pause & exit /b 1
)
if not exist "%KAFKA_HOME%\config\server.properties" (
    echo [ERROR] Kafka config not found: %KAFKA_HOME%\config\server.properties
    pause & exit /b 1
)

REM ─── HELPER: check if a process name is in jps output ─
REM Usage: call :check_running <ProcessName> <ResultVar>
goto :after_functions

:check_running
set "_found=0"
for /f "tokens=2" %%P in ('jps ^| findstr /i "%~1"') do set "_found=1"
set "%2=!_found!"
exit /b 0

:after_functions

REM ─── STEP 0: Kafka KRaft storage format (first-run only) ─
echo [0/5] Checking Kafka KRaft storage format...
for /f "tokens=2 delims==" %%A in ('findstr /i "^log.dirs" "%KAFKA_HOME%\config\server.properties"') do set KAFKA_LOG_DIR=%%A
set KAFKA_LOG_DIR=%KAFKA_LOG_DIR: =%
if "%KAFKA_LOG_DIR%"=="" set KAFKA_LOG_DIR=C:\tmp\kraft-combined-logs

if not exist "%KAFKA_LOG_DIR%\meta.properties" (
    echo      Storage not formatted. Formatting now with a new cluster UUID...
    pushd "%KAFKA_HOME%"
    for /f %%U in ('bin\windows\kafka-storage.bat random-uuid 2^>nul ^| findstr /v "ERROR"') do set CLUSTER_UUID=%%U
    if "!CLUSTER_UUID!"=="" (
        echo [ERROR] Could not generate cluster UUID.
        popd & pause & exit /b 1
    )
    echo      Generated UUID: !CLUSTER_UUID!
    bin\windows\kafka-storage.bat format -t !CLUSTER_UUID! -c config\server.properties --standalone
    if errorlevel 1 (
        echo [ERROR] Kafka storage format failed.
        popd & pause & exit /b 1
    )
    popd
    echo      [OK] Kafka storage formatted.
) else (
    echo      [OK] Storage already formatted. Skipping.
)
echo.

REM ─── STEP 1: NameNode format check (first-run only) ───
echo [1/5] Checking HDFS NameNode metadata...
if not exist "%HADOOP_HOME%\data\namenode\current\VERSION" (
    echo      NameNode not yet formatted.

    REM Kill any stray Java/Hadoop processes that may be holding a
    REM lock on the namenode directory from a previous failed attempt.
    echo      Clearing any stale locks from previous attempts...
    taskkill /F /IM java.exe >nul 2>&1
    timeout /t 2 /nobreak >nul

    REM Wipe the namenode data dir entirely rather than trusting
    REM whatever state it's currently in (stale "current" folders
    REM and orphaned in_use.lock files are the #1 cause of
    REM AccessDeniedException during format on Windows).
    if exist "%HADOOP_HOME%\data\namenode" (
        echo      Removing existing namenode data directory...
        rmdir /S /Q "%HADOOP_HOME%\data\namenode" 2>nul
        if exist "%HADOOP_HOME%\data\namenode" (
            echo [ERROR] Could not fully remove %HADOOP_HOME%\data\namenode
            echo         It may still be locked by another process. Diagnostics:
            echo.
            tasklist | findstr /i java
            echo.
            icacls "%HADOOP_HOME%\data\namenode"
            pause & exit /b 1
        )
    )
    mkdir "%HADOOP_HOME%\data\namenode" 2>nul

    REM Explicitly grant the current user full control, in case
    REM inherited ACLs from the parent are incomplete or stale.
    icacls "%HADOOP_HOME%\data\namenode" /grant "%USERNAME%:(OI)(CI)F" /T >nul 2>&1

    echo      Formatting now...
    call hdfs namenode -format -force -nonInteractive
    if errorlevel 1 (
        echo [ERROR] NameNode format failed. Diagnostics:
        echo.
        echo --- Processes holding java.exe ---
        tasklist | findstr /i java
        echo.
        echo --- Permissions on namenode dir ---
        icacls "%HADOOP_HOME%\data\namenode"
        echo.
        echo If AccessDeniedException persists:
        echo   1^) Re-run this script as Administrator.
        echo   2^) Add a Windows Defender exclusion for %HADOOP_HOME%\data
        echo   3^) Confirm %HADOOP_HOME% is not inside a OneDrive-synced folder.
        pause & exit /b 1
    )
    echo      [OK] NameNode formatted.
) else (
    echo      [OK] NameNode already formatted. Skipping.
)
echo.

REM ─── STEP 2: HDFS ──────────────────────────────────────
call :check_running NameNode ALREADY_UP
if "!ALREADY_UP!"=="1" (
    echo [2/5] HDFS already running. Skipping start.
) else (
    echo [2/5] Starting HDFS ^(NameNode + DataNode^)...
    start "SpinWatch - HDFS" cmd /k "title SpinWatch HDFS & cd /d %HADOOP_HOME% & echo Starting HDFS... & sbin\start-dfs.cmd"
    echo      Waiting 15s for HDFS to initialise...
    timeout /t 15 /nobreak >nul
)
call :check_running NameNode HDFS_OK
if "!HDFS_OK!"=="1" (
    echo      [OK] NameNode confirmed running.
) else (
    echo      [WARN] NameNode not detected yet. Check the "SpinWatch - HDFS" window for errors.
)
echo.

REM ─── STEP 3: YARN ──────────────────────────────────────
call :check_running ResourceManager ALREADY_UP
if "!ALREADY_UP!"=="1" (
    echo [3/5] YARN already running. Skipping start.
) else (
    echo [3/5] Starting YARN ^(ResourceManager + NodeManager^)...
    start "SpinWatch - YARN" cmd /k "title SpinWatch YARN & cd /d %HADOOP_HOME% & echo Starting YARN... & sbin\start-yarn.cmd"
    echo      Waiting 10s for YARN to initialise...
    timeout /t 10 /nobreak >nul
)
call :check_running ResourceManager YARN_OK
if "!YARN_OK!"=="1" (
    echo      [OK] ResourceManager confirmed running.
) else (
    echo      [WARN] ResourceManager not detected yet. Check the "SpinWatch - YARN" window for errors.
)
echo.

REM ─── STEP 4: KAFKA ─────────────────────────────────────
call :check_running Kafka ALREADY_UP
if "!ALREADY_UP!"=="1" (
    echo [4/5] Kafka already running. Skipping start.
) else (
    echo [4/5] Starting Kafka Broker...
    start "SpinWatch - Kafka" cmd /k "title SpinWatch Kafka & cd /d %KAFKA_HOME% & echo Starting Kafka... & bin\windows\kafka-server-start.bat config\server.properties"
    echo      Waiting 12s for Kafka broker to become ready...
    timeout /t 12 /nobreak >nul
)
call :check_running Kafka KAFKA_OK
if "!KAFKA_OK!"=="1" (
    echo      [OK] Kafka confirmed running.
) else (
    echo      [WARN] Kafka not detected yet. Check the "SpinWatch - Kafka" window for errors.
)
echo.

REM ─── STEP 5: CREATE KAFKA TOPIC (idempotent) ──────────
echo [5/5] Ensuring Kafka topic "%KAFKA_TOPIC%" exists...
pushd "%KAFKA_HOME%"
bin\windows\kafka-topics.bat --create ^
    --topic %KAFKA_TOPIC% ^
    --partitions %KAFKA_PARTITIONS% ^
    --replication-factor %KAFKA_REPLICATION% ^
    --if-not-exists ^
    --bootstrap-server localhost:9092 2>nul
if errorlevel 1 (
    echo      [WARN] Could not create/verify topic yet. Kafka may still be starting.
    echo             Retry manually in a few seconds:
    echo             %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic %KAFKA_TOPIC% --partitions %KAFKA_PARTITIONS% --replication-factor %KAFKA_REPLICATION% --bootstrap-server localhost:9092
) else (
    echo      [OK] Topic "%KAFKA_TOPIC%" is ready.
)
popd
echo.

REM ─── FINAL STATUS SUMMARY ──────────────────────────────
echo =====================================================
echo   Infrastructure Status Summary
echo =====================================================
echo.
jps
echo.
echo   Expected: NameNode, DataNode, ResourceManager,
echo             NodeManager, Kafka
echo.
echo   If any are missing above, check their respective
echo   "SpinWatch - ..." windows for the actual error.
echo.
echo   Next step — launch SpinWatch pipeline:
echo     python run_project.py
echo.
echo =====================================================
echo   HDFS Web UI  : http://localhost:9870
echo   YARN Web UI  : http://localhost:8088
echo   Dashboard    : http://localhost:8050   (after run_project.py)
echo =====================================================
echo.
pause
endlocal