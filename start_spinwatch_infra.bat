@echo off
setlocal enabledelayedexpansion
title Hadoop + Kafka Infrastructure Launcher

echo.
echo =====================================================
echo     SpinWatch  ^|  Full Infrastructure Launcher
echo =====================================================
echo.

REM --- CONFIGURE YOUR PATHS HERE -------------------------
set "HADOOP_HOME=C:\hadoop"
set "KAFKA_HOME=C:\kafka"
set "JAVA_HOME=C:\jdk-17.0.20+8"
set "KAFKA_TOPIC=machine-readings"
set "KAFKA_PARTITIONS=3"
set "KAFKA_REPLICATION=1"
set "KAFKA_PORT=9092"
set "PATH=%HADOOP_HOME%\bin;%HADOOP_HOME%\sbin;%JAVA_HOME%\bin;%PATH%"
REM -------------------------------------------------------

REM --- ADMIN CHECK ---------------------------------------
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [WARN] Not running as Administrator.
    echo        If NameNode formatting fails with AccessDeniedException,
    echo        re-run from an elevated ^(Run as Administrator^) prompt.
    echo.
)

REM --- DETECT KAFKA LAYOUT (3.x vs 4.x) ------------------
REM 4.x: config\server.properties, format needs --standalone
REM 3.x: config\kraft\server.properties, no --standalone
set "KAFKA_CFG=config\server.properties"
set "KAFKA_FORMAT_FLAGS=--standalone"
if exist "%KAFKA_HOME%\config\kraft\server.properties" (
    set "KAFKA_CFG=config\kraft\server.properties"
    set "KAFKA_FORMAT_FLAGS="
)

REM --- VALIDATE INSTALLS ---------------------------------
if not exist "%JAVA_HOME%\bin\java.exe" (
    echo [ERROR] Java not found at %JAVA_HOME%
    pause & exit /b 1
)
if not exist "%HADOOP_HOME%\sbin\start-dfs.cmd" (
    echo [ERROR] Hadoop not found at %HADOOP_HOME%
    pause & exit /b 1
)
if not exist "%KAFKA_HOME%\bin\windows\kafka-server-start.bat" (
    echo [ERROR] Kafka Windows scripts not found at %KAFKA_HOME%\bin\windows
    pause & exit /b 1
)
if not exist "%KAFKA_HOME%\%KAFKA_CFG%" (
    echo [ERROR] Kafka config not found: %KAFKA_HOME%\%KAFKA_CFG%
    pause & exit /b 1
)

goto :main

REM =========================================================
REM  HELPER FUNCTIONS
REM =========================================================

:check_running
REM Usage: call :check_running <ProcessName> <ResultVar>   (uses jps)
set "_found=0"
for /f "tokens=2" %%P in ('jps ^| findstr /i "%~1"') do set "_found=1"
set "%2=!_found!"
exit /b 0

:port_listening
REM Usage: call :port_listening <port> <ResultVar>
set "_pl=0"
netstat -ano | findstr /r /c:":%~1 .*LISTENING" >nul && set "_pl=1"
set "%2=!_pl!"
exit /b 0

:wait_for_port
REM Usage: call :wait_for_port <port> <max_seconds> <ResultVar>
set "_wp=0"
for /l %%I in (1,1,%~2) do (
    if "!_wp!"=="0" (
        netstat -ano | findstr /r /c:":%~1 .*LISTENING" >nul && set "_wp=1"
        if "!_wp!"=="0" timeout /t 1 /nobreak >nul
    )
)
set "%3=!_wp!"
exit /b 0

:make_uuid
REM Usage: call :make_uuid <ResultVar>
REM Uses kafka-storage random-uuid, with a PowerShell fallback.
set "_u="
call "%KAFKA_HOME%\bin\windows\kafka-storage.bat" random-uuid > "%TEMP%\kuuid.txt" 2> "%TEMP%\kuuid.err"
if exist "%TEMP%\kuuid.txt" set /p _u=<"%TEMP%\kuuid.txt"
if not defined _u (
    echo      kafka-storage random-uuid failed. Java said:
    type "%TEMP%\kuuid.err"
    echo      Falling back to PowerShell UUID generation...
    for /f "delims=" %%G in ('powershell -NoProfile -Command "[Convert]::ToBase64String([guid]::NewGuid().ToByteArray()).TrimEnd('=').Replace('+','-').Replace('/','_')"') do set "_u=%%G"
)
set "%1=!_u!"
exit /b 0

:ensure_topic
REM Usage: call :ensure_topic <ResultVar>   (retries; idempotent)
set "_ok=0"
pushd "%KAFKA_HOME%"
for /l %%I in (1,1,5) do (
    if "!_ok!"=="0" (
        call bin\windows\kafka-topics.bat --create --topic %KAFKA_TOPIC% --partitions %KAFKA_PARTITIONS% --replication-factor %KAFKA_REPLICATION% --if-not-exists --bootstrap-server localhost:%KAFKA_PORT% >"%TEMP%\spinwatch_topic.log" 2>&1
        if not errorlevel 1 (set "_ok=1") else timeout /t 3 /nobreak >nul
    )
)
popd
set "%1=!_ok!"
exit /b 0

REM =========================================================
:main

REM --- STEP 0: Kafka KRaft storage format (first run only) -
echo [0/5] Checking Kafka KRaft storage...

set "KAFKA_LOG_DIR="
for /f "tokens=1,* delims==" %%A in ('findstr /b /i "log.dirs" "%KAFKA_HOME%\%KAFKA_CFG%"') do set "KAFKA_LOG_DIR=%%B"
if not defined KAFKA_LOG_DIR set "KAFKA_LOG_DIR=/tmp/kafka-logs"

for /f "tokens=1 delims=," %%D in ("!KAFKA_LOG_DIR!") do set "KAFKA_LOG_DIR=%%D"
set "KAFKA_LOG_DIR=!KAFKA_LOG_DIR: =!"
set "KAFKA_LOG_DIR=!KAFKA_LOG_DIR:/=\!"
if not "!KAFKA_LOG_DIR:~1,1!"==":" set "KAFKA_LOG_DIR=%KAFKA_HOME:~0,2%!KAFKA_LOG_DIR!"
echo      Kafka data directory: !KAFKA_LOG_DIR!

if exist "!KAFKA_LOG_DIR!\meta.properties" (
    echo      [OK] Storage already formatted. Skipping.
) else (
    echo      Storage not formatted. Formatting now...
    set "CLUSTER_UUID="
    call :make_uuid CLUSTER_UUID
    if not defined CLUSTER_UUID (
        echo [ERROR] Could not generate a cluster UUID. Check JAVA_HOME.
        pause & exit /b 1
    )
    echo      Cluster UUID: !CLUSTER_UUID!
    pushd "%KAFKA_HOME%"
    call bin\windows\kafka-storage.bat format -t !CLUSTER_UUID! -c %KAFKA_CFG% !KAFKA_FORMAT_FLAGS!
    if errorlevel 1 (
        echo [ERROR] Kafka storage format failed. See the message above.
        popd
        pause & exit /b 1
    )
    popd
    echo      [OK] Kafka storage formatted.
)
echo.

REM --- STEP 1: NameNode format check (first run only) ----
echo [1/5] Checking HDFS NameNode metadata...
if not exist "%HADOOP_HOME%\data\namenode\current\VERSION" (
    echo      NameNode not yet formatted.

    REM Stop only stale Hadoop JVMs. Never kill Kafka or other Java apps.
    echo      Clearing stale NameNode/DataNode processes...
    for /f "tokens=1" %%P in ('jps ^| findstr /i "NameNode DataNode"') do taskkill /F /PID %%P >nul 2>&1
    timeout /t 2 /nobreak >nul

    if exist "%HADOOP_HOME%\data\namenode" (
        echo      Removing existing namenode data directory...
        rmdir /S /Q "%HADOOP_HOME%\data\namenode" 2>nul
        if exist "%HADOOP_HOME%\data\namenode" (
            echo [ERROR] Could not fully remove %HADOOP_HOME%\data\namenode
            tasklist | findstr /i java
            icacls "%HADOOP_HOME%\data\namenode"
            pause & exit /b 1
        )
    )
    mkdir "%HADOOP_HOME%\data\namenode" 2>nul
    icacls "%HADOOP_HOME%\data\namenode" /grant "%USERNAME%:(OI)(CI)F" /T >nul 2>&1

    echo      Formatting now...
    call hdfs namenode -format -force -nonInteractive
    if errorlevel 1 (
        echo [ERROR] NameNode format failed.
        tasklist | findstr /i java
        icacls "%HADOOP_HOME%\data\namenode"
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

REM --- STEP 2: HDFS --------------------------------------
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
    echo      [WARN] NameNode not detected. Check the "SpinWatch - HDFS" window.
)
echo.

REM --- STEP 3: YARN --------------------------------------
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
    echo      [WARN] ResourceManager not detected. Check the "SpinWatch - YARN" window.
)
echo.

REM --- STEP 4: KAFKA -------------------------------------
set "KAFKA_OK=0"
call :port_listening %KAFKA_PORT% KAFKA_UP
if "!KAFKA_UP!"=="1" (
    echo [4/5] Kafka already listening on port %KAFKA_PORT%. Skipping start.
    set "KAFKA_OK=1"
) else (
    echo [4/5] Starting Kafka broker...
    start "SpinWatch - Kafka" cmd /k "title SpinWatch Kafka & set JAVA_HOME=%JAVA_HOME%& set PATH=%JAVA_HOME%\bin;%PATH%& cd /d %KAFKA_HOME% & bin\windows\kafka-server-start.bat %KAFKA_CFG%"
    echo      Waiting up to 60s for the broker on port %KAFKA_PORT%...
    call :wait_for_port %KAFKA_PORT% 60 KAFKA_OK
)
if "!KAFKA_OK!"=="1" (
    echo      [OK] Kafka is listening on port %KAFKA_PORT%.
) else (
    echo      [ERROR] Kafka is not listening on port %KAFKA_PORT%.
    echo              Read the error in the "SpinWatch - Kafka" window.
    echo              Common causes: Java older than 17, storage not formatted,
    echo              or port %KAFKA_PORT% used by another program.
)
echo.

REM --- STEP 5: CREATE KAFKA TOPIC (idempotent, retries) --
echo [5/5] Ensuring Kafka topic "%KAFKA_TOPIC%" exists...
if not "!KAFKA_OK!"=="1" (
    echo      [SKIP] Kafka is not running, so the topic was not created.
) else (
    call :ensure_topic TOPIC_OK
    if "!TOPIC_OK!"=="1" (
        echo      [OK] Topic "%KAFKA_TOPIC%" is ready.
        pushd "%KAFKA_HOME%"
        call bin\windows\kafka-topics.bat --describe --topic %KAFKA_TOPIC% --bootstrap-server localhost:%KAFKA_PORT%
        popd
    ) else (
        echo      [WARN] Could not create the topic. Last error:
        type "%TEMP%\spinwatch_topic.log"
    )
)
echo.

REM --- FINAL STATUS SUMMARY ------------------------------
echo =====================================================
echo   Infrastructure Status Summary
echo =====================================================
echo.
jps
echo.
echo   Expected: NameNode, DataNode, ResourceManager,
echo             NodeManager, Kafka
echo.
echo   Next step - launch the SpinWatch pipeline:
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