@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem =========================
rem Usage & args
rem =========================
if "%~1"=="" (
  echo Usage: %~nx0 ^<CONSOLE_ID 1-6^> [/clean]
  exit /b 1
)

set "CONSOLE_ID=%~1"
rem Allowed: 1..6
echo.%CONSOLE_ID%| findstr /r "^[1-6]$" >nul || (
  echo ERROR: CONSOLE_ID must be 1..6
  exit /b 1
)

set "CLEAN_FLAG=%~2"

rem =========================
rem Base paths
rem =========================
set "BASE=%~dp0"
set "CMS_ROOT=%BASE%"
set "COTS_DIR=%BASE%cots"
set "MONGO_TOOLS=%BASE%cots\mongo"
set "KAFKA_TOOLS=%BASE%cots\kafka\bin\windows"
set "TARGET_DIR=%BASE%target"
set "LOG_DIR=%BASE%logs"
set "JAVA_HOME=%COTS_DIR%\jdk"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

rem =========================
rem Identity & addressing
rem =========================
set "NODE_ID=%CONSOLE_ID%"
set "NODE_NAME=node-%CONSOLE_ID%"
set "CSB_IP=10.10.5%CONSOLE_ID%.1"

title Node-%CONSOLE_ID%
echo This is node %CONSOLE_ID% (IP %CSB_IP%)

rem =========================
rem PATH
rem =========================

set "PATH=%BASE%;%PATH%"
set "PATH=%COTS_DIR%;%PATH%"
set "PATH=%MONGO_TOOLS%;%PATH%"
set "PATH=%KAFKA_TOOLS%;%PATH%"
set "PATH=%JAVA_HOME%\bin;%PATH%"

rem =========================
rem Data directories
rem =========================
set "NOMAD_DATA_DIR=%BASE%data\nomad\%CONSOLE_ID%"
set "MONGO_DB_PATH=%BASE%data\mongo\%CONSOLE_ID%"
set "MONGO_LOG_PATH=%BASE%data\mongo\%CONSOLE_ID%\mongo.log"
set "CONSUL_DATA_DIR=%BASE%data\consul\%CONSOLE_ID%"
set "KAFKA_DATA_DIR=%BASE%data\kafka\%CONSOLE_ID%\data"
set "KAFKA_META_DIR=%BASE%data\kafka\%CONSOLE_ID%\meta"
set "KAFKA_LOG_DIR=%BASE%data\kafka\%CONSOLE_ID%\log"

for %%D in ("%NOMAD_DATA_DIR%" "%MONGO_DB_PATH%" "%CONSUL_DATA_DIR%" "%KAFKA_DATA_DIR%" "%KAFKA_META_DIR%" "%KAFKA_LOG_DIR%") do (
  if not exist "%%~D" mkdir "%%~D"
)

if /i "%CLEAN_FLAG%"=="/clean" (
  echo CLEAN mode: wiping Nomad and Consul data for node %CONSOLE_ID%...
  if exist "%NOMAD_DATA_DIR%"  rmdir /s /q "%NOMAD_DATA_DIR%"
  if exist "%CONSUL_DATA_DIR%" rmdir /s /q "%CONSUL_DATA_DIR%"
  if not exist "%NOMAD_DATA_DIR%"  mkdir "%NOMAD_DATA_DIR%"
  if not exist "%CONSUL_DATA_DIR%" mkdir "%CONSUL_DATA_DIR%"
)

rem =========================
rem Service URLs / ports
rem =========================
set "NOMAD_ADDR=http://%CSB_IP%:4646"
set "CONSUL_HTTP_ADDR=http://%CSB_IP%:8500"
set "MONGO_PORT=27017"
set "MONGO_ADDR=%CSB_IP%"
set "MONGO_LOCAL_ADDR=%CSB_IP%"
set "KAFKA_BROKER_ADDR=%CSB_IP%"
set "KAFKA_CONTROLLER_ADDR=%CSB_IP%"
set "KAFKA_BROKER_PORT=9092"
set "KAFKA_CONTROLLER_PORT=9093"

rem =========================
rem Kafka IDs (static map)
rem =========================
set "KAFKA_CLUSTER_ID=b_ue-dU-TrybRneDxGS_Ow"
if "%CONSOLE_ID%"=="1" set "KAFKA_STORAGE_ID=279THXvBR4WGfBj_y1nstQ"
if "%CONSOLE_ID%"=="2" set "KAFKA_STORAGE_ID=TMoKnP9HRz-NVmhcEVuSGQ"
if "%CONSOLE_ID%"=="3" set "KAFKA_STORAGE_ID=x1AWfhMTQ7u_isq4a04w6Q"
if "%CONSOLE_ID%"=="4" set "KAFKA_STORAGE_ID=mP3P8tD3RqSCn5ytqs6Sqw"
if "%CONSOLE_ID%"=="5" set "KAFKA_STORAGE_ID=dJrECDp5Rue3vxpq51EaLQ"
if "%CONSOLE_ID%"=="6" set "KAFKA_STORAGE_ID=Bhml8362S5eGsFfP7o7noQ"

rem =========================
rem Mongo RS
rem =========================
set "RS_NAME=rs0"

rem =========================
rem Java classpath (if needed)
rem =========================
set "CLASSPATH=%TARGET_DIR%\*;%TARGET_DIR%\lib\*;"

rem =========================
rem Start Consul
rem =========================
echo Starting Consul...
start "consul-%CONSOLE_ID%" /min cmd /c consul agent -server -ui -config-file %BASE%consul.hcl -data-dir %CONSUL_DATA_DIR% -node %NODE_NAME% -client %CSB_IP% -bind %CSB_IP% -bootstrap-expect 3 -retry-join 10.10.51.1 -retry-join 10.10.52.1 -retry-join 10.10.53.1 -retry-join 10.10.54.1 -retry-join 10.10.55.1 -retry-join 10.10.56.1

start "Node-Worker-%CONSOLE_ID%" cmd /k go run -C .. .\cmd\replctl
goto end
echo Waiting Consul to be ready at %CONSUL_HTTP_ADDR% ...
node "%BASE%wait_nomad.js" "%CONSUL_HTTP_ADDR%"

start "Node-Worker-%CONSOLE_ID%" /min cmd /k go run -C C:\Users\ubozkurt\Downloads\kaptan .\cmd\replctl
goto :cmd
rem =========================
rem Start Nomad (server+client)
rem =========================
echo Starting Nomad...
start "nomad-%CONSOLE_ID%" /min cmd /c nomad agent -server -config %BASE%client.hcl -config %BASE%server.hcl -bind %CSB_IP% -consul-address %CSB_IP%:8500 -data-dir %NOMAD_DATA_DIR%  -node %NODE_NAME% -bootstrap-expect 3 -retry-join 10.10.51.1:4648 -retry-join 10.10.52.1:4648 -retry-join 10.10.53.1:4648 -retry-join 10.10.54.1:4648 -retry-join 10.10.55.1:4648 -retry-join 10.10.56.1:4648

rem =========================
rem Wait Nomad ready (custom script)
rem =========================
echo Waiting Nomad to be ready at %NOMAD_ADDR% ...
node "%BASE%wait_nomad.js" "%NOMAD_ADDR%"
if errorlevel 1 (
  echo ERROR: wait_nomad.js failed.
  exit /b 1
)
echo Nomad ready!

rem =========================
rem Submit jobs
rem =========================
echo Starting mongo-control job...
nomad run -detach "%CMS_ROOT%jobs\mongo\mongo-control.hcl" 1>>"%LOG_DIR%\nomad-%NODE_ID%.log" 2>>&1
if errorlevel 1 (
  echo ERROR: nomad run failed. See "%LOG_DIR%\nomad-%NODE_ID%.log"
  goto :end
) else (
  echo mongo-control submitted.
)

echo Starting kafka job...
nomad run -detach "%CMS_ROOT%jobs\kafka\kafka-control.hcl" 1>>"%LOG_DIR%\kafka-%NODE_ID%.log" 2>>&1
if errorlevel 1 (
  echo ERROR: nomad run failed. See "%LOG_DIR%\nomad-%NODE_ID%.log"
  goto :end
) else (
  echo kafka submitted.
)
rem =========================
rem Prestart (Go helper)
rem =========================
echo Running prestart (Go)...
go run -C "%BASE%goctl" ./cmd/goctl -type mongo -task prestart 1>"%LOG_DIR%\prestart-%NODE_ID%.log" 2>&1
if errorlevel 1 (
  echo WARNING: go prestart exited with errors. See "%LOG_DIR%\prestart-%NODE_ID%.log"
) else (
  echo Prestart OK. Logs: "%LOG_DIR%\prestart-%NODE_ID%.log"
)
go run -C "%BASE%goctl" ./cmd/goctl -type kafka -task prestart 1>"%LOG_DIR%\kafka-prestart-%NODE_ID%.log" 2>&1
if errorlevel 1 (
  echo WARNING: go kafka-prestart exited with errors. See "%LOG_DIR%\kafka-prestart-%NODE_ID%.log"
) else (
  echo kafka-Prestart OK. Logs: "%LOG_DIR%\kafka-prestart-%NODE_ID%.log"
)
:cmd
rem start cmd /k title Node-%NODE_ID%
goto :end

rem =========================
rem Legacy/maintenance (examples)
rem =========================
:maintenance
rem nomad node meta apply -unset role.mongo
rem nomad var purge status/mongo/%NODE_NAME%
rem set SHELL=cmd.exe
rem nomad var lock -verbose -ttl=10s -max-retry=1 job/deploy /c nomad job run jobs\mongo\mongo.hcl
rem nomad var lock -verbose -ttl=10s -max-retry=1 job/deploy2 /c nomad job run jobs\mongo\mongo-member.hcl
goto :eof

:end
echo Done.
exit /b 0
