@echo off

:check
echo checking quorum status...
call kafka-metadata-quorum.bat --bootstrap-controller 10.10.51.1:9093,10.10.52.1:9093,10.10.53.1:9093,10.10.54.1:9093 describe --replication 2>nul
timeout /t 5 >nul
goto check
echo done