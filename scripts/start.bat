@echo off
taskkill /T /f /fi "WINDOWTITLE eq Node-*"
taskkill /T /f /fi "WINDOWTITLE eq consul-*"
go build -C %~dp0\..  .\cmd\replctl || (echo Build failed & exit /b 1)
echo build succeeded
for %%l in (1 2 3 4) do (
    start /min /D %~dp0 cmd /c start-agent.bat %%l %1
)