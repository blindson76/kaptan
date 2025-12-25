@echo off
taskkill /f /im replctl.exe
taskkill /f /fi "WINDOWTITLE eq Node-*"
taskkill /f /fi "WINDOWTITLE eq Node-*"
taskkill /f /im mongod.exe
taskkill /f /im nomad.exe
taskkill /f /im consul.exe
go build -C %~dp0\..  .\cmd\replctl || (echo Build failed & exit /b 1)
echo build succeeded
for %%l in (1 3 6) do (
    start /min /D %~dp0 cmd /c start-agent.bat %%l %1
)