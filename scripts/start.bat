@echo off
taskkill -f -im mongod.exe
taskkill /f /im nomad.exe
taskkill /f /im consul.exe
taskkill /f /fi "WINDOWTITLE eq Node-*"
taskkill /f /fi "WINDOWTITLE eq Node-*"
for %%l in (4 5 6) do (
    start /min /D %~dp0 cmd /c start-agent.bat %%l %1
)