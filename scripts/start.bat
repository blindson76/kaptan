@echo off
taskkill /f /fi "WINDOWTITLE eq Node-*"
taskkill /f /fi "WINDOWTITLE eq Node-*"
taskkill /f /im mongod.exe
taskkill /f /im nomad.exe
taskkill /f /im consul.exe
for %%l in (1 2 3 4 5 6) do (
    start /min /D %~dp0 cmd /c start-agent.bat %%l %1
)