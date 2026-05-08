@echo off
set JAVA_HOME=%~dp0\cots\jdk
set PATH=%JAVA_HOME%\bin;%PATH%
java -jar %~dp0\cots\kafka-ui-api-v0.7.2.jar --spring.config.location=%~dp0\cots\config.yml