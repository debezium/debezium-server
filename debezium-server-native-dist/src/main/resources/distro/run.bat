@echo off

REM Copyright Debezium Authors.
REM Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

set "SCRIPT_DIR=%~dp0"

set "RUNNER="
for %%i in ("%SCRIPT_DIR%debezium-server-*runner.exe") do set "RUNNER=%%~i"

if "%RUNNER%"=="" (
    echo ERROR: Could not find the native executable (debezium-server-*runner.exe) in %SCRIPT_DIR%
    exit /b 1
)

"%RUNNER%" %*
