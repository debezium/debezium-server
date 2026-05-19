
SET PATH_SEP=;
SET JAVA_BINARY=%JAVA_HOME%\bin\java

for %%i in (debezium-server-*runner.jar) do set RUNNER=%%~i
echo %RUNNER%
SET LIB_PATH=lib\*
@REM Configuration files and directories that need to be on the classpath
SET LIB_CONFIG=config\lib
SET ENABLE_LIB_OPT=false
IF "%ENABLE_DEBEZIUM_SCRIPTING%"=="true" SET ENABLE_LIB_OPT=true
IF "%ENABLE_CHRONICLE_QUEUE%"=="true" SET ENABLE_LIB_OPT=true
IF "%ENABLE_LIB_OPT%"=="true" SET LIB_PATH=%LIB_PATH%%PATH_SEP%lib_opt\*
IF "%ENABLE_CHRONICLE_QUEUE%"=="true" SET JAVA_OPTS=%JAVA_OPTS% --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
call "%JAVA_BINARY%" %DEBEZIUM_OPTS% %JAVA_OPTS% -cp %RUNNER%%PATH_SEP%%LIB_CONFIG_PATH%%PATH_SEP%%LIB_PATH% io.debezium.server.Main
