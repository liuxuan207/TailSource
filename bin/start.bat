@echo off
rem main enter.


setlocal enabledelayedexpansion
rem Using pushd popd to set BASE_DIR to the absolute path^M
pushd %~dp0..\
set BASE_DIR=%CD%
popd

IF ["%BASE_DIR%"] EQU [""] (
	set BASE_DIR=D:\tailsource
)
echo %BASE_DIR%
cd /d %BASE_DIR%

IF [%1] EQU [] (
   set TAILSOURCE_CONF_OPTS=-Dconfig=config/config.properties     
) else (
   set TAILSOURCE_CONF_OPTS=-Dconfig=%1
)
set TAILSOURCE_HEAP_OPTS=-Xmx512M -Xms64M
set JAVA_HOME=%BASE_DIR%\ext\jdk1.7
set JAVA=%JAVA_HOME%\bin\java
set CLASSPATH=
rem set CLASSPATH
for %%i in (libs\*.jar) do (
        IF ["!CLASSPATH!"] EQU [""] (
  			set CLASSPATH=%%i
		) ELSE (
  			set CLASSPATH=!CLASSPATH!;%%i
		)
)
echo CLASSPATH:%CLASSPATH%

set COMMAND=%JAVA% %TAILSOURCE_HEAP_OPTS% %TAILSOURCE_CONF_OPTS% -cp %CLASSPATH% com.gaea.tailsource.App %*

echo BEGIN TO START JAVA PROGROM.
echo %COMMAND%
%COMMAND%
EndLocal