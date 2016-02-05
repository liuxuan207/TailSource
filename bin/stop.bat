@echo off
rem stop current program.

setlocal enabledelayedexpansion
rem Using pushd popd to set BASE_DIR to the absolute path^M
pushd %~dp0..\
set BASE_DIR=%CD%
popd

cd %BASE_DIR%

for /f "tokens=1,2 delims==" %%i in (config/config.properties) do (

	IF ["%%i"] EQU ["stopflag"] (
		echo stopflag: %%j
		echo.>%%j
		goto :end
	)
)
EndLocal
:end

echo touched exit flag...
pause