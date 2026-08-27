@echo off
REM =============================================================================
REM HYDRA-UMC-TELEMETRY-COLLECTOR - build.bat
REM Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
REM GPL-3.0 - see LICENSE
REM =============================================================================
REM Builds HYDRA-UMC-TELEMETRY-COLLECTOR: bumps the version, then compiles
REM the Go module in src/ into build/telemetry-collector.exe. Run this
REM before run.bat.
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo.
echo  ===============================================================
echo   H Y D R A - U M C - T E L E M E T R Y - C O L L E C T O R  -  build
echo  ===============================================================
echo   High-throughput ingestion node for CAN and WebSocket logs
echo   Author:  JuanenRac (Electro Hobby 3D)
echo   License: GPL-3.0 (see LICENSE.md)
echo  ===============================================================
echo.

echo [1/2] Bumping version number (odometer bump, see bump_version.py)...
python bump_version.py
if errorlevel 1 ( echo NATIVE VERSION BUMP FAILED. & pause & exit /b 1 )
python "%~dp0bump_manifest_version.py" --sync
if errorlevel 1 ( echo VERSION SYNCHRONIZATION FAILED. & pause & exit /b 1 )
if errorlevel 1 goto :error
echo       Done.
echo.

echo [2/2] Compiling Go module (src/)...
if not exist build mkdir build
pushd src
go build -o ..\build\telemetry-collector.exe .
if errorlevel 1 (
    popd
    goto :error
)
popd
echo       Done. Binary: build\telemetry-collector.exe
echo.

REM No longer a "run it once to verify" step here (there was one, up
REM through the andamiaje stage): main.go now starts a real HTTP server
REM that blocks until Ctrl+C, so launching it from inside build.bat
REM would hang the build forever instead of "verifying" anything.
REM Actually running the binary is what run.bat is for.

echo  ===============================================================
echo   Build complete. Run run.bat to start the collector.
echo  ===============================================================
echo.
pause
exit /b 0

:error
echo.
echo   BUILD FAILED - see the output above.
pause
exit /b 1
