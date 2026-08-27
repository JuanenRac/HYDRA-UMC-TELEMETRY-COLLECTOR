@echo off
REM =============================================================================
REM HYDRA-UMC-TELEMETRY-COLLECTOR - run.bat
REM Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
REM GPL-3.0 - see LICENSE
REM =============================================================================
REM Runs HYDRA-UMC-TELEMETRY-COLLECTOR's compiled binary. Run build.bat first.
cd /d "%~dp0"

if not exist build\telemetry-collector.exe (
    echo ERROR: build\telemetry-collector.exe not found. Run build.bat first.
    pause
    exit /b 1
)

build\telemetry-collector.exe %*
pause
