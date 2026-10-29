@echo off
rem PRETTY_RUN_BEGIN
if not defined PRETTY_MODE set "PRETTY_MODE=build"
set "PRETTY_PY=python"
where py >nul 2>&1 && set "PRETTY_PY=py -3"
if not defined PRETTY_RUN if exist "%~dp0tools\pretty_run.py" (
    set "PRETTY_RUN=1"
    %PRETTY_PY% "%~dp0tools\pretty_run.py" --wrap --mode %PRETTY_MODE% -- cmd /d /c "%~f0" %*
    exit /b
)
rem PRETTY_RUN_END
REM HYDRA_UMC_SCRIPT_STANDARD_HEADER_BEGIN
REM *****************************************************************************
REM Project   : HYDRA-UMC-TELEMETRY-COLLECTOR
REM Script    : build.bat
REM Purpose   : Incremental project build, verification and packaging workflow.
REM Author    : JuanenRac (Electro Hobby 3D)
REM Email     : electrohobby3d@gmail.com
REM Copyright : (C) 2026 JuanenRac
REM License   : GPL-3.0 - see LICENSE
REM *****************************************************************************
REM HYDRA_UMC_SCRIPT_STANDARD_HEADER_END
REM HYDRA_UMC_SCRIPT_STANDARD_BANNER_BEGIN
echo.
echo *****************************************************************************
echo * HYDRA-UMC-TELEMETRY-COLLECTOR - build.bat
echo * Mode      : INCREMENTAL BUILD
echo * Author    : JuanenRac (Electro Hobby 3D)
echo * Email     : electrohobby3d@gmail.com
echo * Copyright : (C) 2026 JuanenRac
echo * License   : GPL-3.0 - see LICENSE
echo * ------------------------------------------------------------------------- *
echo * 1. Read the project version (building never changes it; use the explicit bump command).
echo * 2. Run this project's declared build, verification and packaging commands.
echo * 3. Report the result and keep an interactive terminal open.
echo *****************************************************************************
echo.
REM HYDRA_UMC_SCRIPT_STANDARD_BANNER_END
REM Builds HYDRA-UMC-TELEMETRY-COLLECTOR: compiles
REM the Go module in src/ into build/telemetry-collector.exe. Run this
REM before run.bat.
setlocal enabledelayedexpansion
rem Keep the window open for people, never in automation (CI / HYDRA_UMC_CI).
set "KEEP_OPEN=pause"
if defined CI set "KEEP_OPEN=ver >nul"
if "%HYDRA_UMC_CI%"=="1" set "KEEP_OPEN=ver >nul"
cd /d "%~dp0"
REM HYDRA_UMC_SCRIPT_STANDARD_VERSION_STEP
echo [1/2] Reading the project version (building never changes it)...
for /f "usebackq delims=" %%V in (`python -c "import json; print(json.load(open(r'%~dp0hydra-umc.project.json', encoding='utf-8'))['version'])"`) do set "HYDRA_UMC_VERSION_BUILT=%%V"
if not defined HYDRA_UMC_VERSION_BUILT set "HYDRA_UMC_VERSION_BUILT=unknown"
echo       Building v%HYDRA_UMC_VERSION_BUILT% (building never changes the version)
echo.
echo.
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
echo Built v%HYDRA_UMC_VERSION_BUILT%. Building never changes the version; to raise it run: python tools\hydra_project_tool.py bump
echo.
%KEEP_OPEN%
exit /b 0

:error
echo.
echo   BUILD FAILED - see the output above.
%KEEP_OPEN%
exit /b 1
