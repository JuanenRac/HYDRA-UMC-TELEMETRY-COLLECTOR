#!/usr/bin/env bash
# =============================================================================
# HYDRA-UMC-TELEMETRY-COLLECTOR - build.sh
# Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
# GPL-3.0 - see LICENSE
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

# Keep the window open if this was double-clicked (e.g. from a file
# manager) instead of run from an already-open terminal - fires on
# success AND on a `set -e` early exit alike, but only prompts when
# stdin is actually a terminal (never in CI/piped/non-interactive runs).
trap '[ -t 0 ] && read -r -p "Press Enter to close..." _' EXIT

echo
echo " ==============================================================="
echo "  H Y D R A - U M C - T E L E M E T R Y - C O L L E C T O R  -  build"
echo " ==============================================================="
echo "  High-throughput ingestion node for CAN and WebSocket logs"
echo "  Author:  JuanenRac (Electro Hobby 3D)"
echo "  License: GPL-3.0 (see LICENSE.md)"
echo " ==============================================================="
echo

echo "[1/2] Bumping version number (odometer bump, see bump_version.py)..."
python3 bump_version.py || exit 1
python3 "$(dirname "$0")/bump_manifest_version.py" --sync || exit 1
echo "      Done."
echo

echo "[2/2] Compiling Go module (src/)..."
mkdir -p build
BIN_NAME="telemetry-collector"
if [ "${OS:-}" = "Windows_NT" ]; then
    BIN_NAME="telemetry-collector.exe"
fi
( cd src && go build -o "../build/${BIN_NAME}" . )
echo "      Done. Binary: build/${BIN_NAME}"
echo

# No longer a "run it once to verify" step here (there was one, up
# through the andamiaje stage): main.go now starts a real HTTP server
# that blocks until SIGINT/SIGTERM, so launching it from inside build.sh
# would hang the build forever instead of "verifying" anything. Actually
# running the binary is what run.sh is for.

echo " ==============================================================="
echo "  Build complete. Run ./run.sh to start the collector."
echo " ==============================================================="
echo
