#!/usr/bin/env bash
# =============================================================================
# HYDRA-UMC-TELEMETRY-COLLECTOR - run.sh
# Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
# GPL-3.0 - see LICENSE
# =============================================================================
set -uo pipefail  # no -e: we need to reach the trap below even if the binary exits non-zero
cd "$(dirname "$0")"

# Keep the window open if this was double-clicked instead of run from an
# already-open terminal - only prompts when stdin is actually a terminal
# (never in CI/piped/non-interactive runs).
trap '[ -t 0 ] && read -r -p "Press Enter to close..." _' EXIT

BIN_NAME="telemetry-collector"
if [ "${OS:-}" = "Windows_NT" ]; then
    BIN_NAME="telemetry-collector.exe"
fi

if [ ! -f "build/${BIN_NAME}" ]; then
    echo "ERROR: build/${BIN_NAME} not found. Run ./build.sh first." >&2
    exit 1
fi

"./build/${BIN_NAME}" "$@"
exit $?
