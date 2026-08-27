#!/usr/bin/env python3
# =============================================================================
# HYDRA-UMC-TELEMETRY-COLLECTOR - bump_version.py
# Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
# GPL-3.0 - see LICENSE
#
# Applies the ecosystem-wide "odometer" version bump to this project's own
# src/version.go (`const Version = "X.Y.Z"`) before every real build:
# PATCH goes up by 1; if that would push PATCH past 9, it resets to 0 and
# MINOR goes up by 1 instead (e.g. 0.0.9 -> 0.1.0). MAJOR is never touched
# by this script - deliberate manual-only decision, same convention across
# the ecosystem (see the Python bump_version.py in sibling projects such as
# HYDRA-UMC-DATALAKE for the same pattern applied to pyproject.toml).
#
# Called from build.bat/build.sh right before `go build`, so every real
# build carries a version 1 higher than the last real build. Also runs
# standalone (`python bump_version.py`) - stdlib only, no deps.
# =============================================================================
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VERSION_FILE = ROOT / "src" / "version.go"

VERSION_RE = re.compile(r'^const Version = "(\d+)\.(\d+)\.(\d+)"\s*$', re.MULTILINE)


def bump(major: int, minor: int, patch: int) -> tuple[int, int, int]:
    """Odometer-style carry: PATCH+1, rolling over into MINOR past 9. MAJOR
    is never touched here."""
    patch += 1
    if patch > 9:
        patch = 0
        minor += 1
    return major, minor, patch


def main() -> int:
    if not VERSION_FILE.is_file():
        print(f"ERROR: {VERSION_FILE} does not exist.", file=sys.stderr)
        return 1

    text = VERSION_FILE.read_text(encoding="utf-8")
    match = VERSION_RE.search(text)
    if not match:
        print(f'ERROR: no const Version = "X.Y.Z" line found in {VERSION_FILE}', file=sys.stderr)
        return 1

    old = tuple(int(part) for part in match.groups())
    new = bump(*old)
    old_str = ".".join(str(part) for part in old)
    new_str = ".".join(str(part) for part in new)

    new_text = text[: match.start()] + f'const Version = "{new_str}"' + text[match.end():]
    VERSION_FILE.write_text(new_text, encoding="utf-8")

    print(f"Version bumped: {old_str} -> {new_str}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
