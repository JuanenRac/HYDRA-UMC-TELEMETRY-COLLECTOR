// HYDRA-UMC-TELEMETRY-COLLECTOR - telemetry/clock.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// nowFunc is a package-level indirection over time.Now(), used only by
// parsers that need to stamp an arrival time themselves (CAN frames
// carry no timestamp of their own on the wire - WebSocket messages
// carry their own, see ws.go). A real, small seam - tests overwrite it
// to get deterministic timestamps instead of asserting against
// wall-clock time.
package telemetry

import "time"

var nowFunc = func() int64 {
	return time.Now().UnixMilli()
}
