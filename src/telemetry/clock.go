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

import (
	"sync/atomic"
	"time"
)

// CANSequencing makes ParseCANFrame give every frame it decodes a sequence number (see NextCANSequence). On by default.
var CANSequencing = true

var canSequence atomic.Uint64

// NextCANSequence is the next number of one process-wide counter that only grows (1, 2, 3, ...). A CAN frame carries no timestamp of its
// own, so it is stamped with its arrival in milliseconds, and two frames of the same robot and signal that arrive in the same
// millisecond (the bus carries several per millisecond) would have the same identity downstream. The sequence tells them apart.
func NextCANSequence() uint64 { return canSequence.Add(1) }

var nowFunc = func() int64 {
	return time.Now().UnixMilli()
}
