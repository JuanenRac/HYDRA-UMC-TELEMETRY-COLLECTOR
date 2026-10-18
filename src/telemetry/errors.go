// HYDRA-UMC-TELEMETRY-COLLECTOR - telemetry/errors.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package telemetry

import "errors"

var (
	ErrMissingSourceID      = errors.New("telemetry: sample has no sourceId")
	ErrMissingKind          = errors.New("telemetry: sample has no kind")
	ErrInvalidTimestamp     = errors.New("telemetry: sample has no positive timestamp")
	ErrInvalidField         = errors.New("telemetry: sample has an invalid field")
	ErrInvalidSourceID      = errors.New("telemetry: sample has an invalid sourceId (at most 128 visible ASCII characters)")
	ErrImplausibleTimestamp = errors.New("telemetry: sample timestamp is not plausible (milliseconds since the epoch, not seconds, and not in the future)")
	ErrInvalidCANFrame      = errors.New("telemetry: invalid CAN frame")
)
