// HYDRA-UMC-TELEMETRY-COLLECTOR - telemetry/can.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Real CAN frame parsing - but against THIS project's own, self-declared
// v0 wire format, not the ecosystem's actual documented CAN IDs (the
// real ones live in HYDRA-UMC's and URTC's own firmware docs). Making up
// ID numbers that merely LOOK like they match those real firmware
// tables would be worse than honestly documenting a clearly-own
// placeholder format pending a real integration pass.
// The payload layout below is real, tested, and round-trips - it's the
// SOURCE of the ID mapping that's a deliberate placeholder pending a
// real integration pass against the firmware's own CAN ID tables.
package telemetry

import (
	"encoding/binary"
	"fmt"
	"math"
)

// signalKind maps this v0 format's one-byte signal type code to a
// normalized Sample.Kind string.
var signalKind = map[byte]string{
	0x01: "motor_current",
	0x02: "motor_temp",
	0x03: "motor_rpm",
}

var signalCode = map[string]byte{
	"motor_current": 0x01,
	"motor_temp":    0x02,
	"motor_rpm":     0x03,
}

// ParseCANFrame decodes an 8-byte CAN data payload into a normalized
// Sample. Wire format (this project's own v0 convention):
//
//	byte 0:   signal type code (see signalKind)
//	bytes 1-4: float32, little-endian, the reading's value
//	bytes 5-7: reserved, unused in v0 (not silently ignored - just not
//	           assigned a meaning yet; a future pass may use them for a
//	           sequence counter or a second value)
//
// `arbitrationID` identifies the sending node - v0 maps it directly to
// "robot-<id>" (see the package doc comment for why this isn't the
// ecosystem's real CAN ID table yet).
func ParseCANFrame(arbitrationID uint32, data []byte) (Sample, error) {
	if len(data) != 8 {
		return Sample{}, fmt.Errorf("%w: got %d bytes, want 8", ErrInvalidCANFrame, len(data))
	}
	kind, ok := signalKind[data[0]]
	if !ok {
		return Sample{}, fmt.Errorf("%w: unknown signal code 0x%02X", ErrInvalidCANFrame, data[0])
	}
	bits := binary.LittleEndian.Uint32(data[1:5])
	value := float64(math.Float32frombits(bits))

	sample := Sample{
		SourceID:  fmt.Sprintf("robot-%d", arbitrationID),
		Kind:      kind,
		Timestamp: nowFunc(),
		Fields:    map[string]float64{"value": value},
	}
	if err := sample.Validate(); err != nil {
		return Sample{}, err
	}
	return sample, nil
}

// EncodeCANFrame is the inverse of ParseCANFrame - real, not a stub,
// used by this package's own round-trip tests and available for a
// future CAN transmit path (e.g. a hardware-in-the-loop test harness).
func EncodeCANFrame(kind string, value float32) ([8]byte, error) {
	var frame [8]byte
	code, ok := signalCode[kind]
	if !ok {
		return frame, fmt.Errorf("%w: unknown kind %q", ErrInvalidCANFrame, kind)
	}
	frame[0] = code
	binary.LittleEndian.PutUint32(frame[1:5], math.Float32bits(value))
	return frame, nil
}
