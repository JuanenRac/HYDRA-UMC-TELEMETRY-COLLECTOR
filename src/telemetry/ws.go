// HYDRA-UMC-TELEMETRY-COLLECTOR - telemetry/ws.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Real parsing of the WebSocket/HTTP JSON telemetry format - the other
// heterogeneous source this collector normalizes alongside CAN (see
// can.go). Unlike a CAN frame, a WS message already carries its own
// timestamp and field map, so this parser is mostly real validation
// (see Sample.Validate) rather than a byte-level decode.
package telemetry

import "encoding/json"

// ParseWSMessage decodes one JSON telemetry message (the same shape as
// Sample's own JSON tags) and validates it. A message missing a
// required field is rejected here rather than silently buffered as a
// half-formed Sample.
func ParseWSMessage(raw []byte) (Sample, error) {
	var s Sample
	if err := json.Unmarshal(raw, &s); err != nil {
		return Sample{}, err
	}
	if err := s.Validate(); err != nil {
		return Sample{}, err
	}
	return s, nil
}
