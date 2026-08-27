// HYDRA-UMC-TELEMETRY-COLLECTOR - telemetry/sample.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Sample is the normalized shape every heterogeneous input format
// (CAN frames, WebSocket JSON, later gRPC) gets parsed into - the real
// "Data Normalization" the README describes: a motor current spike on a
// CAN bus and an AI inference result from a Vision Node end up as the
// same struct shape, distinguished by Kind/SourceID, so downstream code
// (the buffer, the sink) never needs to know which protocol a sample
// originally arrived over.
package telemetry

// Sample is one normalized telemetry reading.
type Sample struct {
	SourceID  string             `json:"sourceId"`  // e.g. "robot-1", "vision-node-1"
	Kind      string             `json:"kind"`      // e.g. "motor_current", "motor_temp"
	Timestamp int64              `json:"timestamp"` // unix milliseconds
	Fields    map[string]float64 `json:"fields"`     // open key/value, same convention as hydra.common.v1's HealthReport.metrics
}

// Validate reports the first reason a Sample is not fit to buffer - kept
// as one real, explicit rule set rather than trusting every parser to
// enforce it individually (a bug in one parser would silently produce
// bad samples otherwise).
func (s Sample) Validate() error {
	if s.SourceID == "" {
		return ErrMissingSourceID
	}
	if s.Kind == "" {
		return ErrMissingKind
	}
	if s.Timestamp <= 0 {
		return ErrInvalidTimestamp
	}
	return nil
}
