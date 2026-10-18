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

import (
	"math"
	"time"
)

// MinPlausibleTimestampMs is the earliest timestamp accepted (in milliseconds
// since the epoch, early 1973). A value below it is a producer that sent
// seconds instead of milliseconds: 1700000000 would be January 1970.
const MinPlausibleTimestampMs int64 = 100_000_000_000

// MaxFutureSkew is how far ahead of this machine's clock a timestamp may be (a
// little skew between the producer and this machine is normal). Zero turns the
// check off.
var MaxFutureSkew = 5 * time.Minute

// MaxSourceIDLength bounds a sourceId: it is free text sent by the client and
// the key of every table downstream.
const MaxSourceIDLength = 128

// Sample is one normalized telemetry reading.
type Sample struct {
	SourceID  string             `json:"sourceId"`  // e.g. "robot-1", "vision-node-1"
	Kind      string             `json:"kind"`      // e.g. "motor_current", "motor_temp"
	Timestamp int64              `json:"timestamp"` // unix milliseconds
	Fields    map[string]float64 `json:"fields"`    // open key/value, same convention as hydra.common.v1's HealthReport.metrics
	// Sequence is an optional, per-producer monotonic counter a real
	// device can attach so a reconnect that resends its last few
	// unacked messages doesn't silently inflate ingest counts (see
	// dedup.Tracker). Zero means "not provided" - a producer that
	// doesn't send Sequence gets today's un-deduplicated behavior,
	// unchanged, rather than being forced to opt in.
	Sequence uint64 `json:"sequence,omitempty"`
}

// Validate reports the first reason a Sample is not fit to buffer - kept
// as one real, explicit rule set rather than trusting every parser to
// enforce it individually (a bug in one parser would silently produce
// bad samples otherwise).
func (s Sample) Validate() error {
	if s.SourceID == "" {
		return ErrMissingSourceID
	}
	if len(s.SourceID) > MaxSourceIDLength || !printable(s.SourceID) {
		return ErrInvalidSourceID
	}
	if s.Kind == "" {
		return ErrMissingKind
	}
	if s.Timestamp <= 0 {
		return ErrInvalidTimestamp
	}
	if s.Timestamp < MinPlausibleTimestampMs {
		return ErrImplausibleTimestamp
	}
	if MaxFutureSkew > 0 && s.Timestamp > nowFunc()+MaxFutureSkew.Milliseconds() {
		return ErrImplausibleTimestamp
	}
	for key, value := range s.Fields {
		if key == "" || math.IsNaN(value) || math.IsInf(value, 0) {
			return ErrInvalidField
		}
	}
	return nil
}

// printable reports whether s has only visible ASCII characters and spaces.
func printable(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}
