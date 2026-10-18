// HYDRA-UMC-TELEMETRY-COLLECTOR - telemetry/sample_plausibility_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package telemetry

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func validSample() Sample {
	return Sample{SourceID: "robot-1", Kind: "motor_temp", Timestamp: 1_700_000_000_000, Fields: map[string]float64{"v": 1}}
}

func TestValidate_RefusesATimestampInSecondsAndOneFromTheFuture(t *testing.T) {
	orig := nowFunc
	nowFunc = func() int64 { return 1_800_000_000_000 }
	defer func() { nowFunc = orig }()

	s := validSample()
	if err := s.Validate(); err != nil {
		t.Fatalf("a normal sample: %v", err)
	}
	s.Timestamp = 1_700_000_000 // seconds, January 1970 if read as milliseconds
	if !errors.Is(s.Validate(), ErrImplausibleTimestamp) {
		t.Fatalf("seconds: Validate() = %v, want ErrImplausibleTimestamp", s.Validate())
	}
	s.Timestamp = 1_800_000_000_000 + (6 * time.Minute).Milliseconds()
	if !errors.Is(s.Validate(), ErrImplausibleTimestamp) {
		t.Fatalf("six minutes ahead: Validate() = %v, want ErrImplausibleTimestamp", s.Validate())
	}
	s.Timestamp = 1_800_000_000_000 + (4 * time.Minute).Milliseconds()
	if err := s.Validate(); err != nil {
		t.Fatalf("four minutes ahead is ordinary clock skew: %v", err)
	}
}

func TestValidate_TheFutureCheckCanBeSwitchedOff(t *testing.T) {
	orig, origSkew := nowFunc, MaxFutureSkew
	nowFunc = func() int64 { return 1_700_000_000_000 }
	MaxFutureSkew = 0
	defer func() { nowFunc, MaxFutureSkew = orig, origSkew }()

	s := validSample()
	s.Timestamp = 1_900_000_000_000
	if err := s.Validate(); err != nil {
		t.Fatalf("with the check off a far-future timestamp is accepted: %v", err)
	}
	s.Timestamp = 5 // the lower bound stays
	if !errors.Is(s.Validate(), ErrImplausibleTimestamp) {
		t.Fatalf("Validate() = %v, want ErrImplausibleTimestamp", s.Validate())
	}
}

func TestValidate_RefusesAnOversizedOrUnprintableSourceID(t *testing.T) {
	s := validSample()
	s.SourceID = strings.Repeat("a", MaxSourceIDLength)
	if err := s.Validate(); err != nil {
		t.Fatalf("a source id at the limit: %v", err)
	}
	s.SourceID = strings.Repeat("a", MaxSourceIDLength+1)
	if !errors.Is(s.Validate(), ErrInvalidSourceID) {
		t.Fatalf("too long: Validate() = %v, want ErrInvalidSourceID", s.Validate())
	}
	for _, bad := range []string{"robot\n1", "robot\x00", "robot\x7f", "róbot"} {
		s.SourceID = bad
		if !errors.Is(s.Validate(), ErrInvalidSourceID) {
			t.Fatalf("%q: Validate() = %v, want ErrInvalidSourceID", bad, s.Validate())
		}
	}
}
