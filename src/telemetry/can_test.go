// HYDRA-UMC-TELEMETRY-COLLECTOR - telemetry/can_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package telemetry

import "testing"

func TestParseCANFrame_RoundTripsWithEncodeCANFrame(t *testing.T) {
	orig := nowFunc
	nowFunc = func() int64 { return 1000 }
	defer func() { nowFunc = orig }()

	frame, err := EncodeCANFrame("motor_current", 12.5)
	if err != nil {
		t.Fatalf("EncodeCANFrame: %v", err)
	}
	sample, err := ParseCANFrame(7, frame[:])
	if err != nil {
		t.Fatalf("ParseCANFrame: %v", err)
	}
	if sample.SourceID != "robot-7" {
		t.Errorf("SourceID = %q, want %q", sample.SourceID, "robot-7")
	}
	if sample.Kind != "motor_current" {
		t.Errorf("Kind = %q, want %q", sample.Kind, "motor_current")
	}
	if sample.Timestamp != 1000 {
		t.Errorf("Timestamp = %d, want 1000", sample.Timestamp)
	}
	got := sample.Fields["value"]
	if got < 12.4999 || got > 12.5001 {
		t.Errorf("Fields[value] = %v, want ~12.5", got)
	}
}

func TestParseCANFrame_RejectsWrongLength(t *testing.T) {
	_, err := ParseCANFrame(1, []byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected an error for a 3-byte frame, got nil")
	}
}

func TestParseCANFrame_RejectsUnknownSignalCode(t *testing.T) {
	frame := [8]byte{0xFF, 0, 0, 0, 0, 0, 0, 0}
	_, err := ParseCANFrame(1, frame[:])
	if err == nil {
		t.Fatal("expected an error for an unknown signal code, got nil")
	}
}

func TestEncodeCANFrame_RejectsUnknownKind(t *testing.T) {
	_, err := EncodeCANFrame("not_a_real_kind", 1.0)
	if err == nil {
		t.Fatal("expected an error for an unknown kind, got nil")
	}
}
