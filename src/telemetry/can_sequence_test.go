// HYDRA-UMC-TELEMETRY-COLLECTOR - telemetry/can_sequence_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package telemetry

import "testing"

func TestParseCANFrame_BurstInOneMillisecondGetsDistinctGrowingSequences(t *testing.T) {
	orig := nowFunc
	nowFunc = func() int64 { return 1_700_000_000_000 } // every frame of the burst arrives in the same millisecond
	defer func() { nowFunc = orig }()

	frame, err := EncodeCANFrame("motor_current", 3.5)
	if err != nil {
		t.Fatalf("EncodeCANFrame: %v", err)
	}
	var last uint64
	seen := map[[3]interface{}]bool{}
	for i := 0; i < 50; i++ {
		s, err := ParseCANFrame(7, frame[:])
		if err != nil {
			t.Fatalf("ParseCANFrame: %v", err)
		}
		if s.Sequence == 0 || s.Sequence <= last {
			t.Fatalf("frame %d: sequence %d does not grow past %d", i, s.Sequence, last)
		}
		last = s.Sequence
		identity := [3]interface{}{s.SourceID + "/" + s.Kind, s.Timestamp, s.Sequence}
		if seen[identity] {
			t.Fatalf("frame %d repeats an identity", i)
		}
		seen[identity] = true
	}
}

func TestParseCANFrame_SequencingCanBeSwitchedOff(t *testing.T) {
	orig := CANSequencing
	CANSequencing = false
	defer func() { CANSequencing = orig }()

	frame, err := EncodeCANFrame("motor_temp", 40)
	if err != nil {
		t.Fatalf("EncodeCANFrame: %v", err)
	}
	s, err := ParseCANFrame(2, frame[:])
	if err != nil {
		t.Fatalf("ParseCANFrame: %v", err)
	}
	if s.Sequence != 0 {
		t.Errorf("Sequence = %d, want 0 with sequencing off", s.Sequence)
	}
}

func TestParseCANFrame_SequencingIsOnByDefault(t *testing.T) {
	if !CANSequencing {
		t.Fatal("CANSequencing must default to true")
	}
}
