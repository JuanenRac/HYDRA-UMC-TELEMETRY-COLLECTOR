// HYDRA-UMC-TELEMETRY-COLLECTOR - collector/collector_dedup_state_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package collector

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestCollector_AResendAfterARestartIsStillADuplicateWhenTheWindowIsKept(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dedup.json")
	first := New(10, &memorySink{})
	if err := first.IngestWS(wsMessageWithSeq("robot-1", 7)); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if err := first.SaveDedupState(path); err != nil {
		t.Fatal(err)
	}

	restarted := New(10, &memorySink{})
	if n, err := restarted.LoadDedupState(path); err != nil || n != 1 {
		t.Fatalf("LoadDedupState = %d, %v; want 1", n, err)
	}
	if err := restarted.IngestWS(wsMessageWithSeq("robot-1", 7)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("resend after the restart: err = %v, want ErrDuplicate", err)
	}
	if restarted.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0", restarted.BufferLen())
	}
}

func TestCollector_WithoutTheSavedWindowAResendAfterARestartIsAccepted(t *testing.T) {
	restarted := New(10, &memorySink{})
	if err := restarted.IngestWS(wsMessageWithSeq("robot-1", 7)); err != nil {
		t.Fatalf("a collector that kept nothing accepts the resend (the behaviour without the switch): %v", err)
	}
}
