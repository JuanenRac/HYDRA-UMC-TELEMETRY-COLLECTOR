// HYDRA-UMC-TELEMETRY-COLLECTOR - dedup/dedup_restart_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package dedup

import (
	"fmt"
	"testing"
	"time"
)

func TestTracker_AProducerThatRestartsFromOneIsNotRejectedAsDuplicates(t *testing.T) {
	tr := New(256)
	for seq := uint64(1); seq <= 1000; seq++ {
		if !tr.Allow("robot-1", seq) {
			t.Fatalf("sequence %d refused before the restart", seq)
		}
	}
	// the producer restarts: its counter begins again
	for seq := uint64(1); seq <= 50; seq++ {
		if !tr.Allow("robot-1", seq) {
			t.Fatalf("sequence %d refused after the producer restarted (the counter starts over)", seq)
		}
	}
	if tr.Resets() != 1 {
		t.Fatalf("Resets() = %d, want 1", tr.Resets())
	}
	// ...and the deduplication keeps working from the new counter
	if tr.Allow("robot-1", 50) {
		t.Fatal("a repeat of a sequence of the new run must still be a duplicate")
	}
}

func TestTracker_ARestartThatDoesNotStartNearZeroIsRecognisedAfterARun(t *testing.T) {
	tr := New(64)
	for seq := uint64(5000); seq <= 5200; seq++ {
		tr.Allow("robot-1", seq)
	}
	// a counter that restarted at 700: far behind the window, not near zero
	if tr.Allow("robot-1", 700) || tr.Allow("robot-1", 701) {
		t.Fatal("the first arrivals behind the window are indistinguishable from a replay and are refused")
	}
	if !tr.Allow("robot-1", 702) {
		t.Fatal("the third increasing arrival behind the window is a restart and must be accepted")
	}
	if !tr.Allow("robot-1", 703) {
		t.Fatal("the new run must go on being accepted")
	}
	if tr.Resets() != 1 {
		t.Fatalf("Resets() = %d, want 1", tr.Resets())
	}
}

func TestTracker_AStaleReplayIsStillRejected(t *testing.T) {
	tr := New(64)
	for seq := uint64(1000); seq <= 1200; seq++ {
		tr.Allow("robot-1", seq)
	}
	// replays of old messages, not in an increasing run, never reset anything
	for _, seq := range []uint64{900, 850, 900, 870, 900} {
		if tr.Allow("robot-1", seq) {
			t.Fatalf("a stale replay of %d must be refused", seq)
		}
	}
	if tr.Resets() != 0 {
		t.Fatalf("Resets() = %d, want 0", tr.Resets())
	}
}

func TestTracker_AReplayRunIsBrokenByANormalSample(t *testing.T) {
	tr := New(64)
	for seq := uint64(1000); seq <= 1200; seq++ {
		tr.Allow("robot-1", seq)
	}
	tr.Allow("robot-1", 900)
	tr.Allow("robot-1", 901)
	if !tr.Allow("robot-1", 1201) {
		t.Fatal("a normal sample must be accepted")
	}
	if tr.Allow("robot-1", 902) {
		t.Fatal("the run was broken by a normal sample: 902 is a stale replay, not the third of a run")
	}
}

func TestTracker_RememberedProducersAreBounded(t *testing.T) {
	tr := New(64)
	tr.SetMaxSources(100)
	for i := 0; i < 1000; i++ {
		tr.Allow(fmt.Sprintf("source-%d", i), 1)
	}
	if got := tr.Sources(); got != 100 {
		t.Fatalf("Sources() = %d, want 100 (the limit)", got)
	}
	if tr.Evicted() != 900 {
		t.Fatalf("Evicted() = %d, want 900", tr.Evicted())
	}
}

func TestTracker_TheProducerSilentTheLongestIsTheOneForgotten(t *testing.T) {
	tr := New(64)
	tr.SetMaxSources(2)
	clock := time.Unix(1000, 0)
	tr.now = func() time.Time { return clock }
	tr.Allow("old", 1)
	clock = clock.Add(time.Second)
	tr.Allow("recent", 1)
	clock = clock.Add(time.Second)
	tr.Allow("newcomer", 1)
	if tr.Allow("recent", 1) {
		t.Fatal("the recent producer must still be remembered: its sequence 1 is a duplicate")
	}
	if !tr.Allow("old", 1) {
		t.Fatal("the producer silent the longest must have been forgotten")
	}
}

func TestTracker_ARefusedSampleOfAnUnknownProducerCreatesNothing(t *testing.T) {
	tr := New(64)
	_, err := tr.AllowThen("robot-1", 7, func() error { return fmt.Errorf("buffer full") })
	if err == nil {
		t.Fatal("the accept error must be returned")
	}
	if tr.Sources() != 0 {
		t.Fatalf("Sources() = %d, want 0: nothing was accepted, so nothing is remembered", tr.Sources())
	}
	allowed, err := tr.AllowThen("robot-1", 7, func() error { return nil })
	if !allowed || err != nil {
		t.Fatalf("the retry of the same sample must be accepted, got allowed=%v err=%v", allowed, err)
	}
}

func TestTracker_ARestartIsNotCommittedWhenTheAcceptFails(t *testing.T) {
	tr := New(64)
	for seq := uint64(1); seq <= 500; seq++ {
		tr.Allow("robot-1", seq)
	}
	_, err := tr.AllowThen("robot-1", 1, func() error { return fmt.Errorf("buffer full") })
	if err == nil {
		t.Fatal("the accept error must be returned")
	}
	if tr.Resets() != 0 {
		t.Fatalf("Resets() = %d, want 0 until the sample was really accepted", tr.Resets())
	}
	allowed, err := tr.AllowThen("robot-1", 1, func() error { return nil })
	if !allowed || err != nil || tr.Resets() != 1 {
		t.Fatalf("the retry must be accepted as the restart: allowed=%v err=%v resets=%d", allowed, err, tr.Resets())
	}
}
