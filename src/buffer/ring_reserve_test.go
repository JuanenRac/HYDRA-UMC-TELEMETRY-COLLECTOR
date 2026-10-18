// HYDRA-UMC-TELEMETRY-COLLECTOR - buffer/ring_reserve_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package buffer

import (
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

func TestRing_ADrainedBatchKeepsItsSlotsUntilItIsSettled(t *testing.T) {
	r := New(3)
	for _, id := range []string{"a", "b", "c"} {
		if err := r.Push(sample(id)); err != nil {
			t.Fatal(err)
		}
	}
	batch := r.DrainReserved(3)
	if len(batch) != 3 || r.InFlight() != 3 {
		t.Fatalf("batch=%d inflight=%d, want 3 and 3", len(batch), r.InFlight())
	}
	// the sink is down: what arrives meanwhile finds no room, it does not take the batch's place
	if err := r.Push(sample("d")); err != ErrFull {
		t.Fatalf("Push while the batch is in flight = %v, want ErrFull", err)
	}
	if dropped := r.Settle(3, batch); dropped != 0 {
		t.Fatalf("Settle dropped %d, want 0: the batch's own room was reserved", dropped)
	}
	if r.InFlight() != 0 || r.Len() != 3 {
		t.Fatalf("inflight=%d len=%d, want 0 and 3", r.InFlight(), r.Len())
	}
	got := r.Drain(10)
	if got[0].SourceID != "a" || got[1].SourceID != "b" || got[2].SourceID != "c" {
		t.Fatalf("order after settle = %v %v %v, want a b c", got[0].SourceID, got[1].SourceID, got[2].SourceID)
	}
}

func TestRing_ASettledBatchFreesItsSlots(t *testing.T) {
	r := New(3)
	for _, id := range []string{"a", "b", "c"} {
		_ = r.Push(sample(id))
	}
	batch := r.DrainReserved(3)
	r.Settle(len(batch), nil)
	if r.InFlight() != 0 {
		t.Fatalf("inflight = %d, want 0", r.InFlight())
	}
	if err := r.Push(sample("d")); err != nil {
		t.Fatalf("Push after the batch was delivered: %v", err)
	}
}

func TestRing_AnOutageKeepsTheOldestSamplesAndRefusesTheNewest(t *testing.T) {
	r := New(5)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		_ = r.Push(sample(id))
	}
	for round := 0; round < 4; round++ {
		batch := r.DrainReserved(3)
		// new samples try to come in while the batch is being written, and find the buffer full
		for _, id := range []string{"x", "y", "z"} {
			if err := r.Push(sample(id)); err != ErrFull {
				t.Fatalf("round %d: Push(%s) = %v, want ErrFull", round, id, err)
			}
		}
		if dropped := r.Settle(len(batch), batch); dropped != 0 {
			t.Fatalf("round %d: %d samples lost on requeue, want 0", round, dropped)
		}
	}
	got := r.Drain(10)
	order := ""
	for _, s := range got {
		order += s.SourceID
	}
	if order != "abcde" {
		t.Fatalf("after four failed rounds the buffer holds %q, want \"abcde\" (the oldest, in order)", order)
	}
}

func TestRing_APriorityRingWithAllItsSlotsReservedRefusesInsteadOfPanicking(t *testing.T) {
	r := New(2)
	r.SetPriority(func(s telemetry.Sample) int { return 1 })
	_ = r.Push(sample("a"))
	_ = r.Push(sample("b"))
	_ = r.DrainReserved(2)
	if err := r.Push(sample("c")); err != ErrFull {
		t.Fatalf("Push = %v, want ErrFull", err)
	}
}
