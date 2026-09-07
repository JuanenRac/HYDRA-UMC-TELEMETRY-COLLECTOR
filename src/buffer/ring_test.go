// HYDRA-UMC-TELEMETRY-COLLECTOR - buffer/ring_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package buffer

import (
	"sync"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

func sample(id string) telemetry.Sample {
	return telemetry.Sample{SourceID: id, Kind: "motor_temp", Timestamp: 1, Fields: map[string]float64{"value": 1}}
}

func TestRing_PushAndDrainPreservesOrder(t *testing.T) {
	r := New(10)
	for _, id := range []string{"a", "b", "c"} {
		if err := r.Push(sample(id)); err != nil {
			t.Fatalf("Push(%s): %v", id, err)
		}
	}
	got := r.Drain(10)
	if len(got) != 3 {
		t.Fatalf("Drain returned %d samples, want 3", len(got))
	}
	for i, want := range []string{"a", "b", "c"} {
		if got[i].SourceID != want {
			t.Errorf("got[%d].SourceID = %q, want %q", i, got[i].SourceID, want)
		}
	}
}

func TestRing_PushRejectsWhenFull(t *testing.T) {
	r := New(2)
	if err := r.Push(sample("a")); err != nil {
		t.Fatalf("Push 1: %v", err)
	}
	if err := r.Push(sample("b")); err != nil {
		t.Fatalf("Push 2: %v", err)
	}
	if err := r.Push(sample("c")); err != ErrFull {
		t.Fatalf("Push 3 err = %v, want ErrFull", err)
	}
	if r.Len() != 2 {
		t.Fatalf("Len() = %d, want 2 (rejected push must not grow the buffer)", r.Len())
	}
}

func TestRing_DrainPartial(t *testing.T) {
	r := New(10)
	for _, id := range []string{"a", "b", "c"} {
		_ = r.Push(sample(id))
	}
	got := r.Drain(2)
	if len(got) != 2 {
		t.Fatalf("Drain(2) returned %d, want 2", len(got))
	}
	if r.Len() != 1 {
		t.Fatalf("Len() after partial drain = %d, want 1", r.Len())
	}
}

func TestRing_RequeuePutsSamplesBackAtTheFront(t *testing.T) {
	r := New(10)
	_ = r.Push(sample("c"))
	dropped := r.Requeue([]telemetry.Sample{sample("a"), sample("b")})
	if dropped != 0 {
		t.Fatalf("dropped = %d, want 0 (plenty of room)", dropped)
	}
	got := r.Drain(10)
	order := []string{got[0].SourceID, got[1].SourceID, got[2].SourceID}
	want := []string{"a", "b", "c"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v (requeued samples must come back before what was already there)", order, want)
		}
	}
}

func TestRing_RequeueDropsExcessWhenBufferIsAlmostFull(t *testing.T) {
	r := New(2)
	_ = r.Push(sample("kept-already"))
	dropped := r.Requeue([]telemetry.Sample{sample("a"), sample("b"), sample("c")})
	if dropped != 2 {
		t.Fatalf("dropped = %d, want 2 (only 1 slot of room existed)", dropped)
	}
	if r.Len() != 2 {
		t.Fatalf("Len() = %d, want 2 (full capacity)", r.Len())
	}
}

func TestRing_ConcurrentPushIsSafe(t *testing.T) {
	r := New(1000)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = r.Push(sample("x"))
		}(i)
	}
	wg.Wait()
	if r.Len() != 100 {
		t.Fatalf("Len() = %d, want 100 after 100 concurrent pushes", r.Len())
	}
}
