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

func kindSample(kind string, ts int64) telemetry.Sample {
	return telemetry.Sample{SourceID: "s", Kind: kind, Timestamp: ts, Fields: map[string]float64{"v": 1}}
}

func TestPriority_AFullRingDropsTheLeastImportantOldestSampleForAMoreImportantOne(t *testing.T) {
	r := New(3)
	r.SetPriority(PriorityByKind(map[string]int{"safety": 10, "motor_temp": 5}, 1))
	for i, kind := range []string{"debug", "motor_temp", "debug"} {
		if err := r.Push(kindSample(kind, int64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Push(kindSample("safety", 4)); err != nil {
		t.Fatalf("a more important sample must make room: %v", err)
	}
	if r.Evicted() != 1 {
		t.Fatalf("evicted = %d, want 1", r.Evicted())
	}
	got := r.Drain(10)
	if len(got) != 3 || got[0].Timestamp != 2 || got[1].Timestamp != 3 || got[2].Kind != "safety" {
		t.Fatalf("unexpected contents (the oldest debug sample should have gone): %+v", got)
	}
}

func TestPriority_ASampleNoMoreImportantThanAnythingQueuedIsRejected(t *testing.T) {
	r := New(2)
	r.SetPriority(PriorityByKind(map[string]int{"safety": 10}, 1))
	_ = r.Push(kindSample("safety", 1))
	_ = r.Push(kindSample("safety", 2))
	if err := r.Push(kindSample("debug", 3)); err != ErrFull {
		t.Fatalf("expected ErrFull, got %v", err)
	}
	if err := r.Push(kindSample("safety", 4)); err != ErrFull {
		t.Fatalf("an equal-priority sample must not evict: %v", err)
	}
	if r.Evicted() != 0 {
		t.Fatalf("nothing should have been evicted, got %d", r.Evicted())
	}
}

func TestPriority_WithoutAPolicyAFullRingStillRejectsWithErrFull(t *testing.T) {
	r := New(1)
	_ = r.Push(kindSample("debug", 1))
	if err := r.Push(kindSample("safety", 2)); err != ErrFull {
		t.Fatalf("expected ErrFull, got %v", err)
	}
}
