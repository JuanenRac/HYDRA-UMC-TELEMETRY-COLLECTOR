// HYDRA-UMC-TELEMETRY-COLLECTOR - buffer/ring_model_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The ring is checked against the plain slice implementation it replaced (kept here as a
// reference model): any sequence of operations must give the same answers, and a Push into a
// full buffer must cost a few steps, not a scan of everything queued.
package buffer

import (
	"math/rand"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

// refRing is the previous implementation: one slice, a linear scan to find what to drop.
type refRing struct {
	items    []telemetry.Sample
	capacity int
	priority func(telemetry.Sample) int
	evicted  int
	inflight int
}

func (r *refRing) push(s telemetry.Sample) error {
	if len(r.items)+r.inflight >= r.capacity {
		if r.priority == nil || len(r.items) == 0 {
			return ErrFull
		}
		lowest := 0
		for i := 1; i < len(r.items); i++ {
			if r.priority(r.items[i]) < r.priority(r.items[lowest]) {
				lowest = i
			}
		}
		if r.priority(s) <= r.priority(r.items[lowest]) {
			return ErrFull
		}
		r.items = append(r.items[:lowest], r.items[lowest+1:]...)
		r.evicted++
	}
	r.items = append(r.items, s)
	return nil
}

func (r *refRing) drain(max int, reserve bool) []telemetry.Sample {
	if max > len(r.items) {
		max = len(r.items)
	}
	out := make([]telemetry.Sample, max)
	copy(out, r.items[:max])
	r.items = r.items[max:]
	if reserve {
		r.inflight += max
	}
	return out
}

func (r *refRing) requeue(samples []telemetry.Sample) (dropped int) {
	room := r.capacity - len(r.items) - r.inflight
	if room <= 0 {
		return len(samples)
	}
	keep := samples
	if len(keep) > room {
		dropped = len(keep) - room
		keep = keep[:room]
	}
	r.items = append(append(make([]telemetry.Sample, 0, len(keep)+len(r.items)), keep...), r.items...)
	return dropped
}

func (r *refRing) settle(drained int, requeue []telemetry.Sample) int {
	r.inflight -= drained
	if r.inflight < 0 {
		r.inflight = 0
	}
	return r.requeue(requeue)
}

func sampleN(n int, kind string) telemetry.Sample {
	return telemetry.Sample{Timestamp: int64(n), Kind: kind}
}

func sameSamples(t *testing.T, step int, got, want []telemetry.Sample) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("step %d: %d samples, want %d", step, len(got), len(want))
	}
	for i := range got {
		if got[i].Timestamp != want[i].Timestamp || got[i].Kind != want[i].Kind {
			t.Fatalf("step %d: sample %d is %v, want %v", step, i, got[i], want[i])
		}
	}
}

func TestRingMatchesThePlainSliceItReplaced(t *testing.T) {
	kinds := []string{"alarm", "state", "metric", "debug"}
	table := map[string]int{"alarm": 3, "state": 2, "metric": 1}
	for _, withPriority := range []bool{false, true} {
		for seed := int64(1); seed <= 40; seed++ {
			rng := rand.New(rand.NewSource(seed))
			capacity := 1 + rng.Intn(12)
			ring := New(capacity)
			ref := &refRing{capacity: capacity}
			if withPriority {
				p := PriorityByKind(table, 0)
				ring.SetPriority(p)
				ref.priority = p
			}
			var held [][]telemetry.Sample // batches taken with DrainReserved and not settled yet
			next := 0
			for step := 0; step < 400; step++ {
				switch op := rng.Intn(7); op {
				case 0, 1, 2:
					s := sampleN(next, kinds[rng.Intn(len(kinds))])
					next++
					if a, b := ring.Push(s), ref.push(s); a != b {
						t.Fatalf("seed %d step %d: Push gave %v, model %v", seed, step, a, b)
					}
				case 3:
					n := rng.Intn(5)
					sameSamples(t, step, ring.Drain(n), ref.drain(n, false))
				case 4:
					n := rng.Intn(5)
					a, b := ring.DrainReserved(n), ref.drain(n, true)
					sameSamples(t, step, a, b)
					held = append(held, a)
				case 5:
					if len(held) == 0 {
						continue
					}
					i := rng.Intn(len(held))
					batch := held[i]
					held = append(held[:i], held[i+1:]...)
					back := batch[:rng.Intn(len(batch)+1)]
					if a, b := ring.Settle(len(batch), back), ref.settle(len(batch), back); a != b {
						t.Fatalf("seed %d step %d: Settle dropped %d, model %d", seed, step, a, b)
					}
				case 6:
					batch := ring.Drain(rng.Intn(4))
					refBatch := ref.drain(len(batch), false)
					sameSamples(t, step, batch, refBatch)
					if a, b := ring.Requeue(batch), ref.requeue(refBatch); a != b {
						t.Fatalf("seed %d step %d: Requeue dropped %d, model %d", seed, step, a, b)
					}
				}
				if ring.Len() != len(ref.items) || ring.InFlight() != ref.inflight || ring.Evicted() != ref.evicted {
					t.Fatalf("seed %d step %d: len/inflight/evicted %d/%d/%d, model %d/%d/%d", seed, step,
						ring.Len(), ring.InFlight(), ring.Evicted(), len(ref.items), ref.inflight, ref.evicted)
				}
				ts, ok := ring.OldestTimestamp()
				if ok != (len(ref.items) > 0) || (ok && ts != ref.items[0].Timestamp) {
					t.Fatalf("seed %d step %d: oldest %d,%v; model %v", seed, step, ts, ok, ref.items)
				}
			}
			// whatever is left comes out in the same order
			sameSamples(t, -1, ring.Drain(1000), ref.drain(1000, false))
		}
	}
}

func TestSetPriorityRegroupsWhatIsAlreadyQueued(t *testing.T) {
	ring := New(4)
	for i, kind := range []string{"metric", "alarm", "metric", "alarm"} {
		if err := ring.Push(sampleN(i, kind)); err != nil {
			t.Fatal(err)
		}
	}
	ring.SetPriority(PriorityByKind(map[string]int{"alarm": 3, "metric": 1}, 0))
	if err := ring.Push(sampleN(9, "alarm")); err != nil {
		t.Fatalf("a more important sample should evict the oldest metric: %v", err)
	}
	got := ring.Drain(10)
	want := []int64{1, 2, 3, 9} // metric 0 was the one dropped; the order of arrival is kept
	for i := range want {
		if got[i].Timestamp != want[i] {
			t.Fatalf("drained %v, want timestamps %v", got, want)
		}
	}
}

func TestAPushIntoAFullBufferDoesNotScanWhatIsQueued(t *testing.T) {
	const capacity = 5000
	calls := 0
	ring := New(capacity)
	ring.SetPriority(func(s telemetry.Sample) int {
		calls++
		if s.Kind == "alarm" {
			return 2
		}
		return 1
	})
	for i := 0; i < capacity; i++ {
		if err := ring.Push(sampleN(i, "metric")); err != nil {
			t.Fatal(err)
		}
	}
	calls = 0
	for i := 0; i < capacity; i++ {
		if err := ring.Push(sampleN(capacity+i, "alarm")); err != nil {
			t.Fatal(err)
		}
	}
	if calls > 2*capacity {
		t.Fatalf("%d priority calls for %d pushes into a full buffer: it scans what is queued", calls, capacity)
	}
	if ring.Evicted() != capacity || ring.Len() != capacity {
		t.Fatalf("evicted %d, len %d", ring.Evicted(), ring.Len())
	}
}
