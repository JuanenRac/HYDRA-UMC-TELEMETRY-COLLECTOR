// HYDRA-UMC-TELEMETRY-COLLECTOR - buffer/ring.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// A real bounded, concurrency-safe FIFO buffer - the "Buffered Delivery:
// Ensures zero data loss during temporary database outages" the README
// promises. Deliberately rejects a Push when full (ErrFull) instead of
// silently overwriting the oldest sample - "zero data loss" and "drop
// the oldest reading when busy" are contradictory promises, so this
// buffer keeps the honest one: a full buffer is real backpressure the
// caller must react to (retry, shed load upstream, or - in
// collector.go's case - simply not remove a drained batch from
// accounting until the sink confirms it was written).
package buffer

import (
	"errors"
	"sync"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

var ErrFull = errors.New("buffer: full")

// Ring is a fixed-capacity FIFO queue of telemetry.Sample.
type Ring struct {
	mu       sync.Mutex
	items    []telemetry.Sample
	capacity int
}

// New returns an empty Ring that holds at most `capacity` samples.
func New(capacity int) *Ring {
	if capacity <= 0 {
		capacity = 1
	}
	return &Ring{
		items:    make([]telemetry.Sample, 0, capacity),
		capacity: capacity,
	}
}

// Push appends one sample, or returns ErrFull if the buffer is already
// at capacity.
func (r *Ring) Push(s telemetry.Sample) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.items) >= r.capacity {
		return ErrFull
	}
	r.items = append(r.items, s)
	return nil
}

// Drain removes and returns up to `max` samples from the front of the
// buffer (oldest first). Returns an empty (non-nil) slice if the buffer
// is empty.
func (r *Ring) Drain(max int) []telemetry.Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	if max > len(r.items) {
		max = len(r.items)
	}
	out := make([]telemetry.Sample, max)
	copy(out, r.items[:max])
	r.items = r.items[max:]
	return out
}

// Requeue puts samples back at the FRONT of the buffer - used when a
// sink write fails after Drain already removed them, so a temporary
// outage doesn't lose data (see collector.go). If there isn't enough
// room for all of them, it keeps as many as fit (oldest-first priority)
// and reports how many had to be dropped - a real, honest limit: this
// buffer is bounded, not infinite, and an outage that outlasts its
// capacity WILL lose the oldest excess, which is a capacity-sizing
// decision for whoever deploys this, not something this package can
// solve by pretending to have unlimited memory.
func (r *Ring) Requeue(samples []telemetry.Sample) (dropped int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	room := r.capacity - len(r.items)
	if room <= 0 {
		return len(samples)
	}
	toKeep := samples
	if len(toKeep) > room {
		dropped = len(toKeep) - room
		toKeep = toKeep[:room]
	}
	r.items = append(toKeep, r.items...)
	return dropped
}

func (r *Ring) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

func (r *Ring) Cap() int {
	return r.capacity
}
