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
	// priority, when set, lets a full buffer make room for a more important
	// sample by dropping the least important one already queued (oldest
	// first among equals). Nil keeps the plain behaviour: a full buffer
	// rejects the newcomer with ErrFull.
	priority func(telemetry.Sample) int
	evicted  int
	// inflight counts samples handed out by DrainReserved whose delivery is
	// not settled yet. Their slots stay reserved, so what is ingested while a
	// batch is being written can never take the room that batch needs to come
	// back to if the write fails.
	inflight int
}

// SetPriority installs the drop policy: higher numbers matter more. Safe to
// call before the ring is used; a nil function restores plain FIFO backpressure.
func (r *Ring) SetPriority(priority func(telemetry.Sample) int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.priority = priority
}

// Evicted is how many queued samples were dropped to make room for a more
// important one since this ring was created.
func (r *Ring) Evicted() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.evicted
}

// PriorityByKind builds a priority function from a kind -> priority table;
// kinds not in the table get `otherwise`.
func PriorityByKind(table map[string]int, otherwise int) func(telemetry.Sample) int {
	return func(s telemetry.Sample) int {
		if p, ok := table[s.Kind]; ok {
			return p
		}
		return otherwise
	}
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
	if len(r.items)+r.inflight >= r.capacity {
		if r.priority == nil || len(r.items) == 0 {
			return ErrFull
		}
		// Find the least important queued sample (the oldest among equals).
		lowest := 0
		for i := 1; i < len(r.items); i++ {
			if r.priority(r.items[i]) < r.priority(r.items[lowest]) {
				lowest = i
			}
		}
		if r.priority(s) <= r.priority(r.items[lowest]) {
			return ErrFull // the newcomer is no more important than anything queued
		}
		r.items = append(r.items[:lowest], r.items[lowest+1:]...)
		r.evicted++
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
// outage doesn't lose data. If there isn't enough room for all of them,
// it keeps the OLDEST ones that fit and reports how many (the newest of the
// requeued samples) had to be dropped. The collector does not rely on this:
// it takes its batches with DrainReserved and gives them back with Settle,
// so a failed write always finds the room it left.
func (r *Ring) Requeue(samples []telemetry.Sample) (dropped int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.requeueLocked(samples)
}

func (r *Ring) requeueLocked(samples []telemetry.Sample) (dropped int) {
	room := r.capacity - len(r.items) - r.inflight
	if room <= 0 {
		return len(samples)
	}
	toKeep := samples
	if len(toKeep) > room {
		dropped = len(toKeep) - room
		toKeep = toKeep[:room]
	}
	r.items = append(append(make([]telemetry.Sample, 0, len(toKeep)+len(r.items)), toKeep...), r.items...)
	return dropped
}

// DrainReserved is Drain, except the slots of the returned samples stay
// reserved until Settle is called: the buffer keeps counting them as taken, so
// a sink outage cannot make the batch lose its place to newer samples.
func (r *Ring) DrainReserved(max int) []telemetry.Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	if max > len(r.items) {
		max = len(r.items)
	}
	out := make([]telemetry.Sample, max)
	copy(out, r.items[:max])
	r.items = r.items[max:]
	r.inflight += max
	return out
}

// Settle ends the reservation of a batch taken with DrainReserved: `drained`
// is the size of that batch, `requeue` the part of it that has to be tried again
// (it goes back to the front, in order, into the room the reservation kept free;
// the rest was delivered or permanently discarded). Returns how many of `requeue`
// did not fit, which cannot happen unless the reservation was not honoured.
func (r *Ring) Settle(drained int, requeue []telemetry.Sample) (dropped int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inflight -= drained
	if r.inflight < 0 {
		r.inflight = 0
	}
	return r.requeueLocked(requeue)
}

// InFlight is how many samples are reserved by a batch being delivered.
func (r *Ring) InFlight() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inflight
}

// OldestTimestamp is the timestamp (unix milliseconds) of the sample at the front of the
// queue, the oldest one waiting to be delivered; false when nothing is queued.
func (r *Ring) OldestTimestamp() (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.items) == 0 {
		return 0, false
	}
	return r.items[0].Timestamp, true
}

func (r *Ring) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

func (r *Ring) Cap() int {
	return r.capacity
}
