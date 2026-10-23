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
//
// Inside, the samples live in one circular queue per priority level (a single
// queue when no priority is set), every sample stamped with an arrival number.
// Taking the oldest sample looks at the front of each queue, and dropping the
// least important one pops the front of the lowest queue, so a Push into a full
// buffer costs a handful of steps however many samples are queued (it used to
// scan every one of them under the lock and shift the slice).
package buffer

import (
	"errors"
	"sort"
	"sync"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

var ErrFull = errors.New("buffer: full")

type entry struct {
	seq int64 // arrival number: smaller is older; samples put back at the front get numbers below every other
	s   telemetry.Sample
}

// deque is a growable circular queue.
type deque struct {
	buf  []entry
	head int
	n    int
}

func (d *deque) grow() {
	size := len(d.buf) * 2
	if size == 0 {
		size = 8
	}
	next := make([]entry, size)
	for i := 0; i < d.n; i++ {
		next[i] = d.buf[(d.head+i)%len(d.buf)]
	}
	d.buf, d.head = next, 0
}

func (d *deque) pushBack(e entry) {
	if d.n == len(d.buf) {
		d.grow()
	}
	d.buf[(d.head+d.n)%len(d.buf)] = e
	d.n++
}

func (d *deque) pushFront(e entry) {
	if d.n == len(d.buf) {
		d.grow()
	}
	d.head = (d.head - 1 + len(d.buf)) % len(d.buf)
	d.buf[d.head] = e
	d.n++
}

func (d *deque) front() entry { return d.buf[d.head] }

func (d *deque) popFront() entry {
	e := d.buf[d.head]
	d.buf[d.head] = entry{} // let the sample go
	d.head = (d.head + 1) % len(d.buf)
	d.n--
	return e
}

// at is the i-th entry from the front.
func (d *deque) at(i int) entry { return d.buf[(d.head+i)%len(d.buf)] }

// Ring is a fixed-capacity FIFO queue of telemetry.Sample.
type Ring struct {
	mu       sync.Mutex
	queues   map[int]*deque // priority level -> its samples, oldest first
	levels   []int          // the levels that have a queue, ascending
	total    int
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
	nextSeq  int64
	frontSeq int64
}

// level is the queue a sample belongs to: its priority, or 0 for everybody when none is set.
func (r *Ring) level(s telemetry.Sample) int {
	if r.priority == nil {
		return 0
	}
	return r.priority(s)
}

func (r *Ring) queueFor(level int) *deque {
	q, ok := r.queues[level]
	if !ok {
		q = &deque{}
		r.queues[level] = q
		r.levels = append(r.levels, level)
		sort.Ints(r.levels)
	}
	return q
}

// oldest is the queue whose front sample arrived first, or nil when nothing is queued.
func (r *Ring) oldest() *deque {
	var best *deque
	for _, level := range r.levels {
		q := r.queues[level]
		if q.n == 0 {
			continue
		}
		if best == nil || q.front().seq < best.front().seq {
			best = q
		}
	}
	return best
}

// ordered is every queued sample, oldest first.
func (r *Ring) ordered() []entry {
	out := make([]entry, 0, r.total)
	for _, level := range r.levels {
		q := r.queues[level]
		for i := 0; i < q.n; i++ {
			out = append(out, q.at(i))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out
}

// SetPriority installs the drop policy: higher numbers matter more. Safe to
// call before the ring is used; a nil function restores plain FIFO backpressure.
// Samples already queued are sorted into the queues of the new policy.
func (r *Ring) SetPriority(priority func(telemetry.Sample) int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	queued := r.ordered()
	r.priority = priority
	r.queues = map[int]*deque{}
	r.levels = nil
	for _, e := range queued {
		r.queueFor(r.level(e.s)).pushBack(e)
	}
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
	return &Ring{queues: map[int]*deque{}, capacity: capacity}
}

// Push appends one sample, or returns ErrFull if the buffer is already
// at capacity.
func (r *Ring) Push(s telemetry.Sample) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	level := r.level(s)
	if r.total+r.inflight >= r.capacity {
		if r.priority == nil || r.total == 0 {
			return ErrFull
		}
		// The least important queued sample is the front of the lowest queue that has one (the oldest among equals).
		var lowest *deque
		lowestLevel := 0
		for _, l := range r.levels {
			if q := r.queues[l]; q.n > 0 {
				lowest, lowestLevel = q, l
				break
			}
		}
		if lowest == nil || level <= lowestLevel {
			return ErrFull // the newcomer is no more important than anything queued
		}
		lowest.popFront()
		r.total--
		r.evicted++
	}
	r.queueFor(level).pushBack(entry{seq: r.nextSeq, s: s})
	r.nextSeq++
	r.total++
	return nil
}

func (r *Ring) take(max int) []telemetry.Sample {
	if max > r.total {
		max = r.total
	}
	if max < 0 {
		max = 0
	}
	out := make([]telemetry.Sample, 0, max)
	for len(out) < max {
		q := r.oldest()
		if q == nil {
			break
		}
		out = append(out, q.popFront().s)
	}
	r.total -= len(out)
	return out
}

// Drain removes and returns up to `max` samples from the front of the
// buffer (oldest first). Returns an empty (non-nil) slice if the buffer
// is empty.
func (r *Ring) Drain(max int) []telemetry.Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.take(max)
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
	room := r.capacity - r.total - r.inflight
	if room <= 0 {
		return len(samples)
	}
	toKeep := samples
	if len(toKeep) > room {
		dropped = len(toKeep) - room
		toKeep = toKeep[:room]
	}
	// last first, each one in front of the others, so the first of the batch ends up the oldest
	for i := len(toKeep) - 1; i >= 0; i-- {
		r.frontSeq--
		r.queueFor(r.level(toKeep[i])).pushFront(entry{seq: r.frontSeq, s: toKeep[i]})
	}
	r.total += len(toKeep)
	return dropped
}

// DrainReserved is Drain, except the slots of the returned samples stay
// reserved until Settle is called: the buffer keeps counting them as taken, so
// a sink outage cannot make the batch lose its place to newer samples.
func (r *Ring) DrainReserved(max int) []telemetry.Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.take(max)
	r.inflight += len(out)
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
	q := r.oldest()
	if q == nil {
		return 0, false
	}
	return q.front().s.Timestamp, true
}

func (r *Ring) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}

func (r *Ring) Cap() int {
	return r.capacity
}
