// HYDRA-UMC-TELEMETRY-COLLECTOR - dedup/dedup.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Real per-producer sequence deduplication with a bounded reorder
// window - the mechanism behind "a device that reconnects and resends
// its last few unacked messages doesn't inflate ingest counts or
// re-buffer the same sample twice".
package dedup

import (
	"sync"
	"time"
)

const defaultWindow = 64

// DefaultMaxSources bounds how many producers are remembered at once; past it
// the one that has been silent the longest is forgotten. A sourceId is free
// text sent by the client, so without a bound a flood of made-up ids would
// grow the memory of the process without limit.
const DefaultMaxSources = 10000

// restartRun is how many consecutive, increasing sequence numbers that all
// fall behind the window it takes to call it a producer restart (when the
// restarted counter did not start near zero).
const restartRun = 3

// Tracker deduplicates (sourceID, sequence) pairs. A sequence within the
// window of the highest one already seen for that source is allowed
// exactly once - a real, legitimately reordered arrival (seq 3 before
// seq 2) is accepted, but the same seq arriving twice, or one older than
// the window (a stale replay of an already-acked message), is not.
//
// A producer that restarts begins its counter again, far behind everything
// it sent before. That is told apart from a stale replay: a sequence number
// that falls behind the window and is no larger than the window itself is a
// counter that started over (a replay is of the last few messages, never of
// the first ones of a long run), and so is a run of increasing numbers that
// all fall behind the window. Either resets that producer's state, so its new
// samples are not rejected as duplicates until the counter catches up with
// where it was.
type Tracker struct {
	mu         sync.Mutex
	window     uint64
	maxSources int
	now        func() time.Time
	sources    map[string]*sourceState
	resets     int64
	evicted    int64
}

type sourceState struct {
	maxSeen  uint64
	seen     map[uint64]struct{}
	lastUsed time.Time
	// the run of increasing sequences, all behind the window, that may be a restart
	behindRun   int
	behindLast  uint64
	behindFirst uint64
}

// New returns a Tracker that remembers, per source, sequence numbers
// within `window` of the highest one accepted so far. window <= 0 uses a
// real, empirically reasonable default (64) rather than an unbounded or
// zero-size window.
func New(window int) *Tracker {
	if window <= 0 {
		window = defaultWindow
	}
	return &Tracker{window: uint64(window), maxSources: DefaultMaxSources, now: time.Now, sources: make(map[string]*sourceState)}
}

// SetMaxSources changes how many producers are remembered (at least 1).
func (t *Tracker) SetMaxSources(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if n < 1 {
		n = 1
	}
	t.maxSources = n
}

// Resets is how many times a producer's counter was found to have started
// over (a restart) since the tracker was created.
func (t *Tracker) Resets() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.resets
}

// Sources is how many producers are currently remembered.
func (t *Tracker) Sources() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.sources)
}

// Evicted is how many producers were forgotten because the tracker was full.
func (t *Tracker) Evicted() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.evicted
}

type verdict int

const (
	verdictNew verdict = iota
	verdictDuplicate
	verdictRestart
)

// checkLocked reports whether `sequence` is real, new data for
// `sourceID` - the SAME rule Allow/AllowThen both enforce, factored out
// so the two never drift apart. Caller must already hold t.mu. Never
// mutates state (a source that is not known yet is not created here) - see
// commitLocked for the half that does.
func (t *Tracker) checkLocked(sourceID string, sequence uint64) (*sourceState, verdict) {
	st, ok := t.sources[sourceID]
	if !ok {
		return nil, verdictNew
	}
	if _, dup := st.seen[sequence]; dup {
		return st, verdictDuplicate
	}
	if st.maxSeen > 0 && sequence+t.window <= st.maxSeen {
		// Behind the reorder window: a stale replay of old data - or a
		// producer whose counter started over.
		if sequence <= t.window {
			return st, verdictRestart
		}
		if st.behindRun >= restartRun-1 && sequence > st.behindLast {
			return st, verdictRestart
		}
		return st, verdictDuplicate
	}
	return st, verdictNew
}

// noteBehindLocked remembers a rejected sequence that fell behind the window,
// so that a run of increasing ones can be recognised as a restart.
func (t *Tracker) noteBehindLocked(st *sourceState, sequence uint64) {
	if st.behindRun > 0 && sequence > st.behindLast {
		st.behindRun++
	} else {
		st.behindRun = 1
		st.behindFirst = sequence
	}
	st.behindLast = sequence
}

// commitLocked records `sequence` as seen and advances/prunes the
// reorder window - the mutating half checkLocked deliberately leaves
// out, so a caller (AllowThen) can skip it entirely when whatever it
// meant to do with an allowed sequence didn't actually happen. Caller
// must already hold t.mu.
func (t *Tracker) commitLocked(sourceID string, st *sourceState, sequence uint64, v verdict) {
	if st == nil {
		t.makeRoomLocked()
		st = &sourceState{seen: make(map[uint64]struct{}, t.window)}
		t.sources[sourceID] = st
	}
	if v == verdictRestart {
		st.maxSeen = 0
		st.seen = make(map[uint64]struct{}, t.window)
		st.behindRun = 0
		t.resets++
	}
	st.behindRun = 0
	st.lastUsed = t.now()
	st.seen[sequence] = struct{}{}
	if sequence > st.maxSeen {
		st.maxSeen = sequence
	}
	if st.maxSeen >= t.window {
		floor := st.maxSeen - t.window + 1
		for seq := range st.seen {
			if seq < floor {
				delete(st.seen, seq)
			}
		}
	}
}

// makeRoomLocked forgets the producer that has been silent the longest when
// the tracker is at its limit.
func (t *Tracker) makeRoomLocked() {
	for len(t.sources) >= t.maxSources {
		var oldestID string
		var oldest time.Time
		first := true
		for id, st := range t.sources {
			if first || st.lastUsed.Before(oldest) {
				oldestID, oldest, first = id, st.lastUsed, false
			}
		}
		if first {
			return
		}
		delete(t.sources, oldestID)
		t.evicted++
	}
}

// Allow reports whether `sequence` from `sourceID` is real, new data.
// false means "already seen, or too stale behind this source's own
// high-water mark to trust" - the caller should treat it as a duplicate
// and not re-buffer it. A true result is committed immediately -
// collector.go does NOT use this directly anymore for exactly the
// reason AllowThen's own header comment explains; kept for any
// caller that genuinely wants "check and immediately commit" as one
// step (and every existing unit test in this package already assumes
// this exact behavior).
func (t *Tracker) Allow(sourceID string, sequence uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	st, v := t.checkLocked(sourceID, sequence)
	if v == verdictDuplicate {
		if st != nil && st.maxSeen > 0 && sequence+t.window <= st.maxSeen {
			t.noteBehindLocked(st, sequence)
		}
		return false
	}
	t.commitLocked(sourceID, st, sequence, v)
	return true
}

// AllowThen is Allow, except a real new sequence is only committed as
// seen once `accept` (the caller's own downstream side effect - here,
// always collector.go's `c.buf.Push(s)`) actually succeeds.
//
// `ingest` used to call plain Allow and, separately and
// unconditionally, buf.Push() right after. Allow() had ALREADY marked
// the sequence as permanently seen the moment it returned true -
// completely independent of whether the following buf.Push() itself
// then succeeded. A full buffer meant Push() returned ErrFull and the
// sample was dropped, but dedup's own state still said "already
// buffered". A legitimate retry of the EXACT SAME sample (the producer
// backing off and resending after the 503 api.go turns ErrFull into)
// arrived with the identical (sourceID, sequence) and was rejected as
// ErrDuplicate forever - the sample was never persisted on the first
// attempt (buffer full) NOR on any later retry (falsely "already seen"),
// permanent silent data loss despite the producer doing everything right.
//
// Holding t.mu for the ENTIRE check-then-accept-then-commit sequence
// also closes a real concurrency gap the acceptance criteria calls out
// explicitly: two producers racing the identical (sourceID, sequence)
// can no longer both be told "yes, new, go ahead" and both persist a
// copy - the second one through this same lock always sees the first's
// now-committed mark, exactly like any other duplicate.
func (t *Tracker) AllowThen(sourceID string, sequence uint64, accept func() error) (allowed bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	st, v := t.checkLocked(sourceID, sequence)
	if v == verdictDuplicate {
		if st != nil && st.maxSeen > 0 && sequence+t.window <= st.maxSeen {
			t.noteBehindLocked(st, sequence)
		}
		return false, nil
	}
	if err := accept(); err != nil {
		// A real, new sequence - but the caller's own downstream accept
		// failed (a full buffer, most commonly). Nothing committed:
		// the exact same (sourceID, sequence) retried later sees the
		// identical "not yet seen" state and can succeed then.
		return true, err
	}
	t.commitLocked(sourceID, st, sequence, v)
	return true, nil
}
