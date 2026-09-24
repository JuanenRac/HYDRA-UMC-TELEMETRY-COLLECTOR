// HYDRA-UMC-TELEMETRY-COLLECTOR - dedup/dedup.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Real per-producer sequence deduplication with a bounded reorder
// window - the mechanism behind "a device that reconnects and resends
// its last few unacked messages doesn't inflate ingest counts or
// re-buffer the same sample twice".
package dedup

import "sync"

const defaultWindow = 64

// Tracker deduplicates (sourceID, sequence) pairs. A sequence within the
// window of the highest one already seen for that source is allowed
// exactly once - a real, legitimately reordered arrival (seq 3 before
// seq 2) is accepted, but the same seq arriving twice, or one older than
// the window (a stale replay of an already-acked message), is not.
type Tracker struct {
	mu      sync.Mutex
	window  uint64
	sources map[string]*sourceState
}

type sourceState struct {
	maxSeen uint64
	seen    map[uint64]struct{}
}

// New returns a Tracker that remembers, per source, sequence numbers
// within `window` of the highest one accepted so far. window <= 0 uses a
// real, empirically reasonable default (64) rather than an unbounded or
// zero-size window.
func New(window int) *Tracker {
	if window <= 0 {
		window = defaultWindow
	}
	return &Tracker{window: uint64(window), sources: make(map[string]*sourceState)}
}

// checkLocked reports whether `sequence` is real, new data for
// `sourceID` - the SAME rule Allow/AllowThen both enforce, factored out
// so the two never drift apart. Caller must already hold t.mu. Never
// mutates state - see commitLocked for the half that does.
func (t *Tracker) checkLocked(sourceID string, sequence uint64) (*sourceState, bool) {
	st, ok := t.sources[sourceID]
	if !ok {
		st = &sourceState{seen: make(map[uint64]struct{}, t.window)}
		t.sources[sourceID] = st
	}

	if _, dup := st.seen[sequence]; dup {
		return st, false
	}
	if st.maxSeen > 0 && sequence+t.window <= st.maxSeen {
		// Outside the reorder window behind what we've already
		// accepted - a real replay of old data, not a legitimately
		// late arrival.
		return st, false
	}
	return st, true
}

// commitLocked records `sequence` as seen and advances/prunes the
// reorder window - the mutating half checkLocked deliberately leaves
// out, so a caller (AllowThen) can skip it entirely when whatever it
// meant to do with an allowed sequence didn't actually happen. Caller
// must already hold t.mu.
func (t *Tracker) commitLocked(st *sourceState, sequence uint64) {
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

	st, ok := t.checkLocked(sourceID, sequence)
	if !ok {
		return false
	}
	t.commitLocked(st, sequence)
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

	st, ok := t.checkLocked(sourceID, sequence)
	if !ok {
		return false, nil
	}
	if err := accept(); err != nil {
		// A real, new sequence - but the caller's own downstream accept
		// failed (a full buffer, most commonly). Nothing committed:
		// the exact same (sourceID, sequence) retried later sees the
		// identical "not yet seen" state and can succeed then.
		return true, err
	}
	t.commitLocked(st, sequence)
	return true, nil
}
