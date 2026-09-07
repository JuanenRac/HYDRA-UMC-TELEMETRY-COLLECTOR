// HYDRA-UMC-TELEMETRY-COLLECTOR - dedup/dedup.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Real per-producer sequence deduplication with a bounded reorder
// window - the mechanism behind "a device that reconnects and resends
// its last few unacked messages doesn't inflate ingest counts or
// re-buffer the same sample twice" (promotion audit line 661-662).
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

// Allow reports whether `sequence` from `sourceID` is real, new data.
// false means "already seen, or too stale behind this source's own
// high-water mark to trust" - the caller should treat it as a duplicate
// and not re-buffer it.
func (t *Tracker) Allow(sourceID string, sequence uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	st, ok := t.sources[sourceID]
	if !ok {
		st = &sourceState{seen: make(map[uint64]struct{}, t.window)}
		t.sources[sourceID] = st
	}

	if _, dup := st.seen[sequence]; dup {
		return false
	}
	if st.maxSeen > 0 && sequence+t.window <= st.maxSeen {
		// Outside the reorder window behind what we've already
		// accepted - a real replay of old data, not a legitimately
		// late arrival.
		return false
	}

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
	return true
}
