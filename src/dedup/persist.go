// HYDRA-UMC-TELEMETRY-COLLECTOR - dedup/persist.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Keeping the deduplication window across a restart of the collector, so that
// a producer which resends its last few unacknowledged messages right after
// the collector came back is not counted twice. Off unless a file is named.
package dedup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const stateVersion = 1

type savedSource struct {
	MaxSeen uint64   `json:"max_seen"`
	Seen    []uint64 `json:"seen"`
}

type savedState struct {
	Version int                    `json:"version"`
	Window  uint64                 `json:"window"`
	Sources map[string]savedSource `json:"sources"`
}

// Save writes the windows of the producers that are remembered to path, through a
// temporary file that is renamed over the old one, so a crash while writing leaves
// the previous file whole.
func (t *Tracker) Save(path string) error {
	t.mu.Lock()
	state := savedState{Version: stateVersion, Window: t.window, Sources: make(map[string]savedSource, len(t.sources))}
	for id, st := range t.sources {
		seen := make([]uint64, 0, len(st.seen))
		for seq := range st.seen {
			seen = append(seen, seq)
		}
		sort.Slice(seen, func(i, j int) bool { return seen[i] < seen[j] })
		state.Sources[id] = savedSource{MaxSeen: st.maxSeen, Seen: seen}
	}
	t.mu.Unlock()

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

// Load reads a file written by Save into an empty tracker. A file that does not exist
// is not an error (the first start); one that cannot be read, is of another version or
// was written with another window is refused and leaves the tracker empty, because a
// window that does not match would let duplicates through or refuse new data.
// At most maxSources producers are taken, the ones with the highest sequence first.
func (t *Tracker) Load(path string) (int, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var state savedState
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, fmt.Errorf("dedup state %s is not readable: %w", path, err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if state.Version != stateVersion || state.Window != t.window {
		return 0, fmt.Errorf("dedup state %s was written for another version or window", path)
	}
	ids := make([]string, 0, len(state.Sources))
	for id := range state.Sources {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return state.Sources[ids[i]].MaxSeen > state.Sources[ids[j]].MaxSeen })
	if len(ids) > t.maxSources {
		ids = ids[:t.maxSources]
	}
	now := t.now()
	for _, id := range ids {
		src := state.Sources[id]
		st := &sourceState{maxSeen: src.MaxSeen, seen: make(map[uint64]struct{}, len(src.Seen)), lastUsed: now}
		for _, seq := range src.Seen {
			if seq <= src.MaxSeen && seq+t.window > src.MaxSeen {
				st.seen[seq] = struct{}{}
			}
		}
		t.sources[id] = st
	}
	return len(ids), nil
}
