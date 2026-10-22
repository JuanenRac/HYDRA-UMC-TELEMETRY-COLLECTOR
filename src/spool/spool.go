// HYDRA-UMC-TELEMETRY-COLLECTOR - spool/spool.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// A small on-disk queue for the samples a long sink outage would otherwise fill the
// in-memory buffer with. When the sink fails, the collector appends the batch here (one
// JSON object per line) and keeps accepting new samples; when the sink answers again the
// spool is replayed before anything newer. The spool is optional (the collector starts
// without one) and bounded by a byte budget: a spool that is full refuses the append and
// the collector falls back to keeping the batch in memory, exactly as it does without a
// spool.
//
// Delivery is at least once: if the process stops after a batch was delivered but before
// the spool recorded it, the batch is delivered again after the restart. HYDRA-UMC-DATALAKE
// stores a point once per (source, kind, field, timestamp), so a repeat replaces itself.
package spool

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

// ErrFull means the spool has no room left in its byte budget.
var ErrFull = errors.New("spool: full")

// Entry is one spooled sample and the offset just after its line, which Ack takes.
type Entry struct {
	Sample telemetry.Sample
	End    int64
}

// File is a spool kept in a single file.
type File struct {
	mu       sync.Mutex
	path     string
	maxBytes int64
	size     int64 // bytes in the file
	read     int64 // offset of the first line not yet delivered
	pending  int   // lines between read and size
}

// Open opens (or creates) the spool at path, limited to maxBytes of unsent data. What a
// previous run left in the file is kept and will be replayed.
func Open(path string, maxBytes int64) (*File, error) {
	if maxBytes <= 0 {
		return nil, errors.New("spool: the byte budget must be positive")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("spool: %w", err)
	}
	f := &File{path: path, maxBytes: maxBytes}
	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return f, nil
	case err != nil:
		return nil, fmt.Errorf("spool: %w", err)
	}
	// A crash can leave a half-written last line: cut the file back to the last complete line.
	if i := bytes.LastIndexByte(data, '\n'); i < len(data)-1 {
		data = data[:i+1]
		if err := os.WriteFile(path, data, 0o640); err != nil {
			return nil, fmt.Errorf("spool: %w", err)
		}
	}
	f.size = int64(len(data))
	f.pending = bytes.Count(data, []byte{'\n'})
	return f, nil
}

// Pending is how many spooled samples have not been delivered.
func (f *File) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pending
}

// Bytes is how much unsent data the spool holds.
func (f *File) Bytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.size - f.read
}

// Append adds the samples at the end of the spool, all or none. It returns ErrFull when the
// byte budget would be exceeded.
func (f *File) Append(samples []telemetry.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	var buf bytes.Buffer
	for _, s := range samples {
		line, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("spool: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.size-f.read+int64(buf.Len()) > f.maxBytes {
		return ErrFull
	}
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("spool: %w", err)
	}
	n, werr := file.Write(buf.Bytes())
	cerr := file.Close()
	if werr != nil || cerr != nil || n != buf.Len() {
		// a partial write would leave a torn line: cut it off again
		_ = os.Truncate(f.path, f.size)
		return fmt.Errorf("spool: write failed: %v %v", werr, cerr)
	}
	f.size += int64(n)
	f.pending += len(samples)
	return nil
}

// Next returns up to max undelivered samples, oldest first, without removing them. A line that
// is not a valid sample (a damaged file) is skipped and counted as delivered by Ack.
func (f *File) Next(max int) ([]Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if max <= 0 || f.read >= f.size {
		return nil, nil
	}
	file, err := os.Open(f.path)
	if err != nil {
		return nil, fmt.Errorf("spool: %w", err)
	}
	defer file.Close()
	if _, err := file.Seek(f.read, io.SeekStart); err != nil {
		return nil, fmt.Errorf("spool: %w", err)
	}
	reader := bufio.NewReader(io.LimitReader(file, f.size-f.read))
	offset := f.read
	var out []Entry
	for len(out) < max {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] == '\n' {
			offset += int64(len(line))
			var s telemetry.Sample
			if json.Unmarshal(bytes.TrimSpace(line), &s) == nil && s.Validate() == nil {
				out = append(out, Entry{Sample: s, End: offset})
			}
		}
		if err != nil {
			break
		}
	}
	if len(out) == 0 && offset > f.read {
		// only damaged lines were left: drop them so they can never block the queue
		f.ackLocked(offset)
	}
	return out, nil
}

// Ack records that everything up to the offset (an Entry.End) was delivered or discarded. When
// the spool is empty again the file is truncated.
func (f *File) Ack(end int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackLocked(end)
}

func (f *File) ackLocked(end int64) {
	if end <= f.read {
		return
	}
	if end > f.size {
		end = f.size
	}
	// the lines being acknowledged are the ones between the old and the new offset (a small region)
	region := make([]byte, end-f.read)
	if file, err := os.Open(f.path); err == nil {
		n, _ := file.ReadAt(region, f.read)
		file.Close()
		f.pending -= bytes.Count(region[:n], []byte{'\n'})
	}
	if f.pending < 0 {
		f.pending = 0
	}
	f.read = end
	if f.read >= f.size {
		if err := os.Truncate(f.path, 0); err == nil {
			f.size, f.read, f.pending = 0, 0, 0
		}
	}
}
