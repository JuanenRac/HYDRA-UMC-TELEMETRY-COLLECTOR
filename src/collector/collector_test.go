// HYDRA-UMC-TELEMETRY-COLLECTOR - collector/collector_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package collector

import (
	"errors"
	"sync"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

// memorySink is a real Sink implementation used only by tests - it
// genuinely stores what it's given (or genuinely fails, on command), not
// a mock framework standing in for behavior.
type memorySink struct {
	mu      sync.Mutex
	written [][]telemetry.Sample
	fail    bool
}

func (m *memorySink) Write(batch []telemetry.Sample) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("simulated outage")
	}
	cp := make([]telemetry.Sample, len(batch))
	copy(cp, batch)
	m.written = append(m.written, cp)
	return nil
}

func wsMessage(sourceID string) []byte {
	return []byte(`{"sourceId":"` + sourceID + `","kind":"motor_temp","timestamp":1700000000000,"fields":{"value":1}}`)
}

func TestCollector_IngestAndFlushHappyPath(t *testing.T) {
	s := &memorySink{}
	c := New(10, s)

	if err := c.IngestWS(wsMessage("robot-1")); err != nil {
		t.Fatalf("IngestWS: %v", err)
	}
	n := c.FlushOnce(10)
	if n != 1 {
		t.Fatalf("FlushOnce returned %d, want 1", n)
	}
	if c.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0 after a successful flush", c.BufferLen())
	}
	stats := c.Stats()
	if stats.Ingested != 1 || stats.Flushed != 1 {
		t.Fatalf("stats = %+v, want Ingested=1 Flushed=1", stats)
	}
}

func TestCollector_SinkFailureRequeuesInsteadOfLosingData(t *testing.T) {
	s := &memorySink{fail: true}
	c := New(10, s)

	_ = c.IngestWS(wsMessage("robot-1"))
	_ = c.IngestWS(wsMessage("robot-2"))

	n := c.FlushOnce(10)
	if n != 0 {
		t.Fatalf("FlushOnce during outage returned %d, want 0", n)
	}
	if c.BufferLen() != 2 {
		t.Fatalf("BufferLen() = %d, want 2 - a failed flush must not lose the samples", c.BufferLen())
	}

	// The "outage" ends - the next flush must succeed with the SAME
	// samples that were requeued, not new ones.
	s.mu.Lock()
	s.fail = false
	s.mu.Unlock()

	n = c.FlushOnce(10)
	if n != 2 {
		t.Fatalf("FlushOnce after recovery returned %d, want 2", n)
	}
	if c.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0 after the retry succeeds", c.BufferLen())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.written) != 1 || len(s.written[0]) != 2 {
		t.Fatalf("sink received %+v, want exactly one batch of 2 samples", s.written)
	}
	if s.written[0][0].SourceID != "robot-1" || s.written[0][1].SourceID != "robot-2" {
		t.Fatalf("recovered batch = %+v, want robot-1 then robot-2 (order preserved)", s.written[0])
	}

	stats := c.Stats()
	if stats.FlushErrors != 1 {
		t.Fatalf("FlushErrors = %d, want 1 (the one outage attempt)", stats.FlushErrors)
	}
	if stats.Dropped != 0 {
		t.Fatalf("Dropped = %d, want 0 - there was room to requeue everything", stats.Dropped)
	}
}

func TestCollector_IngestErrorsAreCountedNotBuffered(t *testing.T) {
	s := &memorySink{}
	c := New(10, s)

	err := c.IngestWS([]byte(`{"kind":"motor_temp","timestamp":1,"fields":{}}`)) // missing sourceId
	if err == nil {
		t.Fatal("expected an error for an invalid WS message, got nil")
	}
	if c.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0 - an invalid sample must never reach the buffer", c.BufferLen())
	}
	if c.Stats().IngestErrors != 1 {
		t.Fatalf("IngestErrors = %d, want 1", c.Stats().IngestErrors)
	}
}

func TestCollector_BackpressureIsReportedNotSwallowed(t *testing.T) {
	s := &memorySink{}
	c := New(1, s) // capacity 1

	if err := c.IngestWS(wsMessage("robot-1")); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	err := c.IngestWS(wsMessage("robot-2"))
	if err == nil {
		t.Fatal("expected a buffer-full error on the second ingest, got nil")
	}
}

func TestCollector_FlushOnEmptyBufferIsANoOp(t *testing.T) {
	s := &memorySink{}
	c := New(10, s)
	n := c.FlushOnce(10)
	if n != 0 {
		t.Fatalf("FlushOnce on an empty buffer returned %d, want 0", n)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.written) != 0 {
		t.Fatalf("sink was written to on an empty flush: %+v", s.written)
	}
}
