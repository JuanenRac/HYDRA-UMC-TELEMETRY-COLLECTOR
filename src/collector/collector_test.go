// HYDRA-UMC-TELEMETRY-COLLECTOR - collector/collector_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package collector

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/sink"
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

func wsMessageWithSeq(sourceID string, seq uint64) []byte {
	return []byte(fmt.Sprintf(
		`{"sourceId":%q,"kind":"motor_temp","timestamp":1700000000000,"fields":{"value":1},"sequence":%d}`,
		sourceID, seq,
	))
}

// invalidDataSink is a real Sink that always fails with a real
// *sink.InvalidDataError, standing in for DATALAKE genuinely rejecting a
// sample's content (as opposed to a transport failure).
type invalidDataSink struct{}

func (invalidDataSink) Write(batch []telemetry.Sample) error {
	return &sink.InvalidDataError{Sample: batch[0], Status: 400, Body: "bad data"}
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

func TestCollector_DuplicateSequenceIsRejectedNotBuffered(t *testing.T) {
	s := &memorySink{}
	c := New(10, s)

	if err := c.IngestWS(wsMessageWithSeq("robot-1", 1)); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	err := c.IngestWS(wsMessageWithSeq("robot-1", 1))
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("second ingest of the same sequence: err = %v, want ErrDuplicate", err)
	}
	if c.BufferLen() != 1 {
		t.Fatalf("BufferLen() = %d, want 1 - the duplicate must not be re-buffered", c.BufferLen())
	}
	stats := c.Stats()
	if stats.Ingested != 1 {
		t.Fatalf("Ingested = %d, want 1 - the duplicate must not inflate the count", stats.Ingested)
	}
	if stats.Duplicates != 1 {
		t.Fatalf("Duplicates = %d, want 1", stats.Duplicates)
	}
}

func TestCollector_RealDisconnectReconnectResendIsDeduplicated(t *testing.T) {
	// The real scenario the promotion audit calls out: a device sends
	// sequences 1-3, the connection drops before it receives acks, it
	// reconnects and - unsure what actually made it through - resends
	// 2 and 3 again before continuing with the genuinely new 4.
	s := &memorySink{}
	c := New(10, s)

	for _, seq := range []uint64{1, 2, 3} {
		if err := c.IngestWS(wsMessageWithSeq("robot-1", seq)); err != nil {
			t.Fatalf("initial ingest of sequence %d: %v", seq, err)
		}
	}

	// "reconnect" - nothing special about the collector's own state, the
	// same device just resends what it wasn't sure arrived.
	for _, seq := range []uint64{2, 3} {
		err := c.IngestWS(wsMessageWithSeq("robot-1", seq))
		if !errors.Is(err, ErrDuplicate) {
			t.Fatalf("resent sequence %d after reconnect: err = %v, want ErrDuplicate", seq, err)
		}
	}

	if err := c.IngestWS(wsMessageWithSeq("robot-1", 4)); err != nil {
		t.Fatalf("genuinely new sequence 4 after the resend: %v", err)
	}

	stats := c.Stats()
	if stats.Ingested != 4 {
		t.Fatalf("Ingested = %d, want 4 (1,2,3,4 exactly once each) - the resend must not inflate this", stats.Ingested)
	}
	if stats.Duplicates != 2 {
		t.Fatalf("Duplicates = %d, want 2 (the resent 2 and 3)", stats.Duplicates)
	}
	if c.BufferLen() != 4 {
		t.Fatalf("BufferLen() = %d, want 4 - one buffered copy of each real sample", c.BufferLen())
	}
}

func TestCollector_SamplesWithoutSequenceAreNeverDeduplicated(t *testing.T) {
	// Sequence 0 ("not provided") must behave exactly like before dedup
	// existed - a producer that doesn't opt in isn't affected.
	s := &memorySink{}
	c := New(10, s)

	if err := c.IngestWS(wsMessage("robot-1")); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if err := c.IngestWS(wsMessage("robot-1")); err != nil {
		t.Fatalf("second ingest (no sequence, must not be deduplicated): %v", err)
	}
	if c.Stats().Duplicates != 0 {
		t.Fatalf("Duplicates = %d, want 0 for samples that never provided a sequence", c.Stats().Duplicates)
	}
}

func TestCollector_InvalidDataFlushErrorIsClassifiedSeparatelyFromTransport(t *testing.T) {
	c := New(10, invalidDataSink{})
	_ = c.IngestWS(wsMessage("robot-1"))
	c.FlushOnce(10)

	stats := c.Stats()
	if stats.InvalidDataErrors != 1 {
		t.Fatalf("InvalidDataErrors = %d, want 1", stats.InvalidDataErrors)
	}
	if stats.TransportErrors != 0 {
		t.Fatalf("TransportErrors = %d, want 0 - this failure was a real InvalidDataError, not transport", stats.TransportErrors)
	}
}

func TestCollector_GenericSinkFailureIsClassifiedAsTransport(t *testing.T) {
	s := &memorySink{fail: true}
	c := New(10, s)
	_ = c.IngestWS(wsMessage("robot-1"))
	c.FlushOnce(10)

	stats := c.Stats()
	if stats.TransportErrors != 1 {
		t.Fatalf("TransportErrors = %d, want 1", stats.TransportErrors)
	}
	if stats.InvalidDataErrors != 0 {
		t.Fatalf("InvalidDataErrors = %d, want 0 - a generic sink error is not a real InvalidDataError", stats.InvalidDataErrors)
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
