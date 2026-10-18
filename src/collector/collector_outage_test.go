// HYDRA-UMC-TELEMETRY-COLLECTOR - collector/collector_outage_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package collector

import (
	"fmt"
	"testing"
)

// A sink outage must not make the collector throw away the OLDEST samples:
// the batch being written keeps its place, new ingestion is refused (503 for
// the producer) once the buffer is full, and when the sink is back every
// accepted sample is delivered, in order.
func TestCollector_AnOutageLosesNothingThatWasAcceptedAndDeliversInOrder(t *testing.T) {
	s := &memorySink{fail: true}
	c := New(10, s)

	accepted := []string{}
	refused := 0
	for round := 0; round < 6; round++ {
		for i := 0; i < 4; i++ {
			id := fmt.Sprintf("robot-%d-%d", round, i)
			if err := c.IngestWS(wsMessage(id)); err != nil {
				refused++
				continue
			}
			accepted = append(accepted, id)
		}
		c.FlushOnce(5) // the sink is down: the batch goes back where it was
	}

	if len(accepted) != 10 {
		t.Fatalf("accepted %d samples, want 10 (the capacity)", len(accepted))
	}
	if refused != 14 {
		t.Fatalf("refused %d samples, want 14 (backpressure instead of silent loss)", refused)
	}
	if got := c.Stats().Dropped; got != 0 {
		t.Fatalf("Dropped = %d, want 0: no accepted sample may be lost", got)
	}
	if c.BufferLen() != 10 {
		t.Fatalf("BufferLen() = %d, want 10", c.BufferLen())
	}

	s.mu.Lock()
	s.fail = false
	s.mu.Unlock()
	for c.BufferLen() > 0 {
		if c.FlushOnce(4) == 0 {
			t.Fatal("the sink is back but a flush delivered nothing")
		}
	}
	delivered := []string{}
	for _, batch := range s.written {
		for _, sample := range batch {
			delivered = append(delivered, sample.SourceID)
		}
	}
	if len(delivered) != len(accepted) {
		t.Fatalf("delivered %d samples, accepted %d", len(delivered), len(accepted))
	}
	for i := range accepted {
		if delivered[i] != accepted[i] {
			t.Fatalf("delivery order broken at %d: got %s, want %s", i, delivered[i], accepted[i])
		}
	}
}

func TestCollector_StatsReportProducerResetsAndInFlight(t *testing.T) {
	c := New(1000, &memorySink{})
	for seq := uint64(1); seq <= 400; seq++ {
		if err := c.IngestWS(wsMessageWithSeq("robot-1", seq)); err != nil {
			t.Fatalf("ingest %d: %v", seq, err)
		}
		if seq%100 == 0 {
			c.FlushOnce(1000)
		}
	}
	// the producer restarts
	if err := c.IngestWS(wsMessageWithSeq("robot-1", 1)); err != nil {
		t.Fatalf("the restarted producer's first sample was refused: %v", err)
	}
	stats := c.Stats()
	if stats.Resets != 1 || stats.Duplicates != 0 || stats.Sources != 1 || stats.InFlight != 0 {
		t.Fatalf("stats = %+v, want Resets=1 Duplicates=0 Sources=1 InFlight=0", stats)
	}
}

func TestCollector_RejectsAnImplausibleTimestampBeforeBuffering(t *testing.T) {
	c := New(10, &memorySink{})
	seconds := []byte(`{"sourceId":"robot-1","kind":"motor_temp","timestamp":1700000000,"fields":{"value":1}}`)
	if err := c.IngestWS(seconds); err == nil {
		t.Fatal("a timestamp in seconds must be refused")
	}
	if c.BufferLen() != 0 || c.Stats().IngestErrors != 1 {
		t.Fatalf("BufferLen()=%d IngestErrors=%d, want 0 and 1", c.BufferLen(), c.Stats().IngestErrors)
	}
}
