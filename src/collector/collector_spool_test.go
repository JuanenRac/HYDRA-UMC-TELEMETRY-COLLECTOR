// HYDRA-UMC-TELEMETRY-COLLECTOR - collector/collector_spool_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package collector

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/spool"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

func newSpool(t *testing.T, max int64) *spool.File {
	t.Helper()
	f, err := spool.Open(filepath.Join(t.TempDir(), "spool.ndjson"), max)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (m *memorySink) setFail(v bool) {
	m.mu.Lock()
	m.fail = v
	m.mu.Unlock()
}

func (m *memorySink) ids() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, b := range m.written {
		for _, s := range b {
			out = append(out, s.SourceID)
		}
	}
	return out
}

// With a spool a long outage waits on disk: the buffer keeps taking samples (no 503) and when
// the sink is back every sample is delivered, the spooled ones first.
func TestCollector_ASpoolAbsorbsALongOutageAndReplaysItInOrder(t *testing.T) {
	s := &memorySink{fail: true}
	c := New(10, s)
	c.SetSpool(newSpool(t, 1<<20))

	var accepted []string
	for round := 0; round < 6; round++ {
		for i := 0; i < 4; i++ {
			id := fmt.Sprintf("robot-%d-%d", round, i)
			if err := c.IngestWS(wsMessage(id)); err != nil {
				t.Fatalf("with a spool the buffer must keep accepting during an outage: %v", err)
			}
			accepted = append(accepted, id)
		}
		c.FlushOnce(5)
	}
	st := c.Stats()
	if st.Dropped != 0 || st.Spooled == 0 || st.SpoolPending == 0 {
		t.Fatalf("stats = %+v, want nothing dropped and a non-empty spool", st)
	}

	s.setFail(false)
	for guard := 0; (c.BufferLen() > 0 || c.Stats().SpoolPending > 0) && guard < 50; guard++ {
		c.FlushOnce(5)
	}
	got := s.ids()
	if len(got) != len(accepted) {
		t.Fatalf("delivered %d samples, accepted %d", len(got), len(accepted))
	}
	for i := range accepted {
		if got[i] != accepted[i] {
			t.Fatalf("order broken at %d: got %s, want %s", i, got[i], accepted[i])
		}
	}
	if c.Stats().SpoolPending != 0 || c.Stats().SpoolBytes != 0 {
		t.Fatalf("the spool must be empty again: %+v", c.Stats())
	}
}

// A spool that is full falls back to the in-memory behaviour: nothing is lost, the buffer fills.
func TestCollector_AFullSpoolFallsBackToKeepingTheBatchInMemory(t *testing.T) {
	s := &memorySink{fail: true}
	c := New(10, s)
	c.SetSpool(newSpool(t, 1)) // room for nothing
	for i := 0; i < 5; i++ {
		_ = c.IngestWS(wsMessage(fmt.Sprintf("r-%d", i)))
	}
	c.FlushOnce(5)
	if c.BufferLen() != 5 || c.Stats().Spooled != 0 || c.Stats().Dropped != 0 {
		t.Fatalf("BufferLen=%d stats=%+v, want the 5 samples back in memory", c.BufferLen(), c.Stats())
	}
}

// A sample the sink rejects for good while it is replayed from the spool is discarded, and the
// rest is delivered.
func TestCollector_APoisonedSpooledSampleIsQuarantinedAndTheRestDelivered(t *testing.T) {
	sp := newSpool(t, 1<<20)
	good := func(id string) telemetry.Sample {
		return telemetry.Sample{SourceID: id, Kind: "k", Timestamp: 1_700_000_000_000, Fields: map[string]float64{"v": 1}}
	}
	if err := sp.Append([]telemetry.Sample{good("a"), good("poison"), good("c")}); err != nil {
		t.Fatal(err)
	}
	s := &invalidDataSink{PoisonSourceID: "poison"}
	c := New(10, s)
	c.SetSpool(sp)
	for guard := 0; sp.Pending() > 0 && guard < 10; guard++ {
		c.FlushOnce(10)
	}
	if sp.Pending() != 0 {
		t.Fatalf("the spool still holds %d samples", sp.Pending())
	}
	if c.Stats().Quarantined != 1 {
		t.Fatalf("Quarantined = %d, want 1", c.Stats().Quarantined)
	}
}

func TestCollector_PriorityLetsAFullBufferDropTheLeastImportantSampleAndCountsIt(t *testing.T) {
	c := New(3, &memorySink{fail: true})
	c.SetPriority(func(s telemetry.Sample) int {
		if s.Kind == "fault" {
			return 9
		}
		return 1
	})
	for i := 0; i < 3; i++ {
		_ = c.IngestWS(wsMessage(fmt.Sprintf("r-%d", i)))
	}
	if err := c.IngestWS([]byte(`{"sourceId":"alarm","kind":"fault","timestamp":1700000000000,"fields":{"v":1}}`)); err != nil {
		t.Fatalf("a more important sample must make room: %v", err)
	}
	if st := c.Stats(); st.Evicted != 1 || st.Dropped != 0 {
		t.Fatalf("stats = %+v, want Evicted=1 (counted apart from Dropped)", st)
	}
}

func TestCollector_StatsReportFlushLatencyAndTheAgeOfTheOldestSample(t *testing.T) {
	c := New(10, &memorySink{})
	_ = c.IngestWS(wsMessage("old")) // its timestamp is far in the past
	if age := c.Stats().OldestSampleAgeMs; age <= 0 {
		t.Fatalf("OldestSampleAgeMs = %d, want a positive age", age)
	}
	c.FlushOnce(10)
	st := c.Stats()
	if st.OldestSampleAgeMs != 0 {
		t.Fatalf("an empty buffer has no oldest sample, got %d", st.OldestSampleAgeMs)
	}
	if st.MaxFlushMs < st.LastFlushMs || st.LastFlushMs < 0 {
		t.Fatalf("flush latency inconsistent: %+v", st)
	}
}
