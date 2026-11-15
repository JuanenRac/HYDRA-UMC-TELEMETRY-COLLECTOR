// HYDRA-UMC-TELEMETRY-COLLECTOR - collector/collector_simulation_faults_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package collector

// Simulation bench, part two: the faults that the first part does not draw. A collector that crashes with samples on its disk spool and starts again; a data lake
// that rejects one producer for good while it flaps for everybody else; a spool that fills up; and the real DatalakeSink against an HTTP twin of the data lake that
// answers 503 and 500, resets the connection and loses the answer of a sample it stored. All on a fixed seed, no hardware, no network beyond this machine.
// What the bench proves is the promise of the collector: what it accepted is delivered once and in the order of its producer, or is counted, and the only
// duplicates are those of an answer that was lost after the data lake stored the sample.

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/sink"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/spool"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

// flakyLake is a simulated data lake that answers like the real sink does: a transport failure at the position of the sample that failed (PartialWriteError),
// and an InvalidDataError for a producer it rejects for good.
type flakyLake struct {
	mu       sync.Mutex
	rng      *rand.Rand
	down     bool
	failRate int // one sample in failRate fails with a transport error; zero never
	poison   map[string]bool
	got      map[string][]uint64
}

func newFlakyLake(seed int64, failRate int) *flakyLake {
	return &flakyLake{rng: rand.New(rand.NewSource(seed)), failRate: failRate, poison: map[string]bool{}, got: map[string][]uint64{}}
}

func (l *flakyLake) setDown(v bool) {
	l.mu.Lock()
	l.down = v
	l.mu.Unlock()
}

func (l *flakyLake) isDown() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.down
}

func (l *flakyLake) Write(batch []telemetry.Sample) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, s := range batch {
		if l.poison[s.SourceID] {
			return &sink.InvalidDataError{Sample: s, Index: i, Status: 400, Body: "rejected"}
		}
		if l.down || (l.failRate > 0 && l.rng.Intn(l.failRate) == 0) {
			return &sink.PartialWriteError{Err: errSimulatedOutage, Succeeded: i}
		}
		l.got[s.SourceID] = append(l.got[s.SourceID], s.Sequence)
	}
	return nil
}

func (l *flakyLake) delivered(name string) []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]uint64(nil), l.got[name]...)
}

func drain(c *Collector, batch int) bool {
	for guard := 0; guard < 20000; guard++ {
		if c.BufferLen() == 0 && c.Stats().SpoolPending == 0 {
			return true
		}
		c.FlushOnce(batch)
	}
	return false
}

func sameSequences(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSimulation_ACollectorThatCrashesResumesFromItsSpoolAndItsDedupState(t *testing.T) {
	dir := t.TempDir()
	spoolPath := filepath.Join(dir, "spool.ndjson")
	dedupPath := filepath.Join(dir, "dedup.json")
	lake := newFlakyLake(benchSeed, 0)
	lake.setDown(true)
	names := []string{"robot-aa", "robot-ba", "robot-ca", "robot-da", "robot-ea"}
	accepted := map[string][]uint64{}
	next := map[string]uint64{}

	first, err := spool.Open(spoolPath, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	c1 := New(64, lake)
	c1.SetSpool(first)
	for seq := uint64(1); seq <= 30; seq++ {
		for _, name := range names {
			if err := c1.IngestWS(wsMessageWithSeq(name, seq)); err != nil {
				t.Fatalf("%s seq %d refused: %v", name, seq, err)
			}
			accepted[name] = append(accepted[name], seq)
			next[name] = seq
		}
		c1.FlushOnce(16)
	}
	for guard := 0; c1.BufferLen() > 0 && guard < 1000; guard++ {
		c1.FlushOnce(16)
	}
	if c1.BufferLen() != 0 || c1.Stats().SpoolPending == 0 {
		t.Fatalf("with the lake down everything should wait on the disk: buffer %d, spool %d", c1.BufferLen(), c1.Stats().SpoolPending)
	}
	if err := c1.SaveDedupState(dedupPath); err != nil {
		t.Fatal(err)
	}

	// the process is gone: a new collector opens the same spool and the same state, and the lake is back
	second, err := spool.Open(spoolPath, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	c2 := New(64, lake)
	c2.SetSpool(second)
	if restored, err := c2.LoadDedupState(dedupPath); err != nil || restored == 0 {
		t.Fatalf("the dedup state was not restored: %d producers, %v", restored, err)
	}
	if second.Pending() == 0 {
		t.Fatal("the new spool did not find what the first one left on the disk")
	}
	lake.setDown(false)

	// the producers do not know what reached the collector: each sends its last three messages again, then goes on
	for _, name := range names {
		for seq := next[name] - 2; seq <= next[name]; seq++ {
			if err := c2.IngestWS(wsMessageWithSeq(name, seq)); err == nil {
				t.Fatalf("the resent message %s/%d was accepted twice after the restart", name, seq)
			}
		}
	}
	for seq := uint64(31); seq <= 40; seq++ {
		for _, name := range names {
			if err := c2.IngestWS(wsMessageWithSeq(name, seq)); err != nil {
				t.Fatalf("%s seq %d refused after the restart: %v", name, seq, err)
			}
			accepted[name] = append(accepted[name], seq)
		}
		c2.FlushOnce(16)
	}
	if !drain(c2, 16) {
		t.Fatalf("the restarted collector did not drain: buffer %d, spool %d", c2.BufferLen(), c2.Stats().SpoolPending)
	}
	for _, name := range names {
		if got := lake.delivered(name); !sameSequences(got, accepted[name]) {
			t.Fatalf("%s: delivered %v, accepted %v", name, got, accepted[name])
		}
	}
	st := c2.Stats()
	if st.Duplicates != int64(len(names)*3) {
		t.Fatalf("Duplicates = %d, want %d", st.Duplicates, len(names)*3)
	}
	if st.Dropped != 0 || st.Quarantined != 0 {
		t.Fatalf("stats = %+v: nothing accepted may be dropped or quarantined", st)
	}
}

func TestSimulation_AProducerTheLakeRejectsForGoodIsQuarantinedWhileTheLakeFlapsForTheOthers(t *testing.T) {
	lake := newFlakyLake(benchSeed+1, 9)
	lake.poison["robot-poison"] = true
	c := New(64, lake)
	c.SetSpool(newSpool(t, 8<<20))
	rng := rand.New(rand.NewSource(benchSeed))
	names := []string{"robot-aa", "robot-ba", "robot-ca", "robot-poison"}
	next := map[string]uint64{}
	accepted := map[string][]uint64{}

	for step := 0; step < 300; step++ {
		if rng.Intn(6) == 0 {
			lake.setDown(!lake.isDown())
		}
		for _, name := range names {
			if rng.Intn(2) == 0 {
				continue
			}
			seq := next[name] + 1
			if err := c.IngestWS(wsMessageWithSeq(name, seq)); err != nil {
				t.Fatalf("step %d: %s seq %d refused: %v", step, name, seq, err)
			}
			next[name] = seq
			accepted[name] = append(accepted[name], seq)
		}
		c.FlushOnce(8)
	}
	lake.setDown(false)
	if !drain(c, 8) {
		t.Fatalf("the collector did not drain: buffer %d, spool %d", c.BufferLen(), c.Stats().SpoolPending)
	}
	for _, name := range names[:3] {
		if got := lake.delivered(name); !sameSequences(got, accepted[name]) {
			t.Fatalf("%s: delivered %d samples, accepted %d, or the order is broken", name, len(got), len(accepted[name]))
		}
	}
	if got := lake.delivered("robot-poison"); len(got) != 0 {
		t.Fatalf("the rejected producer reached the lake: %v", got)
	}
	st := c.Stats()
	if st.Quarantined != int64(len(accepted["robot-poison"])) {
		t.Fatalf("Quarantined = %d, want one for each of the %d samples of the rejected producer", st.Quarantined, len(accepted["robot-poison"]))
	}
	if st.Dropped != 0 {
		t.Fatalf("Dropped = %d: nothing else may be lost", st.Dropped)
	}
	if st.TransportErrors == 0 {
		t.Fatalf("the lake flapped but no transport error was counted: %+v", st)
	}
}

func TestSimulation_AFullSpoolRefusesNewSamplesAndLosesNothingItAccepted(t *testing.T) {
	lake := newFlakyLake(benchSeed+2, 0)
	lake.setDown(true)
	c := New(16, lake)
	c.SetSpool(newSpool(t, 4096))
	rng := rand.New(rand.NewSource(benchSeed))
	names := []string{"robot-aa", "robot-ba", "robot-ca", "robot-da"}
	next := map[string]uint64{}
	accepted := map[string][]uint64{}
	refused := 0

	for step := 0; step < 150; step++ {
		for _, name := range names {
			if rng.Intn(2) == 0 {
				continue
			}
			seq := next[name] + 1
			if err := c.IngestWS(wsMessageWithSeq(name, seq)); err != nil {
				refused++ // the producer keeps this message and sends it again later
				continue
			}
			next[name] = seq
			accepted[name] = append(accepted[name], seq)
		}
		c.FlushOnce(8)
	}
	if refused == 0 {
		t.Fatal("the lake was down for a long time and the spool is small, but nothing was refused")
	}
	lake.setDown(false)
	if !drain(c, 8) {
		t.Fatalf("the collector did not drain: buffer %d, spool %d", c.BufferLen(), c.Stats().SpoolPending)
	}
	// the producers send what was refused again: it is accepted now, because a refused message was never marked as seen
	for _, name := range names {
		seq := next[name] + 1
		if err := c.IngestWS(wsMessageWithSeq(name, seq)); err != nil {
			t.Fatalf("%s seq %d is refused again with an empty buffer: %v", name, seq, err)
		}
		next[name] = seq
		accepted[name] = append(accepted[name], seq)
	}
	if !drain(c, 8) {
		t.Fatal("the collector did not drain the resent messages")
	}
	for _, name := range names {
		if got := lake.delivered(name); !sameSequences(got, accepted[name]) {
			t.Fatalf("%s: delivered %v, accepted %v", name, got, accepted[name])
		}
	}
	st := c.Stats()
	if st.Dropped != 0 || st.Quarantined != 0 {
		t.Fatalf("stats = %+v: nothing accepted may be dropped or quarantined", st)
	}
	if st.Duplicates != 0 {
		t.Fatalf("Duplicates = %d: a refused message was marked as seen", st.Duplicates)
	}
}

// httpLake is an HTTP twin of POST /ingest of the data lake. Each request draws a fault from the seed: 503, 500, a reset connection, or a sample that is STORED
// and whose answer is lost (the connection is cut after it was stored). A producer named robot-poison is answered with 400.
type httpLake struct {
	mu       sync.Mutex
	rng      *rand.Rand
	received map[string]int // times the lake stored a sample
	lost     map[string]int // times the lake stored it and lost the answer
	first    map[string][]uint64
	faults   int
}

func (l *httpLake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/ingest" || r.Method != http.MethodPost {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	var s telemetry.Sample
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if s.SourceID == "robot-poison" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	key := fmt.Sprintf("%s/%d", s.SourceID, s.Sequence)
	cut := func() {
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
	}
	switch draw := l.rng.Intn(100); {
	case draw < 12:
		l.faults++
		w.WriteHeader(http.StatusServiceUnavailable)
	case draw < 20:
		l.faults++
		w.WriteHeader(http.StatusInternalServerError)
	case draw < 28:
		l.faults++
		cut()
	case draw < 34:
		l.faults++
		l.store(key, s)
		l.lost[key]++
		cut()
	default:
		l.store(key, s)
		w.WriteHeader(http.StatusAccepted)
	}
}

func (l *httpLake) store(key string, s telemetry.Sample) {
	if l.received[key] == 0 {
		l.first[s.SourceID] = append(l.first[s.SourceID], s.Sequence)
	}
	l.received[key]++
}

func TestSimulation_TheRealSinkAgainstALakeThatAnswersBadlyKeepsTheOrderAndLosesNothing(t *testing.T) {
	lake := &httpLake{rng: rand.New(rand.NewSource(benchSeed + 3)), received: map[string]int{}, lost: map[string]int{}, first: map[string][]uint64{}}
	server := httptest.NewServer(lake)
	defer server.Close()
	lakeSink := sink.NewDatalakeSink(server.URL)
	lakeSink.Client = &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	c := New(64, lakeSink)
	c.SetSpool(newSpool(t, 8<<20))
	rng := rand.New(rand.NewSource(benchSeed))
	names := []string{"robot-aa", "robot-ba", "robot-ca", "robot-poison"}
	next := map[string]uint64{}
	accepted := map[string][]uint64{}

	for step := 0; step < 120; step++ {
		for _, name := range names {
			if rng.Intn(2) == 0 {
				continue
			}
			seq := next[name] + 1
			if err := c.IngestWS(wsMessageWithSeq(name, seq)); err != nil {
				t.Fatalf("step %d: %s seq %d refused: %v", step, name, seq, err)
			}
			next[name] = seq
			accepted[name] = append(accepted[name], seq)
		}
		c.FlushOnce(8)
	}
	if !drain(c, 8) {
		t.Fatalf("the collector did not drain: buffer %d, spool %d", c.BufferLen(), c.Stats().SpoolPending)
	}

	lake.mu.Lock()
	defer lake.mu.Unlock()
	if lake.faults < 20 {
		t.Fatalf("only %d faults were drawn: the bench does not exercise the failures", lake.faults)
	}
	for _, name := range names[:3] {
		if !sameSequences(lake.first[name], accepted[name]) {
			t.Fatalf("%s: the order of the first delivery is broken: got %v, want %v", name, lake.first[name], accepted[name])
		}
		for _, seq := range accepted[name] {
			key := fmt.Sprintf("%s/%d", name, seq)
			// stored once, plus once more for each answer that was lost after it was stored
			if lake.received[key] != 1+lake.lost[key] {
				t.Fatalf("%s: stored %d times with %d lost answers", key, lake.received[key], lake.lost[key])
			}
		}
	}
	if len(lake.first["robot-poison"]) != 0 {
		t.Fatalf("the rejected producer was stored: %v", lake.first["robot-poison"])
	}
	st := c.Stats()
	if st.Quarantined != int64(len(accepted["robot-poison"])) {
		t.Fatalf("Quarantined = %d, want %d", st.Quarantined, len(accepted["robot-poison"]))
	}
	if st.Dropped != 0 {
		t.Fatalf("Dropped = %d: nothing else may be lost", st.Dropped)
	}
	if st.TransportErrors == 0 || st.InvalidDataErrors == 0 {
		t.Fatalf("both kinds of failure must be counted: %+v", st)
	}
}
