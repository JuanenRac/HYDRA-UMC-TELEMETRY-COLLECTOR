// HYDRA-UMC-TELEMETRY-COLLECTOR - collector/collector_simulation_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package collector

// Simulation bench: the real Collector (and the real disk spool) with a simulated fleet of robots whose producers resend, restart and
// fall silent, and a simulated data lake that flaps on a fixed seed. No hardware is involved. What the bench proves is the promise of the
// collector: every sample it accepted is delivered exactly once and in the order of its producer, or is counted, and nothing else is.

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

const benchSeed = 20260101

// delivery remembers what the simulated data lake received, per producer, in the order it arrived.
type delivery struct {
	mu   sync.Mutex
	got  map[string][]uint64
	down bool
}

func (d *delivery) Write(batch []telemetry.Sample) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.down {
		return errSimulatedOutage
	}
	for _, s := range batch {
		d.got[s.SourceID] = append(d.got[s.SourceID], s.Sequence)
	}
	return nil
}

func (d *delivery) setDown(v bool) {
	d.mu.Lock()
	d.down = v
	d.mu.Unlock()
}

type simulatedOutage struct{}

func (simulatedOutage) Error() string { return "simulated outage" }

var errSimulatedOutage = simulatedOutage{}

func newDelivery() *delivery { return &delivery{got: map[string][]uint64{}} }

// fleet is a set of producers with a monotonic counter each; resend() repeats the last message of a producer, as a reconnect does.
type fleet struct {
	next map[string]uint64
	last map[string]uint64
	rng  *rand.Rand
}

func newFleet(robots int, seed int64) *fleet {
	f := &fleet{next: map[string]uint64{}, last: map[string]uint64{}, rng: rand.New(rand.NewSource(seed))}
	for i := 0; i < robots; i++ {
		f.next[robotName(i)] = 1
	}
	return f
}

func robotName(i int) string { return "robot-" + string(rune('a'+i%26)) + string(rune('a'+i/26)) }

func (f *fleet) names() []string {
	out := make([]string, 0, len(f.next))
	for i := 0; i < len(f.next); i++ {
		out = append(out, robotName(i))
	}
	return out
}

func TestSimulation_AFleetThroughAFlappingLakeLosesNothingAndKeepsEachProducersOrder(t *testing.T) {
	lake := newDelivery()
	c := New(64, lake)
	c.SetSpool(newSpool(t, 8<<20))
	f := newFleet(12, benchSeed)

	accepted := map[string][]uint64{}
	for step := 0; step < 400; step++ {
		// the lake goes down and up on a fixed schedule drawn from the seed
		if f.rng.Intn(7) == 0 {
			lake.setDown(!lake.down)
		}
		for _, name := range f.names() {
			if f.rng.Intn(3) != 0 {
				continue // this robot is silent this step
			}
			seq := f.next[name]
			if err := c.IngestWS(wsMessageWithSeq(name, seq)); err != nil {
				t.Fatalf("step %d: %s seq %d refused with a spool behind the buffer: %v", step, name, seq, err)
			}
			accepted[name] = append(accepted[name], seq)
			f.last[name] = seq
			f.next[name] = seq + 1
			if f.rng.Intn(5) == 0 {
				// the producer reconnects and resends its last message: it must be counted once
				if err := c.IngestWS(wsMessageWithSeq(name, seq)); err == nil {
					t.Fatalf("step %d: the resent message %s/%d was accepted twice", step, name, seq)
				}
			}
		}
		c.FlushOnce(16)
	}

	lake.setDown(false)
	for guard := 0; (c.BufferLen() > 0 || c.Stats().SpoolPending > 0) && guard < 2000; guard++ {
		c.FlushOnce(16)
	}
	if c.BufferLen() != 0 || c.Stats().SpoolPending != 0 {
		t.Fatalf("the collector did not drain: buffer %d, spool %d", c.BufferLen(), c.Stats().SpoolPending)
	}

	lake.mu.Lock()
	defer lake.mu.Unlock()
	total := 0
	for name, want := range accepted {
		got := lake.got[name]
		if len(got) != len(want) {
			t.Fatalf("%s: delivered %d samples, accepted %d", name, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s: the producer's order is broken at %d: got %d, want %d", name, i, got[i], want[i])
			}
		}
		total += len(got)
	}
	st := c.Stats()
	if st.Dropped != 0 || st.Quarantined != 0 {
		t.Fatalf("stats = %+v: nothing accepted may be dropped or quarantined", st)
	}
	if st.Duplicates == 0 {
		t.Fatalf("the simulation resent messages but none was counted as a duplicate: %+v", st)
	}
	if st.Spooled == 0 {
		t.Fatalf("the lake was down for a long time but nothing went to the spool: %+v", st)
	}
	if total == 0 || st.Flushed == 0 {
		t.Fatalf("nothing was delivered: total %d, Flushed %d", total, st.Flushed)
	}
}

func TestSimulation_WithoutASpoolAnOutageRefusesInsteadOfLosing(t *testing.T) {
	lake := newDelivery()
	c := New(32, lake)
	f := newFleet(8, benchSeed+1)

	accepted, refused := map[string][]uint64{}, 0
	lake.setDown(true)
	for step := 0; step < 60; step++ {
		for _, name := range f.names() {
			seq := f.next[name]
			if err := c.IngestWS(wsMessageWithSeq(name, seq)); err != nil {
				refused++
				continue // a refused sample stays with its producer, which sends it again with the same number
			}
			accepted[name] = append(accepted[name], seq)
			f.next[name] = seq + 1
		}
		c.FlushOnce(8)
	}
	if refused == 0 {
		t.Fatal("the buffer filled during the outage but nothing was refused")
	}
	if c.Stats().Dropped != 0 {
		t.Fatalf("Dropped = %d: backpressure must replace loss", c.Stats().Dropped)
	}
	lake.setDown(false)
	for guard := 0; c.BufferLen() > 0 && guard < 500; guard++ {
		c.FlushOnce(8)
	}
	lake.mu.Lock()
	defer lake.mu.Unlock()
	for name, want := range accepted {
		got := lake.got[name]
		if len(got) != len(want) {
			t.Fatalf("%s: delivered %d, accepted %d", name, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s: order broken at %d", name, i)
			}
		}
	}
	// the producers now send again what was refused, with the numbers they kept: all of it must be accepted once the lake is back
	for _, name := range f.names() {
		if err := c.IngestWS(wsMessageWithSeq(name, f.next[name])); err != nil {
			t.Fatalf("%s: a sample refused during the outage was refused again after it: %v", name, err)
		}
	}
}

func TestSimulation_ARestartedProducerIsAcceptedAgainAndCountedOnce(t *testing.T) {
	lake := newDelivery()
	c := New(256, lake)
	for seq := uint64(1); seq <= 50; seq++ {
		if err := c.IngestWS(wsMessageWithSeq("robot-aa", seq)); err != nil {
			t.Fatal(err)
		}
	}
	c.FlushOnce(256)
	// the producer restarts: its counter starts again at 1
	for seq := uint64(1); seq <= 10; seq++ {
		if err := c.IngestWS(wsMessageWithSeq("robot-aa", seq)); err != nil {
			t.Fatalf("after the restart, seq %d was refused: %v", seq, err)
		}
	}
	c.FlushOnce(256)
	st := c.Stats()
	if st.Resets != 1 {
		t.Fatalf("Resets = %d, want 1", st.Resets)
	}
	lake.mu.Lock()
	defer lake.mu.Unlock()
	if len(lake.got["robot-aa"]) != 60 {
		t.Fatalf("delivered %d samples, want 60 (50 before the restart, 10 after)", len(lake.got["robot-aa"]))
	}
}

func TestSimulation_GarbageFromTheWireNeverReachesTheLake(t *testing.T) {
	lake := newDelivery()
	c := New(64, lake)
	rng := rand.New(rand.NewSource(benchSeed + 2))
	bad := [][]byte{
		nil,
		[]byte(""),
		[]byte("not json"),
		[]byte(`{"sourceId":"","kind":"k","timestamp":1700000000000,"fields":{"v":1}}`),
		[]byte(`{"sourceId":"r","kind":"","timestamp":1700000000000,"fields":{"v":1}}`),
		[]byte(`{"sourceId":"r","kind":"k","timestamp":1700000000000}`),
		[]byte(`{"sourceId":"r","kind":"k","timestamp":-5,"fields":{"v":1}}`),
		[]byte(`{"sourceId":"r","kind":"k","timestamp":1700000000000,"fields":{"v":"hot"}}`),
		[]byte(`[1,2,3]`),
		[]byte(`{"sourceId":123}`),
	}
	for i := 0; i < 200; i++ {
		raw := make([]byte, rng.Intn(120))
		for j := range raw {
			raw[j] = byte(rng.Intn(256))
		}
		bad = append(bad, raw)
	}
	for i, raw := range bad {
		if err := c.IngestWS(raw); err == nil {
			t.Fatalf("garbage %d was accepted: %q", i, raw)
		}
	}
	if c.BufferLen() != 0 {
		t.Fatalf("garbage reached the buffer: %d", c.BufferLen())
	}
	if c.Stats().IngestErrors == 0 {
		t.Fatal("refusals were not counted")
	}
	if got := c.FlushOnce(10); got != 0 || len(lake.got) != 0 {
		t.Fatalf("something was delivered after only garbage: %d", got)
	}
}
