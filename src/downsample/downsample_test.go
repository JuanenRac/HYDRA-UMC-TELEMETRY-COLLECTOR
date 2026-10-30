// HYDRA-UMC-TELEMETRY-COLLECTOR - downsample/downsample_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package downsample

import (
	"math"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

const base int64 = 1_700_000_000_000

func sample(source string, ts int64, v float64) telemetry.Sample {
	return telemetry.Sample{SourceID: source, Kind: "vibration", Timestamp: ts, Fields: map[string]float64{"rms": v}}
}

func TestParseRules(t *testing.T) {
	rules, err := ParseRules("vibration=1000:max, motor_current=100")
	if err != nil {
		t.Fatal(err)
	}
	if rules["vibration"] != (Rule{1000, Max}) || rules["motor_current"] != (Rule{100, Mean}) {
		t.Fatalf("rules = %+v", rules)
	}
	if none, err := ParseRules("  "); err != nil || none != nil {
		t.Fatalf("an empty list is no rules: %v %v", none, err)
	}
	for _, bad := range []string{"vibration", "=100", "v=abc", "v=1", "v=100:median", "v=100,v=200"} {
		if _, err := ParseRules(bad); err == nil {
			t.Errorf("%q should be refused", bad)
		}
	}
}

func TestABucketIsClosedByALaterSampleAndKeepsTheMeanAtTheStartOfTheBucket(t *testing.T) {
	a := New(map[string]Rule{"vibration": {1000, Mean}})
	for i, v := range []float64{1, 2, 3} {
		if got := a.Add(sample("m1", base+int64(i)*100, v)); len(got) != 0 {
			t.Fatalf("the bucket is still open, got %v", got)
		}
	}
	closed := a.Add(sample("m1", base+1000, 10))
	if len(closed) != 1 {
		t.Fatalf("closed = %v", closed)
	}
	if closed[0].Timestamp != base || closed[0].Fields["rms"] != 2 || closed[0].SourceID != "m1" || closed[0].Kind != "vibration" {
		t.Fatalf("aggregate = %+v", closed[0])
	}
	a.Done(1)
	if a.Backlog() != 0 {
		t.Fatal("Done should drop the pending bucket")
	}
}

func TestFunctions(t *testing.T) {
	cases := map[Function]float64{Mean: 2, Min: 1, Max: 3, Last: 3, Sum: 6}
	for fn, want := range cases {
		a := New(map[string]Rule{"vibration": {1000, fn}})
		for i, v := range []float64{1, 2, 3} {
			a.Add(sample("m1", base+int64(i), v))
		}
		out := a.Drain()
		if len(out) != 1 || math.Abs(out[0].Fields["rms"]-want) > 1e-9 {
			t.Errorf("%s: %v, want %v", fn, out, want)
		}
	}
}

func TestSourcesAndKindsHaveTheirOwnBuckets(t *testing.T) {
	a := New(map[string]Rule{"vibration": {1000, Mean}})
	a.Add(sample("m1", base, 1))
	a.Add(sample("m2", base, 5))
	out := a.Drain()
	if len(out) != 2 {
		t.Fatalf("out = %v", out)
	}
}

func TestAnUnconfirmedBucketStaysPendingForTheNextCall(t *testing.T) {
	a := New(map[string]Rule{"vibration": {1000, Mean}})
	a.Add(sample("m1", base, 1))
	if got := a.Add(sample("m1", base+1000, 2)); len(got) != 1 {
		t.Fatalf("got %v", got)
	}
	// the buffer was full: nothing confirmed, the bucket comes back
	if got := a.Add(sample("m1", base+1100, 3)); len(got) != 1 || got[0].Fields["rms"] != 1 {
		t.Fatalf("the unconfirmed bucket must be offered again, got %v", got)
	}
}

func TestASampleOlderThanTheOpenBucketLeavesAsABucketOfItsOwn(t *testing.T) {
	a := New(map[string]Rule{"vibration": {1000, Mean}})
	a.Add(sample("m1", base+2000, 1))
	got := a.Add(sample("m1", base+500, 7))
	if len(got) != 1 || got[0].Timestamp != base || got[0].Fields["rms"] != 7 {
		t.Fatalf("got %v", got)
	}
	if _, _, late := a.Counters(); late != 1 {
		t.Fatalf("late = %d", late)
	}
}

func TestFlushClosesOnlyBucketsThatEndedBeforeTheGracePeriod(t *testing.T) {
	a := New(map[string]Rule{"vibration": {1000, Mean}})
	a.Add(sample("m1", base, 1))
	if got := a.Flush(base+1500, 1000); len(got) != 0 {
		t.Fatalf("too early: %v", got)
	}
	if got := a.Flush(base+2000, 1000); len(got) != 1 {
		t.Fatalf("should be closed: %v", got)
	}
}

func TestHandlesAndNilAggregator(t *testing.T) {
	var none *Aggregator
	if none.Handles("vibration") {
		t.Fatal("a nil aggregator handles nothing")
	}
	a := New(map[string]Rule{"vibration": {1000, Mean}})
	if !a.Handles("vibration") || a.Handles("motor_temp") {
		t.Fatal("Handles must follow the rules")
	}
}

func TestTimestampsBeforeTheEpochStillFallInTheirBucket(t *testing.T) {
	if got := startOf(-1, 1000); got != -1000 {
		t.Fatalf("startOf(-1) = %d", got)
	}
	if got := startOf(base+999, 1000); got != base {
		t.Fatalf("startOf = %d", got)
	}
}
