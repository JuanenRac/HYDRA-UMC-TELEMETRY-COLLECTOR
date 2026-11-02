// HYDRA-UMC-TELEMETRY-COLLECTOR - anomaly/anomaly_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package anomaly

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/sink"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

func vib(source string, values ...float64) []telemetry.Sample {
	var out []telemetry.Sample
	for i, v := range values {
		out = append(out, telemetry.Sample{SourceID: source, Kind: "vibration", Timestamp: 1_700_000_000_000 + int64(i), Fields: map[string]float64{"accel": v, "other": 1}})
	}
	return out
}

func TestParseRules(t *testing.T) {
	rules, err := ParseRules("vibration/accel=4, motor_current/i=8")
	if err != nil || len(rules) != 2 || rules[0].Window != 4 || rules[1].Field != "i" {
		t.Fatalf("got %v %v", rules, err)
	}
	if r, err := ParseRules(""); err != nil || r != nil {
		t.Fatalf("empty: %v %v", r, err)
	}
	for _, bad := range []string{"vibration=4", "vibration/accel", "vibration/accel=1", "vibration/accel=x", "a/b=4,a/b=5", "a/b=999999"} {
		if _, err := ParseRules(bad); err == nil {
			t.Errorf("%q should be refused", bad)
		}
	}
}

type fakeDetector struct {
	mu     sync.Mutex
	paths  []string
	auth   []string
	window [][]float64
}

func (f *fakeDetector) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.paths)
}

func newFake(t *testing.T, status int) (*fakeDetector, *httptest.Server) {
	f := &fakeDetector{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Window []float64 `json:"window"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.paths = append(f.paths, r.URL.EscapedPath())
		f.auth = append(f.auth, r.Header.Get("Authorization"))
		f.window = append(f.window, body.Window)
		f.mu.Unlock()
		w.WriteHeader(status)
		if status == 200 {
			_, _ = w.Write([]byte(`{"score": 12.5, "anomalous": true}`))
		}
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 300; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}

func TestStreamsFullWindowsToTheDetector(t *testing.T) {
	f, srv := newFake(t, 200)
	rules, _ := ParseRules("vibration/accel=3")
	p := New(Options{URL: srv.URL, Token: "a-token-of-16-chars", Rules: rules})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	p.Offer(vib("robot 1", 1, 2, 3, 4, 5))
	waitFor(t, func() bool { return f.calls() == 1 })
	f.mu.Lock()
	if f.paths[0] != "/detectors/robot%201/vibration/accel/detect" || f.auth[0] != "Bearer a-token-of-16-chars" {
		t.Fatalf("path %q auth %q", f.paths[0], f.auth[0])
	}
	if len(f.window[0]) != 3 || f.window[0][2] != 3 {
		t.Fatalf("window %v", f.window[0])
	}
	f.mu.Unlock()
	p.Offer(vib("robot 1", 6)) // 4,5 + 6 completes the second window
	waitFor(t, func() bool { return f.calls() == 2 })
	waitFor(t, func() bool { return p.Counters().Anomalous == 2 })
}

func TestNotFittedAndErrorsAreCountedNotFatal(t *testing.T) {
	_, srv := newFake(t, 409)
	rules, _ := ParseRules("vibration/accel=2")
	p := New(Options{URL: srv.URL, Rules: rules})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	p.Offer(vib("a", 1, 2))
	waitFor(t, func() bool { return p.Counters().NotFitted == 1 })

	dead := New(Options{URL: "http://127.0.0.1:1", Rules: rules, Timeout: 200 * time.Millisecond})
	go dead.Run(ctx)
	dead.Offer(vib("a", 1, 2))
	waitFor(t, func() bool { return dead.Counters().Errors == 1 })
}

func TestFullQueueAndSeriesCapDropInsteadOfBlocking(t *testing.T) {
	rules, _ := ParseRules("vibration/accel=2")
	p := New(Options{URL: "http://127.0.0.1:1", Rules: rules, QueueMax: 1, MaxSeries: 1}) // Run not started
	p.Offer(vib("a", 1, 2, 3, 4))
	p.Offer(vib("b", 1, 2))
	c := p.Counters()
	if c.Windows != 2 || c.DroppedQueue != 1 || c.DroppedSeries != 2 {
		t.Fatalf("%+v", c)
	}
}

type fakeSink struct{ err error }

func (f fakeSink) Write([]telemetry.Sample) error { return f.err }

func TestTeeOffersOnlyWhatTheSinkConfirmed(t *testing.T) {
	rules, _ := ParseRules("vibration/accel=2")
	p := New(Options{URL: "http://127.0.0.1:1", Rules: rules})
	batch := vib("a", 1, 2, 3, 4)

	if err := (Tee{Primary: fakeSink{errors.New("down")}, Pub: p}).Write(batch); err == nil {
		t.Fatal("the sink error must pass through")
	}
	if c := p.Counters(); c.Windows != 0 {
		t.Fatalf("nothing was confirmed: %+v", c)
	}
	partial := &sink.PartialWriteError{Err: errors.New("half"), Succeeded: 2}
	_ = (Tee{Primary: fakeSink{partial}, Pub: p}).Write(batch)
	if c := p.Counters(); c.Windows != 1 {
		t.Fatalf("only the first two were confirmed: %+v", c)
	}
	if err := (Tee{Primary: fakeSink{nil}, Pub: p}).Write(batch[2:]); err != nil {
		t.Fatal(err)
	}
	if c := p.Counters(); c.Windows != 2 {
		t.Fatalf("%+v", c)
	}
}

func drain(p *Publisher) [][]float64 {
	var out [][]float64
	for {
		select {
		case j := <-p.queue:
			out = append(out, j.window)
		default:
			return out
		}
	}
}

func seq(from, to int) []float64 {
	var out []float64
	for i := from; i <= to; i++ {
		out = append(out, float64(i))
	}
	return out
}

func TestParseRulesOverlap(t *testing.T) {
	rules, err := ParseRulesOverlap("vibration/accel=8@50, motor_current/i=8", 25)
	if err != nil || rules[0].Overlap != 50 || rules[1].Overlap != 25 {
		t.Fatalf("got %v %v", rules, err)
	}
	if r, _ := ParseRules("a/b=4"); r[0].Overlap != 0 {
		t.Fatalf("default overlap must be 0: %v", r)
	}
	for _, bad := range []string{"a/b=4@91", "a/b=4@-1", "a/b=4@x", "a/b=4@"} {
		if _, err := ParseRules(bad); err == nil {
			t.Errorf("%q should be refused", bad)
		}
	}
	if _, err := ParseRulesOverlap("a/b=4", 91); err == nil {
		t.Error("a default overlap above 90 should be refused")
	}
	if r, err := ParseRules("a/b=4@90"); err != nil || r[0].Overlap != 90 {
		t.Errorf("90 is allowed: %v %v", r, err)
	}
}

func TestNoOverlapIsTheOldBehaviour(t *testing.T) {
	p := New(Options{URL: "http://x", Rules: []Rule{{Kind: "vibration", Field: "accel", Window: 4}}})
	p.Offer(vib("r", seq(1, 9)...))
	got := drain(p)
	if len(got) != 2 || len(got[0]) != 4 || got[0][0] != 1 || got[1][0] != 5 || got[1][3] != 8 {
		t.Fatalf("got %v", got)
	}
}

func TestOverlapHalfSlidesTheWindow(t *testing.T) {
	p := New(Options{URL: "http://x", Rules: []Rule{{Kind: "vibration", Field: "accel", Window: 4, Overlap: 50}}})
	p.Offer(vib("r", seq(1, 8)...))
	got := drain(p)
	// windows 1-4, 3-6, 5-8: every sample stored once, only the hop of new values per window
	if len(got) != 3 || got[0][0] != 1 || got[1][0] != 3 || got[2][0] != 5 || got[2][3] != 8 {
		t.Fatalf("got %v", got)
	}
	if p.Counters().Windows != 3 {
		t.Fatalf("windows %d", p.Counters().Windows)
	}
}

func TestOverlapLimitsAndBoundedSeries(t *testing.T) {
	p := New(Options{URL: "http://x", QueueMax: 100000, Rules: []Rule{{Kind: "vibration", Field: "accel", Window: 2, Overlap: 90}}})
	p.Offer(vib("r", seq(1, 20)...))
	got := drain(p)
	// 90% of 2 values keeps 1: a hop of one value, so 19 windows
	if len(got) != 19 || got[18][1] != 20 {
		t.Fatalf("got %d windows", len(got))
	}
	big := New(Options{URL: "http://x", Rules: []Rule{{Kind: "vibration", Field: "accel", Window: 100, Overlap: 90}}})
	for i := 0; i < 50; i++ {
		big.Offer(vib("r", seq(1, 37)...))
		drain(big)
		for _, v := range big.series {
			if len(v) >= 100 {
				t.Fatalf("series grew: len %d", len(v))
			}
		}
	}
}

func TestOverlapDoesNotResendConfirmedSamples(t *testing.T) {
	// the sink confirms 3 of 6: only those are offered; the retry offers the other 3 once
	p := New(Options{URL: "http://x", Rules: []Rule{{Kind: "vibration", Field: "accel", Window: 4, Overlap: 50}}})
	batch := vib("r", seq(1, 6)...)
	tee := Tee{Primary: fakeSink{&sink.PartialWriteError{Err: errors.New("half"), Succeeded: 3}}, Pub: p}
	if err := tee.Write(batch); err == nil {
		t.Fatal("expected the partial error")
	}
	if len(drain(p)) != 0 {
		t.Fatal("3 samples cannot fill a window of 4")
	}
	tee.Primary = fakeSink{}
	if err := tee.Write(batch[3:]); err != nil {
		t.Fatal(err)
	}
	got := drain(p)
	if len(got) != 2 || got[0][0] != 1 || got[0][3] != 4 || got[1][0] != 3 || got[1][3] != 6 {
		t.Fatalf("got %v", got)
	}
}

func TestOverlapQueueStaysBounded(t *testing.T) {
	p := New(Options{URL: "http://x", QueueMax: 3, Rules: []Rule{{Kind: "vibration", Field: "accel", Window: 4, Overlap: 75}}})
	p.Offer(vib("r", seq(1, 400)...))
	if len(p.queue) != 3 || p.Counters().DroppedQueue == 0 {
		t.Fatalf("queue %d dropped %d", len(p.queue), p.Counters().DroppedQueue)
	}
}
