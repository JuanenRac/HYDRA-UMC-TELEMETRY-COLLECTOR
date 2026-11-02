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
