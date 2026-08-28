// HYDRA-UMC-TELEMETRY-COLLECTOR - sink/datalake_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Tests against a real net/http/httptest.Server implementing DATALAKE's
// actual documented contract (POST /ingest -> 202, or an error status) -
// not a mock of the Sink interface, a real HTTP round trip.
package sink

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

func TestDatalakeSink_WritesEachSampleForReal(t *testing.T) {
	var mu sync.Mutex
	var received []telemetry.Sample

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ingest" || r.Method != http.MethodPost {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var s telemetry.Sample
		if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
			t.Errorf("decoding request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, s)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	sink := NewDatalakeSink(server.URL)
	batch := []telemetry.Sample{
		{SourceID: "robot-1", Kind: "motor_temp", Timestamp: 1000, Fields: map[string]float64{"value": 42.5}},
		{SourceID: "robot-2", Kind: "motor_current", Timestamp: 1001, Fields: map[string]float64{"value": 3.2}},
	}

	if err := sink.Write(batch); err != nil {
		t.Fatalf("Write: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 {
		t.Fatalf("DATALAKE received %d samples, want 2", len(received))
	}
	if received[0].SourceID != "robot-1" || received[1].SourceID != "robot-2" {
		t.Fatalf("received = %+v, order/content mismatch", received)
	}
}

func TestDatalakeSink_ReturnsErrorOnNon202(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	sink := NewDatalakeSink(server.URL)
	err := sink.Write([]telemetry.Sample{
		{SourceID: "robot-1", Kind: "motor_temp", Timestamp: 1000, Fields: map[string]float64{"value": 1}},
	})
	if err == nil {
		t.Fatal("expected an error when DATALAKE rejects the sample, got nil")
	}
}

func TestDatalakeSink_Classifies400AsInvalidData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("schema validation failed"))
	}))
	defer server.Close()

	sink := NewDatalakeSink(server.URL)
	err := sink.Write([]telemetry.Sample{
		{SourceID: "robot-1", Kind: "motor_temp", Timestamp: 1000, Fields: map[string]float64{"value": 1}},
	})
	if !IsInvalidData(err) {
		t.Fatalf("err = %v, want a real InvalidDataError for a 400 response", err)
	}
}

func TestDatalakeSink_ClassifiesA5xxAsNotInvalidData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	sink := NewDatalakeSink(server.URL)
	err := sink.Write([]telemetry.Sample{
		{SourceID: "robot-1", Kind: "motor_temp", Timestamp: 1000, Fields: map[string]float64{"value": 1}},
	})
	if IsInvalidData(err) {
		t.Fatalf("err = %v, a real 500 must NOT be classified as invalid data - it's a transport-level problem", err)
	}
}

func TestDatalakeSink_ClassifiesAConnectionFailureAsNotInvalidData(t *testing.T) {
	// Port 1 is real but nothing real listens there - a genuine
	// connection failure, the clearest possible transport error.
	sink := NewDatalakeSink("http://127.0.0.1:1")
	err := sink.Write([]telemetry.Sample{
		{SourceID: "robot-1", Kind: "motor_temp", Timestamp: 1000, Fields: map[string]float64{"value": 1}},
	})
	if err == nil {
		t.Fatal("expected a real connection error, got nil")
	}
	if IsInvalidData(err) {
		t.Fatalf("err = %v, a connection failure must NOT be classified as invalid data", err)
	}
}

func TestDatalakeSink_StopsAtFirstFailureInABatch(t *testing.T) {
	var mu sync.Mutex
	callCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		n := callCount
		mu.Unlock()
		if n == 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	sink := NewDatalakeSink(server.URL)
	batch := []telemetry.Sample{
		{SourceID: "a", Kind: "k", Timestamp: 1, Fields: map[string]float64{"v": 1}},
		{SourceID: "b", Kind: "k", Timestamp: 2, Fields: map[string]float64{"v": 2}},
		{SourceID: "c", Kind: "k", Timestamp: 3, Fields: map[string]float64{"v": 3}},
	}

	err := sink.Write(batch)
	if err == nil {
		t.Fatal("expected an error when the 2nd sample fails, got nil")
	}

	mu.Lock()
	defer mu.Unlock()
	// The 3rd sample must never have been attempted once the 2nd failed -
	// Write stops at the first failure (see its own doc comment on the
	// resulting requeue-and-retry, at-least-once trade-off).
	if callCount != 2 {
		t.Fatalf("DATALAKE received %d requests, want exactly 2 (stopped at the failure)", callCount)
	}
}
