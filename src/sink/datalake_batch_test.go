// HYDRA-UMC-TELEMETRY-COLLECTOR - sink/datalake_batch_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package sink

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

func manySamples(n int) []telemetry.Sample {
	out := make([]telemetry.Sample, n)
	for i := range out {
		out[i] = telemetry.Sample{SourceID: fmt.Sprintf("s%d", i), Kind: "k", Timestamp: 1_700_000_000_000 + int64(i), Fields: map[string]float64{"v": 1}}
	}
	return out
}

// fakeDatalake records the requests it gets and answers with the status chosen by `answer`.
type fakeDatalake struct {
	mu       sync.Mutex
	paths    []string
	batches  [][]telemetry.Sample
	singles  int
	answer   func(path string, n int) int
	requests int
}

func (f *fakeDatalake) server(t *testing.T) *DatalakeSink {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.requests++
		f.paths = append(f.paths, r.URL.Path)
		n := 1
		if r.URL.Path == "/ingest/batch" {
			var body struct {
				Samples []telemetry.Sample `json:"samples"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.batches = append(f.batches, body.Samples)
			n = len(body.Samples)
		} else {
			f.singles++
		}
		w.WriteHeader(f.answer(r.URL.Path, n))
	}))
	t.Cleanup(srv.Close)
	return NewDatalakeSink(srv.URL)
}

func TestDatalakeSink_BatchSendsOneRequestPerChunk(t *testing.T) {
	f := &fakeDatalake{answer: func(string, int) int { return http.StatusAccepted }}
	s := f.server(t)
	s.Batch, s.MaxBatch = true, 4
	if err := s.Write(manySamples(10)); err != nil {
		t.Fatal(err)
	}
	if len(f.batches) != 3 || len(f.batches[0]) != 4 || len(f.batches[2]) != 2 || f.singles != 0 {
		t.Fatalf("batches = %d (first %d), singles = %d; want 3 chunks of 4,4,2 and no single request", len(f.batches), len(f.batches[0]), f.singles)
	}
}

func TestDatalakeSink_WithoutBatchStillSendsOneRequestPerSample(t *testing.T) {
	f := &fakeDatalake{answer: func(string, int) int { return http.StatusAccepted }}
	s := f.server(t)
	if err := s.Write(manySamples(3)); err != nil {
		t.Fatal(err)
	}
	if f.singles != 3 || len(f.batches) != 0 {
		t.Fatalf("singles = %d batches = %d, want 3 and 0", f.singles, len(f.batches))
	}
}

func TestDatalakeSink_ADatalakeWithoutTheBatchRouteIsRememberedAndUsedSampleBySample(t *testing.T) {
	f := &fakeDatalake{answer: func(path string, _ int) int {
		if path == "/ingest/batch" {
			return http.StatusNotFound
		}
		return http.StatusAccepted
	}}
	s := f.server(t)
	s.Batch = true
	if err := s.Write(manySamples(3)); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(manySamples(2)); err != nil {
		t.Fatal(err)
	}
	batchTries := 0
	for _, p := range f.paths {
		if p == "/ingest/batch" {
			batchTries++
		}
	}
	if batchTries != 1 || f.singles != 5 {
		t.Fatalf("batch tries = %d singles = %d, want the route tried once and 5 single requests", batchTries, f.singles)
	}
}

func TestDatalakeSink_ARejectedBatchIsRepeatedSampleBySampleToFindTheCulprit(t *testing.T) {
	calls := 0
	f := &fakeDatalake{answer: func(path string, _ int) int {
		if path == "/ingest/batch" {
			return http.StatusBadRequest
		}
		calls++
		if calls == 3 {
			return http.StatusBadRequest // the third sample is the invalid one
		}
		return http.StatusAccepted
	}}
	s := f.server(t)
	s.Batch = true
	err := s.Write(manySamples(5))
	invalid, ok := AsInvalidData(err)
	if !ok || invalid.Index != 2 {
		t.Fatalf("err = %v, want invalid data at index 2", err)
	}
}

func TestDatalakeSink_ATransientBatchFailureReportsHowManyChunksWereConfirmed(t *testing.T) {
	n := 0
	f := &fakeDatalake{answer: func(string, int) int {
		n++
		if n == 2 {
			return http.StatusServiceUnavailable
		}
		return http.StatusAccepted
	}}
	s := f.server(t)
	s.Batch, s.MaxBatch = true, 4
	err := s.Write(manySamples(10))
	partial, ok := AsPartialWrite(err)
	if !ok || partial.Succeeded != 4 {
		t.Fatalf("err = %v, want a partial write with 4 confirmed", err)
	}
	if IsInvalidData(err) {
		t.Fatal("a 503 is transient, not invalid data")
	}
}

func TestDatalakeSink_PermanentStatusesDiscardInsteadOfRetryingForever(t *testing.T) {
	f := &fakeDatalake{answer: func(string, int) int { return http.StatusNotFound }}
	s := f.server(t)
	s.PermanentStatuses = map[int]bool{http.StatusNotFound: true}
	if err := s.Write(oneSample()); !IsInvalidData(err) {
		t.Fatalf("a 404 listed as permanent must be invalid data, got %v", err)
	}
	// and without the list it still waits, as before
	plain := (&fakeDatalake{answer: func(string, int) int { return http.StatusNotFound }}).server(t)
	if err := plain.Write(oneSample()); err == nil || IsInvalidData(err) {
		t.Fatalf("without the list a 404 must stay retryable, got %v", err)
	}
}
