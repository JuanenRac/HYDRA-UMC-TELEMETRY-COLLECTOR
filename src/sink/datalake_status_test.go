// HYDRA-UMC-TELEMETRY-COLLECTOR - sink/datalake_status_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package sink

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

func statusSink(t *testing.T, status int) *DatalakeSink {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
	t.Cleanup(server.Close)
	return NewDatalakeSink(server.URL)
}

func oneSample() []telemetry.Sample {
	return []telemetry.Sample{{SourceID: "a", Kind: "k", Timestamp: 1_700_000_000_000, Fields: map[string]float64{"v": 1}}}
}

func TestDatalakeSink_AnOversizedSampleIsPermanentlyRejectedLikeAnInvalidOne(t *testing.T) {
	err := statusSink(t, http.StatusRequestEntityTooLarge).Write(oneSample())
	if !IsInvalidData(err) {
		t.Fatalf("a 413 must be reported as invalid data (retrying the same bytes never helps), got %v", err)
	}
}

func TestDatalakeSink_AMisconfiguredTokenOrURLWaitsInsteadOfDiscardingSamples(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		err := statusSink(t, status).Write(oneSample())
		if err == nil || IsInvalidData(err) {
			t.Fatalf("HTTP %d: got %v, want a retryable error (the configuration can be fixed)", status, err)
		}
		if _, ok := AsPartialWrite(err); !ok {
			t.Fatalf("HTTP %d: the error should carry how far the batch got", status)
		}
	}
}
