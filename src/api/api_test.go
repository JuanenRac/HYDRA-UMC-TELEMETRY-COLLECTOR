// HYDRA-UMC-TELEMETRY-COLLECTOR - api/api_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/collector"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

type memorySink struct {
	written [][]telemetry.Sample
}

func (m *memorySink) Write(batch []telemetry.Sample) error {
	m.written = append(m.written, batch)
	return nil
}

func TestHandleIngestWS_RealRoundTripThroughStats(t *testing.T) {
	c := collector.New(10, &memorySink{})
	s := New(c)

	body := []byte(`{"sourceId":"robot-1","kind":"motor_temp","timestamp":1700000000000,"fields":{"value":1}}`)
	req := httptest.NewRequest(http.MethodPost, "/ingest/ws", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /ingest/ws status = %d, body = %s", rec.Code, rec.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec2 := httptest.NewRecorder()
	s.ServeHTTP(rec2, req2)
	var stats map[string]any
	if err := json.Unmarshal(rec2.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decoding /stats: %v", err)
	}
	if stats["ingested"].(float64) != 1 {
		t.Fatalf("stats[ingested] = %v, want 1", stats["ingested"])
	}
	if stats["bufferLen"].(float64) != 1 {
		t.Fatalf("stats[bufferLen] = %v, want 1", stats["bufferLen"])
	}
}

func TestHandleIngestCAN_RealFrameAccepted(t *testing.T) {
	c := collector.New(10, &memorySink{})
	s := New(c)

	frame, err := telemetry.EncodeCANFrame("motor_current", 5.0)
	if err != nil {
		t.Fatalf("EncodeCANFrame: %v", err)
	}
	payload := map[string]any{
		"arbitrationId": 3,
		"data":          base64.StdEncoding.EncodeToString(frame[:]),
	}
	raw, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/ingest/can", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /ingest/can status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestHandleIngestWS_RejectsInvalidSample(t *testing.T) {
	c := collector.New(10, &memorySink{})
	s := New(c)

	req := httptest.NewRequest(http.MethodPost, "/ingest/ws", bytes.NewReader([]byte(`{"kind":"motor_temp"}`)))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a sample missing sourceId", rec.Code)
	}
}

func TestHandleIngestWS_ReportsBackpressureAs503(t *testing.T) {
	c := collector.New(1, &memorySink{})
	s := New(c)
	body := []byte(`{"sourceId":"robot-1","kind":"motor_temp","timestamp":1700000000000,"fields":{}}`)

	req1 := httptest.NewRequest(http.MethodPost, "/ingest/ws", bytes.NewReader(body))
	rec1 := httptest.NewRecorder()
	s.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("first ingest status = %d, want 202", rec1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/ingest/ws", bytes.NewReader(body))
	rec2 := httptest.NewRecorder()
	s.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("second ingest (buffer full) status = %d, want 503", rec2.Code)
	}
}

func TestHandleIngestWS_ReconnectResendIsAcceptedAsIdempotentNotError(t *testing.T) {
	c := collector.New(10, &memorySink{})
	s := New(c)
	body := []byte(`{"sourceId":"robot-1","kind":"motor_temp","timestamp":1700000000000,"fields":{},"sequence":1}`)

	req1 := httptest.NewRequest(http.MethodPost, "/ingest/ws", bytes.NewReader(body))
	rec1 := httptest.NewRecorder()
	s.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("first ingest status = %d, want 202", rec1.Code)
	}

	// A real reconnect resending the same unacked message must be a
	// harmless 200, not a 400/500 the device would treat as a real
	// failure worth alerting on.
	req2 := httptest.NewRequest(http.MethodPost, "/ingest/ws", bytes.NewReader(body))
	rec2 := httptest.NewRecorder()
	s.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("resent (duplicate) ingest status = %d, want 200", rec2.Code)
	}

	req3 := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec3 := httptest.NewRecorder()
	s.ServeHTTP(rec3, req3)
	var stats map[string]any
	if err := json.Unmarshal(rec3.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decoding /stats: %v", err)
	}
	if stats["ingested"].(float64) != 1 {
		t.Fatalf("stats[ingested] = %v, want 1 - the resend must not inflate this", stats["ingested"])
	}
	if stats["duplicates"].(float64) != 1 {
		t.Fatalf("stats[duplicates] = %v, want 1", stats["duplicates"])
	}
}

func TestHandleStats_MethodNotAllowed(t *testing.T) {
	c := collector.New(10, &memorySink{})
	s := New(c)
	req := httptest.NewRequest(http.MethodPost, "/stats", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405 for POST /stats", rec.Code)
	}
}
