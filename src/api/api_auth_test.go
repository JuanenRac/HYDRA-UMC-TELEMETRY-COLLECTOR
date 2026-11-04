// HYDRA-UMC-TELEMETRY-COLLECTOR - api/api_auth_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/collector"
)

const validWSBody = `{"sourceId":"robot-1","kind":"motor_temp","timestamp":1700000000000,"fields":{"value":1}}`

func post(s *Server, path, body, authorization string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestIngestRequiresTheTokenWhenOneIsConfigured(t *testing.T) {
	s := NewWithOptions(collector.New(10, &memorySink{}), Options{Token: "0123456789abcdef"})
	for _, path := range []string{"/ingest/ws", "/ingest/can"} {
		if rec := post(s, path, validWSBody, ""); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s without a token: status = %d, want 401", path, rec.Code)
		}
		if rec := post(s, path, validWSBody, "Bearer wrong-token-value"); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s with a wrong token: status = %d, want 401", path, rec.Code)
		}
		if rec := post(s, path, validWSBody, "Basic 0123456789abcdef"); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s with another scheme: status = %d, want 401", path, rec.Code)
		}
	}
	if rec := post(s, "/ingest/ws", validWSBody, "Bearer 0123456789abcdef"); rec.Code != http.StatusAccepted {
		t.Fatalf("a correct token must ingest: status = %d, body = %s", rec.Code, rec.Body.String())
	}
	rec := post(s, "/ingest/ws", validWSBody, "")
	if rec.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Errorf("a 401 must say which scheme to use, got %q", rec.Header().Get("WWW-Authenticate"))
	}
}

func TestStatsAndMetricsStayOpenWithAToken(t *testing.T) {
	s := NewWithOptions(collector.New(10, &memorySink{}), Options{Token: "0123456789abcdef"})
	for _, path := range []string{"/stats", "/metrics"} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s: status = %d, want 200", path, rec.Code)
		}
	}
}

func TestIngestWithoutATokenStaysOpen(t *testing.T) {
	s := New(collector.New(10, &memorySink{}))
	if rec := post(s, "/ingest/ws", validWSBody, ""); rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (the original open behaviour)", rec.Code)
	}
}

func TestAnOversizedBodyIsRefusedWith413(t *testing.T) {
	s := NewWithOptions(collector.New(10, &memorySink{}), Options{MaxBodyBytes: 256})
	big := `{"sourceId":"robot-1","kind":"motor_temp","timestamp":1700000000000,"fields":{"value":1},"pad":"` +
		strings.Repeat("x", 4096) + `"}`
	for _, path := range []string{"/ingest/ws", "/ingest/can"} {
		if rec := post(s, path, big, ""); rec.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("%s: status = %d, want 413 (body = %s)", path, rec.Code, rec.Body.String())
		}
	}
	// a body under the cap still goes through
	if rec := post(s, "/ingest/ws", validWSBody, ""); rec.Code != http.StatusAccepted {
		t.Fatalf("a small body must still be accepted: status = %d", rec.Code)
	}
}

func TestMalformedJSONIsStillA400(t *testing.T) {
	s := New(collector.New(10, &memorySink{}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ingest/can", bytes.NewReader([]byte("{not json")))
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestAMetricsTokenProtectsStatsAndMetricsOnly(t *testing.T) {
	s := NewWithOptions(collector.New(10, &memorySink{}), Options{Token: "0123456789abcdef", MetricsToken: "scrape-token-16chars"})
	for _, path := range []string{"/stats", "/metrics"} {
		for _, header := range []string{"", "Bearer wrong-token-value-x", "Bearer 0123456789abcdef"} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			if header != "" {
				req.Header.Set("Authorization", header)
			}
			s.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Errorf("GET %s with %q: status = %d, want 401 with WWW-Authenticate", path, header, rec.Code)
			}
		}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer scrape-token-16chars")
		s.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s with the metrics token: status = %d, want 200", path, rec.Code)
		}
	}
	// the scrape token does not allow writing, and the ingest token is still the one for writing
	if rec := post(s, "/ingest/ws", validWSBody, "Bearer scrape-token-16chars"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("the metrics token must not allow ingest: status = %d", rec.Code)
	}
	if rec := post(s, "/ingest/ws", validWSBody, "Bearer 0123456789abcdef"); rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
}

func TestStatsAndMetricsExposeTheNewSignals(t *testing.T) {
	s := New(collector.New(10, &memorySink{}))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, name := range []string{"evicted_total", "spooled_total", "spool_pending", "last_flush_milliseconds", "max_flush_milliseconds", "oldest_sample_age_milliseconds"} {
		if !strings.Contains(body, "hydra_umc_telemetry_collector_"+name) {
			t.Errorf("/metrics lacks %s", name)
		}
	}
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	for _, key := range []string{"evicted", "lastFlushMs", "maxFlushMs", "oldestSampleAgeMs", "spooled", "spoolPending", "spoolBytes"} {
		if !strings.Contains(rec.Body.String(), `"`+key+`"`) {
			t.Errorf("/stats lacks %s", key)
		}
	}
}

func getHealth(s *Server, remoteAddr, authorization string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = remoteAddr
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestHealthIsClosedByDefault(t *testing.T) {
	s := NewWithOptions(collector.New(10, &memorySink{}), Options{Token: "0123456789abcdef"})
	if rec := getHealth(s, "127.0.0.1:5000", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("closed health answered %d, want 404", rec.Code)
	}
}

func TestOpenHealthAnswersLoopbackWithTheSharedBody(t *testing.T) {
	s := NewWithOptions(collector.New(10, &memorySink{}), Options{HealthOpen: true, Service: "svc", Version: "9.9.9"})
	for _, addr := range []string{"127.0.0.1:5000", "[::1]:5000"} {
		rec := getHealth(s, addr, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: got %d, want 200", addr, rec.Code)
		}
		body := rec.Body.String()
		for _, want := range []string{`"status":"ok"`, `"service":"svc"`, `"version":"9.9.9"`} {
			if !strings.Contains(body, want) {
				t.Fatalf("%s: body %q lacks %s", addr, body, want)
			}
		}
	}
}

func TestOpenHealthNeedsATokenOffLoopback(t *testing.T) {
	s := NewWithOptions(collector.New(10, &memorySink{}), Options{HealthOpen: true, Token: "0123456789abcdef", MetricsToken: "fedcba9876543210"})
	if rec := getHealth(s, "192.0.2.7:5000", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: got %d, want 401", rec.Code)
	}
	if rec := getHealth(s, "192.0.2.7:5000", "Bearer wrong"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: got %d, want 401", rec.Code)
	}
	for _, token := range []string{"0123456789abcdef", "fedcba9876543210"} {
		if rec := getHealth(s, "192.0.2.7:5000", "Bearer "+token); rec.Code != http.StatusOK {
			t.Fatalf("right token: got %d, want 200", rec.Code)
		}
	}
}

func TestOpenHealthWithoutATokenNeverAnswersOffLoopback(t *testing.T) {
	s := NewWithOptions(collector.New(10, &memorySink{}), Options{HealthOpen: true})
	if rec := getHealth(s, "192.0.2.7:5000", "Bearer anything"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", rec.Code)
	}
}
