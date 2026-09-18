// HYDRA-UMC-TELEMETRY-COLLECTOR - api/api.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Plain JSON/HTTP surface (stdlib net/http, no framework) over
// collector.Collector - same convention already established by
// HYDRA-UMC-JOB-DISPATCHER for this kind of ops-facing control/ingest
// surface. A real CAN bus and a real WebSocket stream from
// HYDRA-UMC-SERVER aren't available yet - POST /ingest/can and /ingest/ws let a real
// caller (or a test/curl) feed this collector genuine frames without
// needing that hardware/network dependency to prove the pipeline works.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/buffer"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/collector"
)

type Server struct {
	collector *collector.Collector
	mux       *http.ServeMux
}

func New(c *collector.Collector) *Server {
	s := &Server{collector: c, mux: http.NewServeMux()}
	s.mux.HandleFunc("/ingest/can", s.handleIngestCAN)
	s.mux.HandleFunc("/ingest/ws", s.handleIngestWS)
	s.mux.HandleFunc("/stats", s.handleStats)
	s.mux.HandleFunc("/metrics", s.handleMetrics)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// canFrameRequest mirrors one CAN frame for JSON transport - real
// hardware delivers raw bytes on the wire, but there is no real bus to
// listen to in this environment (see the package doc comment), so the
// HTTP layer is the honest stand-in.
type canFrameRequest struct {
	ArbitrationID uint32 `json:"arbitrationId"`
	Data          []byte `json:"data"` // base64 in JSON, per encoding/json's []byte convention
}

func (s *Server) handleIngestCAN(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, errors.New("use POST"))
		return
	}
	var req canFrameRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.collector.IngestCAN(req.ArbitrationID, req.Data); err != nil {
		if errors.Is(err, buffer.ErrFull) {
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
		if errors.Is(err, collector.ErrDuplicate) {
			writeJSON(w, http.StatusOK, map[string]string{"status": "duplicate"})
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "buffered"})
}

func (s *Server) handleIngestWS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, errors.New("use POST"))
		return
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.collector.IngestWS(raw); err != nil {
		if errors.Is(err, buffer.ErrFull) {
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
		if errors.Is(err, collector.ErrDuplicate) {
			writeJSON(w, http.StatusOK, map[string]string{"status": "duplicate"})
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "buffered"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, errors.New("use GET"))
		return
	}
	stats := s.collector.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"ingested":          stats.Ingested,
		"ingestErrors":      stats.IngestErrors,
		"duplicates":        stats.Duplicates,
		"flushed":           stats.Flushed,
		"flushErrors":       stats.FlushErrors,
		"invalidDataErrors": stats.InvalidDataErrors,
		"transportErrors":   stats.TransportErrors,
		"dropped":           stats.Dropped,
		"quarantined":       stats.Quarantined,
		"bufferLen":         s.collector.BufferLen(),
		"bufferCap":         s.collector.BufferCap(),
	})
}

// promMetric is one line of this handler's own real, static metric
// catalog - Name/Help/Kind are fixed per metric (Prometheus requires the
// same metric to always carry the same HELP/TYPE across scrapes), Value
// is read fresh from collector.Stats()/BufferLen()/BufferCap() every
// request, the same live counters GET /stats above already exposes -
// this is a second real encoding of that same already-tracked data, not
// a second, independently-maintained counting path.
type promMetric struct {
	Name  string
	Help  string
	Kind  string // "counter" or "gauge"
	Value int64
}

// handleMetrics serves the same live counters GET /stats does, in real
// Prometheus text exposition format (https://prometheus.io/docs/instrumenting/exposition_formats/)
// so a real Prometheus (or any OpenMetrics-compatible scraper) can poll
// this collector directly - no separate exporter process, no dependency
// added (stdlib fmt.Fprintf only, matching this package's own
// no-framework convention). Dropped/Quarantined in particular are the
// real ingestion/drop-rate signal this project's own buffer.Ring and
// FlushOnce already compute (see collector.go's own Stats() doc
// comments) - this handler only formats and exposes them, it does not
// recompute or re-derive either count.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, errors.New("use GET"))
		return
	}
	stats := s.collector.Stats()
	const prefix = "hydra_umc_telemetry_collector_"
	metrics := []promMetric{
		{prefix + "ingested_total", "Samples successfully parsed and buffered.", "counter", stats.Ingested},
		{prefix + "ingest_errors_total", "Raw messages that failed to parse (CAN frame or WS/HTTP JSON).", "counter", stats.IngestErrors},
		{prefix + "duplicates_total", "Samples rejected as an already-seen (sourceId, sequence) pair.", "counter", stats.Duplicates},
		{prefix + "flushed_total", "Samples successfully written to the sink.", "counter", stats.Flushed},
		{prefix + "flush_errors_total", "Flush attempts (batches) that failed for any reason.", "counter", stats.FlushErrors},
		{prefix + "invalid_data_errors_total", "Flush failures where the sink permanently rejected the sample's own content (not retryable by resending the same bytes).", "counter", stats.InvalidDataErrors},
		{prefix + "transport_errors_total", "Flush failures from a transport-level problem (network, timeout, 5xx) - retrying may help.", "counter", stats.TransportErrors},
		{prefix + "dropped_total", "Samples permanently lost because a requeue outran the ring buffer's own bounded capacity.", "counter", stats.Dropped},
		{prefix + "quarantined_total", "Samples permanently discarded because the sink rejected their exact content as invalid (never retried).", "counter", stats.Quarantined},
		{prefix + "buffer_length", "Samples currently sitting in the ring buffer, waiting to be flushed.", "gauge", int64(s.collector.BufferLen())},
		{prefix + "buffer_capacity", "The ring buffer's own fixed maximum capacity.", "gauge", int64(s.collector.BufferCap())},
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	for _, m := range metrics {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", m.Name, m.Help, m.Name, m.Kind, m.Name, m.Value)
	}
}
