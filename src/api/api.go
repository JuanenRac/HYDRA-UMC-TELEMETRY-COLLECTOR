// HYDRA-UMC-TELEMETRY-COLLECTOR - api/api.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Plain JSON/HTTP surface (stdlib net/http, no framework) over
// collector.Collector - same convention already established by
// HYDRA-UMC-JOB-DISPATCHER for this kind of ops-facing control/ingest
// surface. A real CAN bus and a real WebSocket stream from
// HYDRA-UMC-SERVER aren't available in this environment (see
// mejoras_futuras.txt) - POST /ingest/can and /ingest/ws let a real
// caller (or a test/curl) feed this collector genuine frames without
// needing that hardware/network dependency to prove the pipeline works.
package api

import (
	"encoding/json"
	"errors"
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
		"ingested":     stats.Ingested,
		"ingestErrors": stats.IngestErrors,
		"flushed":      stats.Flushed,
		"flushErrors":  stats.FlushErrors,
		"dropped":      stats.Dropped,
		"bufferLen":    s.collector.BufferLen(),
		"bufferCap":    s.collector.BufferCap(),
	})
}
