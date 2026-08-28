// HYDRA-UMC-TELEMETRY-COLLECTOR - sink/datalake.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The real sink ConsoleSink's own doc comment said was still missing:
// HYDRA-UMC-DATALAKE now has a real `POST /ingest` endpoint (see that
// project's own src/hydra_umc_datalake/api.py), so this writes each
// sample there for real over plain HTTP/JSON - the same normalized
// shape (sourceId/kind/timestamp/fields) both projects already agree
// on, no translation layer needed.
package sink

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

// InvalidDataError means DATALAKE itself rejected this exact sample's
// content as invalid (a real HTTP 400) - as opposed to a transport-level
// failure (network error, timeout, 5xx) where retrying the identical
// bytes might genuinely succeed later. A distinct type so a caller can
// tell the two apart for real diagnosis (promotion audit line 665-666) -
// collector.go's requeue-and-retry policy is unchanged either way, this
// only makes the reason visible.
type InvalidDataError struct {
	Sample telemetry.Sample
	Status int
	Body   string
}

func (e *InvalidDataError) Error() string {
	return fmt.Sprintf(
		"datalake rejected sample (sourceId=%q kind=%q) as invalid: HTTP %d: %s",
		e.Sample.SourceID, e.Sample.Kind, e.Status, e.Body,
	)
}

// IsInvalidData reports whether err (or something it wraps) is a real
// InvalidDataError, as opposed to a transport-level failure.
func IsInvalidData(err error) bool {
	var invalid *InvalidDataError
	return errors.As(err, &invalid)
}

// DatalakeSink writes each sample to a real HYDRA-UMC-DATALAKE instance's
// POST /ingest, one HTTP request per sample - DATALAKE's own API is
// single-sample (see its own api.py), so a "batch write" here is really
// N real requests, not one.
//
// Honest limitation, not silently glossed over: if a batch partially
// succeeds before a request fails, Write returns an error and
// collector.go requeues the WHOLE batch (see collector.go's own
// all-or-nothing contract for Sink) - the already-written samples get
// re-sent on retry, landing as duplicate rows in DATALAKE rather than
// being lost. Exactly-once delivery (idempotency keys, upserts on the
// DATALAKE side) is real future work, not attempted here - see
// mejoras_futuras.txt. At-least-once with occasional duplicates on a
// real outage is the honest v0 trade-off, not at-most-once (silently
// dropping data on a retry).
type DatalakeSink struct {
	// BaseURL is the DATALAKE instance's address, e.g. "http://localhost:8095".
	BaseURL string
	Client  *http.Client
}

// NewDatalakeSink returns a DatalakeSink with a real, bounded-timeout
// HTTP client (5s per request) - a hung DATALAKE must not hang the
// collector's own flush loop forever.
func NewDatalakeSink(baseURL string) *DatalakeSink {
	return &DatalakeSink{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (d *DatalakeSink) Write(batch []telemetry.Sample) error {
	for i, s := range batch {
		if err := d.writeOne(s); err != nil {
			return fmt.Errorf("sink: datalake: sample %d/%d (sourceId=%q kind=%q): %w",
				i+1, len(batch), s.SourceID, s.Kind, err)
		}
	}
	return nil
}

func (d *DatalakeSink) writeOne(s telemetry.Sample) error {
	body, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, d.BaseURL+"/ingest", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.Client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	// DATALAKE's own api.py returns 202 Accepted on a real successful
	// ingest (see its POST /ingest handler) - anything else is a real
	// failure, not assumed to be fine.
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if resp.StatusCode == http.StatusBadRequest {
			// DATALAKE's own api.py returns 400 specifically when it
			// validated and rejected this sample's content - a real,
			// permanent rejection of these exact bytes, not a transient
			// problem retrying will fix.
			return &InvalidDataError{Sample: s, Status: resp.StatusCode, Body: string(body)}
		}
		return fmt.Errorf("datalake returned HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
