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
// tell the two apart for real diagnosis.
//
// TEL-01 (P1):
// Index is this sample's own position within the batch Write() was
// given - set by Write() itself right before returning, since writeOne()
// has no notion of "batch position". collector.go's FlushOnce uses it to
// quarantine (permanently drop, never requeue) exactly this one
// permanently-rejected sample instead of the whole batch: retrying the
// identical rejected bytes can never succeed, and requeuing them
// unconditionally used to put this same sample back at the front of the
// queue forever, blocking every real, valid sample behind it.
type InvalidDataError struct {
	Sample telemetry.Sample
	Index  int
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
	_, ok := AsInvalidData(err)
	return ok
}

// AsInvalidData extracts the real *InvalidDataError from err (or
// something it wraps), if there is one. TEL-01: collector.go uses this
// instead of the plain bool IsInvalidData so it can read Index and
// quarantine exactly the one sample that will never succeed, rather than
// the whole batch.
func AsInvalidData(err error) (*InvalidDataError, bool) {
	var invalid *InvalidDataError
	if errors.As(err, &invalid) {
		return invalid, true
	}
	return nil, false
}

// PartialWriteError wraps a real transport-level Write() failure
// (network error, timeout, non-2xx/non-400 status) with Succeeded: the
// real number of samples at the FRONT of the batch DatalakeSink's own
// per-sample loop had already gotten a genuine HTTP 202 for before the
// request at index Succeeded failed. This IS the real partial-success
// signal DATALAKE's own API provides - it has no batch /ingest endpoint
// at all (see api.py's own single-sample POST /ingest), so there is no
// JSON field to read; the loop's own position, captured at the moment it
// fails, is the actual, accurate account of what was and wasn't
// confirmed written.
//
// collector.go's own FlushOnce uses this to requeue only
// batch[Succeeded:] - the samples the sink never got to attempt - instead
// of the WHOLE batch, which used to re-send every already-written sample
// again on the very next retry, landing as a duplicate row in DATALAKE
// for each one.
type PartialWriteError struct {
	Err       error
	Succeeded int
}

func (e *PartialWriteError) Error() string { return e.Err.Error() }
func (e *PartialWriteError) Unwrap() error { return e.Err }

// AsPartialWrite extracts the real *PartialWriteError from err (or
// something it wraps), if there is one - same idiom as AsInvalidData.
// A Sink implementation that doesn't return this (a generic error, or
// any Sink other than DatalakeSink) has no accurate partial-success
// signal to offer, so the caller must fall back to the same safe
// whole-batch requeue this project always used before this type existed.
func AsPartialWrite(err error) (*PartialWriteError, bool) {
	var partial *PartialWriteError
	if errors.As(err, &partial) {
		return partial, true
	}
	return nil, false
}

// DatalakeSink writes each sample to a real HYDRA-UMC-DATALAKE instance's
// POST /ingest, one HTTP request per sample - DATALAKE's own API is
// single-sample (see its own api.py), so a "batch write" here is really
// N real requests, not one.
//
// A transport-level failure (network error, timeout, unexpected status)
// partway through a batch is wrapped in a *PartialWriteError carrying
// Succeeded (how many requests at the front of the batch already got a
// real 202) - collector.go's own FlushOnce uses that to requeue only the
// real, still-unattempted remainder, not the whole batch. Exactly-once
// delivery (idempotency keys, upserts on the DATALAKE side) is still real
// future work; a duplicate row can still happen if DATALAKE itself
// accepted a request (202) but this process crashed/lost the response
// before recording that - at-least-once with rare duplicates on a real
// crash mid-flush is the honest v0 trade-off, not at-most-once (silently
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
			wrapped := fmt.Errorf("sink: datalake: sample %d/%d (sourceId=%q kind=%q): %w",
				i+1, len(batch), s.SourceID, s.Kind, err)
			// TEL-01: writeOne() has no notion of "batch position" - stamp
			// it here, where i is known, so collector.go can quarantine
			// exactly this one sample instead of the whole batch. err is
			// the direct, unwrapped return value of writeOne() at this
			// point (the fmt.Errorf %w wrap happens above), so a plain
			// type assertion is enough - no errors.As needed yet.
			if invalid, ok := err.(*InvalidDataError); ok {
				invalid.Index = i
				return wrapped
			}
			// Every request before index i already got a real HTTP 202
			// from DATALAKE (writeOne() only returns nil on that, and the
			// loop only reaches i by way of every earlier iteration
			// returning nil) - i itself is the real, accurate count of
			// confirmed-written samples, not a guess.
			return &PartialWriteError{Err: wrapped, Succeeded: i}
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
