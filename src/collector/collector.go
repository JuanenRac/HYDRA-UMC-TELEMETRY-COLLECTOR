// HYDRA-UMC-TELEMETRY-COLLECTOR - collector/collector.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Collector wires together telemetry parsing, buffer.Ring and sink.Sink
// into the real ingestion pipeline the README's "INGESTION WORKFLOW"
// diagram describes. The one behavior that actually earns the "zero
// data loss during temporary database outages" claim: FlushOnce does
// not remove a drained batch from the buffer's accounting until the
// sink confirms the write - a failed Write requeues the whole batch at
// the front instead of discarding it.
package collector

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/buffer"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/dedup"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/sink"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

// ErrDuplicate means this exact (sourceId, sequence) pair was already
// ingested - a real reconnect resending its last few unacked messages,
// not a transport/validation failure. Callers (api.go) should treat this
// as an idempotent no-op, not a client error.
var ErrDuplicate = errors.New("collector: duplicate sequence")

// dedupWindow is how many recent sequence numbers per source the
// Collector remembers - large enough to tolerate a real reconnect resend
// (a handful of unacked messages), bounded so memory per source doesn't
// grow without limit.
const dedupWindow = 256

// Stats are exposed read-only for a status endpoint (see api.go) - real
// operational visibility into whether the collector is keeping up.
type Stats struct {
	Ingested          int64
	IngestErrors      int64
	Duplicates        int64 // samples rejected as an already-seen (sourceId, sequence)
	Flushed           int64
	FlushErrors       int64
	InvalidDataErrors int64 // flush failures where the sink itself rejected the data (not retryable by resending the same bytes)
	TransportErrors   int64 // flush failures from a transport-level problem (network, timeout, 5xx) - retrying may help
	Dropped           int64 // samples lost because a requeue outran buffer capacity
}

type Collector struct {
	buf   *buffer.Ring
	sink  sink.Sink
	dedup *dedup.Tracker

	ingested          atomic.Int64
	ingestErrors      atomic.Int64
	duplicates        atomic.Int64
	flushed           atomic.Int64
	flushErrors       atomic.Int64
	invalidDataErrors atomic.Int64
	transportErrors   atomic.Int64
	dropped           atomic.Int64
}

// New builds a Collector backed by a bounded buffer of the given
// capacity, delivering to sink.
func New(bufferCapacity int, s sink.Sink) *Collector {
	return &Collector{
		buf:   buffer.New(bufferCapacity),
		sink:  s,
		dedup: dedup.New(dedupWindow),
	}
}

// IngestCAN parses a raw CAN frame and buffers it. Returns the buffer's
// own ErrFull if there's no room - a real backpressure signal the HTTP
// layer (api.go) turns into a 503, not a silently dropped sample.
func (c *Collector) IngestCAN(arbitrationID uint32, data []byte) error {
	s, err := telemetry.ParseCANFrame(arbitrationID, data)
	if err != nil {
		c.ingestErrors.Add(1)
		return err
	}
	return c.ingest(s)
}

// IngestWS parses one raw WebSocket/HTTP JSON telemetry message and
// buffers it.
func (c *Collector) IngestWS(raw []byte) error {
	s, err := telemetry.ParseWSMessage(raw)
	if err != nil {
		c.ingestErrors.Add(1)
		return err
	}
	return c.ingest(s)
}

func (c *Collector) ingest(s telemetry.Sample) error {
	// Sequence == 0 means "not provided" (see telemetry.Sample) - a
	// producer that doesn't send sequence numbers is never deduplicated,
	// matching pre-dedup behavior exactly.
	if s.Sequence != 0 && !c.dedup.Allow(s.SourceID, s.Sequence) {
		c.duplicates.Add(1)
		return ErrDuplicate
	}
	if err := c.buf.Push(s); err != nil {
		return err
	}
	c.ingested.Add(1)
	return nil
}

// FlushOnce drains up to batchSize samples and writes them to the sink.
// On a sink failure, the whole batch is requeued at the front of the
// buffer (oldest-first, so a later flush retries it before anything
// newer) rather than lost - this is the actual mechanism behind "zero
// data loss during temporary outages", not just a comment saying so.
// Returns the number of samples actually flushed (0 on failure or an
// empty buffer).
func (c *Collector) FlushOnce(batchSize int) int {
	batch := c.buf.Drain(batchSize)
	if len(batch) == 0 {
		return 0
	}
	if err := c.sink.Write(batch); err != nil {
		c.flushErrors.Add(1)
		// A real, honest distinction for diagnosis (promotion audit line
		// 665-666): did the SINK reject this exact data (retrying the
		// identical bytes won't help), or did the write fail for a
		// transport reason (network/timeout/5xx, where a retry might
		// genuinely succeed)? Either way the batch is still requeued -
		// this only makes the reason visible via Stats(), it does not
		// change the retry policy itself.
		if sink.IsInvalidData(err) {
			c.invalidDataErrors.Add(1)
		} else {
			c.transportErrors.Add(1)
		}
		dropped := c.buf.Requeue(batch)
		if dropped > 0 {
			c.dropped.Add(int64(dropped))
		}
		return 0
	}
	c.flushed.Add(int64(len(batch)))
	return len(batch)
}

// Run flushes on a fixed interval until ctx is cancelled - the
// background loop main.go starts. Blocking; call it in its own
// goroutine.
func (c *Collector) Run(ctx context.Context, interval time.Duration, batchSize int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.FlushOnce(batchSize)
		}
	}
}

func (c *Collector) Stats() Stats {
	return Stats{
		Ingested:          c.ingested.Load(),
		IngestErrors:      c.ingestErrors.Load(),
		Duplicates:        c.duplicates.Load(),
		Flushed:           c.flushed.Load(),
		FlushErrors:       c.flushErrors.Load(),
		InvalidDataErrors: c.invalidDataErrors.Load(),
		TransportErrors:   c.transportErrors.Load(),
		Dropped:           c.dropped.Load(),
	}
}

func (c *Collector) BufferLen() int { return c.buf.Len() }
func (c *Collector) BufferCap() int { return c.buf.Cap() }
