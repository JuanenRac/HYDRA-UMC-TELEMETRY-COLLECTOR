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
	"sync"
	"sync/atomic"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/buffer"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/dedup"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/downsample"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/sink"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/spool"
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
	// Quarantined counts samples permanently discarded because the sink
	// itself rejected their exact content as invalid - unlike
	// Dropped (a byproduct of a full buffer during a transient outage),
	// these were never going to succeed no matter how many times they
	// were retried, so they are reported here instead of silently
	// vanishing from every other counter.
	Quarantined int64
	// Resets counts producers whose sequence counter was found to have started
	// over (a restart): the samples after it are accepted instead of being
	// rejected as duplicates until the counter caught up.
	Resets int64
	// Sources is how many producers the duplicate check currently remembers.
	Sources int
	// InFlight is how many buffered slots a batch being delivered holds.
	InFlight int
	// Evicted counts queued samples dropped to make room for a more important one when a
	// priority policy is configured (they are not part of Dropped).
	Evicted int64
	// LastFlushMs and MaxFlushMs are how long the last and the slowest delivery of a batch
	// to the sink took, successful or not.
	LastFlushMs int64
	MaxFlushMs  int64
	// OldestSampleAgeMs is how old, by its own timestamp, the sample that has waited longest
	// in the buffer is; 0 when the buffer is empty.
	OldestSampleAgeMs int64
	// Spooled counts samples written to the disk spool; SpoolPending and SpoolBytes are what it
	// holds now (all zero without a spool).
	Spooled      int64
	SpoolPending int
	SpoolBytes   int64
	// Downsampled counts samples folded into a bucket by a downsampling rule, Aggregates the buckets
	// that left as one sample, and LateBuckets the samples that arrived for a bucket older than the open one.
	Downsampled int64
	Aggregates  int64
	LateBuckets int64
	// AggregateBacklog is how many closed buckets wait for room in the buffer.
	AggregateBacklog int
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
	quarantined       atomic.Int64
	spooled           atomic.Int64
	lastFlushNanos    atomic.Int64
	maxFlushNanos     atomic.Int64

	spool *spool.File

	agg   *downsample.Aggregator
	aggMu sync.Mutex
}

// aggregateGraceMs is how long after the end of a bucket Run closes it when no later sample arrived.
const aggregateGraceMs = 2000

// SetDownsample makes the kinds named by the rules arrive as one sample per time bucket (see package
// downsample); nil keeps every sample. Call it before the collector runs.
func (c *Collector) SetDownsample(rules map[string]downsample.Rule) {
	if len(rules) == 0 {
		c.agg = nil
		return
	}
	c.agg = downsample.New(rules)
}

// accept puts a sample in the buffer, or folds it into its bucket when its kind is downsampled.
func (c *Collector) accept(s telemetry.Sample) error {
	if !c.agg.Handles(s.Kind) {
		return c.buf.Push(s)
	}
	c.aggMu.Lock()
	defer c.aggMu.Unlock()
	if c.agg.Backlog() >= c.buf.Cap() {
		return buffer.ErrFull
	}
	c.pushClosed(c.agg.Add(s))
	return nil
}

// pushClosed moves closed buckets into the buffer, oldest first, and stops at the first refusal: what did not
// fit stays pending in the aggregator. The caller holds aggMu.
func (c *Collector) pushClosed(closed []telemetry.Sample) {
	n := 0
	for _, bucket := range closed {
		if c.buf.Push(bucket) != nil {
			break
		}
		n++
	}
	c.agg.Done(n)
}

// closeBuckets closes the buckets of producers that went quiet (all of them with drain) and buffers them.
func (c *Collector) closeBuckets(drain bool) {
	if c.agg == nil {
		return
	}
	c.aggMu.Lock()
	defer c.aggMu.Unlock()
	if drain {
		c.pushClosed(c.agg.Drain())
		return
	}
	c.pushClosed(c.agg.Flush(time.Now().UnixMilli(), aggregateGraceMs))
}

// SetSpool makes a sink outage write the failed batches to the disk spool instead of keeping
// them in memory, and replays the spool before anything newer once the sink answers again.
// Call it before the collector runs. A nil spool (the default) keeps the in-memory behaviour.
func (c *Collector) SetSpool(s *spool.File) { c.spool = s }

// SetPriority sets the policy a full buffer uses to make room for a more important sample
// (higher numbers matter more); nil keeps plain backpressure. Call it before the collector runs.
func (c *Collector) SetPriority(priority func(telemetry.Sample) int) { c.buf.SetPriority(priority) }

// write delivers a batch to the sink and records how long it took.
func (c *Collector) write(batch []telemetry.Sample) error {
	started := time.Now()
	err := c.sink.Write(batch)
	elapsed := time.Since(started).Nanoseconds()
	c.lastFlushNanos.Store(elapsed)
	for {
		current := c.maxFlushNanos.Load()
		if elapsed <= current || c.maxFlushNanos.CompareAndSwap(current, elapsed) {
			break
		}
	}
	return err
}

// replaySpool delivers one batch of spooled samples. alive is false when the sink did not accept
// the batch for a transport reason, so the caller knows not to try the sink again this round.
func (c *Collector) replaySpool(batchSize int) (delivered int, alive bool) {
	entries, err := c.spool.Next(batchSize)
	if err != nil || len(entries) == 0 {
		return 0, err == nil
	}
	batch := make([]telemetry.Sample, len(entries))
	for i, e := range entries {
		batch[i] = e.Sample
	}
	if err := c.write(batch); err != nil {
		c.flushErrors.Add(1)
		if invalid, ok := sink.AsInvalidData(err); ok && invalid.Index >= 0 && invalid.Index < len(entries) {
			// the sink will never take this one: discard it (and what was confirmed before it)
			c.invalidDataErrors.Add(1)
			c.quarantined.Add(1)
			c.spool.Ack(entries[invalid.Index].End)
			return 0, true
		}
		c.transportErrors.Add(1)
		if partial, ok := sink.AsPartialWrite(err); ok && partial.Succeeded > 0 && partial.Succeeded <= len(entries) {
			c.spool.Ack(entries[partial.Succeeded-1].End)
			c.flushed.Add(int64(partial.Succeeded))
		}
		return 0, false
	}
	c.spool.Ack(entries[len(entries)-1].End)
	c.flushed.Add(int64(len(batch)))
	return len(batch), true
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

// IngestSample buffers a sample that a producer inside this process already built (the MQTT adapter). It is checked like any
// other: a sample that does not pass Validate is refused and counted as an ingest error.
func (c *Collector) IngestSample(s telemetry.Sample) error {
	if err := s.Validate(); err != nil {
		c.ingestErrors.Add(1)
		return err
	}
	return c.ingest(s)
}

func (c *Collector) ingest(s telemetry.Sample) error {
	// Sequence == 0 means "not provided" (see telemetry.Sample) - a
	// producer that doesn't send sequence numbers is never deduplicated,
	// matching pre-dedup behavior exactly.
	if s.Sequence == 0 {
		if err := c.accept(s); err != nil {
			return err
		}
		c.ingested.Add(1)
		return nil
	}
	// AllowThen (not the plain Allow+buf.Push done separately, as
	// this used to be) only commits the sequence as seen once buf.Push
	// itself actually succeeds - see that method's own header comment
	// for the real, permanent data-loss bug this closes (a sample
	// rejected once for a full buffer could never be accepted on any
	// later retry, since dedup had already marked it "seen" regardless
	// of whether it was ever really buffered).
	allowed, err := c.dedup.AllowThen(s.SourceID, s.Sequence, func() error {
		return c.accept(s)
	})
	if !allowed {
		c.duplicates.Add(1)
		return ErrDuplicate
	}
	if err != nil {
		return err
	}
	c.ingested.Add(1)
	return nil
}

// FlushOnce drains up to batchSize samples and writes them to the sink.
// On a sink failure, the unwritten part of the batch is requeued at the
// front of the buffer (oldest-first, so a later flush retries it before
// anything newer) rather than lost - this is the actual mechanism behind
// "zero data loss during temporary outages", not just a comment saying
// so.
//
// (P1):
// that used to include a sample the sink PERMANENTLY rejected as invalid
// (HTTP 400) - requeuing it put it right back at the front of the queue,
// where the very next flush attempted it again, failed the identical
// way, and requeued it again, forever. Every real, valid sample behind
// it in the queue was blocked indefinitely by one poisoned sample that
// was never going to succeed no matter how many times it was retried.
// When the sink identifies exactly which sample it rejected (see
// sink.InvalidDataError's own Index field), that one sample is dropped
// (quarantined, counted in Stats().Quarantined - never silently) and the
// REST of the batch is still requeued normally.
//
// A plain transport-level failure used to requeue the WHOLE batch
// unconditionally, including whatever prefix DatalakeSink's own
// per-sample loop had already gotten a real 202 for - that prefix got
// resent on the very next retry and landed as a duplicate row in
// DATALAKE. sink.PartialWriteError now reports exactly how far the loop
// got (see its own header comment), so only the real, never-attempted
// remainder is requeued; a Sink that can't offer that signal still falls
// back to the whole batch, the same safe default as before.
//
// Returns the number of samples actually flushed (0 on failure or an
// empty buffer).
func (c *Collector) FlushOnce(batchSize int) int {
	delivered := 0
	// With a spool, what an earlier outage left on disk goes first. If the sink is still down
	// the new batch is spooled too (below) instead of being tried against it again.
	sinkDown := false
	if c.spool != nil && c.spool.Pending() > 0 {
		var alive bool
		delivered, alive = c.replaySpool(batchSize)
		sinkDown = !alive
	}
	return delivered + c.flushBuffer(batchSize, sinkDown)
}

func (c *Collector) flushBuffer(batchSize int, sinkDown bool) int {
	// The batch keeps its slots reserved until it is settled, so what is
	// ingested while it is being written cannot take the room it needs to
	// come back to on a failure: a sink outage makes the buffer fill up (and
	// ingestion answer 503) instead of discarding the oldest samples.
	batch := c.buf.DrainReserved(batchSize)
	if len(batch) == 0 {
		return 0
	}
	if sinkDown && c.spool != nil && c.spool.Append(batch) == nil {
		c.spooled.Add(int64(len(batch)))
		c.buf.Settle(len(batch), nil)
		return 0
	}
	if err := c.write(batch); err != nil {
		c.flushErrors.Add(1)
		// A real, honest distinction for diagnosis: did the SINK reject
		// this exact data (retrying the
		// identical bytes won't help), or did the write fail for a
		// transport reason (network/timeout/5xx, where a retry might
		// genuinely succeed)?
		if invalid, ok := sink.AsInvalidData(err); ok {
			c.invalidDataErrors.Add(1)
			c.quarantined.Add(1)
			dropped := c.buf.Settle(len(batch), quarantineSample(batch, invalid.Index))
			if dropped > 0 {
				c.dropped.Add(int64(dropped))
			}
			return 0
		}
		c.transportErrors.Add(1)
		// TEL-XX: a real partial-success signal - DatalakeSink's own
		// per-sample loop reports exactly how many samples at the front
		// of the batch already got a genuine HTTP 202 before the
		// transport failure (see sink.PartialWriteError's own header
		// comment; DATALAKE's real /ingest API is single-sample, so this
		// loop position IS the partial-success signal, there is no batch
		// JSON field to read instead). Only the real, never-attempted
		// remainder needs a retry - requeuing the already-written prefix
		// too would just resend it and land as a duplicate row in
		// DATALAKE. A Sink that can't offer this signal (a generic error,
		// any Sink other than DatalakeSink) falls back to the same
		// whole-batch requeue this project always used before this type
		// existed - the safe default when "how much already succeeded"
		// genuinely isn't known.
		toRequeue := batch
		if partial, ok := sink.AsPartialWrite(err); ok && partial.Succeeded >= 0 && partial.Succeeded <= len(batch) {
			toRequeue = batch[partial.Succeeded:]
		}
		if c.spool != nil && c.spool.Append(toRequeue) == nil {
			// a long outage waits on disk, not in memory: the buffer keeps taking new samples
			c.spooled.Add(int64(len(toRequeue)))
			c.buf.Settle(len(batch), nil)
			return 0
		}
		dropped := c.buf.Settle(len(batch), toRequeue)
		if dropped > 0 {
			c.dropped.Add(int64(dropped))
		}
		return 0
	}
	c.buf.Settle(len(batch), nil)
	c.flushed.Add(int64(len(batch)))
	return len(batch)
}

// quarantineSample returns what has to be tried again after the sample at
// index was permanently rejected: the samples AFTER it, which the sink never
// got to attempt. The ones before it were each answered with a real 202 (the
// sink writes a batch in order and stops at the first failure) and are not
// sent again; the rejected one is discarded. A negative or out-of-range index
// (a sink that couldn't actually determine which sample failed) falls back to
// keeping the whole batch, the same safe behavior as a plain transport error.
func quarantineSample(batch []telemetry.Sample, index int) []telemetry.Sample {
	if index < 0 || index >= len(batch) {
		return batch
	}
	return batch[index+1:]
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
			c.closeBuckets(true)
			c.FlushOnce(batchSize)
			return
		case <-ticker.C:
			c.closeBuckets(false)
			c.FlushOnce(batchSize)
		}
	}
}

func (c *Collector) Stats() Stats {
	var oldestAge int64
	if ts, ok := c.buf.OldestTimestamp(); ok {
		if age := time.Now().UnixMilli() - ts; age > 0 {
			oldestAge = age
		}
	}
	var spoolPending int
	var spoolBytes int64
	if c.spool != nil {
		spoolPending, spoolBytes = c.spool.Pending(), c.spool.Bytes()
	}
	var folded, emitted, late int64
	var backlog int
	if c.agg != nil {
		folded, emitted, late = c.agg.Counters()
		backlog = c.agg.Backlog()
	}
	return Stats{
		Downsampled:       folded,
		Aggregates:        emitted,
		LateBuckets:       late,
		AggregateBacklog:  backlog,
		Evicted:           int64(c.buf.Evicted()),
		LastFlushMs:       c.lastFlushNanos.Load() / int64(time.Millisecond),
		MaxFlushMs:        c.maxFlushNanos.Load() / int64(time.Millisecond),
		OldestSampleAgeMs: oldestAge,
		Spooled:           c.spooled.Load(),
		SpoolPending:      spoolPending,
		SpoolBytes:        spoolBytes,
		Ingested:          c.ingested.Load(),
		IngestErrors:      c.ingestErrors.Load(),
		Duplicates:        c.duplicates.Load(),
		Flushed:           c.flushed.Load(),
		FlushErrors:       c.flushErrors.Load(),
		InvalidDataErrors: c.invalidDataErrors.Load(),
		TransportErrors:   c.transportErrors.Load(),
		Dropped:           c.dropped.Load(),
		Quarantined:       c.quarantined.Load(),
		Resets:            c.dedup.Resets(),
		Sources:           c.dedup.Sources(),
		InFlight:          c.buf.InFlight(),
	}
}

func (c *Collector) BufferLen() int { return c.buf.Len() }
func (c *Collector) BufferCap() int { return c.buf.Cap() }
