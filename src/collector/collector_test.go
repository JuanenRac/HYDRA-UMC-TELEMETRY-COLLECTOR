// HYDRA-UMC-TELEMETRY-COLLECTOR - collector/collector_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package collector

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/sink"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

// memorySink is a real Sink implementation used only by tests - it
// genuinely stores what it's given (or genuinely fails, on command), not
// a mock framework standing in for behavior.
type memorySink struct {
	mu      sync.Mutex
	written [][]telemetry.Sample
	fail    bool
}

func (m *memorySink) Write(batch []telemetry.Sample) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("simulated outage")
	}
	cp := make([]telemetry.Sample, len(batch))
	copy(cp, batch)
	m.written = append(m.written, cp)
	return nil
}

func wsMessage(sourceID string) []byte {
	return []byte(`{"sourceId":"` + sourceID + `","kind":"motor_temp","timestamp":1700000000000,"fields":{"value":1}}`)
}

func wsMessageWithSeq(sourceID string, seq uint64) []byte {
	return []byte(fmt.Sprintf(
		`{"sourceId":%q,"kind":"motor_temp","timestamp":1700000000000,"fields":{"value":1},"sequence":%d}`,
		sourceID, seq,
	))
}

// invalidDataSink is a real Sink that fails with a real
// *sink.InvalidDataError, standing in for DATALAKE genuinely rejecting a
// sample's content (as opposed to a transport failure). With the zero
// value (PoisonSourceID == "") it always rejects whichever sample is
// first in the batch it's given, matching this type's original,
// single-purpose behavior. Set PoisonSourceID to reject one specific
// sample instead, wherever it actually lands in the batch - the real
// DatalakeSink identifies the bad sample by content, not by a hardcoded
// position, and this project's own fix depends on that Index being accurate.
type invalidDataSink struct {
	PoisonSourceID string
}

func (s invalidDataSink) Write(batch []telemetry.Sample) error {
	if s.PoisonSourceID == "" {
		return &sink.InvalidDataError{Sample: batch[0], Index: 0, Status: 400, Body: "bad data"}
	}
	for i, sample := range batch {
		if sample.SourceID == s.PoisonSourceID {
			return &sink.InvalidDataError{Sample: sample, Index: i, Status: 400, Body: "bad data"}
		}
	}
	// The poisoned sample isn't in this batch - a real, legitimate
	// success, not an error (this is exactly what a healthy retry after
	// quarantine should look like).
	return nil
}

func TestCollector_IngestAndFlushHappyPath(t *testing.T) {
	s := &memorySink{}
	c := New(10, s)

	if err := c.IngestWS(wsMessage("robot-1")); err != nil {
		t.Fatalf("IngestWS: %v", err)
	}
	n := c.FlushOnce(10)
	if n != 1 {
		t.Fatalf("FlushOnce returned %d, want 1", n)
	}
	if c.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0 after a successful flush", c.BufferLen())
	}
	stats := c.Stats()
	if stats.Ingested != 1 || stats.Flushed != 1 {
		t.Fatalf("stats = %+v, want Ingested=1 Flushed=1", stats)
	}
}

func TestCollector_SinkFailureRequeuesInsteadOfLosingData(t *testing.T) {
	s := &memorySink{fail: true}
	c := New(10, s)

	_ = c.IngestWS(wsMessage("robot-1"))
	_ = c.IngestWS(wsMessage("robot-2"))

	n := c.FlushOnce(10)
	if n != 0 {
		t.Fatalf("FlushOnce during outage returned %d, want 0", n)
	}
	if c.BufferLen() != 2 {
		t.Fatalf("BufferLen() = %d, want 2 - a failed flush must not lose the samples", c.BufferLen())
	}

	// The "outage" ends - the next flush must succeed with the SAME
	// samples that were requeued, not new ones.
	s.mu.Lock()
	s.fail = false
	s.mu.Unlock()

	n = c.FlushOnce(10)
	if n != 2 {
		t.Fatalf("FlushOnce after recovery returned %d, want 2", n)
	}
	if c.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0 after the retry succeeds", c.BufferLen())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.written) != 1 || len(s.written[0]) != 2 {
		t.Fatalf("sink received %+v, want exactly one batch of 2 samples", s.written)
	}
	if s.written[0][0].SourceID != "robot-1" || s.written[0][1].SourceID != "robot-2" {
		t.Fatalf("recovered batch = %+v, want robot-1 then robot-2 (order preserved)", s.written[0])
	}

	stats := c.Stats()
	if stats.FlushErrors != 1 {
		t.Fatalf("FlushErrors = %d, want 1 (the one outage attempt)", stats.FlushErrors)
	}
	if stats.Dropped != 0 {
		t.Fatalf("Dropped = %d, want 0 - there was room to requeue everything", stats.Dropped)
	}
}

func TestCollector_IngestErrorsAreCountedNotBuffered(t *testing.T) {
	s := &memorySink{}
	c := New(10, s)

	err := c.IngestWS([]byte(`{"kind":"motor_temp","timestamp":1,"fields":{}}`)) // missing sourceId
	if err == nil {
		t.Fatal("expected an error for an invalid WS message, got nil")
	}
	if c.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0 - an invalid sample must never reach the buffer", c.BufferLen())
	}
	if c.Stats().IngestErrors != 1 {
		t.Fatalf("IngestErrors = %d, want 1", c.Stats().IngestErrors)
	}
}

func TestCollector_BackpressureIsReportedNotSwallowed(t *testing.T) {
	s := &memorySink{}
	c := New(1, s) // capacity 1

	if err := c.IngestWS(wsMessage("robot-1")); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	err := c.IngestWS(wsMessage("robot-2"))
	if err == nil {
		t.Fatal("expected a buffer-full error on the second ingest, got nil")
	}
}

func TestCollector_DuplicateSequenceIsRejectedNotBuffered(t *testing.T) {
	s := &memorySink{}
	c := New(10, s)

	if err := c.IngestWS(wsMessageWithSeq("robot-1", 1)); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	err := c.IngestWS(wsMessageWithSeq("robot-1", 1))
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("second ingest of the same sequence: err = %v, want ErrDuplicate", err)
	}
	if c.BufferLen() != 1 {
		t.Fatalf("BufferLen() = %d, want 1 - the duplicate must not be re-buffered", c.BufferLen())
	}
	stats := c.Stats()
	if stats.Ingested != 1 {
		t.Fatalf("Ingested = %d, want 1 - the duplicate must not inflate the count", stats.Ingested)
	}
	if stats.Duplicates != 1 {
		t.Fatalf("Duplicates = %d, want 1", stats.Duplicates)
	}
}

// the collector's own acceptance criteria, reproduced exactly -
// fill the buffer, get rejected, free capacity, retry the IDENTICAL
// sample - it must persist exactly once. Before the fix, dedup.Allow()
// marked the sequence as seen the moment it returned true, regardless
// of whether the buf.Push() right after it actually succeeded - a full
// buffer meant the sample was dropped AND permanently blocked from ever
// being accepted on a later retry (rejected as ErrDuplicate forever),
// silent, permanent data loss for a producer that did everything right.
func TestCollector_RetryAfterBufferFullEventuallyPersistsExactlyOnce(t *testing.T) {
	s := &memorySink{}
	c := New(1, s) // capacity 1 - the second ingest below has nowhere to go

	if err := c.IngestWS(wsMessageWithSeq("robot-1", 1)); err != nil {
		t.Fatalf("first ingest (fills the buffer): %v", err)
	}

	// Fill: the buffer is now full. This ingest must be rejected for
	// being full, NOT for being a duplicate - it has never been seen
	// before.
	err := c.IngestWS(wsMessageWithSeq("robot-1", 2))
	if err == nil || errors.Is(err, ErrDuplicate) {
		t.Fatalf("ingest into a full buffer: err = %v, want a real buffer-full error, not ErrDuplicate", err)
	}

	// Free capacity: flush the first sample out.
	if n := c.FlushOnce(10); n != 1 {
		t.Fatalf("FlushOnce = %d, want 1", n)
	}
	if c.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0 after freeing capacity", c.BufferLen())
	}

	// Retry: the EXACT SAME sample (sourceId=robot-1, sequence=2) that
	// was rejected above. This is the real bug - it used to come back
	// ErrDuplicate forever, even though it was never actually buffered
	// the first time.
	if err := c.IngestWS(wsMessageWithSeq("robot-1", 2)); err != nil {
		t.Fatalf("retry after freeing capacity: err = %v, want nil - the sample was never actually persisted the first time", err)
	}
	if c.BufferLen() != 1 {
		t.Fatalf("BufferLen() = %d, want 1 after the retry succeeds", c.BufferLen())
	}

	if n := c.FlushOnce(10); n != 1 {
		t.Fatalf("final FlushOnce = %d, want 1", n)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, batch := range s.written {
		total += len(batch)
	}
	if total != 2 {
		t.Fatalf("sink received %d samples total across %+v, want exactly 2 (seq 1 once, seq 2 once)", total, s.written)
	}
}

// this project's own explicit second half: repeat with concurrent producers. N
// goroutines race to ingest the IDENTICAL (sourceId, sequence) sample
// while the buffer is genuinely full of something ELSE - proving the old
// code's real concurrency gap, not just its single-goroutine version
// above. Under the OLD code, plain dedup.Allow() committed the sequence
// as seen the instant the FIRST of these concurrent goroutines won its
// own internal mutex, entirely independent of whether that goroutine's
// own buf.Push() then succeeded - every OTHER concurrent goroutine
// racing the identical sequence at nearly the same moment saw
// Allow()==false (already marked) and returned ErrDuplicate immediately,
// without ever even attempting Push. So under the old code this phase
// produces exactly 1 real "buffer full" error plus (attempts-1)
// ErrDuplicate - and phase 2 below then finds EVERY retry permanently
// blocked as a false duplicate, zero successes. AllowThen's lock
// spanning the whole check-then-buffer-then-commit decision fixes both:
// phase 1 must produce real errors from every goroutine (none committed
// early), and phase 2 must produce exactly one real success once there's
// finally room.
func TestCollector_ConcurrentProducersOfTheSameSampleNeverPersistMoreThanOnce(t *testing.T) {
	s := &memorySink{}
	c := New(1, s) // capacity 1, and about to be filled by something else entirely

	if err := c.IngestWS(wsMessageWithSeq("robot-other", 1)); err != nil {
		t.Fatalf("filling the only slot with an unrelated sample: %v", err)
	}

	const attempts = 20
	var wg sync.WaitGroup
	results := make([]error, attempts)
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = c.IngestWS(wsMessageWithSeq("robot-1", 7))
		}(i)
	}
	wg.Wait()

	// Phase 1: the buffer is full of the UNRELATED sample above, not of
	// this sequence - every one of these concurrent attempts is a
	// genuinely new sequence being told "no room", not an actual
	// duplicate. None may be prematurely marked as seen.
	for i, err := range results {
		if err == nil || errors.Is(err, ErrDuplicate) {
			t.Fatalf("attempt %d during phase 1 (buffer genuinely full): err = %v, want a real buffer-full error, never nil or ErrDuplicate", i, err)
		}
	}

	// Free capacity: drain the unrelated sample.
	if n := c.FlushOnce(10); n != 1 {
		t.Fatalf("FlushOnce = %d, want 1", n)
	}
	if c.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0 after freeing capacity", c.BufferLen())
	}

	// Phase 2: M concurrent RETRIES of the exact same sample that was
	// rejected above, racing for the single free slot. Exactly one must
	// win and actually persist; the rest are now genuinely duplicates of
	// that real winner (not of a phantom commit that never happened).
	const retries = 10
	var wg2 sync.WaitGroup
	retryResults := make([]error, retries)
	wg2.Add(retries)
	for i := 0; i < retries; i++ {
		go func(i int) {
			defer wg2.Done()
			retryResults[i] = c.IngestWS(wsMessageWithSeq("robot-1", 7))
		}(i)
	}
	wg2.Wait()

	successes := 0
	for i, err := range retryResults {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrDuplicate) {
			t.Fatalf("retry %d: unexpected error %v, want nil (the winner) or ErrDuplicate", i, err)
		}
	}
	if successes != 1 {
		t.Fatalf("successes = %d across %d concurrent identical retries, want exactly 1", successes, retries)
	}
	if c.BufferLen() != 1 {
		t.Fatalf("BufferLen() = %d, want 1 - the same sample must never be buffered twice under a race", c.BufferLen())
	}
}

func TestCollector_RealDisconnectReconnectResendIsDeduplicated(t *testing.T) {
	// The real scenario handled here: a device sends
	// sequences 1-3, the connection drops before it receives acks, it
	// reconnects and - unsure what actually made it through - resends
	// 2 and 3 again before continuing with the genuinely new 4.
	s := &memorySink{}
	c := New(10, s)

	for _, seq := range []uint64{1, 2, 3} {
		if err := c.IngestWS(wsMessageWithSeq("robot-1", seq)); err != nil {
			t.Fatalf("initial ingest of sequence %d: %v", seq, err)
		}
	}

	// "reconnect" - nothing special about the collector's own state, the
	// same device just resends what it wasn't sure arrived.
	for _, seq := range []uint64{2, 3} {
		err := c.IngestWS(wsMessageWithSeq("robot-1", seq))
		if !errors.Is(err, ErrDuplicate) {
			t.Fatalf("resent sequence %d after reconnect: err = %v, want ErrDuplicate", seq, err)
		}
	}

	if err := c.IngestWS(wsMessageWithSeq("robot-1", 4)); err != nil {
		t.Fatalf("genuinely new sequence 4 after the resend: %v", err)
	}

	stats := c.Stats()
	if stats.Ingested != 4 {
		t.Fatalf("Ingested = %d, want 4 (1,2,3,4 exactly once each) - the resend must not inflate this", stats.Ingested)
	}
	if stats.Duplicates != 2 {
		t.Fatalf("Duplicates = %d, want 2 (the resent 2 and 3)", stats.Duplicates)
	}
	if c.BufferLen() != 4 {
		t.Fatalf("BufferLen() = %d, want 4 - one buffered copy of each real sample", c.BufferLen())
	}
}

func TestCollector_FiftySustainedReconnectCyclesNeverLeakOrMisbehave(t *testing.T) {
	// Real long-session/reconnection coverage (previously absent
	// anywhere in this repo - every other reconnect test here, including
	// the one right above, only ever exercises ONE disconnect/reconnect).
	// 50 simulated cycles against the same device, each sending a few
	// genuinely new sequences then "reconnecting" and resending its own
	// last two as a real device unsure what already got through would -
	// 300 total new sequences, comfortably past dedup's own 256-sequence
	// reorder window (see dedup.go's dedupWindow), so this also proves
	// the window's own prune-as-you-go bookkeeping (commitLocked's
	// `floor` deletion) stays correct across many successive prunes, not
	// just the one or two a short test exercises.
	s := &memorySink{}
	c := New(1000, s) // large enough that only dedup longevity, not buffer capacity, is under test here (see the tests above for that interaction)

	const cycles = 50
	const newPerCycle = 6
	var seq uint64
	var wantIngested, wantDuplicates int64

	for cycle := 0; cycle < cycles; cycle++ {
		for i := 0; i < newPerCycle; i++ {
			seq++
			if err := c.IngestWS(wsMessageWithSeq("robot-1", seq)); err != nil {
				t.Fatalf("cycle %d: genuinely new sequence %d: %v", cycle, seq, err)
			}
			wantIngested++
		}
		// "Reconnect": unsure whether its last 2 messages of this cycle
		// were acked, the device resends them before continuing.
		for _, resend := range []uint64{seq - 1, seq} {
			if err := c.IngestWS(wsMessageWithSeq("robot-1", resend)); !errors.Is(err, ErrDuplicate) {
				t.Fatalf("cycle %d: resend of sequence %d after reconnect: err = %v, want ErrDuplicate", cycle, resend, err)
			}
			wantDuplicates++
		}
	}

	stats := c.Stats()
	if stats.Ingested != wantIngested {
		t.Fatalf("after %d cycles: Ingested = %d, want %d - dedup state must never inflate or lose a real sample across a sustained series", cycles, stats.Ingested, wantIngested)
	}
	if stats.Duplicates != wantDuplicates {
		t.Fatalf("after %d cycles: Duplicates = %d, want %d", cycles, stats.Duplicates, wantDuplicates)
	}
	if c.BufferLen() != int(wantIngested) {
		t.Fatalf("BufferLen() = %d, want %d - one buffered copy of each real sample, none lost or duplicated across the sustained series", c.BufferLen(), wantIngested)
	}

	// The tracker must still be genuinely alive and functional after all
	// that churn, not latched into some stuck bad state.
	seq++
	if err := c.IngestWS(wsMessageWithSeq("robot-1", seq)); err != nil {
		t.Fatalf("final genuinely-new sequence %d after %d sustained cycles: %v", seq, cycles, err)
	}
}

func TestCollector_SamplesWithoutSequenceAreNeverDeduplicated(t *testing.T) {
	// Sequence 0 ("not provided") must behave exactly like before dedup
	// existed - a producer that doesn't opt in isn't affected.
	s := &memorySink{}
	c := New(10, s)

	if err := c.IngestWS(wsMessage("robot-1")); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if err := c.IngestWS(wsMessage("robot-1")); err != nil {
		t.Fatalf("second ingest (no sequence, must not be deduplicated): %v", err)
	}
	if c.Stats().Duplicates != 0 {
		t.Fatalf("Duplicates = %d, want 0 for samples that never provided a sequence", c.Stats().Duplicates)
	}
}

func TestCollector_InvalidDataFlushErrorIsClassifiedSeparatelyFromTransport(t *testing.T) {
	c := New(10, invalidDataSink{})
	_ = c.IngestWS(wsMessage("robot-1"))
	c.FlushOnce(10)

	stats := c.Stats()
	if stats.InvalidDataErrors != 1 {
		t.Fatalf("InvalidDataErrors = %d, want 1", stats.InvalidDataErrors)
	}
	if stats.TransportErrors != 0 {
		t.Fatalf("TransportErrors = %d, want 0 - this failure was a real InvalidDataError, not transport", stats.TransportErrors)
	}
}

// this project's own exact reproduction: a batch of [valid, POISONED, valid]
// used to requeue the WHOLE batch on the sink's InvalidDataError,
// putting the poisoned sample right back at the front - the next flush
// failed identically, requeued identically, forever, and the two
// perfectly good samples behind it never got a chance to flush. This
// confirms only the poisoned sample is quarantined and the survivors
// genuinely flush successfully on the very next attempt.
func TestCollector_InvalidDataQuarantinesOnlyThePoisonedSample(t *testing.T) {
	poisoned := invalidDataSink{PoisonSourceID: "robot-2"}
	c := New(10, poisoned)

	for _, id := range []string{"robot-1", "robot-2", "robot-3"} {
		if err := c.IngestWS(wsMessage(id)); err != nil {
			t.Fatalf("ingest %s: %v", id, err)
		}
	}

	n := c.FlushOnce(10)
	if n != 0 {
		t.Fatalf("FlushOnce with a poisoned sample in the batch returned %d, want 0", n)
	}
	if c.BufferLen() != 2 {
		t.Fatalf("BufferLen() = %d, want 2 - only the poisoned sample should have been dropped; robot-1 and robot-3 must survive", c.BufferLen())
	}
	stats := c.Stats()
	if stats.InvalidDataErrors != 1 {
		t.Fatalf("InvalidDataErrors = %d, want 1", stats.InvalidDataErrors)
	}
	if stats.Quarantined != 1 {
		t.Fatalf("Quarantined = %d, want 1", stats.Quarantined)
	}
	if stats.Dropped != 0 {
		t.Fatalf("Dropped = %d, want 0 - there was room to requeue the 2 survivors", stats.Dropped)
	}

	// Before this fix, this second flush would fail identically forever
	// (the poisoned sample would still be sitting at the front of the
	// buffer). The exact same sink - still configured to reject
	// "robot-2" if it ever saw it again - now flushes the 2 survivors
	// successfully, because the poison is actually gone.
	n = c.FlushOnce(10)
	if n != 2 {
		t.Fatalf("FlushOnce after quarantine returned %d, want 2 (robot-1 and robot-3 must now flush successfully)", n)
	}
	if c.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0 after the survivors flush", c.BufferLen())
	}
}

// The one-sample case (the pre-existing invalidDataSink{} default
// behavior): quarantining the only sample in the batch must leave
// nothing to requeue, not panic on an empty slice.
func TestCollector_InvalidDataQuarantineOfASingleSampleBatchLeavesBufferEmpty(t *testing.T) {
	c := New(10, invalidDataSink{})
	_ = c.IngestWS(wsMessage("robot-1"))

	n := c.FlushOnce(10)
	if n != 0 {
		t.Fatalf("FlushOnce returned %d, want 0", n)
	}
	if c.BufferLen() != 0 {
		t.Fatalf("BufferLen() = %d, want 0 - the only sample in the batch was the one quarantined", c.BufferLen())
	}
	if c.Stats().Quarantined != 1 {
		t.Fatalf("Quarantined = %d, want 1", c.Stats().Quarantined)
	}
}

func TestCollector_GenericSinkFailureIsClassifiedAsTransport(t *testing.T) {
	s := &memorySink{fail: true}
	c := New(10, s)
	_ = c.IngestWS(wsMessage("robot-1"))
	c.FlushOnce(10)

	stats := c.Stats()
	if stats.TransportErrors != 1 {
		t.Fatalf("TransportErrors = %d, want 1", stats.TransportErrors)
	}
	if stats.InvalidDataErrors != 0 {
		t.Fatalf("InvalidDataErrors = %d, want 0 - a generic sink error is not a real InvalidDataError", stats.InvalidDataErrors)
	}
}

// partialFailureSink is a real Sink standing in for DatalakeSink's own
// per-sample loop: it genuinely records every sample up to (and
// excluding) FailAt, then returns a real *sink.PartialWriteError naming
// exactly how many really got written - the same signal the real
// DatalakeSink now reports on a transport-level failure partway through
// a batch.
type partialFailureSink struct {
	mu      sync.Mutex
	FailAt  int // batch index that fails; -1 = never fails
	written []telemetry.Sample
}

func (s *partialFailureSink) Write(batch []telemetry.Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, sample := range batch {
		if i == s.FailAt {
			return &sink.PartialWriteError{Err: errors.New("simulated transport failure"), Succeeded: i}
		}
		s.written = append(s.written, sample)
	}
	return nil
}

// This is the real regression PartialWriteError exists to fix: a batch
// of 3 where the sink genuinely wrote the first 2 before failing on the
// 3rd must only requeue that 1 unwritten sample, never resend the 2
// already-confirmed ones - the whole-batch requeue this project used to
// do unconditionally would have landed robot-1/robot-2 as duplicate rows
// in DATALAKE on the very next retry.
func TestCollector_TransportFailureRequeuesOnlyTheUnwrittenRemainder(t *testing.T) {
	s := &partialFailureSink{FailAt: 2}
	c := New(10, s)

	for _, id := range []string{"robot-1", "robot-2", "robot-3"} {
		if err := c.IngestWS(wsMessage(id)); err != nil {
			t.Fatalf("ingest %s: %v", id, err)
		}
	}

	n := c.FlushOnce(10)
	if n != 0 {
		t.Fatalf("FlushOnce returned %d, want 0", n)
	}
	if c.BufferLen() != 1 {
		t.Fatalf("BufferLen() = %d, want 1 - only robot-3 (never attempted) should still be queued", c.BufferLen())
	}

	// Recover: stop failing, then flush the requeued remainder.
	s.mu.Lock()
	s.FailAt = -1
	s.mu.Unlock()
	n = c.FlushOnce(10)
	if n != 1 {
		t.Fatalf("FlushOnce after recovery returned %d, want 1", n)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.written) != 3 {
		t.Fatalf("sink recorded %d samples total, want exactly 3 - robot-1/robot-2 must never be re-sent as duplicates", len(s.written))
	}
	ids := []string{s.written[0].SourceID, s.written[1].SourceID, s.written[2].SourceID}
	if ids[0] != "robot-1" || ids[1] != "robot-2" || ids[2] != "robot-3" {
		t.Fatalf("sink recorded %+v, want [robot-1 robot-2 robot-3] with no duplicates", ids)
	}

	stats := c.Stats()
	if stats.TransportErrors != 1 {
		t.Fatalf("TransportErrors = %d, want 1", stats.TransportErrors)
	}
	if stats.Dropped != 0 {
		t.Fatalf("Dropped = %d, want 0 - there was room to requeue the 1 real remainder", stats.Dropped)
	}
}

func TestQuarantineSample(t *testing.T) {
	batch := []telemetry.Sample{
		{SourceID: "a"}, {SourceID: "b"}, {SourceID: "c"},
	}

	got := quarantineSample(batch, 1)
	if len(got) != 2 || got[0].SourceID != "a" || got[1].SourceID != "c" {
		t.Fatalf("quarantineSample(batch, 1) = %+v, want [a c]", got)
	}

	// A sink that can't actually identify the offending sample (a
	// negative or out-of-range Index - e.g. a future Sink implementation
	// that doesn't set it) must fall back to the whole batch, the same
	// safe behavior as a plain transport error - never panic, never
	// silently drop an arbitrary sample instead of the real one.
	for _, badIndex := range []int{-1, 3, 100} {
		got := quarantineSample(batch, badIndex)
		if len(got) != len(batch) {
			t.Fatalf("quarantineSample(batch, %d) = %+v, want the untouched original batch", badIndex, got)
		}
	}
}

func TestCollector_FlushOnEmptyBufferIsANoOp(t *testing.T) {
	s := &memorySink{}
	c := New(10, s)
	n := c.FlushOnce(10)
	if n != 0 {
		t.Fatalf("FlushOnce on an empty buffer returned %d, want 0", n)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.written) != 0 {
		t.Fatalf("sink was written to on an empty flush: %+v", s.written)
	}
}
