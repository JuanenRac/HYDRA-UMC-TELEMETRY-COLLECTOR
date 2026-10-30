// HYDRA-UMC-TELEMETRY-COLLECTOR - downsample/downsample.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Package downsample folds the samples of a high-frequency kind into one
// sample per fixed time bucket before they reach the buffer, so a vibration
// or a current read at 1 kHz does not fill the history of DATALAKE with rows
// nobody looks at. It is off unless a rule names the kind.
//
// A bucket is [k*interval, (k+1)*interval) in the timestamp of the samples
// (not the clock of this machine). Every field of the bucket is reduced by the
// rule's function; the emitted sample carries the start of the bucket as its
// timestamp. A bucket is closed when a sample of a later bucket of the same
// source and kind arrives, or when Flush finds it older than the grace period.
// A closed bucket stays in the pending list until the caller confirms with
// Done that it reached the buffer, so a full buffer loses nothing.
package downsample

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

// Function reduces the values of one field inside a bucket.
type Function string

const (
	Mean Function = "mean"
	Min  Function = "min"
	Max  Function = "max"
	Last Function = "last"
	Sum  Function = "sum"
)

// Rule says how one kind is reduced.
type Rule struct {
	IntervalMs int64
	Function   Function
}

// ParseRules reads "motor_current=100,vibration=1000:max": a kind, the bucket
// width in milliseconds and, after a colon, the function (mean when omitted).
// An empty list means no downsampling.
func ParseRules(list string) (map[string]Rule, error) {
	if strings.TrimSpace(list) == "" {
		return nil, nil
	}
	out := map[string]Rule{}
	for _, part := range strings.Split(list, ",") {
		kind, spec, found := strings.Cut(strings.TrimSpace(part), "=")
		kind = strings.TrimSpace(kind)
		if !found || kind == "" {
			return nil, fmt.Errorf("%q is not kind=intervalMs[:function]", strings.TrimSpace(part))
		}
		width, function, _ := strings.Cut(spec, ":")
		ms, err := strconv.ParseInt(strings.TrimSpace(width), 10, 64)
		if err != nil || ms < 2 {
			return nil, fmt.Errorf("%q: the interval is a whole number of milliseconds, at least 2", strings.TrimSpace(part))
		}
		fn := Function(strings.TrimSpace(function))
		if fn == "" {
			fn = Mean
		}
		switch fn {
		case Mean, Min, Max, Last, Sum:
		default:
			return nil, fmt.Errorf("%q: the function is one of mean, min, max, last, sum", strings.TrimSpace(part))
		}
		if _, dup := out[kind]; dup {
			return nil, fmt.Errorf("the kind %q appears twice", kind)
		}
		out[kind] = Rule{IntervalMs: ms, Function: fn}
	}
	return out, nil
}

type accumulator struct {
	count    int
	sum      float64
	min, max float64
	last     float64
}

type bucket struct {
	sample telemetry.Sample // identity of the bucket: source, kind, start
	fields map[string]*accumulator
	rule   Rule
	n      int // samples folded
}

type key struct {
	source, kind string
}

// Aggregator folds samples; safe for concurrent use.
type Aggregator struct {
	mu      sync.Mutex
	rules   map[string]Rule
	open    map[key]*bucket
	pending []telemetry.Sample

	folded  int64
	emitted int64
	late    int64
}

// New builds an Aggregator; with no rules it handles nothing.
func New(rules map[string]Rule) *Aggregator {
	return &Aggregator{rules: rules, open: map[key]*bucket{}}
}

// Handles reports whether the kind is downsampled.
func (a *Aggregator) Handles(kind string) bool {
	if a == nil {
		return false
	}
	_, ok := a.rules[kind]
	return ok
}

// Backlog is how many closed buckets still wait for the buffer.
func (a *Aggregator) Backlog() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pending)
}

// Counters: samples folded into a bucket, buckets emitted, samples that arrived for a bucket older than the open one.
func (a *Aggregator) Counters() (folded, emitted, late int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.folded, a.emitted, a.late
}

func startOf(ts, width int64) int64 { return ts - ((ts%width)+width)%width }

// Add folds one sample of a handled kind and returns every closed bucket that
// still waits (oldest first). The caller pushes them and calls Done with how
// many it could push.
func (a *Aggregator) Add(s telemetry.Sample) []telemetry.Sample {
	a.mu.Lock()
	defer a.mu.Unlock()
	rule := a.rules[s.Kind]
	k := key{s.SourceID, s.Kind}
	start := startOf(s.Timestamp, rule.IntervalMs)
	if open, ok := a.open[k]; ok {
		switch {
		case start > open.sample.Timestamp:
			a.closeLocked(k)
		case start < open.sample.Timestamp:
			// an older bucket than the open one: it is not lost, it leaves as a bucket of its own
			a.late++
			late := a.newBucket(s, rule, start)
			a.fold(late, s)
			a.pending = append(a.pending, a.finish(late))
			a.emitted++
			return append([]telemetry.Sample(nil), a.pending...)
		}
	}
	open, ok := a.open[k]
	if !ok {
		open = a.newBucket(s, rule, start)
		a.open[k] = open
	}
	a.fold(open, s)
	return append([]telemetry.Sample(nil), a.pending...)
}

// Flush closes every bucket whose end is at least graceMs before nowMs (a
// producer that stopped must not keep its last bucket for ever) and returns
// what waits.
func (a *Aggregator) Flush(nowMs, graceMs int64) []telemetry.Sample {
	a.mu.Lock()
	defer a.mu.Unlock()
	keys := make([]key, 0, len(a.open))
	for k, b := range a.open {
		if b.sample.Timestamp+b.rule.IntervalMs+graceMs <= nowMs {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return a.open[keys[i]].sample.Timestamp < a.open[keys[j]].sample.Timestamp })
	for _, k := range keys {
		a.closeLocked(k)
	}
	return append([]telemetry.Sample(nil), a.pending...)
}

// Drain closes every open bucket (shutdown) and returns what waits.
func (a *Aggregator) Drain() []telemetry.Sample {
	return a.Flush(math.MaxInt64/2, 0)
}

// Done drops the first n pending buckets: they reached the buffer.
func (a *Aggregator) Done(n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n > len(a.pending) {
		n = len(a.pending)
	}
	a.pending = a.pending[n:]
}

func (a *Aggregator) newBucket(s telemetry.Sample, rule Rule, start int64) *bucket {
	return &bucket{
		sample: telemetry.Sample{SourceID: s.SourceID, Kind: s.Kind, Timestamp: start},
		fields: map[string]*accumulator{},
		rule:   rule,
	}
}

func (a *Aggregator) fold(b *bucket, s telemetry.Sample) {
	a.folded++
	b.n++
	for name, v := range s.Fields {
		acc, ok := b.fields[name]
		if !ok {
			acc = &accumulator{min: v, max: v}
			b.fields[name] = acc
		}
		acc.count++
		acc.sum += v
		acc.min = math.Min(acc.min, v)
		acc.max = math.Max(acc.max, v)
		acc.last = v
	}
}

func (a *Aggregator) closeLocked(k key) {
	b := a.open[k]
	delete(a.open, k)
	a.pending = append(a.pending, a.finish(b))
	a.emitted++
}

func (a *Aggregator) finish(b *bucket) telemetry.Sample {
	out := b.sample
	out.Fields = make(map[string]float64, len(b.fields))
	for name, acc := range b.fields {
		switch b.rule.Function {
		case Min:
			out.Fields[name] = acc.min
		case Max:
			out.Fields[name] = acc.max
		case Last:
			out.Fields[name] = acc.last
		case Sum:
			out.Fields[name] = acc.sum
		default:
			out.Fields[name] = acc.sum / float64(acc.count)
		}
	}
	return out
}
