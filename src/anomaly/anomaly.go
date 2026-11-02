// HYDRA-UMC-TELEMETRY-COLLECTOR - anomaly/anomaly.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Package anomaly streams the samples that reached the sink to a
// HYDRA-UMC-ANOMALY-DETECTOR as they are delivered, in addition to the batch
// path through the datalake. The detector scores one window of a signal at a
// time (POST /detectors/{sourceId}/{kind}/{field}/detect), so a rule names a
// kind, a field and the window length; the values of that field are collected
// per source and each full window is sent. It is off unless a rule is given.
//
// Publishing is best effort and never blocks or fails the delivery to the
// datalake: the windows wait in a bounded queue, a full queue drops the new
// window (counted), and an unreachable detector only raises a counter.
package anomaly

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/sink"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

// MaxWindow is the longest window a rule may ask for.
const MaxWindow = 65536

// MaxOverlap is the largest share of a window, in percent, that the next window may repeat.
const MaxOverlap = 90

// Rule selects one field of one kind, the length of the window sent to the detector and how
// much of it the next window repeats (Overlap, in percent; 0 means windows that do not overlap).
type Rule struct {
	Kind    string
	Field   string
	Window  int
	Overlap int
}

// keep is how many of the newest values of a sent window open the next one.
func (r Rule) keep() int { return r.Window * r.Overlap / 100 }

// ParseRules reads "kind/field=window" or "kind/field=window@overlap" pairs separated by commas;
// the overlap is a percentage from 0 to 90. The empty string is no rules.
func ParseRules(spec string) ([]Rule, error) { return ParseRulesOverlap(spec, 0) }

// ParseRulesOverlap is ParseRules where a rule without its own "@overlap" uses defaultOverlap.
func ParseRulesOverlap(spec string, defaultOverlap int) ([]Rule, error) {
	if defaultOverlap < 0 || defaultOverlap > MaxOverlap {
		return nil, fmt.Errorf("the overlap must be a whole number of percent from 0 to %d", MaxOverlap)
	}
	var rules []Rule
	seen := map[string]bool{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, size, ok := strings.Cut(part, "=")
		kind, field, ok2 := strings.Cut(name, "/")
		if !ok || !ok2 || kind == "" || field == "" {
			return nil, fmt.Errorf("%q must look like kind/field=window", part)
		}
		overlap := defaultOverlap
		if sizeOnly, share, has := strings.Cut(size, "@"); has {
			size = sizeOnly
			var oerr error
			if overlap, oerr = strconv.Atoi(strings.TrimSpace(share)); oerr != nil || overlap < 0 || overlap > MaxOverlap {
				return nil, fmt.Errorf("%q: the overlap must be a whole number of percent from 0 to %d", part, MaxOverlap)
			}
		}
		window, err := strconv.Atoi(strings.TrimSpace(size))
		if err != nil || window < 2 || window > MaxWindow {
			return nil, fmt.Errorf("%q: the window must be a whole number from 2 to %d", part, MaxWindow)
		}
		if seen[name] {
			return nil, fmt.Errorf("%q is listed twice", name)
		}
		seen[name] = true
		rules = append(rules, Rule{Kind: kind, Field: field, Window: window, Overlap: overlap})
	}
	return rules, nil
}

// Options configure a Publisher.
type Options struct {
	URL       string
	Token     string
	Rules     []Rule
	QueueMax  int           // windows waiting to be sent (default 64)
	MaxSeries int           // distinct (source, kind, field) series kept (default 64)
	Timeout   time.Duration // per request (default 5s)
	Client    *http.Client
}

type seriesKey struct{ source, kind, field string }

type job struct {
	key    seriesKey
	window []float64
}

// Publisher collects windows and sends them to the detector.
type Publisher struct {
	opts  Options
	rules map[[2]string]Rule
	queue chan job

	mu     sync.Mutex
	series map[seriesKey][]float64
	stats  Counters
}

// Counters are reported by /stats.
type Counters struct {
	Windows       uint64  `json:"windows"`
	Sent          uint64  `json:"sent"`
	Anomalous     uint64  `json:"anomalous"`
	NotFitted     uint64  `json:"notFitted"`
	Errors        uint64  `json:"errors"`
	DroppedQueue  uint64  `json:"droppedQueue"`
	DroppedSeries uint64  `json:"droppedSeries"`
	LastScore     float64 `json:"lastScore"`
}

// New returns a Publisher; call Run to start sending.
func New(o Options) *Publisher {
	if o.QueueMax < 1 {
		o.QueueMax = 64
	}
	if o.MaxSeries < 1 {
		o.MaxSeries = 64
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	if o.Client == nil {
		o.Client = &http.Client{Timeout: o.Timeout}
	}
	o.URL = strings.TrimRight(o.URL, "/")
	p := &Publisher{opts: o, rules: map[[2]string]Rule{}, queue: make(chan job, o.QueueMax), series: map[seriesKey][]float64{}}
	for _, r := range o.Rules {
		p.rules[[2]string{r.Kind, r.Field}] = r
	}
	return p
}

// Offer collects the delivered samples. It never blocks.
func (p *Publisher) Offer(batch []telemetry.Sample) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range batch {
		for field, value := range s.Fields {
			rule, ok := p.rules[[2]string{s.Kind, field}]
			if !ok {
				continue
			}
			key := seriesKey{s.SourceID, s.Kind, field}
			values, known := p.series[key]
			if !known && len(p.series) >= p.opts.MaxSeries {
				p.stats.DroppedSeries++
				continue
			}
			values = append(values, value)
			if len(values) < rule.Window {
				p.series[key] = values
				continue
			}
			// the sent window is its own slice; the newest values of it open the next one, so a
			// sample is stored once and the series never holds more than one window
			next := make([]float64, 0, rule.Window)
			next = append(next, values[len(values)-rule.keep():]...)
			p.series[key] = next
			p.stats.Windows++
			select {
			case p.queue <- job{key, values}:
			default:
				p.stats.DroppedQueue++
			}
		}
	}
}

// Run sends the queued windows until ctx ends.
func (p *Publisher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case j := <-p.queue:
			p.send(ctx, j)
		}
	}
}

func (p *Publisher) send(ctx context.Context, j job) {
	endpoint := fmt.Sprintf("%s/detectors/%s/%s/%s/detect", p.opts.URL,
		url.PathEscape(j.key.source), url.PathEscape(j.key.kind), url.PathEscape(j.key.field))
	body, _ := json.Marshal(map[string]any{"window": j.window})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		p.count(func(c *Counters) { c.Errors++ })
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if p.opts.Token != "" {
		req.Header.Set("Authorization", "Bearer "+p.opts.Token)
	}
	resp, err := p.opts.Client.Do(req)
	if err != nil {
		p.count(func(c *Counters) { c.Errors++ })
		return
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode == http.StatusOK:
		var verdict struct {
			Score     float64 `json:"score"`
			Anomalous bool    `json:"anomalous"`
		}
		_ = json.Unmarshal(raw, &verdict)
		p.count(func(c *Counters) {
			c.Sent++
			c.LastScore = verdict.Score
			if verdict.Anomalous {
				c.Anomalous++
			}
		})
	case resp.StatusCode == http.StatusConflict:
		// the series has no baseline yet: not a failure of the stream
		p.count(func(c *Counters) { c.NotFitted++ })
	default:
		p.count(func(c *Counters) { c.Errors++ })
	}
}

func (p *Publisher) count(f func(*Counters)) {
	p.mu.Lock()
	f(&p.stats)
	p.mu.Unlock()
}

// Counters returns a copy of the counters.
func (p *Publisher) Counters() Counters {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// Stats returns the counters and the queue state for /stats.
func (p *Publisher) Stats() map[string]any {
	p.mu.Lock()
	c := p.stats
	series := len(p.series)
	p.mu.Unlock()
	return map[string]any{"anomaly": map[string]any{"counters": c, "queued": len(p.queue), "series": series}}
}

// Tee is a sink that delivers to Primary and then streams what Primary confirmed to the detector.
type Tee struct {
	Primary sink.Sink
	Pub     *Publisher
}

func (t Tee) Write(batch []telemetry.Sample) error {
	err := t.Primary.Write(batch)
	if err == nil {
		t.Pub.Offer(batch)
		return nil
	}
	// only what the sink confirmed; the rest is retried and offered then
	if partial, ok := sink.AsPartialWrite(err); ok && partial.Succeeded > 0 && partial.Succeeded <= len(batch) {
		t.Pub.Offer(batch[:partial.Succeeded])
	}
	return err
}
