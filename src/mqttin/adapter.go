// HYDRA-UMC-TELEMETRY-COLLECTOR - mqttin/adapter.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Turns what the bridges say on MQTT into telemetry samples, so the data lake sees the cells and not only the robots:
//
//	hydra/bridges/<name>/state         -> kind "bridge_state", source "bridge-<name>": every number and flag of the state
//	                                      object (one level of nesting, name.sub), and 1 under "machine_state.<value>" for the
//	                                      machine state the bridge observed. The sample is stamped with observed_at when the
//	                                      bridge gives one, else with the arrival time.
//	hydra/bridges/<name>/availability  -> kind "bridge_availability": online = 1 or 0.
//
// A retained state that is older than the maximum age is the memory of the broker, not a reading, and is skipped. Everything the
// adapter cannot read is counted and dropped; nothing is invented.
package mqttin

import (
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

const (
	// The filters the adapter subscribes to.
	StateTopic        = "hydra/bridges/+/state"
	AvailabilityTopic = "hydra/bridges/+/availability"
	// DefaultMaxAge is how old a retained state may be and still count as a reading.
	DefaultMaxAge = 2 * time.Minute
	// MaxFields bounds the fields of one sample.
	MaxFields = 64
)

var (
	namePattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	valuePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,32}$`)
	// the string keys whose value is a state worth counting as a flag
	stateKeys = map[string]bool{"machine_state": true, "state": true}
)

// ErrIgnored marks a message that is not a reading (another topic, a stale retained state).
var ErrIgnored = errors.New("mqttin: message ignored")

// SampleFromMessage reads one message.
func SampleFromMessage(m Message, now time.Time, maxAge time.Duration) (telemetry.Sample, error) {
	parts := strings.Split(m.Topic, "/")
	if len(parts) != 4 || parts[0] != "hydra" || parts[1] != "bridges" || !namePattern.MatchString(parts[2]) {
		return telemetry.Sample{}, ErrIgnored
	}
	source := "bridge-" + parts[2]
	switch parts[3] {
	case "availability":
		var online float64
		switch strings.ToLower(strings.TrimSpace(string(m.Payload))) {
		case "online":
			online = 1
		case "offline":
		default:
			return telemetry.Sample{}, errors.New("mqttin: availability is neither online nor offline")
		}
		s := telemetry.Sample{SourceID: source, Kind: "bridge_availability", Timestamp: now.UnixMilli(), Fields: map[string]float64{"online": online}}
		return s, s.Validate()
	case "state":
		return stateSample(m, source, now, maxAge)
	}
	return telemetry.Sample{}, ErrIgnored
}

func stateSample(m Message, source string, now time.Time, maxAge time.Duration) (telemetry.Sample, error) {
	var doc map[string]any
	if err := json.Unmarshal(m.Payload, &doc); err != nil {
		return telemetry.Sample{}, errors.New("mqttin: the state is not a JSON object")
	}
	stamp := now.UnixMilli()
	observed := false
	if raw, ok := doc["observed_at"].(float64); ok && raw >= float64(telemetry.MinPlausibleTimestampMs) && !math.IsInf(raw, 0) {
		stamp = int64(raw)
		observed = true
	}
	if m.Retained && maxAge > 0 {
		if !observed || now.UnixMilli()-stamp > maxAge.Milliseconds() {
			return telemetry.Sample{}, ErrIgnored // a memory of the broker, not a reading
		}
	}
	fields := map[string]float64{}
	flatten("", doc, 0, fields)
	delete(fields, "observed_at")
	if len(fields) == 0 {
		return telemetry.Sample{}, errors.New("mqttin: the state holds nothing that can be measured")
	}
	s := telemetry.Sample{SourceID: source, Kind: "bridge_state", Timestamp: stamp, Fields: fields}
	return s, s.Validate()
}

func flatten(prefix string, node map[string]any, depth int, out map[string]float64) {
	keys := make([]string, 0, len(node))
	for k := range node {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if len(out) >= MaxFields {
			return
		}
		if !namePattern.MatchString(k) {
			continue
		}
		name := k
		if prefix != "" {
			name = prefix + "." + k
		}
		switch v := node[k].(type) {
		case float64:
			if !math.IsNaN(v) && !math.IsInf(v, 0) {
				out[name] = v
			}
		case bool:
			if v {
				out[name] = 1
			} else {
				out[name] = 0
			}
		case string:
			if stateKeys[k] && valuePattern.MatchString(v) {
				out[name+"."+v] = 1
			}
		case map[string]any:
			if depth == 0 {
				flatten(name, v, depth+1, out)
			}
		}
	}
}

// Adapter feeds a collector from a Client.
type Adapter struct {
	// Submit hands a sample to the collector.
	Submit func(telemetry.Sample) error
	MaxAge time.Duration
	// Now is the clock (nil: the system clock).
	Now func() time.Time

	accepted atomic.Uint64
	ignored  atomic.Uint64
	rejected atomic.Uint64
	failed   atomic.Uint64
}

// Counters are the counts of an Adapter.
type Counters struct {
	Accepted uint64 `json:"accepted"`
	Ignored  uint64 `json:"ignored"`
	Rejected uint64 `json:"rejected"`
	Failed   uint64 `json:"submitFailures"`
}

// Counters reports what the adapter did with the messages it was given.
func (a *Adapter) Counters() Counters {
	return Counters{Accepted: a.accepted.Load(), Ignored: a.ignored.Load(), Rejected: a.rejected.Load(), Failed: a.failed.Load()}
}

// Handle is the handler of a Client.
func (a *Adapter) Handle(m Message) {
	now := time.Now()
	if a.Now != nil {
		now = a.Now()
	}
	maxAge := a.MaxAge
	if maxAge == 0 {
		maxAge = DefaultMaxAge
	}
	s, err := SampleFromMessage(m, now, maxAge)
	switch {
	case errors.Is(err, ErrIgnored):
		a.ignored.Add(1)
	case err != nil:
		a.rejected.Add(1)
	default:
		if err := a.Submit(s); err != nil {
			a.failed.Add(1)
			return
		}
		a.accepted.Add(1)
	}
}
