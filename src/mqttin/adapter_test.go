// HYDRA-UMC-TELEMETRY-COLLECTOR - mqttin/adapter_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package mqttin

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

var testNow = time.Now()

// ago is the time in milliseconds since the epoch, that many milliseconds before testNow.
func ago(ms int64) int64 { return testNow.UnixMilli() - ms }

func msg(topic, payload string, retained bool) Message {
	return Message{Topic: topic, Payload: []byte(payload), Retained: retained}
}

func TestStateBecomesASampleWithItsNumbersAndFlags(t *testing.T) {
	s, err := SampleFromMessage(msg("hydra/bridges/cnc/state",
		fmt.Sprintf(`{"machine_state":"idle","spindle_speed":0,"door_open":false,"estop":true,"position":{"x":1.5,"y":-2},"note":"free text","observed_at":%d}`, ago(1000)), false), testNow, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if s.SourceID != "bridge-cnc" || s.Kind != "bridge_state" || s.Timestamp != ago(1000) {
		t.Fatalf("sample = %+v", s)
	}
	want := map[string]float64{"machine_state.idle": 1, "spindle_speed": 0, "door_open": 0, "estop": 1, "position.x": 1.5, "position.y": -2}
	for k, v := range want {
		if got, ok := s.Fields[k]; !ok || got != v {
			t.Errorf("field %s = %v (present %v), want %v", k, got, ok, v)
		}
	}
	if _, ok := s.Fields["note"]; ok {
		t.Error("free text must not become a field")
	}
	if _, ok := s.Fields["observed_at"]; ok {
		t.Error("observed_at is the timestamp, not a field")
	}
}

func TestStateWithoutObservedAtIsStampedWithTheArrival(t *testing.T) {
	s, err := SampleFromMessage(msg("hydra/bridges/laser/state", `{"power":12.5}`, false), testNow, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if s.Timestamp != testNow.UnixMilli() {
		t.Fatalf("timestamp = %d", s.Timestamp)
	}
}

func TestAStaleRetainedStateIsTheMemoryOfTheBrokerNotAReading(t *testing.T) {
	old := fmt.Sprintf(`{"power":1,"observed_at":%d}`, ago(1_000_000)) // 1000 s earlier
	if _, err := SampleFromMessage(msg("hydra/bridges/laser/state", old, true), testNow, time.Minute); !errors.Is(err, ErrIgnored) {
		t.Fatalf("err = %v", err)
	}
	// the same message, not retained, is a live one
	if _, err := SampleFromMessage(msg("hydra/bridges/laser/state", old, false), testNow, time.Minute); err != nil {
		t.Fatal(err)
	}
	// a retained state with no time of its own cannot be told from an old one
	if _, err := SampleFromMessage(msg("hydra/bridges/laser/state", `{"power":1}`, true), testNow, time.Minute); !errors.Is(err, ErrIgnored) {
		t.Fatalf("err = %v", err)
	}
	// a fresh retained state counts
	if _, err := SampleFromMessage(msg("hydra/bridges/laser/state", fmt.Sprintf(`{"power":1,"observed_at":%d}`, ago(10_000)), true), testNow, time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestAvailabilityBecomesOnlineOrOffline(t *testing.T) {
	on, err := SampleFromMessage(msg("hydra/bridges/amr/availability", "online", true), testNow, time.Minute)
	if err != nil || on.Kind != "bridge_availability" || on.Fields["online"] != 1 {
		t.Fatalf("online: %+v %v", on, err)
	}
	off, err := SampleFromMessage(msg("hydra/bridges/amr/availability", " Offline\n", true), testNow, time.Minute)
	if err != nil || off.Fields["online"] != 0 {
		t.Fatalf("offline: %+v %v", off, err)
	}
	if _, err := SampleFromMessage(msg("hydra/bridges/amr/availability", "maybe", true), testNow, time.Minute); err == nil || errors.Is(err, ErrIgnored) {
		t.Fatalf("err = %v", err)
	}
}

func TestOtherTopicsAreIgnoredAndBadPayloadsAreRefused(t *testing.T) {
	for _, topic := range []string{"hydra/bridges/cnc/cmd/status", "hydra/bridges/cnc", "other/bridges/cnc/state", "hydra/bridges/bad name/state", "hydra/bridges/../state/state"} {
		if _, err := SampleFromMessage(msg(topic, `{"a":1}`, false), testNow, time.Minute); !errors.Is(err, ErrIgnored) {
			t.Errorf("%s: err = %v", topic, err)
		}
	}
	for _, payload := range []string{"", "not json", "[1,2]", `{}`, `{"only":"text"}`, `{"observed_at":1}`} {
		if _, err := SampleFromMessage(msg("hydra/bridges/cnc/state", payload, false), testNow, time.Minute); err == nil || errors.Is(err, ErrIgnored) {
			t.Errorf("%q: err = %v", payload, err)
		}
	}
}

func TestTheFieldsOfOneSampleAreBounded(t *testing.T) {
	var b strings.Builder
	b.WriteString("{")
	for i := 0; i < 200; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf(`"f%03d":1`, i))
	}
	b.WriteString("}")
	s, err := SampleFromMessage(msg("hydra/bridges/x/state", b.String(), false), testNow, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Fields) != MaxFields {
		t.Fatalf("%d fields, want %d", len(s.Fields), MaxFields)
	}
}

func TestTheAdapterCountsWhatItDoes(t *testing.T) {
	var got []telemetry.Sample
	a := &Adapter{Submit: func(s telemetry.Sample) error { got = append(got, s); return nil }, Now: func() time.Time { return testNow }}
	a.Handle(msg("hydra/bridges/cnc/state", `{"x":1}`, false))
	a.Handle(msg("hydra/bridges/cnc/cmd/status", ``, false))
	a.Handle(msg("hydra/bridges/cnc/state", `nope`, false))
	a.Submit = func(telemetry.Sample) error { return errors.New("full") }
	a.Handle(msg("hydra/bridges/cnc/state", `{"x":2}`, false))
	c := a.Counters()
	if c.Accepted != 1 || c.Ignored != 1 || c.Rejected != 1 || c.Failed != 1 || len(got) != 1 {
		t.Fatalf("counters = %+v, samples = %d", c, len(got))
	}
}
