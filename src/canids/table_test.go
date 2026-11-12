// HYDRA-UMC-TELEMETRY-COLLECTOR - canids/table_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The table read from the CanIds contract: identifiers, the health frame, what is left alone, and every way the file can be wrong.
package canids

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

func load(t *testing.T) *Table {
	t.Helper()
	table, err := Load(filepath.Join("testdata", "can_ids.json"))
	if err != nil {
		t.Fatal(err)
	}
	table.Now = func() int64 { return 1_790_000_000_000 }
	return table
}

func health(sequence, flags, tx, rx, stalls, units byte) []byte {
	frame := []byte{healthMarker, sequence, flags, tx, rx, stalls, units, 0}
	check := byte(0xA5)
	for _, b := range frame[:7] {
		check ^= b
	}
	frame[7] = check
	return frame
}

func TestTheRealContractLoads(t *testing.T) {
	table := load(t)
	if !table.IsStop(1) || table.IsStop(2) {
		t.Fatal("the stop identifier")
	}
	m, slot, ok := table.Classify(0x611)
	if !ok || m.Name != HealthFrame || slot != 0 || m.Emitter != "controller-board" {
		t.Fatalf("0x611: %+v slot=%d ok=%v", m, slot, ok)
	}
	m, slot, ok = table.Classify(0x6C1 + 0x10) // slot 3, offset 0x11
	if !ok || m.Name != HealthFrame || slot != 3 {
		t.Fatalf("slot 3: %+v slot=%d ok=%v", m, slot, ok)
	}
	if _, _, ok := table.Classify(0x600 + 0x3F); ok { // an offset the contract does not list
		t.Fatal("an unlisted offset was classified")
	}
	for _, id := range []uint32{0x000, 0x095, 0x5FF, 0x600 + 8*64, 0x7FF} {
		if _, _, ok := table.Classify(id); ok {
			t.Fatalf("0x%X is outside the slots", id)
		}
	}
}

func TestACopyOfTheContractNextToTheSDKIsTheSame(t *testing.T) {
	canonical := filepath.Join("..", "..", "..", "HYDRA-UMC-SDK", "contracts", "can_ids.json")
	theirs, err := os.ReadFile(canonical)
	if err != nil {
		t.Skip("the SDK is not next to this repository")
	}
	ours, err := os.ReadFile(filepath.Join("testdata", "can_ids.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.ReplaceAll(theirs, []byte("\r\n"), []byte("\n")), bytes.ReplaceAll(ours, []byte("\r\n"), []byte("\n"))) {
		t.Fatal("testdata/can_ids.json differs from contracts/can_ids.json of the SDK: copy it again")
	}
}

func TestAHealthFrameBecomesASampleOfItsBoard(t *testing.T) {
	table := load(t)
	frame := health(7, 0x01|0x10|0x20, 3, 4, 5, 6)
	s, err := table.Decode(0x600+2*64+0x11, frame)
	if err != nil {
		t.Fatal(err)
	}
	if s.SourceID != "board-2" || s.Kind != "board_health" || s.Timestamp != 1_790_000_000_000 {
		t.Fatalf("%+v", s)
	}
	want := map[string]float64{"sequence": 7, "tx_errors": 3, "rx_errors": 4, "stalls": 5, "uptime_s": 96, "estop_latched": 1, "bus_off_seen": 0,
		"watchdog_reset_at_start": 0, "setup_failed_at_start": 0, "application_confirmed": 1, "drivers_enabled": 1, "setup_failed_safe_state": 0}
	for k, v := range want {
		if s.Fields[k] != v {
			t.Errorf("%s = %v, want %v", k, s.Fields[k], v)
		}
	}
	if len(s.Fields) != len(want) {
		t.Errorf("fields: %v", s.Fields)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("the sample does not pass the collector's rules: %v", err)
	}
	if table.Counters()["healthFrames"] != 1 {
		t.Fatalf("%v", table.Counters())
	}
}

func TestEveryFlagBitHasItsField(t *testing.T) {
	table := load(t)
	names := map[byte]string{0x01: "estop_latched", 0x02: "bus_off_seen", 0x04: "watchdog_reset_at_start", 0x08: "setup_failed_at_start",
		0x10: "application_confirmed", 0x20: "drivers_enabled", 0x40: "setup_failed_safe_state"}
	for mask, name := range names {
		s, err := table.Decode(0x611, health(0, mask, 0, 0, 0, 0))
		if err != nil || s.Fields[name] != 1 {
			t.Fatalf("%s: %v %v", name, s.Fields, err)
		}
		for other, otherName := range names {
			if other != mask && s.Fields[otherName] != 0 {
				t.Fatalf("%s lit %s", name, otherName)
			}
		}
	}
	s, _ := table.Decode(0x611, health(255, 0, 255, 255, 255, 255))
	if s.Fields["uptime_s"] != 4080 || s.Fields["sequence"] != 255 {
		t.Fatalf("%v", s.Fields)
	}
}

func TestAWrongHealthFrameIsABadPayload(t *testing.T) {
	table := load(t)
	good := health(1, 0, 0, 0, 0, 1)
	broken := map[string][]byte{
		"short":      good[:7],
		"long":       append(append([]byte{}, good...), 0),
		"marker":     append([]byte{0x49}, good[1:]...),
		"check byte": append(append([]byte{}, good[:7]...), good[7]^0xFF),
		"empty":      nil,
	}
	for name, data := range broken {
		if _, err := table.Decode(0x611, data); !errors.Is(err, ErrBadPayload) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if table.Counters()["badPayloads"] != uint64(len(broken)) {
		t.Fatalf("%v", table.Counters())
	}
}

func TestKnownTrafficIsLeftAloneAndAnUnknownIdentifierIsRefused(t *testing.T) {
	table := load(t)
	// the relay request of slot 1 (offset 0x10), a bootloader answer of slot 0 (offset 3), and the stop
	for _, id := range []uint32{0x640 + 0x10, 0x600 + 3, 1} {
		if _, err := table.Decode(id, []byte{1, 2, 3}); !errors.Is(err, ErrIgnored) {
			t.Errorf("0x%X: %v", id, err)
		}
	}
	for _, id := range []uint32{0x000, 0x095, 0x600 + 0x3F, 0x7FF, 0xFFFFFFFF} {
		if _, err := table.Decode(id, health(0, 0, 0, 0, 0, 0)); !errors.Is(err, ErrUnknownID) {
			t.Errorf("0x%X: %v", id, err)
		}
	}
	c := table.Counters()
	if c["knownIgnored"] != 2 || c["stopFrames"] != 1 || c["unknownIds"] != 5 {
		t.Fatalf("%v", c)
	}
}

func contract(mutate func(map[string]any)) []byte {
	raw, _ := os.ReadFile(filepath.Join("testdata", "can_ids.json"))
	var doc map[string]any
	_ = json.Unmarshal(raw, &doc)
	mutate(doc)
	out, _ := json.Marshal(doc)
	return out
}

func TestAWrongContractStopsTheLoad(t *testing.T) {
	messages := func(d map[string]any) []any { return d["messages"].([]any) }
	cases := map[string]func(map[string]any){
		"version":           func(d map[string]any) { d["schema_version"] = "2.0" },
		"unknown field":     func(d map[string]any) { d["surprise"] = 1 },
		"stop zero":         func(d map[string]any) { d["estop_id"] = 0 },
		"stop too big":      func(d map[string]any) { d["estop_id"] = 2048 },
		"stop in a slot":    func(d map[string]any) { d["estop_id"] = 0x601 },
		"slots past 11 bit": func(d map[string]any) { d["slot_base"] = 2000 },
		"zero window":       func(d map[string]any) { d["slot_window"] = 0 },
		"zero slots":        func(d map[string]any) { d["slots"] = 0 },
		"too many slots":    func(d map[string]any) { d["slots"] = 65 },
		"no messages":       func(d map[string]any) { d["messages"] = []any{} },
		"lower case name":   func(d map[string]any) { messages(d)[0].(map[string]any)["name"] = "health" },
		"same name": func(d map[string]any) {
			messages(d)[1].(map[string]any)["name"] = messages(d)[0].(map[string]any)["name"]
		},
		"same offset": func(d map[string]any) {
			messages(d)[1].(map[string]any)["offset"] = messages(d)[0].(map[string]any)["offset"]
		},
		"offset outside":    func(d map[string]any) { messages(d)[0].(map[string]any)["offset"] = 64 },
		"negative offset":   func(d map[string]any) { messages(d)[0].(map[string]any)["offset"] = -1 },
		"bad emitter":       func(d map[string]any) { messages(d)[0].(map[string]any)["emitter"] = "everybody" },
		"message field":     func(d map[string]any) { messages(d)[0].(map[string]any)["surprise"] = 1 },
		"string for number": func(d map[string]any) { d["slots"] = "8" },
	}
	for name, mutate := range cases {
		if _, err := Parse(contract(mutate)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for _, raw := range []string{"", "[]", "{", "null", strings.Repeat(" ", 10)} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("%q: accepted", raw)
		}
	}
	if _, err := Load(filepath.Join("testdata", "nope.json")); err == nil {
		t.Error("a missing file was accepted")
	}
	big := filepath.Join(t.TempDir(), "big.json")
	_ = os.WriteFile(big, bytes.Repeat([]byte(" "), maxFileBytes+1), 0o600)
	if _, err := Load(big); err == nil {
		t.Error("an oversized file was accepted")
	}
}

func TestTheSamplesPassTheCollectorRules(t *testing.T) {
	table := load(t)
	table.Now = func() int64 { return 1 } // implausible time: Validate refuses it, and so does Decode
	if _, err := table.Decode(0x611, health(0, 0, 0, 0, 0, 0)); !errors.Is(err, telemetry.ErrImplausibleTimestamp) {
		t.Fatalf("%v", err)
	}
}
