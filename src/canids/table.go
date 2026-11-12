// HYDRA-UMC-TELEMETRY-COLLECTOR - canids/table.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The CAN identifiers of the HYDRA-UMC stack bus, read from the CanIds contract of the SDK (contracts/can_ids.json) instead of
// the collector's own v0 convention. It is off unless -can-ids names the file. With a table, a frame is classified by its identifier:
//
//	slot_base + slot*slot_window + offset  -> a message of a controller board; the health frame (HEALTH_FRAME) becomes a sample
//	                                          "board_health" of the source "board-<slot>", every other known message is counted and
//	                                          left alone (it is bus traffic of the firmware, not a reading)
//	the emergency stop identifier          -> counted and left alone (its meaning is the business of the safety chain)
//	anything else                          -> refused: an identifier the contract does not know is not guessed at
//
// The table is checked when it is read with the rules of the contract (11-bit identifiers, offsets inside the window and used once,
// the stop identifier outside the slots), so a wrong file stops the start instead of misreading the bus.
package canids

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

const (
	maxIdentifier = 0x7FF
	maxMessages   = 256
	// HealthFrame is the name of the message whose payload this package decodes.
	HealthFrame   = "HEALTH_FRAME"
	healthMarker  = 0x48
	healthKind    = "board_health"
	maxFileBytes  = 1 << 20
	healthPayload = 8
)

var (
	// ErrIgnored marks a frame that is known and is not a reading (a bootloader answer, a relay frame, the stop).
	ErrIgnored = errors.New("canids: known frame, not a reading")
	// ErrUnknownID marks an identifier the contract does not list.
	ErrUnknownID = errors.New("canids: identifier not in the contract")
	// ErrBadPayload marks a frame of a decoded message whose bytes do not fit it.
	ErrBadPayload = errors.New("canids: payload does not fit the message")

	messageName = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)
	emitters    = map[string]bool{"kinematic-brain": true, "controller-board": true, "bootloader": true, "host": true}
)

// Message is one entry of the table.
type Message struct {
	Name    string `json:"name"`
	Offset  int    `json:"offset"`
	Emitter string `json:"emitter"`
}

type document struct {
	SchemaVersion string    `json:"schema_version"`
	Note          string    `json:"note"`
	EstopID       int       `json:"estop_id"`
	SlotBase      int       `json:"slot_base"`
	SlotWindow    int       `json:"slot_window"`
	Slots         int       `json:"slots"`
	Messages      []Message `json:"messages"`
}

// Table classifies identifiers.
type Table struct {
	estop    uint32
	base     uint32
	window   uint32
	slots    uint32
	byOffset map[uint32]Message
	// Now is the clock of the samples (milliseconds); the tests replace it.
	Now func() int64

	health  atomic.Uint64
	ignored atomic.Uint64
	stop    atomic.Uint64
	unknown atomic.Uint64
	bad     atomic.Uint64
}

// Load reads and checks the contract file.
func Load(path string) (*Table, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxFileBytes {
		return nil, errors.New("the CAN identifier file is too large")
	}
	return Parse(raw)
}

// Parse reads and checks the contract bytes. An unknown field is refused, like the closed schema does.
func Parse(raw []byte) (*Table, error) {
	var doc document
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("not a CanIds document: %w", err)
	}
	if doc.SchemaVersion != "1.0" {
		return nil, errors.New("schema_version must be 1.0")
	}
	if doc.EstopID < 1 || doc.EstopID > maxIdentifier {
		return nil, errors.New("estop_id must be from 1 to 2047")
	}
	if doc.SlotBase < 0 || doc.SlotBase > maxIdentifier || doc.SlotWindow < 1 || doc.SlotWindow > maxIdentifier+1 || doc.Slots < 1 || doc.Slots > 64 {
		return nil, errors.New("slot_base, slot_window or slots out of range")
	}
	last := doc.SlotBase + doc.Slots*doc.SlotWindow - 1
	if last > maxIdentifier {
		return nil, fmt.Errorf("the slots reach the identifier %d, past the 11 bits of a standard identifier", last)
	}
	if doc.SlotBase <= doc.EstopID && doc.EstopID <= last {
		return nil, errors.New("the emergency stop identifier is inside the slot windows")
	}
	if len(doc.Messages) < 1 || len(doc.Messages) > maxMessages {
		return nil, fmt.Errorf("messages must hold 1 to %d entries", maxMessages)
	}
	t := &Table{estop: uint32(doc.EstopID), base: uint32(doc.SlotBase), window: uint32(doc.SlotWindow), slots: uint32(doc.Slots), byOffset: map[uint32]Message{}, Now: func() int64 { return time.Now().UnixMilli() }}
	names := map[string]bool{}
	for i, m := range doc.Messages {
		if !messageName.MatchString(m.Name) {
			return nil, fmt.Errorf("messages[%d].name %q is not capital letters, digits and underscores", i, m.Name)
		}
		if names[m.Name] {
			return nil, fmt.Errorf("messages[%d].name %q appears twice", i, m.Name)
		}
		names[m.Name] = true
		if m.Offset < 0 || m.Offset >= doc.SlotWindow {
			return nil, fmt.Errorf("messages[%d].offset %d is outside the window of %d identifiers", i, m.Offset, doc.SlotWindow)
		}
		if _, dup := t.byOffset[uint32(m.Offset)]; dup {
			return nil, fmt.Errorf("messages[%d].offset %d is used by another message", i, m.Offset)
		}
		if !emitters[m.Emitter] {
			return nil, fmt.Errorf("messages[%d].emitter %q is not one of the nodes of the contract", i, m.Emitter)
		}
		t.byOffset[uint32(m.Offset)] = m
	}
	return t, nil
}

// Classify says which message and slot an identifier is. ok is false for an identifier outside the contract.
func (t *Table) Classify(id uint32) (m Message, slot int, ok bool) {
	if id < t.base || id >= t.base+t.slots*t.window {
		return Message{}, 0, false
	}
	slotIndex := (id - t.base) / t.window
	m, ok = t.byOffset[(id-t.base)%t.window]
	return m, int(slotIndex), ok
}

// IsStop reports whether the identifier is the emergency stop frame.
func (t *Table) IsStop(id uint32) bool { return id == t.estop }

// Decode turns one frame into a sample, or says why it is not one: ErrIgnored for known traffic, ErrUnknownID for an identifier the
// contract does not list, ErrBadPayload for a health frame whose bytes are wrong.
func (t *Table) Decode(id uint32, data []byte) (telemetry.Sample, error) {
	if t.IsStop(id) {
		t.stop.Add(1)
		return telemetry.Sample{}, ErrIgnored
	}
	m, slot, ok := t.Classify(id)
	if !ok {
		t.unknown.Add(1)
		return telemetry.Sample{}, fmt.Errorf("%w: 0x%03X", ErrUnknownID, id)
	}
	if m.Name != HealthFrame {
		t.ignored.Add(1)
		return telemetry.Sample{}, ErrIgnored
	}
	sample, err := healthSample(slot, data, t.Now())
	if err != nil {
		t.bad.Add(1)
		return telemetry.Sample{}, err
	}
	t.health.Add(1)
	return sample, nil
}

// healthSample decodes the health frame of a controller board: marker 0x48, sequence, flags, tx errors, rx errors, stalls, uptime in
// units of 16 s (saturating at 255) and a check byte (0xA5 xor the seven bytes before it).
func healthSample(slot int, data []byte, now int64) (telemetry.Sample, error) {
	if len(data) != healthPayload {
		return telemetry.Sample{}, fmt.Errorf("%w: a health frame has %d bytes, got %d", ErrBadPayload, healthPayload, len(data))
	}
	if data[0] != healthMarker {
		return telemetry.Sample{}, fmt.Errorf("%w: marker 0x%02X", ErrBadPayload, data[0])
	}
	check := byte(0xA5)
	for _, b := range data[:7] {
		check ^= b
	}
	if data[7] != check {
		return telemetry.Sample{}, fmt.Errorf("%w: check byte", ErrBadPayload)
	}
	flags := data[2]
	bit := func(mask byte) float64 {
		if flags&mask != 0 {
			return 1
		}
		return 0
	}
	s := telemetry.Sample{
		SourceID:  fmt.Sprintf("board-%d", slot),
		Kind:      healthKind,
		Timestamp: now,
		Fields: map[string]float64{
			"sequence":                float64(data[1]),
			"tx_errors":               float64(data[3]),
			"rx_errors":               float64(data[4]),
			"stalls":                  float64(data[5]),
			"uptime_s":                float64(data[6]) * 16,
			"estop_latched":           bit(0x01),
			"bus_off_seen":            bit(0x02),
			"watchdog_reset_at_start": bit(0x04),
			"setup_failed_at_start":   bit(0x08),
			"application_confirmed":   bit(0x10),
			"drivers_enabled":         bit(0x20),
			"setup_failed_safe_state": bit(0x40),
		},
	}
	if err := s.Validate(); err != nil {
		return telemetry.Sample{}, err
	}
	return s, nil
}

// Counters are the totals since the start, for GET /stats.
func (t *Table) Counters() map[string]uint64 {
	return map[string]uint64{
		"healthFrames": t.health.Load(),
		"knownIgnored": t.ignored.Load(),
		"stopFrames":   t.stop.Load(),
		"unknownIds":   t.unknown.Load(),
		"badPayloads":  t.bad.Load(),
	}
}
