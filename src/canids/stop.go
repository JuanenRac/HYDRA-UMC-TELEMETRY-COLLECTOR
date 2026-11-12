// HYDRA-UMC-TELEMETRY-COLLECTOR - canids/stop.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The optional sections of the CanIds contract: the 8-byte frame of the emergency stop (identifier estop_id) and the identifiers of the tool head
// bus of URTC. With the first, a frame on the stop identifier becomes a sample of the source "stack" (kind "estop_frame"): the kind, the detail (the
// reason of a TRIP, or the state an ALIVE reports), the source, the epoch and the sequence of the frame. The reader never drops a frame: one that is
// not well formed (wrong length, a complement that does not match, an unknown kind or source) is reported with malformed = 1, because the node that
// receives it treats it as a stop. Reading the frame decides nothing about the stop; it is telemetry.
package canids

import (
	"errors"
	"fmt"
	"strings"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

type estopSection struct {
	DLC         int            `json:"dlc"`
	Bytes       []string       `json:"bytes"`
	Kinds       map[string]int `json:"kinds"`
	TripReasons map[string]int `json:"trip_reasons"`
	Sources     map[string]int `json:"sources"`
}

type toolHeadMessage struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Direction string `json:"direction"`
}

type toolHeadSection struct {
	Source          string            `json:"source"`
	ProtocolVersion int               `json:"protocol_version"`
	BitRateBps      int               `json:"bit_rate_bps"`
	Bus             string            `json:"bus"`
	Messages        []toolHeadMessage `json:"messages"`
}

var stopBytes = []string{"kind", "detail", "source", "epoch", "sequence_low", "sequence_high", "kind_complement", "detail_complement"}

// readSections reads the optional sections and checks what the schema cannot say.
func (t *Table) readSections(doc document) error {
	if frame := doc.EstopFrame; frame != nil {
		if frame.DLC != 8 || strings.Join(frame.Bytes, ",") != strings.Join(stopBytes, ",") {
			return errors.New("estop_frame is not the 8-byte frame this reader knows")
		}
		t.estopKinds = map[int]string{}
		for name, value := range frame.Kinds {
			if value < 1 || value > 255 || t.estopKinds[value] != "" {
				return fmt.Errorf("estop_frame.kinds: %q has a wrong or repeated value", name)
			}
			t.estopKinds[value] = name
		}
		if len(t.estopKinds) != 3 || t.estopKinds[1] != "TRIP" || t.estopKinds[2] != "RELEASE" || t.estopKinds[3] != "ALIVE" {
			return errors.New("estop_frame.kinds must be TRIP=1, RELEASE=2 and ALIVE=3")
		}
		seen := map[int]bool{}
		for name, value := range frame.TripReasons {
			if value < 1 || value > 255 || seen[value] {
				return fmt.Errorf("estop_frame.trip_reasons: %q has a wrong or repeated value", name)
			}
			seen[value] = true
		}
		s := frame.Sources
		for name, value := range s {
			if value < 0 || value > 255 {
				return fmt.Errorf("estop_frame.sources: %q out of range", name)
			}
		}
		if len(s) != 4 || s["KINEMATIC_BRAIN"] != 0 || s["SLOT_BOARD_FIRST"] < 1 || s["SLOT_BOARD_FIRST"] > s["SLOT_BOARD_LAST"] || s["SLOT_BOARD_LAST"] >= s["HOST_BRIDGE"] {
			return errors.New("estop_frame.sources: the brain is 0, the slot boards a range above it, the bridge above the range")
		}
		t.estopSources = s
	}
	if head := doc.ToolHead; head != nil {
		if head.Bus != "tool_head_downlink" || len(head.Messages) == 0 || len(head.Messages) > 512 {
			return errors.New("tool_head is not the section of the tool head bus")
		}
		t.toolHead = map[uint32]string{}
		names := map[string]bool{}
		for i, m := range head.Messages {
			if m.ID < 0 || m.ID > maxIdentifier || !messageName.MatchString(m.Name) || t.toolHead[uint32(m.ID)] != "" || names[m.Name] {
				return fmt.Errorf("tool_head.messages[%d] is wrong or repeated", i)
			}
			names[m.Name] = true
			t.toolHead[uint32(m.ID)] = m.Name
		}
	}
	return nil
}

// ToolHeadName is the name the contract gives to an identifier of the tool head bus ("" when it has none or the section is missing).
func (t *Table) ToolHeadName(id uint32) string { return t.toolHead[id] }

func (t *Table) stopSourceOK(source int) bool {
	s := t.estopSources
	return source == s["KINEMATIC_BRAIN"] || (source >= s["SLOT_BOARD_FIRST"] && source <= s["SLOT_BOARD_LAST"]) || source == s["HOST_BRIDGE"]
}

// stopSample reads a frame on the stop identifier.
func (t *Table) stopSample(data []byte) (telemetry.Sample, error) {
	fields := map[string]float64{"malformed": 0}
	malformed := len(data) != 8
	names := []string{"kind", "detail", "source", "epoch"}
	for i, name := range names {
		if len(data) > i {
			fields[name] = float64(data[i])
		}
	}
	if len(data) >= 6 {
		fields["sequence"] = float64(uint16(data[4]) | uint16(data[5])<<8)
	}
	if len(data) == 8 {
		if data[6] != data[0]^0xFF || data[7] != data[1]^0xFF || !t.stopSourceOK(int(data[2])) {
			malformed = true
		}
		if kind := t.estopKinds[int(data[0])]; kind == "" {
			malformed = true
		} else {
			fields["is_"+strings.ToLower(kind)] = 1
		}
	}
	if malformed {
		fields["malformed"] = 1
		t.bad.Add(1)
	}
	s := telemetry.Sample{SourceID: "stack", Kind: "estop_frame", Timestamp: t.Now(), Fields: fields}
	if err := s.Validate(); err != nil {
		return telemetry.Sample{}, err
	}
	return s, nil
}
